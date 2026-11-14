import os
import sqlite3
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.db import append_version, get_db, init_db
from app.main import app


@pytest.fixture()
def api(tmp_path, monkeypatch):
    db_path = tmp_path / "mapping.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    init_db(str(db_path))
    with TestClient(app) as client:
        client.post("/api/login", json={"username": "admin", "password": "123456"})
        yield client


@pytest.fixture()
def db_file(tmp_path, monkeypatch):
    db_path = tmp_path / "mapping.db"
    monkeypatch.setenv("DATABASE_PATH", str(db_path))
    init_db(str(db_path))
    return str(db_path)


def _mapping_id(api, pipeline_id=1):
    return api.get(f"/api/pipelines/{pipeline_id}").json()["mapping_id"]


def _add_and_publish(api, mapping_id, columns="biz_id,metric,status,extra", code_column="biz_id"):
    res = api.post(
        f"/api/mappings/{mapping_id}/versions",
        json={"columns": columns, "code_column": code_column},
    )
    assert res.status_code == 200, res.text
    version = res.json()["version"]
    pub = api.post(f"/api/mappings/{mapping_id}/versions/{version['version_no']}/publish")
    assert pub.status_code == 200, pub.text
    return version


def _lock_policy(api, policy, note=""):
    res = api.post("/api/switch-policy", json={"policy": policy, "note": note})
    assert res.status_code == 200, res.text
    return res.json()["policy"]


# --------------------------------------------------------------------------- #
# binding
# --------------------------------------------------------------------------- #


def test_pipeline_binds_version_on_creation(api):
    mapping_id = _mapping_id(api)
    version = _add_and_publish(api, mapping_id)

    no_mapping = api.post(
        "/api/pipelines",
        json={"title": "新管道", "code": "new_pipe", "source_uri": "mock://new"},
    )
    assert no_mapping.status_code == 400
    assert "绑定一版映射" in no_mapping.json()["error"]

    created = api.post(
        "/api/pipelines",
        json={
            "title": "新管道",
            "code": "new_pipe",
            "source_uri": "mock://new",
            "mapping_version_id": version["id"],
        },
    )
    assert created.status_code == 200, created.text
    detail = api.get(f"/api/pipelines/{created.json()['pipeline']['id']}").json()
    assert detail["bound_version_no"] == version["version_no"]
    assert detail["bound_version_label"] == f"v{version['version_no']}"


def test_cannot_bind_to_older_version(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")

    ok = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert ok.status_code == 200
    assert ok.json()["to_version_no"] == newer["version_no"]

    older = api.post("/api/pipelines/1/switch", json={"to_version_no": 1})
    assert older.status_code == 409
    assert "不能绑到更旧版本" in older.json()["error"]


def test_unconfirmed_batch_may_switch_and_reconvert(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")

    assert api.post("/api/pipelines/1/extract").status_code == 200
    switched = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert switched.status_code == 200
    detail = api.get("/api/pipelines/1").json()
    assert detail["bound_version_no"] == newer["version_no"]
    assert detail["batch_status"] == "已换版" or detail["batch_status"] == "未确认"

    again = api.post("/api/pipelines/1/extract")
    assert again.status_code == 200
    detail = api.get("/api/pipelines/1").json()
    assert detail["batch"]["mapping_version_id"] == newer["id"]
    assert detail["batch_status"] == "未确认"


def test_confirmed_batch_cannot_switch(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")
    api.post("/api/pipelines/1/extract")
    api.post("/api/pipelines/1/load")
    assert api.post("/api/pipelines/1/confirm").status_code == 200

    res = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert res.status_code == 409
    assert "已确认" in res.json()["error"]
    detail = api.get("/api/pipelines/1").json()
    assert detail["switch_failures"]


# --------------------------------------------------------------------------- #
# day policy
# --------------------------------------------------------------------------- #


def test_policy_locks_for_the_day_and_is_written_to_note(api):
    policy = _lock_policy(api, "禁切", note="今天只许原地重跑")
    assert policy["policy"] == "禁切"
    assert "禁切" in policy["note"]
    assert "今天只许原地重跑" in policy["note"]

    again = api.post("/api/switch-policy", json={"policy": "先撤再落"})
    assert again.status_code == 409
    assert "已锁定" in again.json()["error"]

    bad = api.post("/api/switch-policy", json={"policy": "随便换"})
    assert bad.status_code == 409  # locked before validation of the second attempt


def test_block_policy_prevents_switch_while_loading(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "禁切")
    api.post("/api/pipelines/1/extract")
    api.post("/api/pipelines/1/load")

    blocked = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert blocked.status_code == 409
    assert "换版策略走歪了" in blocked.json()["error"]
    assert api.get("/api/pipelines/1").json()["bound_version_no"] == 1

    # After withdrawing the loaded rows, switching becomes possible.
    assert api.post("/api/pipelines/1/withdraw").status_code == 200
    allowed = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert allowed.status_code == 200


def test_withdraw_policy_withdraws_whole_batch_then_reloads(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")
    api.post("/api/pipelines/1/extract")
    api.post("/api/pipelines/1/load")
    before = api.get("/api/overview").json()["items"][0]
    assert before["withdraw_count"] == 0
    assert before["today_transforms"] == 1

    switched = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert switched.status_code == 200
    assert switched.json()["withdrawn_rows"] > 0

    after = api.get("/api/overview").json()["items"][0]
    assert after["withdraw_count"] == 1
    assert after["bound_version_label"] == f"v{newer['version_no']}"
    assert after["batch_rows_withdrawn"] == switched.json()["withdrawn_rows"]

    assert api.post("/api/pipelines/1/extract").status_code == 200
    reload = api.post("/api/pipelines/1/load")
    assert reload.status_code == 200
    assert reload.json()["status"] == "已落库"
    final = api.get("/api/overview").json()["items"][0]
    assert final["today_transforms"] == 2
    assert final["withdraw_count"] == 1


def test_withdraw_policy_can_require_explicit_withdraw(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")
    api.post("/api/pipelines/1/extract")
    api.post("/api/pipelines/1/load")

    blocked = api.post(
        "/api/pipelines/1/switch",
        json={"to_version_no": newer["version_no"], "withdraw_before_switch": False},
    )
    assert blocked.status_code == 409
    assert "整批撤回" in blocked.json()["error"]

    assert api.post("/api/pipelines/1/withdraw").status_code == 200
    allowed = api.post("/api/pipelines/1/switch", json={"to_version_no": newer["version_no"]})
    assert allowed.status_code == 200
    assert allowed.json()["withdrawn_rows"] == 0


# --------------------------------------------------------------------------- #
# drift
# --------------------------------------------------------------------------- #


def test_drift_policy_mismatch_mentions_switch_error(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")
    api.post("/api/pipelines/1/extract")

    res = api.post(
        "/api/pipelines/1/switch",
        json={"to_version_no": newer["version_no"], "policy": "禁切"},
    )
    assert res.status_code == 409
    assert "换版策略走歪了" in res.json()["error"]
    detail = api.get("/api/pipelines/1").json()
    assert detail["bound_version_no"] == 1
    assert detail["switch_failures"]
    assert "推进法漂移" in detail["switch_failures"][0]["reason"]


def test_drift_stale_from_version_is_blocked(api):
    mapping_id = _mapping_id(api)
    newer = _add_and_publish(api, mapping_id)
    _lock_policy(api, "先撤再落")

    res = api.post(
        "/api/pipelines/1/switch",
        json={"to_version_no": newer["version_no"], "from_version_no": 99},
    )
    assert res.status_code == 409
    assert "换版策略走歪了" in res.json()["error"]


# --------------------------------------------------------------------------- #
# concurrent saves / publishing
# --------------------------------------------------------------------------- #


def test_simultaneous_identical_saves_yield_one_version(api, db_file):
    mapping_id = _mapping_id(api)
    barrier = threading.Barrier(3)
    results = []

    def save():
        try:
            barrier.wait()
            with get_db(db_file) as conn:
                version, created = append_version(
                    conn,
                    mapping_id,
                    columns="biz_id,metric,status,extra,flag",
                    code_column="biz_id",
                    user="admin",
                    base_version=1,
                )
            results.append((version["version_no"], created))
        except Exception as exc:  # pragma: no cover - reported via results
            results.append(("error", str(exc)))

    threads = [threading.Thread(target=save) for _ in range(2)]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()

    assert all(isinstance(r[0], int) for r in results), results
    numbers = {r[0] for r in results}
    assert numbers == {2}
    mapping = api.get(f"/api/mappings/{mapping_id}").json()
    assert [v["version_no"] for v in mapping["versions"]] == [1, 2]


def test_simultaneous_conflicting_saves_yield_one_new_version(api, db_file):
    mapping_id = _mapping_id(api)
    barrier = threading.Barrier(3)
    results = []

    def save(columns):
        try:
            barrier.wait()
            with get_db(db_file) as conn:
                version, created = append_version(
                    conn,
                    mapping_id,
                    columns=columns,
                    code_column="biz_id",
                    user="admin",
                    base_version=1,
                )
            results.append(("ok", version["version_no"]))
        except Exception as exc:
            results.append(("error", str(exc)))

    threads = [
        threading.Thread(target=save, args=("biz_id,metric,status,left",)),
        threading.Thread(target=save, args=("biz_id,metric,status,right",)),
    ]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()

    ok = [r for r in results if r[0] == "ok"]
    errors = [r for r in results if r[0] == "error"]
    assert len(ok) == 1, results
    assert len(errors) == 1, results
    mapping = api.get(f"/api/mappings/{mapping_id}").json()
    assert [v["version_no"] for v in mapping["versions"]] == [1, 2]


def test_simultaneous_http_saves_only_make_one_version(api):
    mapping_id = _mapping_id(api)
    barrier = threading.Barrier(3)
    results = []

    def save():
        barrier.wait()
        res = api.post(
            f"/api/mappings/{mapping_id}/versions",
            json={"columns": "biz_id,metric,status,extra,flag", "code_column": "biz_id"},
        )
        results.append((res.status_code, res.json().get("version_no")))

    threads = [threading.Thread(target=save) for _ in range(2)]
    for t in threads:
        t.start()
    barrier.wait()
    for t in threads:
        t.join()

    assert [r[0] for r in results] == [200, 200], results
    assert {r[1] for r in results} == {2}, results
    assert len(api.get(f"/api/mappings/{mapping_id}").json()["versions"]) == 2


def test_mapping_without_code_column_cannot_be_published(api):
    mapping_id = _mapping_id(api)
    res = api.post(
        f"/api/mappings/{mapping_id}/versions",
        json={"columns": "metric,status", "code_column": ""},
    )
    assert res.status_code == 200
    version = res.json()["version"]
    pub = api.post(f"/api/mappings/{mapping_id}/versions/{version['version_no']}/publish")
    assert pub.status_code == 409
    assert "缺编码列" in pub.json()["error"]

    bad_code = api.post(
        f"/api/mappings/{mapping_id}/versions",
        json={"columns": "metric,status", "code_column": "biz_id"},
    )
    assert bad_code.status_code == 200
    pub2 = api.post(
        f"/api/mappings/{mapping_id}/versions/{bad_code.json()['version_no']}/publish"
    )
    assert pub2.status_code == 409

    # Publishing by version label (a plain number in the path) is not required
    # to work, but the missing-code guard must be identical either way.
    pulled = api.post(f"/api/mappings/{mapping_id}/publish", json={"version_no": version["version_no"]})
    assert pulled.status_code == 409


def test_versions_are_append_only(api, db_file):
    mapping_id = _mapping_id(api)
    _add_and_publish(api, mapping_id)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_file) as conn:
            conn.execute(
                "UPDATE mapping_versions SET columns_json = '[]' WHERE mapping_id = ?",
                (mapping_id,),
            )
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_file) as conn:
            conn.execute("DELETE FROM mapping_versions WHERE mapping_id = ?", (mapping_id,))
    assert len(api.get(f"/api/mappings/{mapping_id}").json()["versions"]) == 2


# --------------------------------------------------------------------------- #
# screens
# --------------------------------------------------------------------------- #


def test_overview_reports_pinned_version_and_counts(api):
    _lock_policy(api, "先撤再落")
    data = api.get("/api/overview").json()
    assert data["policy_name"]
    item = data["items"][0]
    for key in ("bound_version_label", "today_transforms", "withdraw_count", "policy_name"):
        assert key in item
    assert item["bound_version_label"].startswith("v")
    assert item["withdraw_count"] == 0
    assert "先撤再落" in item["policy_name"]


def test_spa_distinguishes_version_number_from_withdraw_count(api):
    html = api.get("/app/").text
    assert "绑定版本号" in html
    assert "今日转换数" in html
    assert "撤回次数" in html
    assert "换版设置" in html
    assert "映射册" in html
