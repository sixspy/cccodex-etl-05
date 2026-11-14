"""SQLite storage for the ETL lab.

Mapping versions are append-only: a change is always stored as a new
``mapping_versions`` row and existing content columns can never be edited or
deleted.  Database triggers enforce that invariant even when a caller bypasses
the service layer.

Version-switch decisions (day policy, drift detection, withdraw-then-reload)
are adjudicated here, never in the browser.  Failed switch attempts are written
to ``switch_logs`` by :func:`log_switch_failure` from a separate transaction so
the 409 response does not roll the audit trail back.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Iterable

from app.config import Config

SEED_PIPELINES = [
    ("订单明细日抽", "orders_daily", "待抽取", "mock://orders/day"),
    ("用户画像宽表", "user_profile", "待抽取", "mock://users/profile"),
    ("库存快照落地", "inventory_snap", "待抽取", "mock://stock/snap"),
    ("支付流水清洗", "pay_clean", "待抽取", "mock://pay/flow"),
]

POLICY_BLOCK = "禁切"
POLICY_WITHDRAW = "先撤再落"
POLICY_LABELS = {
    POLICY_BLOCK: "禁切（落库进行中禁止切换映射版本）",
    POLICY_WITHDRAW: "先撤再落（整批撤回已落行后按新版重落）",
}
POLICY_OPTIONS = (POLICY_BLOCK, POLICY_WITHDRAW)

BATCH_UNCONFIRMED = "未确认"
BATCH_CONFIRMED = "已确认"
BATCH_SWITCHED = "已换版"

STATUS_PENDING = "待抽取"
STATUS_EXTRACTED = "已抽取待加载"
STATUS_LOADED = "已落库"

SWITCH_ERROR_PREFIX = "换版策略走歪了"
OLD_VERSION_ERROR = "不能绑到更旧版本"
MISSING_CODE_ERROR = "缺编码列，禁止发布"


class DomainError(Exception):
    """A business rule violation that should surface as an HTTP 4xx."""

    def __init__(self, message: str, status_code: int = 409):
        super().__init__(message)
        self.message = message
        self.status_code = status_code


class SwitchRejected(DomainError):
    """A failed version switch, carrying the audit record to persist."""

    def __init__(
        self,
        message: str,
        pipeline_id: int,
        actor: str,
        from_version_id: int | None = None,
        from_version_no: int | None = None,
        to_version_id: int | None = None,
        to_version_no: int | None = None,
    ):
        super().__init__(message, 409)
        self.pipeline_id = pipeline_id
        self.actor = actor
        self.from_version_id = from_version_id
        self.from_version_no = from_version_no
        self.to_version_id = to_version_id
        self.to_version_no = to_version_no


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today() -> str:
    return date.today().isoformat()


def _hash_version(columns: Iterable[str], code_column: str) -> str:
    payload = json.dumps(
        {"columns": list(columns), "code_column": code_column},
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_columns(value) -> list[str]:
    """Accept a comma/newline separated string or a list of column names."""
    if value is None:
        return []
    if isinstance(value, str):
        parts = value.replace("\n", ",").replace("，", ",").split(",")
    elif isinstance(value, (list, tuple)):
        parts = [str(v) for v in value]
    else:
        raise DomainError("列定义格式不正确，应为逗号分隔或字符串数组", 400)
    out: list[str] = []
    for part in parts:
        name = str(part).strip()
        if name and name not in out:
            out.append(name)
    return out


def connect(db_path: str | None = None) -> sqlite3.Connection:
    path = db_path or Config.database()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


@contextmanager
def get_db(db_path: str | None = None):
    conn = connect(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


SCHEMA = """
CREATE TABLE IF NOT EXISTS pipelines (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  title TEXT NOT NULL,
  code TEXT NOT NULL UNIQUE,
  status TEXT NOT NULL,
  source_uri TEXT NOT NULL,
  extracted INTEGER NOT NULL DEFAULT 0,
  loaded INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mappings (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  code TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS mapping_versions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  mapping_id INTEGER NOT NULL REFERENCES mappings(id),
  version_no INTEGER NOT NULL,
  columns_json TEXT NOT NULL,
  code_column TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT '草稿',
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  published_at TEXT,
  UNIQUE(mapping_id, version_no)
);
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
  mapping_version_id INTEGER NOT NULL REFERENCES mapping_versions(id),
  status TEXT NOT NULL DEFAULT '未确认',
  rows_extracted INTEGER NOT NULL DEFAULT 0,
  rows_loaded INTEGER NOT NULL DEFAULT 0,
  rows_withdrawn INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS batch_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
  batch_id INTEGER,
  mapping_version_id INTEGER,
  kind TEXT NOT NULL,
  rows INTEGER NOT NULL DEFAULT 0,
  note TEXT NOT NULL DEFAULT '',
  actor TEXT NOT NULL DEFAULT '',
  day TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS switch_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  pipeline_id INTEGER NOT NULL REFERENCES pipelines(id),
  from_version_id INTEGER,
  from_version_no INTEGER,
  to_version_id INTEGER,
  to_version_no INTEGER,
  ok INTEGER NOT NULL,
  reason TEXT NOT NULL,
  actor TEXT NOT NULL DEFAULT '',
  day TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS day_policies (
  day TEXT PRIMARY KEY,
  policy TEXT NOT NULL,
  note TEXT NOT NULL,
  set_by TEXT NOT NULL DEFAULT '',
  set_at TEXT NOT NULL
);
CREATE TRIGGER IF NOT EXISTS mapping_versions_append_only_update
BEFORE UPDATE OF mapping_id, version_no, columns_json, code_column, content_hash,
                 created_by, created_at ON mapping_versions
BEGIN
  SELECT RAISE(ABORT, '映射版本只增不改');
END;
CREATE TRIGGER IF NOT EXISTS mapping_versions_append_only_delete
BEFORE DELETE ON mapping_versions
BEGIN
  SELECT RAISE(ABORT, '映射版本只增不改');
END;
"""


def _migrate_pipelines(conn: sqlite3.Connection) -> None:
    existing = {r["name"] for r in conn.execute("PRAGMA table_info(pipelines)")}
    if "mapping_version_id" not in existing:
        conn.execute("ALTER TABLE pipelines ADD COLUMN mapping_version_id INTEGER")
    if "bound_at" not in existing:
        conn.execute("ALTER TABLE pipelines ADD COLUMN bound_at TEXT")


def init_db(db_path: str | None = None) -> None:
    with get_db(db_path) as conn:
        conn.executescript(SCHEMA)
        _migrate_pipelines(conn)

        n = conn.execute("SELECT COUNT(*) AS c FROM pipelines").fetchone()["c"]
        if n == 0:
            conn.executemany(
                "INSERT INTO pipelines (title, code, status, source_uri) VALUES (?,?,?,?)",
                SEED_PIPELINES,
            )
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES ('lab_name', ?)",
                (Config.LAB_NAME,),
            )

        _seed_mapping_bindings(conn)


def _seed_mapping_bindings(conn: sqlite3.Connection) -> None:
    """Give every seeded pipeline a published mapping version to be bound to."""
    rows = conn.execute(
        "SELECT id, title, code, mapping_version_id FROM pipelines ORDER BY id"
    ).fetchall()
    now = _now()
    for row in rows:
        mapping_code = f"{row['code']}_map"
        mapping = conn.execute(
            "SELECT id FROM mappings WHERE code = ?", (mapping_code,)
        ).fetchone()
        if not mapping:
            cur = conn.execute(
                "INSERT INTO mappings (name, code, created_at) VALUES (?,?,?)",
                (f"{row['title']} · 转换映射", mapping_code, now),
            )
            mapping_id = cur.lastrowid
        else:
            mapping_id = mapping["id"]

        latest = conn.execute(
            "SELECT id FROM mapping_versions WHERE mapping_id = ? "
            "ORDER BY version_no DESC LIMIT 1",
            (mapping_id,),
        ).fetchone()
        if not latest:
            columns = ["biz_id", "metric", "status"]
            content_hash = _hash_version(columns, "biz_id")
            cur = conn.execute(
                "INSERT INTO mapping_versions "
                "(mapping_id, version_no, columns_json, code_column, content_hash, "
                " status, created_by, created_at, published_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    mapping_id,
                    1,
                    json.dumps(columns, ensure_ascii=False),
                    "biz_id",
                    content_hash,
                    "已发布",
                    "system",
                    now,
                    now,
                ),
            )
            latest_id = cur.lastrowid
        else:
            latest_id = latest["id"]

        if row["mapping_version_id"] is None:
            conn.execute(
                "UPDATE pipelines SET mapping_version_id = ?, bound_at = ? WHERE id = ?",
                (latest_id, now, row["id"]),
            )


# --------------------------------------------------------------------------- #
# mapping register
# --------------------------------------------------------------------------- #


def _version_dict(row: sqlite3.Row | dict) -> dict:
    data = dict(row)
    try:
        data["columns"] = json.loads(data.pop("columns_json"))
    except (TypeError, json.JSONDecodeError):
        data["columns"] = []
    data["version_label"] = f"v{data['version_no']}"
    data["label"] = data["version_label"]
    return data


def _versions_of(conn: sqlite3.Connection, mapping_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM mapping_versions WHERE mapping_id = ? ORDER BY version_no ASC",
        (mapping_id,),
    )
    return [_version_dict(r) for r in rows]


def list_mappings(conn: sqlite3.Connection) -> list[dict]:
    items = []
    for row in conn.execute(
        "SELECT id, name, code, created_at FROM mappings ORDER BY id"
    ):
        mapping = dict(row)
        versions = _versions_of(conn, mapping["id"])
        mapping["versions"] = versions
        mapping["version_count"] = len(versions)
        mapping["latest_version"] = versions[-1] if versions else None
        items.append(mapping)
    return items


def get_mapping(conn: sqlite3.Connection, mapping_id: int) -> dict | None:
    row = conn.execute(
        "SELECT id, name, code, created_at FROM mappings WHERE id = ?", (mapping_id,)
    ).fetchone()
    if not row:
        return None
    mapping = dict(row)
    versions = _versions_of(conn, mapping_id)
    mapping["versions"] = versions
    mapping["version_count"] = len(versions)
    mapping["latest_version"] = versions[-1] if versions else None
    return mapping


def create_mapping(
    conn: sqlite3.Connection,
    name: str,
    code: str,
    columns,
    code_column: str,
    user: str,
) -> dict:
    name = (name or "").strip()
    code = (code or "").strip()
    if not name or not code:
        raise DomainError("映射名称与编码不能为空", 400)
    cols = normalize_columns(columns)
    code_column = (code_column or "").strip()
    now = _now()
    try:
        cur = conn.execute(
            "INSERT INTO mappings (name, code, created_at) VALUES (?,?,?)",
            (name, code, now),
        )
        mapping_id = cur.lastrowid
        conn.execute(
            "INSERT INTO mapping_versions "
            "(mapping_id, version_no, columns_json, code_column, content_hash, "
            " status, created_by, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                mapping_id,
                1,
                json.dumps(cols, ensure_ascii=False),
                code_column,
                _hash_version(cols, code_column),
                "草稿",
                user,
                now,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise DomainError("映射编码已存在，请换一个编码", 409) from exc
    return get_mapping(conn, mapping_id)


def get_version(conn: sqlite3.Connection, version_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM mapping_versions WHERE id = ?", (version_id,)
    ).fetchone()
    return _version_dict(row) if row else None


def find_version(
    conn: sqlite3.Connection, mapping_id: int, version_no: int
) -> dict | None:
    row = conn.execute(
        "SELECT * FROM mapping_versions WHERE mapping_id = ? AND version_no = ?",
        (mapping_id, version_no),
    ).fetchone()
    return _version_dict(row) if row else None


def _mapping_of_version(conn: sqlite3.Connection, version_id: int) -> dict | None:
    row = conn.execute(
        "SELECT m.id, m.name, m.code FROM mapping_versions v "
        "JOIN mappings m ON m.id = v.mapping_id WHERE v.id = ?",
        (version_id,),
    ).fetchone()
    return dict(row) if row else None


def _head_version_no(conn: sqlite3.Connection, mapping_id: int) -> int:
    row = conn.execute(
        "SELECT MAX(version_no) AS no FROM mapping_versions WHERE mapping_id = ?",
        (mapping_id,),
    ).fetchone()
    return (row["no"] if row else None) or 0


def append_version(
    conn: sqlite3.Connection,
    mapping_id: int,
    columns,
    code_column: str,
    user: str,
    base_version: int | None = None,
) -> tuple[dict, bool]:
    """Append a draft version and return ``(version, created)``.

    Saves are serialized with ``BEGIN IMMEDIATE`` so two simultaneous writers
    cannot allocate the same next version number.  Identical content is
    deduplicated (the existing row is returned with ``created=False``); a writer
    carrying a stale ``base_version`` whose content differs is rejected so only
    the first writer receives a new version number.
    """
    if not get_mapping(conn, mapping_id):
        raise DomainError("映射不存在", 404)
    cols = normalize_columns(columns)
    code_column = (code_column or "").strip()
    content_hash = _hash_version(cols, code_column)

    # Remember the head this writer observed before taking the write lock.
    # Two simultaneous savers both observe the same head; the first one appends
    # a version and the second's observed head is stale, so a race yields one
    # version number rather than two.
    observed_no = _head_version_no(conn, mapping_id)

    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
        owns_tx = True
    else:
        # An outer transaction already holds the write lock; stay in it so the
        # whole request commits atomically.
        owns_tx = False
    try:
        head = conn.execute(
            "SELECT * FROM mapping_versions WHERE mapping_id = ? "
            "ORDER BY version_no DESC LIMIT 1",
            (mapping_id,),
        ).fetchone()
        identical = conn.execute(
            "SELECT * FROM mapping_versions WHERE mapping_id = ? AND content_hash = ? "
            "ORDER BY version_no DESC LIMIT 1",
            (mapping_id, content_hash),
        ).fetchone()
        if identical:
            if owns_tx:
                conn.execute("COMMIT")
            return _version_dict(identical), False

        current_no = head["version_no"] if head else 0
        if base_version is not None:
            try:
                base_no = int(base_version)
            except (TypeError, ValueError):
                raise DomainError("基线版本号不正确", 400)
            if base_no != current_no:
                raise DomainError(
                    "映射已被他人更新（当前最新 v%d），请基于最新版本保存" % current_no,
                    409,
                )
        elif current_no != observed_no:
            raise DomainError(
                "映射已被他人更新（当前最新 v%d），本次保存基于 v%d，请重新保存"
                % (current_no, observed_no),
                409,
            )

        next_no = current_no + 1
        now = _now()
        cur = conn.execute(
            "INSERT INTO mapping_versions "
            "(mapping_id, version_no, columns_json, code_column, content_hash, "
            " status, created_by, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                mapping_id,
                next_no,
                json.dumps(cols, ensure_ascii=False),
                code_column,
                content_hash,
                "草稿",
                user,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM mapping_versions WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        if owns_tx:
            conn.execute("COMMIT")
    except Exception:
        if owns_tx:
            conn.execute("ROLLBACK")
        raise
    return _version_dict(row), True


def publish_version(
    conn: sqlite3.Connection, mapping_id: int, version_no: int
) -> dict:
    version = find_version(conn, mapping_id, version_no)
    if not version:
        raise DomainError("映射版本不存在", 404)
    if version["status"] == "已发布":
        return version
    code_column = (version.get("code_column") or "").strip()
    if not code_column or code_column not in version["columns"]:
        raise DomainError(MISSING_CODE_ERROR, 409)
    conn.execute(
        "UPDATE mapping_versions SET status = '已发布', published_at = ? WHERE id = ?",
        (_now(), version["id"]),
    )
    return get_version(conn, version["id"])


# --------------------------------------------------------------------------- #
# day policy
# --------------------------------------------------------------------------- #


def get_today_policy(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute(
        "SELECT day, policy, note, set_by, set_at FROM day_policies WHERE day = ?",
        (_today(),),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["policy_name"] = POLICY_LABELS.get(data["policy"], data["policy"])
    data["locked"] = True
    return data


def set_today_policy(conn: sqlite3.Connection, policy: str, note: str, user: str) -> dict:
    if get_today_policy(conn):
        raise DomainError("当日换版策略已锁定，不可更换", 409)
    if policy not in POLICY_OPTIONS:
        raise DomainError("换版策略只能是「禁切」或「先撤再落」", 400)
    label = POLICY_LABELS[policy]
    note_text = f"当日说明：换版策略锁定为「{label}」"
    extra = (note or "").strip()
    if extra:
        note_text = f"{note_text}；{extra}"
    conn.execute(
        "INSERT INTO day_policies (day, policy, note, set_by, set_at) VALUES (?,?,?,?,?)",
        (_today(), policy, note_text, user, _now()),
    )
    return get_today_policy(conn)


# --------------------------------------------------------------------------- #
# pipelines / batches
# --------------------------------------------------------------------------- #

_PIPELINE_COLS = (
    "id, title, code, status, source_uri, extracted, loaded, created_at, "
    "mapping_version_id, bound_at"
)


def list_pipelines(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute(f"SELECT {_PIPELINE_COLS} FROM pipelines ORDER BY id").fetchall()
    return [dict(r) for r in rows]


def get_pipeline(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        f"SELECT {_PIPELINE_COLS} FROM pipelines WHERE id = ?", (pipeline_id,)
    ).fetchone()
    return dict(row) if row else None


def create_pipeline(
    conn: sqlite3.Connection,
    title: str,
    code: str,
    source_uri: str,
    user: str,
    mapping_version_id: int | None = None,
    mapping_id: int | None = None,
    version_no: int | None = None,
) -> dict:
    title = (title or "").strip()
    code = (code or "").strip()
    source_uri = (source_uri or "").strip()
    if not title or not code or not source_uri:
        raise DomainError("题名、编码与源地址不能为空", 400)

    if mapping_version_id is None and mapping_id is not None and version_no is not None:
        found = find_version(conn, int(mapping_id), int(version_no))
        if found:
            mapping_version_id = found["id"]
    if mapping_version_id is None:
        raise DomainError("创建管道时必须绑定一版映射", 400)
    version = get_version(conn, int(mapping_version_id))
    if not version:
        raise DomainError("绑定的映射版本不存在", 404)

    now = _now()
    try:
        cur = conn.execute(
            "INSERT INTO pipelines "
            "(title, code, status, source_uri, extracted, loaded, created_at, "
            " mapping_version_id, bound_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (title, code, "待抽取", source_uri, 0, 0, now, version["id"], now),
        )
    except sqlite3.IntegrityError as exc:
        raise DomainError("管道编码已存在，请换一个编码", 409) from exc
    return get_pipeline(conn, cur.lastrowid)


def latest_batch(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    row = conn.execute(
        "SELECT b.*, v.version_no, m.id AS mapping_id, m.name AS mapping_name "
        "FROM batches b JOIN mapping_versions v ON v.id = b.mapping_version_id "
        "JOIN mappings m ON m.id = v.mapping_id "
        "WHERE b.pipeline_id = ? ORDER BY b.id DESC LIMIT 1",
        (pipeline_id,),
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    data["version_label"] = f"v{data['version_no']}"
    return data


def _record_event(
    conn: sqlite3.Connection,
    pipeline_id: int,
    batch_id: int | None,
    mapping_version_id: int | None,
    kind: str,
    rows: int,
    actor: str,
    note: str = "",
) -> None:
    conn.execute(
        "INSERT INTO batch_events "
        "(pipeline_id, batch_id, mapping_version_id, kind, rows, note, actor, day, created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (pipeline_id, batch_id, mapping_version_id, kind, rows, note, actor, _today(), _now()),
    )


def run_extract(
    conn: sqlite3.Connection, pipeline_id: int, user: str = "system"
) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["mapping_version_id"] is None:
        raise DomainError("管道尚未绑定映射版本", 409)
    if item["status"] == BATCH_CONFIRMED:
        raise DomainError("批次已确认，不可重转换", 409)

    batch = latest_batch(conn, pipeline_id)
    reuse = bool(
        batch
        and batch["status"] == BATCH_UNCONFIRMED
        and batch["mapping_version_id"] == item["mapping_version_id"]
    )
    if reuse and batch["rows_loaded"] > batch["rows_withdrawn"]:
        raise DomainError("存在未撤回的已落行，请先整批撤回再重转换", 409)

    extracted = 12 + (pipeline_id % 5)
    now = _now()
    if reuse:
        batch_id = batch["id"]
        conn.execute(
            "UPDATE batches SET rows_extracted = ?, rows_loaded = 0, rows_withdrawn = 0, "
            "updated_at = ? WHERE id = ?",
            (extracted, now, batch_id),
        )
    else:
        cur = conn.execute(
            "INSERT INTO batches "
            "(pipeline_id, mapping_version_id, status, rows_extracted, rows_loaded, "
            " rows_withdrawn, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (
                pipeline_id,
                item["mapping_version_id"],
                BATCH_UNCONFIRMED,
                extracted,
                0,
                0,
                now,
                now,
            ),
        )
        batch_id = cur.lastrowid
    conn.execute(
        "UPDATE pipelines SET status = ?, extracted = ?, loaded = 0 WHERE id = ?",
        (STATUS_EXTRACTED, extracted, pipeline_id),
    )
    _record_event(
        conn,
        pipeline_id,
        batch_id,
        item["mapping_version_id"],
        "transform",
        extracted,
        user,
        "按绑定映射版本转换",
    )
    return get_pipeline(conn, pipeline_id)


def run_load(
    conn: sqlite3.Connection, pipeline_id: int, user: str = "system"
) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    if item["status"] != STATUS_EXTRACTED:
        return item
    batch = latest_batch(conn, pipeline_id)
    if not batch or batch["status"] != BATCH_UNCONFIRMED:
        return item
    conn.execute(
        "UPDATE batches SET rows_loaded = rows_extracted, updated_at = ? WHERE id = ?",
        (_now(), batch["id"]),
    )
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = extracted WHERE id = ?",
        (STATUS_LOADED, pipeline_id),
    )
    _record_event(
        conn,
        pipeline_id,
        batch["id"],
        item["mapping_version_id"],
        "load",
        batch["rows_extracted"],
        user,
        "落库完成",
    )
    return get_pipeline(conn, pipeline_id)


def withdraw_batch(
    conn: sqlite3.Connection, pipeline_id: int, user: str = "system"
) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    batch = latest_batch(conn, pipeline_id)
    if not batch:
        raise DomainError("当前管道没有批次可撤回", 409)
    if batch["status"] == BATCH_CONFIRMED:
        raise DomainError("已确认批次不可撤回", 409)
    if batch["mapping_version_id"] != item["mapping_version_id"]:
        raise DomainError("批次属于旧映射版本，无需撤回；请直接重转换", 409)
    remaining = batch["rows_loaded"] - batch["rows_withdrawn"]
    if remaining <= 0:
        raise DomainError("没有已落行可撤回", 409)
    conn.execute(
        "UPDATE batches SET rows_withdrawn = rows_loaded, updated_at = ? WHERE id = ?",
        (_now(), batch["id"]),
    )
    status = STATUS_EXTRACTED if item["extracted"] > 0 else STATUS_PENDING
    conn.execute(
        "UPDATE pipelines SET status = ?, loaded = 0 WHERE id = ?", (status, pipeline_id)
    )
    _record_event(
        conn,
        pipeline_id,
        batch["id"],
        batch["mapping_version_id"],
        "withdraw",
        remaining,
        user,
        "整批撤回已落行",
    )
    result = get_pipeline(conn, pipeline_id)
    result["withdrawn_rows"] = remaining
    return result


def confirm_batch(
    conn: sqlite3.Connection, pipeline_id: int, user: str = "system"
) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    batch = latest_batch(conn, pipeline_id)
    if not batch:
        raise DomainError("当前管道没有可确认的批次", 409)
    if batch["status"] == BATCH_CONFIRMED:
        return item
    if batch["status"] != BATCH_UNCONFIRMED:
        raise DomainError("当前批次已换版，不能确认", 409)
    if batch["rows_loaded"] <= batch["rows_withdrawn"]:
        raise DomainError("批次尚未落库，不能确认；请先落库", 409)
    conn.execute(
        "UPDATE batches SET status = ?, updated_at = ? WHERE id = ?",
        (BATCH_CONFIRMED, _now(), batch["id"]),
    )
    conn.execute(
        "UPDATE pipelines SET status = ? WHERE id = ?", (BATCH_CONFIRMED, pipeline_id)
    )
    _record_event(
        conn,
        pipeline_id,
        batch["id"],
        batch["mapping_version_id"],
        "confirm",
        batch["rows_loaded"],
        user,
        "批次确认，版本钉死",
    )
    return get_pipeline(conn, pipeline_id)


# --------------------------------------------------------------------------- #
# version switching (server-side adjudication)
# --------------------------------------------------------------------------- #


def _version_context(conn: sqlite3.Connection, item: dict):
    if item["mapping_version_id"] is None:
        return None, None
    current = get_version(conn, item["mapping_version_id"])
    if not current:
        return None, None
    mapping = _mapping_of_version(conn, current["id"])
    if mapping:
        current["mapping_name"] = mapping["name"]
        current["mapping_code"] = mapping["code"]
    return current, mapping


def _log_switch(
    conn: sqlite3.Connection,
    pipeline_id: int,
    current: dict | None,
    target: dict | None,
    ok: bool,
    reason: str,
    actor: str,
) -> None:
    conn.execute(
        "INSERT INTO switch_logs "
        "(pipeline_id, from_version_id, from_version_no, to_version_id, to_version_no, "
        " ok, reason, actor, day, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (
            pipeline_id,
            current["id"] if current else None,
            current["version_no"] if current else None,
            target["id"] if target else None,
            target["version_no"] if target else None,
            1 if ok else 0,
            reason,
            actor,
            _today(),
            _now(),
        ),
    )


def log_switch_failure(
    db_path: str | None,
    pipeline_id: int,
    reason: str,
    actor: str,
    from_version_id: int | None = None,
    from_version_no: int | None = None,
    to_version_id: int | None = None,
    to_version_no: int | None = None,
) -> None:
    """Persist a rejected switch from its own transaction.

    The request transaction rolls back when a switch is rejected; the audit
    row must survive that rollback so the detail screen can explain the failure.
    """
    with get_db(db_path) as conn:
        conn.execute(
            "INSERT INTO switch_logs "
            "(pipeline_id, from_version_id, from_version_no, to_version_id, to_version_no, "
            " ok, reason, actor, day, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                pipeline_id,
                from_version_id,
                from_version_no,
                to_version_id,
                to_version_no,
                0,
                reason,
                actor,
                _today(),
                _now(),
            ),
        )


def _detect_drift(
    item: dict,
    batch: dict | None,
    from_version_id: int | None,
    requested_policy: str | None,
    today_policy: dict,
) -> str | None:
    """Detect that the requested switch does not follow the locked progression."""
    if from_version_id is not None and int(from_version_id) != item["mapping_version_id"]:
        return "推进法漂移：请求的起始版本已过期，与当前绑定版本不一致"
    if requested_policy and requested_policy != today_policy["policy"]:
        return (
            "推进法漂移：请求推进法「%s」与当日锁定策略「%s」不一致"
            % (requested_policy, today_policy["policy"])
        )
    if batch and batch["status"] == BATCH_UNCONFIRMED:
        if batch["mapping_version_id"] != item["mapping_version_id"]:
            return "推进法漂移：批次映射版本与当前绑定版本不一致"
    if item["status"] == STATUS_EXTRACTED and not (
        batch and batch["rows_extracted"] > 0
    ):
        return "推进法漂移：管道声称已抽取但找不到对应批次"
    if item["status"] in (STATUS_LOADED, BATCH_CONFIRMED) and not (
        batch and batch["rows_loaded"] > 0
    ):
        return "推进法漂移：管道声称已落库但批次没有已落行"
    return None


def switch_version(
    conn: sqlite3.Connection,
    pipeline_id: int,
    user: str,
    to_version_id: int | None = None,
    to_version_no: int | None = None,
    from_version_id: int | None = None,
    requested_policy: str | None = None,
    withdraw_before_switch: bool = True,
) -> dict | None:
    """Adjudicate a version switch.  Raises :class:`SwitchRejected` on refusal."""
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    current, mapping = _version_context(conn, item)

    def reject(message: str, target: dict | None = None):
        raise SwitchRejected(
            message,
            pipeline_id=pipeline_id,
            actor=user,
            from_version_id=current["id"] if current else None,
            from_version_no=current["version_no"] if current else None,
            to_version_id=target["id"] if target else None,
            to_version_no=target["version_no"] if target else None,
        )

    if not current or not mapping:
        reject("管道尚未绑定映射版本")

    target = get_version(conn, int(to_version_id)) if to_version_id is not None else None
    if target is None and to_version_no is not None:
        target = find_version(conn, mapping["id"], int(to_version_no))
    if not target:
        raise DomainError("目标映射版本不存在", 404)

    if target["mapping_id"] != current["mapping_id"]:
        reject("只能切换到同一映射的版本", target)
    if target["version_no"] == current["version_no"]:
        reject("目标版本不新于当前绑定版本，请选择更新版本", target)
    if target["version_no"] < current["version_no"]:
        reject(OLD_VERSION_ERROR, target)

    today_policy = get_today_policy(conn)
    if not today_policy:
        reject("今日换版策略未设定，无法发起换版", target)

    batch = latest_batch(conn, pipeline_id)
    drift = _detect_drift(item, batch, from_version_id, requested_policy, today_policy)
    if drift:
        reject(f"{SWITCH_ERROR_PREFIX}：{drift}", target)

    if batch and batch["status"] == BATCH_CONFIRMED:
        reject("批次已确认，不可换版", target)

    withdrawn_rows = 0
    if today_policy["policy"] == POLICY_BLOCK:
        # "Loading in progress" means rows have actually landed and have not
        # been withdrawn.  A merely extracted (not yet loaded) batch may still
        # move to a newer version and be re-transformed.
        if item["status"] == STATUS_LOADED or (
            batch
            and batch["status"] == BATCH_UNCONFIRMED
            and batch["rows_loaded"] > batch["rows_withdrawn"]
        ):
            reject(
                f"{SWITCH_ERROR_PREFIX}：当日策略「{POLICY_LABELS[POLICY_BLOCK]}」，"
                "落库进行中禁止切换映射版本",
                target,
            )
    else:
        if batch and batch["status"] == BATCH_UNCONFIRMED:
            remaining = batch["rows_loaded"] - batch["rows_withdrawn"]
            if remaining > 0:
                if not withdraw_before_switch:
                    reject("须先整批撤回已落行再换版", target)
                withdrawn_rows = remaining
                conn.execute(
                    "UPDATE batches SET rows_withdrawn = rows_loaded, updated_at = ? "
                    "WHERE id = ?",
                    (_now(), batch["id"]),
                )
                _record_event(
                    conn,
                    pipeline_id,
                    batch["id"],
                    batch["mapping_version_id"],
                    "withdraw",
                    remaining,
                    user,
                    "换版前整批撤回已落行",
                )

    if batch and batch["status"] == BATCH_UNCONFIRMED:
        conn.execute(
            "UPDATE batches SET status = ?, updated_at = ? WHERE id = ?",
            (BATCH_SWITCHED, _now(), batch["id"]),
        )

    conn.execute(
        "UPDATE pipelines SET mapping_version_id = ?, status = ?, extracted = 0, "
        "loaded = 0, bound_at = ? WHERE id = ?",
        (target["id"], STATUS_PENDING, _now(), pipeline_id),
    )
    _record_event(
        conn,
        pipeline_id,
        batch["id"] if batch else None,
        target["id"],
        "switch",
        withdrawn_rows,
        user,
        f"切换映射版本 v{current['version_no']} -> v{target['version_no']}",
    )

    if today_policy["policy"] == POLICY_WITHDRAW and withdrawn_rows > 0:
        reason = (
            f"按「{POLICY_WITHDRAW}」整批撤回 {withdrawn_rows} 行后切换，"
            "等待按新版本重转换并重落"
        )
    else:
        reason = (
            f"按策略「{today_policy['policy']}」切换 v{current['version_no']} -> "
            f"v{target['version_no']}，可重转换"
        )
    _log_switch(conn, pipeline_id, current, target, True, reason, user)

    result = get_pipeline(conn, pipeline_id)
    result["withdrawn_rows"] = withdrawn_rows
    result["switch"] = {
        "ok": True,
        "from_version_no": current["version_no"],
        "to_version_no": target["version_no"],
        "policy": today_policy["policy"],
        "withdrawn_rows": withdrawn_rows,
        "reason": reason,
    }
    return result


# --------------------------------------------------------------------------- #
# read models for the four connected screens
# --------------------------------------------------------------------------- #


def _pipeline_view(conn: sqlite3.Connection, item: dict, policy: dict | None) -> dict:
    data = dict(item)
    current, mapping = _version_context(conn, data)
    data["bound_version_id"] = data.get("mapping_version_id")
    data["bound_version_no"] = current["version_no"] if current else None
    data["bound_version_label"] = f"v{current['version_no']}" if current else "未绑定"
    data["mapping_id"] = mapping["id"] if mapping else None
    data["mapping_name"] = mapping["name"] if mapping else ""
    data["mapping_code"] = mapping["code"] if mapping else ""

    transformed = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_events "
        "WHERE pipeline_id = ? AND kind = 'transform' AND day = ?",
        (data["id"], _today()),
    ).fetchone()["c"]
    withdraws = conn.execute(
        "SELECT COUNT(*) AS c FROM batch_events "
        "WHERE pipeline_id = ? AND kind = 'withdraw'",
        (data["id"],),
    ).fetchone()["c"]
    data["transformed_today"] = transformed
    data["today_transforms"] = transformed
    data["today_transformed"] = transformed
    data["withdraw_count"] = withdraws
    data["withdraw_times"] = withdraws

    data["policy"] = policy["policy"] if policy else ""
    data["policy_name"] = policy["policy_name"] if policy else "未设定"
    data["policy_note"] = policy["note"] if policy else ""
    data["policy_locked"] = bool(policy)
    data["bound_version"] = data["bound_version_label"]

    batch = latest_batch(conn, data["id"])
    data["batch"] = batch
    data["batch_status"] = batch["status"] if batch else "无批次"
    data["batch_version_label"] = batch["version_label"] if batch else ""
    data["batch_rows_extracted"] = batch["rows_extracted"] if batch else 0
    data["batch_rows_loaded"] = batch["rows_loaded"] if batch else 0
    data["batch_rows_withdrawn"] = batch["rows_withdrawn"] if batch else 0
    data["confirmed"] = bool(batch and batch["status"] == BATCH_CONFIRMED)
    data["can_withdraw"] = bool(
        batch
        and batch["status"] == BATCH_UNCONFIRMED
        and batch["mapping_version_id"] == data["mapping_version_id"]
        and batch["rows_loaded"] > batch["rows_withdrawn"]
    )
    data["can_confirm"] = bool(
        batch
        and batch["status"] == BATCH_UNCONFIRMED
        and batch["rows_loaded"] > batch["rows_withdrawn"]
    )
    return data


def list_pipeline_views(conn: sqlite3.Connection) -> list[dict]:
    policy = get_today_policy(conn)
    return [_pipeline_view(conn, item, policy) for item in list_pipelines(conn)]


def pipeline_detail(conn: sqlite3.Connection, pipeline_id: int) -> dict | None:
    item = get_pipeline(conn, pipeline_id)
    if not item:
        return None
    policy = get_today_policy(conn)
    data = _pipeline_view(conn, item, policy)
    versions: list[dict] = []
    if data["mapping_id"]:
        versions = _versions_of(conn, data["mapping_id"])
    data["versions"] = versions
    data["newer_versions"] = [
        v for v in versions if (data["bound_version_no"] or 0) < v["version_no"]
    ]
    logs = conn.execute(
        "SELECT * FROM switch_logs WHERE pipeline_id = ? ORDER BY id DESC",
        (pipeline_id,),
    ).fetchall()
    data["switch_logs"] = [dict(r) for r in logs]
    data["switch_failures"] = [dict(r) for r in logs if not r["ok"]]
    data["today_policy"] = policy
    return data
