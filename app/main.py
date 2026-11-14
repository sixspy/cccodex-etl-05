"""FastAPI surface for the ETL lab.

All version-pinning decisions (binding, drift detection, day policy, withdraw
before reload) are made server-side; the browser only renders state and computes
version diffs from the raw column lists the API returns.
"""

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from itsdangerous import BadSignature, URLSafeSerializer

from app import db
from app.config import Config
from app.db import DomainError, SwitchRejected, get_db, init_db

ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
serializer = URLSafeSerializer(Config.SECRET, salt="cc-etl-session")

app = FastAPI(title="cc-etl-lab")
app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")


def _read_session(request: Request) -> dict:
    raw = request.cookies.get("etl_session")
    if not raw:
        return {}
    try:
        data = serializer.loads(raw)
        return data if isinstance(data, dict) else {}
    except BadSignature:
        return {}


def require_user(request: Request) -> str:
    user = _read_session(request).get("user")
    if user != Config.ADMIN_USER:
        raise HTTPException(status_code=401, detail="未登录")
    return user


def _error(message: str, status_code: int = 409) -> JSONResponse:
    return JSONResponse(
        {"ok": False, "error": message, "detail": message, "reason": message},
        status_code=status_code,
    )


async def _body(request: Request) -> dict:
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/json"):
        try:
            data = await request.json()
        except ValueError:
            return {}
        return data if isinstance(data, dict) else {}
    if content_type.startswith(("application/x-www-form-urlencoded", "multipart/form-data")):
        form = await request.form()
        return {k: form.get(k) for k in form}
    return {}


def _param(data: dict, request: Request, *names, default=None):
    for name in names:
        if name in data and data[name] not in (None, ""):
            return data[name]
        value = request.query_params.get(name)
        if value not in (None, ""):
            return value
    return default


def _as_int(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_bool(value, default: bool = True) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() not in ("false", "0", "no", "off", "")


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root(request: Request):
    if _read_session(request).get("user") == Config.ADMIN_USER:
        return RedirectResponse("/app/", status_code=302)
    return RedirectResponse("/login", status_code=302)


@app.get("/login")
def login_page():
    return FileResponse(STATIC / "login.html")


@app.get("/app/")
def spa_page(request: Request):
    if _read_session(request).get("user") != Config.ADMIN_USER:
        return RedirectResponse("/login", status_code=302)
    return FileResponse(STATIC / "index.html")


@app.post("/api/login")
async def api_login(request: Request):
    body = await request.json()
    user = str(body.get("username", "")).strip()
    pwd = str(body.get("password", ""))
    if user != Config.ADMIN_USER or pwd != Config.ADMIN_PASSWORD:
        return JSONResponse({"ok": False, "error": "账号或密码错误"}, status_code=400)
    resp = JSONResponse({"ok": True, "user": user})
    resp.set_cookie(
        "etl_session",
        serializer.dumps({"user": user}),
        httponly=True,
        samesite="lax",
    )
    return resp


@app.post("/api/logout")
def api_logout():
    resp = JSONResponse({"ok": True})
    resp.delete_cookie("etl_session")
    return resp


@app.get("/api/me")
def api_me(user: str = Depends(require_user)):
    return {"user": user, "lab": Config.LAB_NAME, "domain": "数据工程"}


# --------------------------------------------------------------------------- #
# mapping register
# --------------------------------------------------------------------------- #


@app.get("/api/mappings")
def api_mappings(user: str = Depends(require_user)):
    with get_db() as conn:
        return {"items": db.list_mappings(conn)}


@app.post("/api/mappings")
async def api_create_mapping(request: Request, user: str = Depends(require_user)):
    data = await _body(request)
    try:
        with get_db() as conn:
            mapping = db.create_mapping(
                conn,
                name=str(_param(data, request, "name", "title", default="")),
                code=str(_param(data, request, "code", default="")),
                columns=_param(data, request, "columns", default=""),
                code_column=str(_param(data, request, "code_column", "codeColumn", default="")),
                user=user,
            )
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    return {"ok": True, "mapping": mapping}


@app.get("/api/mappings/{mapping_id}")
def api_mapping_detail(mapping_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        mapping = db.get_mapping(conn, mapping_id)
    if not mapping:
        raise HTTPException(status_code=404, detail="映射不存在")
    return mapping


@app.post("/api/mappings/{mapping_id}/versions")
async def api_append_version(
    mapping_id: int, request: Request, user: str = Depends(require_user)
):
    data = await _body(request)
    base_version = _as_int(_param(data, request, "base_version", "baseVersion", default=None))
    columns = _param(data, request, "columns", "column_list", default="")
    if not columns:
        # Default to the current head's columns so a save can change only the
        # code column without re-sending the whole definition.
        with get_db() as conn:
            mapping = db.get_mapping(conn, mapping_id)
        if mapping and mapping["latest_version"]:
            columns = mapping["latest_version"]["columns"]
    try:
        with get_db() as conn:
            # append_version takes the write lock itself after sampling the
            # current head, so two simultaneous savers cannot both append.
            version, created = db.append_version(
                conn,
                mapping_id,
                columns=_param(data, request, "columns", "column_list", default=""),
                code_column=str(
                    _param(data, request, "code_column", "codeColumn", default="")
                ),
                user=user,
                base_version=base_version,
            )
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    return {
        "ok": True,
        "created": created,
        "version": version,
        "version_no": version["version_no"],
        "version_label": version["version_label"],
    }


@app.post("/api/mappings/{mapping_id}/versions/{version_no}/publish")
def api_publish_version(
    mapping_id: int, version_no: int, user: str = Depends(require_user)
):
    return _publish(mapping_id, version_no)


@app.post("/api/mappings/{mapping_id}/publish")
async def api_publish(
    mapping_id: int, request: Request, user: str = Depends(require_user)
):
    data = await _body(request)
    version_no = _as_int(_param(data, request, "version_no", "versionNo", default=None))
    if version_no is None:
        with get_db() as conn:
            mapping = db.get_mapping(conn, mapping_id)
        if not mapping or not mapping["latest_version"]:
            raise HTTPException(status_code=404, detail="映射不存在")
        version_no = mapping["latest_version"]["version_no"]
    return _publish(mapping_id, version_no)


def _publish(mapping_id: int, version_no: int):
    try:
        with get_db() as conn:
            version = db.publish_version(conn, mapping_id, version_no)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    return {"ok": True, "version": version, "version_no": version["version_no"]}


# --------------------------------------------------------------------------- #
# day switch policy
# --------------------------------------------------------------------------- #


@app.get("/api/switch-policy")
@app.get("/api/policies/today")
def api_get_policy(user: str = Depends(require_user)):
    with get_db() as conn:
        policy = db.get_today_policy(conn)
    return {"ok": True, "policy": policy, "locked": bool(policy), "options": list(db.POLICY_OPTIONS)}


@app.post("/api/switch-policy")
@app.post("/api/policies/today")
async def api_set_policy(request: Request, user: str = Depends(require_user)):
    data = await _body(request)
    policy = str(_param(data, request, "policy", default="")).strip()
    note = str(_param(data, request, "note", "说明", default="") or "")
    try:
        with get_db() as conn:
            saved = db.set_today_policy(conn, policy, note, user)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    return {"ok": True, "policy": saved, "locked": True}


# --------------------------------------------------------------------------- #
# pipelines
# --------------------------------------------------------------------------- #


@app.get("/api/overview")
def api_overview(user: str = Depends(require_user)):
    with get_db() as conn:
        items = db.list_pipeline_views(conn)
        policy = db.get_today_policy(conn)
    return {"items": items, "policy": policy, "policy_name": policy["policy_name"] if policy else "未设定"}


@app.get("/api/pipelines")
def api_pipelines(user: str = Depends(require_user)):
    with get_db() as conn:
        items = db.list_pipeline_views(conn)
        policy = db.get_today_policy(conn)
    return {
        "items": items,
        "policy": policy,
        "policy_name": policy["policy_name"] if policy else "未设定",
    }


@app.post("/api/pipelines")
async def api_create_pipeline(request: Request, user: str = Depends(require_user)):
    data = await _body(request)
    try:
        with get_db() as conn:
            item = db.create_pipeline(
                conn,
                title=str(_param(data, request, "title", "name", default="")),
                code=str(_param(data, request, "code", default="")),
                source_uri=str(
                    _param(data, request, "source_uri", "sourceUri", "source", default="")
                ),
                user=user,
                mapping_version_id=_as_int(
                    _param(data, request, "mapping_version_id", "mappingVersionId", default=None)
                ),
                mapping_id=_as_int(_param(data, request, "mapping_id", "mappingId", default=None)),
                version_no=_as_int(_param(data, request, "version_no", "versionNo", default=None)),
            )
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    return {"ok": True, "pipeline": item, "item": item}


@app.get("/api/pipelines/{pipeline_id}/versions")
def api_pipeline_versions(pipeline_id: int, user: str = Depends(require_user)):
    """Version list for the detail screen (raw columns for frontend diffing)."""
    with get_db() as conn:
        item = db.get_pipeline(conn, pipeline_id)
        if not item:
            raise HTTPException(status_code=404, detail="管道不存在")
        current, mapping = db._version_context(conn, item)
        if not current or not mapping:
            return {"items": [], "bound_version_no": None}
        versions = db._versions_of(conn, mapping["id"])
    return {
        "items": versions,
        "version_count": len(versions),
        "bound_version_no": current["version_no"],
        "newer": [v for v in versions if v["version_no"] > current["version_no"]],
    }


@app.get("/api/pipelines/{pipeline_id}")
def api_pipeline_detail(pipeline_id: int, user: str = Depends(require_user)):
    with get_db() as conn:
        item = db.pipeline_detail(conn, pipeline_id)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


def _begin(conn) -> None:
    """Take the write lock so read-decide-write state changes are serialized."""
    conn.execute("BEGIN IMMEDIATE")


@app.post("/api/pipelines/{pipeline_id}/extract")
def api_extract(pipeline_id: int, user: str = Depends(require_user)):
    try:
        with get_db() as conn:
            _begin(conn)
            item = db.run_extract(conn, pipeline_id, user)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/load")
def api_load(pipeline_id: int, user: str = Depends(require_user)):
    try:
        with get_db() as conn:
            _begin(conn)
            item = db.run_load(conn, pipeline_id, user)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/withdraw")
def api_withdraw(pipeline_id: int, user: str = Depends(require_user)):
    try:
        with get_db() as conn:
            _begin(conn)
            item = db.withdraw_batch(conn, pipeline_id, user)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


@app.post("/api/pipelines/{pipeline_id}/confirm")
def api_confirm(pipeline_id: int, user: str = Depends(require_user)):
    try:
        with get_db() as conn:
            _begin(conn)
            item = db.confirm_batch(conn, pipeline_id, user)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    if not item:
        raise HTTPException(status_code=404, detail="管道不存在")
    return item


def _version_by_id(conn, pipeline_id: int, value) -> dict | None:
    number = _as_int(value)
    if number is None:
        return None
    item = db.get_pipeline(conn, pipeline_id)
    if not item or item["mapping_version_id"] is None:
        # A version id still identifies the mapping version directly.
        return db.get_version(conn, number)
    current = db.get_version(conn, item["mapping_version_id"])
    if not current:
        return db.get_version(conn, number)
    candidate = db.get_version(conn, number)
    if candidate and candidate["mapping_id"] == current["mapping_id"]:
        return candidate
    return None


def _version_by_no(conn, pipeline_id: int, value) -> dict | None:
    """Resolve a version number, ``vN`` label, or a version id for presentation APIs."""
    if value is None:
        return None
    text = str(value).strip()
    if text.lower().startswith("v"):
        text = text[1:]
    number = _as_int(text)
    if number is None:
        return None
    item = db.get_pipeline(conn, pipeline_id)
    if not item or item["mapping_version_id"] is None:
        return None
    current = db.get_version(conn, item["mapping_version_id"])
    if not current:
        return None
    by_no = db.find_version(conn, current["mapping_id"], number)
    if by_no:
        return by_no
    by_id = _version_by_id(conn, pipeline_id, number)
    return by_id


@app.post("/api/pipelines/{pipeline_id}/switch")
@app.post("/api/pipelines/{pipeline_id}/switch-version")
@app.post("/api/pipelines/{pipeline_id}/bind")
@app.post("/api/pipelines/{pipeline_id}/rebind")
async def api_switch_version(
    pipeline_id: int, request: Request, user: str = Depends(require_user)
):
    data = await _body(request)
    to_id_raw = _param(
        data, request, "to_version_id", "toVersionId", "version_id", "versionId",
        "mapping_version_id", "mappingVersionId",
    )
    to_no_raw = _param(
        data, request, "to_version_no", "toVersionNo", "version_no", "versionNo",
        "to_version", "toVersion",
    )
    if to_id_raw is None and to_no_raw is None:
        to_no_raw = _param(data, request, "version")
    from_id_raw = _param(data, request, "from_version_id", "fromVersionId")
    from_no_raw = _param(data, request, "from_version_no", "fromVersionNo", "from_version")
    policy = _param(data, request, "policy", "progression", "推进法")
    withdraw_flag = _as_bool(
        _param(data, request, "withdraw_before_switch", "withdrawBeforeSwitch", default=None),
        default=True,
    )

    try:
        with get_db() as conn:
            # Serialize adjudication: read state, decide, write — all under one
            # write lock so two simultaneous switches cannot both win.
            _begin(conn)
            item = db.get_pipeline(conn, pipeline_id)
            if not item:
                raise HTTPException(status_code=404, detail="管道不存在")
            target = None
            if to_id_raw is not None:
                target = _version_by_id(conn, pipeline_id, to_id_raw)
            if target is None and to_no_raw is not None:
                target = _version_by_no(conn, pipeline_id, to_no_raw)
            to_id = target["id"] if target else None
            to_no = target["version_no"] if target else _as_int(to_no_raw)

            from_id = None
            if from_id_raw is not None:
                found = _version_by_id(conn, pipeline_id, from_id_raw)
                from_id = found["id"] if found else None
            elif from_no_raw is not None:
                found = _version_by_no(conn, pipeline_id, from_no_raw)
                from_id = found["id"] if found else None

            if (from_id_raw is not None or from_no_raw is not None) and from_id is None:
                # The caller claims to start from a version this pipeline does
                # not know: that is progression drift, not a missing target.
                raise db.SwitchRejected(
                    f"{db.SWITCH_ERROR_PREFIX}：推进法漂移：请求的起始版本不存在",
                    pipeline_id=pipeline_id,
                    actor=user,
                    from_version_no=_as_int(from_no_raw),
                    to_version_id=to_id,
                    to_version_no=to_no,
                )
            result = db.switch_version(
                conn,
                pipeline_id,
                user,
                to_version_id=to_id,
                to_version_no=to_no,
                from_version_id=from_id,
                requested_policy=str(policy) if policy else None,
                withdraw_before_switch=withdraw_flag,
            )
    except SwitchRejected as exc:
        db.log_switch_failure(
            None,
            exc.pipeline_id,
            exc.message,
            exc.actor,
            from_version_id=exc.from_version_id,
            from_version_no=exc.from_version_no,
            to_version_id=exc.to_version_id,
            to_version_no=exc.to_version_no,
        )
        return _error(exc.message, exc.status_code)
    except DomainError as exc:
        return _error(exc.message, exc.status_code)
    if not result:
        raise HTTPException(status_code=404, detail="管道不存在")
    return {"ok": True, "pipeline": result, "item": result, **result["switch"]}


def create_app() -> FastAPI:
    return app
