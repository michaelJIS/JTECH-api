# mobile_api.py · TREEANT Mobile API (v1)
# 실행(로컬): uvicorn mobile_api:app --host 0.0.0.0 --port 8000 --reload

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import sqlite3
import psycopg2
from psycopg2.extras import DictCursor

# ---- DB 유틸 (db.py) ----
from db import get_conn, init_schema

# ==== 1) FastAPI 앱 생성이 최우선 ====
app = FastAPI(title="TREEANT Mobile API", version="1.0.0")

# ==== 2) 미들웨어/CORS ====
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # 필요 시 도메인으로 좁히기
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==== 3) 정적 파일(/app) 서빙 ====
try:
    app.mount("/app", StaticFiles(directory="wwwroot", html=True), name="app")
except Exception as e:
    print(f"[WARN] wwwroot not found, static disabled: {e}")

# ==== 4) 앱 시작 훅 ====
@app.on_event("startup")
def _startup_init():
    try:
        init_schema()
    except Exception as e:
        print(f"[WARN] init_schema skipped: {e}")

# ==== 5) 공통 헬퍼 ====
IS_POSTGRES = os.getenv("DATABASE_URL", "").strip().startswith(("postgres://", "postgresql://"))

def q(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    DB 쿼리 실행(helper). 결과는 list[dict]로 반환.
    - Postgres: %s 플레이스홀더, DictCursor
    - SQLite  : ?  플레이스홀더, sqlite3.Row
    """
    conn = None
    try:
        conn = get_conn()

        if IS_POSTGRES:
            sql = sql.replace("?", "%s")  # 플레이스홀더 변환
            with conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() if cur.description else []
                    return [dict(r) for r in rows]
        else:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        print(f"[DB ERROR] {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def prefix_from_boxid(boxid: str) -> str:
    """품목-YYYYMMDD-배치-시리얼 → 마지막 '-' 앞까지 + '-'"""
    return boxid.rsplit("-", 1)[0] + "-" if "-" in boxid else boxid

# ---- 외부 유틸(기존 로직 재사용) ----
from location_utils import move_location, assign_initial_location

# ==== 6) Pydantic 모델 ====
class MoveRangeIn(BaseModel):
    boxid: str
    start: int
    end: int
    to_loc: str
    operator: str = ""
    reason: str = "MOVE"

class MoveBulkIn(BaseModel):
    boxids: List[str]
    to_loc: str
    operator: str = ""
    reason: str = "MOVE"

# ==== 7) 라우트 ====
@app.get("/")
def root():
    return {"ok": True, "msg": "TREEANT Mobile API", "try": ["/health", "/docs", "/app/"]}

@app.get("/health")
def health():
    try:
        q("SELECT 1")
        return {"ok": True, "database_connection": "ok"}
    except Exception as e:
        return {"ok": False, "database_connection": "failed", "error": str(e)}

# ---- 조회 ----
@app.get("/api/locations")
def list_locations(limit: int = 2000):
    rows = q(
        "SELECT DISTINCT Location FROM boxid_log "
        "WHERE Location IS NOT NULL AND Location<>'' "
        "ORDER BY 1 LIMIT ?",
        (limit,),
    )
    return {"locations": [r["Location"] for r in rows]}

@app.get("/api/box/by-id")
def box_by_id(boxid: str):
    rows = q("SELECT * FROM boxid_log WHERE BoxID = ? LIMIT 1", (boxid,))
    if not rows:
        raise HTTPException(404, "Not found")
    return rows[0]

@app.get("/api/boxes/search")
def boxes_search(boxid: Optional[str] = None, location: Optional[str] = None, limit: int = 10000):
    sql = (
        "SELECT BoxID, ItemCode, Qty, Location, Status, "
        "COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt "
        "FROM boxid_log WHERE 1=1"
    )
    params: List[Any] = []
    if boxid:
        sql += " AND BoxID LIKE ?"
        params.append(f"%{boxid}%")
    if location:
        sql += " AND Location LIKE ?"
        params.append(f"{location}%")
    # SQLite의 rowid는 PG에 없음 → 휴대용 정렬키로 BoxID 사용
    sql += " ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC, BoxID DESC LIMIT ?"
    params.append(limit)
    return {"rows": q(sql, tuple(params))}

@app.get("/api/boxes/by-scan")
def boxes_by_scan(boxid: str):
    if not boxid:
        raise HTTPException(400, "boxid required")
    prefix = prefix_from_boxid(boxid)
    rows = q(
        "SELECT BoxID, ItemCode, Qty, Location, Status, "
        "COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt "
        "FROM boxid_log WHERE BoxID LIKE ? || '%' ORDER BY BoxID",
        (prefix,),
    )
    if not rows:
        raise HTTPException(404, f"No boxes for prefix {prefix}")
    for r in rows:
        r["Serial"] = r["BoxID"][-4:]
    return {"prefix": prefix, "count": len(rows), "boxes": rows}

# ---- 이동/저장 ----
@app.post("/api/move/by-range")
def move_by_range(body: MoveRangeIn):
    prefix = prefix_from_boxid(body.boxid)

    if IS_POSTGRES:
        sql = (
            "SELECT BoxID FROM boxid_log "
            "WHERE BoxID LIKE %s || '%%' "
            "AND CAST(RIGHT(BoxID, 4) AS INTEGER) BETWEEN %s AND %s "
            "ORDER BY BoxID"
        )
        rows = q(sql, (prefix, body.start, body.end))
    else:
        sql = (
            "SELECT BoxID FROM boxid_log "
            "WHERE BoxID LIKE ? || '%' "
            "AND CAST(substr(BoxID, -4) AS INTEGER) BETWEEN ? AND ? "
            "ORDER BY BoxID"
        )
        rows = q(sql, (prefix, body.start, body.end))

    if not rows:
        raise HTTPException(404, "No boxes in range")

    moved = 0
    for r in rows:
        b = r["BoxID"]
        cur = q("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (b,))
        cur_loc = cur[0]["Location"] if cur else None
        if cur_loc:
            move_location(b, body.to_loc, body.operator, body.reason)
        else:
            assign_initial_location(b, body.to_loc, body.operator, "INITIAL")
        moved += 1
    return {"moved": moved, "to_loc": body.to_loc, "range": [body.start, body.end]}

@app.post("/api/move/bulk")
def move_bulk(body: MoveBulkIn):
    if not body.boxids:
        raise HTTPException(400, "boxids empty")
    moved = 0
    fails = []
    for b in body.boxids:
        try:
            cur = q("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (b,))
            cur_loc = cur[0]["Location"] if cur else None
            if cur_loc:
                move_location(b, body.to_loc, body.operator, body.reason)
            else:
                assign_initial_location(b, body.to_loc, body.operator, "INITIAL")
            moved += 1
        except Exception as ex:
            fails.append({"boxid": b, "err": str(ex)})
    return {"moved": moved, "fails": fails}
