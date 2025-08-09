# mobile_api.py  ·  TREEANT Mobile API (v1)
# 실행: uvicorn mobile_api:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import sqlite3
import psycopg2  # Postgres 사용
from psycopg2.extras import DictCursor
from typing import List, Dict, Any, Optional
import os

# --- DB 유틸 ---
from db import get_conn, DATABASE_URL, init_schema

# --- 이동 유틸 (로컬 SQLite/운영 PG 자동 분기) ---
from location_utils import move_location, assign_initial_location

# ===== FastAPI 앱 생성 및 미들웨어 설정 =====
app = FastAPI(title="TREEANT Mobile API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 공통 =====
def _is_pg() -> bool:
    return DATABASE_URL.startswith(("postgres://", "postgresql://"))

# ===== DB 헬퍼 =====
def q(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    쿼리 실행 → [dict, dict, ...] 반환.
    - PG: '?' → '%s' 치환, DictCursor
    - SQLite: sqlite3.Row
    """
    conn = None
    try:
        conn = get_conn()
        is_postgres = _is_pg()

        if is_postgres:
            sql = sql.replace("?", "%s")
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
            cur.close()
            return [dict(r) for r in rows] if rows else []
    except Exception as e:
        print(f"Database query failed: {e}")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if conn:
            conn.close()

def prefix_from_boxid(boxid: str) -> str:
    """품목-YYYYMMDD-배치-시리얼 → 마지막 '-' 앞까지 + '-'"""
    if "-" not in boxid:
        return boxid
    return boxid.rsplit("-", 1)[0] + "-"

# ===== Pydantic 모델 =====
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

# ===== 앱 시작 시 스키마 보장 =====
@app.on_event("startup")
def _startup():
    init_schema()

# ===== 라우트 =====
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

# ---- 조회 계열 ----
@app.get("/api/locations")
def list_locations(limit: int = 2000):
    # SQLite/PG 공통 동작. LIMIT 파라미터 바인딩.
    rows = q(
        "SELECT DISTINCT Location FROM boxid_log "
        "WHERE Location IS NOT NULL AND Location<>'' "
        "ORDER BY 1 LIMIT ?",
        (limit,)
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
    is_pg = _is_pg()
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

    # SQLite엔 rowid가 있지만 PG엔 없음 → PG에서는 BoxID로 대체 정렬
    if is_pg:
        sql += " ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC, BoxID DESC LIMIT ?"
    else:
        sql += " ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC, rowid DESC LIMIT ?"
    params.append(limit)

    return {"rows": q(sql, tuple(params))}

@app.get("/api/boxes/by-scan")
def boxes_by_scan(boxid: str):
    if not boxid:
        raise HTTPException(400, "boxid required")
    prefix = prefix_from_boxid(boxid)

    # PG는 substr의 음수 시작이 없어 right() 사용
    # 조회 자체는 prefix로 충분하므로 공통 SQL 사용
    rows = q(
        "SELECT BoxID, ItemCode, Qty, Location, Status, "
        "COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt "
        "FROM boxid_log "
        "WHERE BoxID LIKE ? || '%' "
        "ORDER BY BoxID",
        (prefix,)
    )
    if not rows:
        raise HTTPException(404, f"No boxes for prefix {prefix}")

    # Serial(끝 4자리) 계산은 파이썬에서 공통 처리
    for r in rows:
        r["Serial"] = r["BoxID"][-4:] if r.get("BoxID") else None
    return {"prefix": prefix, "count": len(rows), "boxes": rows}

# ---- 이동/저장 ----
@app.post("/api/move/by-range")
def move_by_range(body: MoveRangeIn):
    prefix = prefix_from_boxid(body.boxid)
    is_pg = _is_pg()

    if is_pg:
        # PG: 끝 4자리는 RIGHT(BoxID,4)::INT
        sql = (
            "SELECT BoxID FROM boxid_log "
            "WHERE BoxID LIKE ? || '%' "
            "AND CAST(RIGHT(BoxID, 4) AS INT) BETWEEN ? AND ? "
            "ORDER BY BoxID"
        )
    else:
        # SQLite: substr(BoxID, -4)
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

# ===== 정적 파일 서빙 =====
try:
    app.mount("/app", StaticFiles(directory="wwwroot", html=True), name="app")
except Exception as e:
    print(f"Warning: wwwroot directory not found. Static files disabled. ({e})")
