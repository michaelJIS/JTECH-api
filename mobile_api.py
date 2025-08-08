# mobile_api.py  ·  TREEANT Mobile API (v1)
# 실행: uvicorn mobile_api:app --host 0.0.0.0 --port 8000 --reload
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import sqlite3
import psycopg2 # Postgres 사용을 위해 import
from psycopg2.extras import DictCursor # 결과를 dict처럼 다루기 위해 import
from typing import List, Dict, Any, Optional
from db import get_conn, init_schema

@app.on_event("startup")
def _init():
    init_schema()

# --- 수정된 부분 ---
# 기존 DB_PATH 대신 새로운 db.py 모듈 사용
from db import get_conn, DATABASE_URL
# ------------------

# 기존 프로젝트 파일 재사용 (DB 관련성이 없는 경우 그대로 둠)
from location_utils import move_location, assign_initial_location

# ===== FastAPI 앱 생성 및 미들웨어 설정 =====
app = FastAPI(title="TREEANT Mobile API", version="1.0.0")

# CORS (모바일 브라우저 접근 허용) — 사내망 테스트용으로 우선 전체 허용
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 헬퍼 함수들 =====

# --- 수정된 부분: q 함수 전체 변경 ---
def q(sql: str, params: tuple = ()) -> List[Dict[str, Any]]:
    """
    DB 커넥션을 가져와 쿼리를 실행하고 결과를 [dict, dict, ...] 형태로 반환합니다.
    DATABASE_URL 환경변수에 따라 Postgres 또는 SQLite에 자동으로 연결됩니다.
    """
    conn = None
    try:
        conn = get_conn()
        is_postgres = hasattr(conn, 'cursor') and "psycopg2" in conn.__module__

        with conn: # with문으로 자동 commit/close 보장
            if is_postgres:
                # 1. Postgres는 파라미터 스타일이 '?'가 아닌 '%s' 이므로 교체
                sql = sql.replace("?", "%s")
                # 2. 결과를 딕셔너리 형태로 받기 위해 DictCursor 사용
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(sql, params)
                    # fetchall()은 결과가 없을 때 빈 리스트를 반환할 수 있음
                    results = cur.fetchall()
                    return [dict(row) for row in results] if results else []
            else: # 기존 SQLite 로직
                conn.row_factory = sqlite3.Row
                cur = conn.cursor()
                cur.execute(sql, params)
                results = cur.fetchall()
                return [dict(row) for row in results] if results else []

    except Exception as e:
        # 실제 운영시에는 로깅 프레임워크 사용 권장 (e.g., logging)
        print(f"Database query failed: {e}")
        # 에러 발생 시 빈 리스트 반환 또는 상황에 맞는 예외 처리
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    finally:
        if conn:
            # get_conn()에서 생성된 커넥션은 여기서 확실히 닫아줍니다.
            conn.close()
# -------------------------------------

def prefix_from_boxid(boxid: str) -> str:
    """품목-YYYYMMDD-배치-시리얼 → 마지막 '-' 앞까지 + '-'"""
    if "-" not in boxid:
        return boxid
    return boxid.rsplit("-", 1)[0] + "-"

# ===== Pydantic 모델들 (변경 없음) =====
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

# ===== API 라우트들 (변경 없음) =====
# 모든 라우트는 q() 함수를 통해 DB에 접근하므로,
# q() 함수만 수정하면 나머지 코드는 변경할 필요가 없습니다.

@app.get("/")
def root():
    return {"ok": True, "msg": "TREEANT Mobile API", "try": ["/health", "/docs", "/app/"]}

@app.get("/health")
def health():
    # 간단한 DB 연결 테스트를 추가하여 health check 강화 가능
    try:
        # DB에 간단한 쿼리를 날려 연결 상태 확인
        q("SELECT 1")
        return {"ok": True, "database_connection": "ok"}
    except Exception as e:
        return {"ok": False, "database_connection": "failed", "error": str(e)}

# ---- 조회 계열 API ---------------------------------
@app.get("/api/locations")
def list_locations(limit: int = 2000):
    rows = q("SELECT DISTINCT Location FROM boxid_log WHERE Location IS NOT NULL AND Location<>'' ORDER BY 1 LIMIT ?", (limit,))
    return {"locations": [r["Location"] for r in rows]}

@app.get("/api/box/by-id")
def box_by_id(boxid: str):
    # PostgreSQL에서는 테이블/컬럼명에 대소문자가 있으면 따옴표로 감싸주는 것이 안전합니다.
    # 예: "BoxID" 대신 BoxID (SQLite는 대소문자 구분 안 함)
    # 현재 스키마가 소문자로 되어있다고 가정하고 코드는 그대로 둡니다.
    rows = q("SELECT * FROM boxid_log WHERE BoxID = ? LIMIT 1", (boxid,))
    if not rows:
        raise HTTPException(404, "Not found")
    return rows[0]

@app.get("/api/boxes/search")
def boxes_search(boxid: Optional[str] = None, location: Optional[str] = None, limit: int = 10000):
    sql = "SELECT BoxID, ItemCode, Qty, Location, Status, COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt FROM boxid_log WHERE 1=1"
    params: List[Any] = []
    if boxid:
        sql += " AND BoxID LIKE ?"
        params.append(f"%{boxid}%")
    if location:
        sql += " AND Location LIKE ?"
        params.append(f"{location}%")
    sql += " ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC, rowid DESC LIMIT ?"
    params.append(limit)
    return {"rows": q(sql, tuple(params))}

@app.get("/api/boxes/by-scan")
def boxes_by_scan(boxid: str):
    if not boxid:
        raise HTTPException(400, "boxid required")
    prefix = prefix_from_boxid(boxid)
    # CAST(substr(BoxID, -4) AS INTEGER)와 같은 SQLite 전용 함수는
    # PostgreSQL에서도 호환되지만, 만약 호환되지 않는 함수가 있다면
    # q() 함수 내부에서 DB 종류에 따라 분기 처리해야 할 수도 있습니다.
    rows = q("""
        SELECT BoxID, ItemCode, Qty, Location, Status, COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt
        FROM boxid_log
        WHERE BoxID LIKE ? || '%'
        ORDER BY BoxID
    """, (prefix,))
    if not rows:
        raise HTTPException(404, f"No boxes for prefix {prefix}")
    for r in rows:
        r["Serial"] = r["BoxID"][-4:]  # 끝 4자리
    return {"prefix": prefix, "count": len(rows), "boxes": rows}

# ---- 이동/저장 API ---------------------------------
@app.post("/api/move/by-range")
def move_by_range(body: MoveRangeIn):
    prefix = prefix_from_boxid(body.boxid)
    rows = q("""
        SELECT BoxID FROM boxid_log
        WHERE BoxID LIKE ? || '%'
          AND CAST(substr(BoxID, -4) AS INTEGER) BETWEEN ? AND ?
        ORDER BY BoxID
    """, (prefix, body.start, body.end))
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

# ===== 정적 파일 서빙 (마지막에 배치) =====
try:
    app.mount("/app", StaticFiles(directory="wwwroot", html=True), name="app")
except Exception as e:

    print(f"Warning: wwwroot directory not found. Static files disabled. ({e})")
