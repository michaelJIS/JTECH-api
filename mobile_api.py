# mobile_api.py  ·  TREEANT Mobile API (no external deps)
# Start: uvicorn mobile_api:app --host 0.0.0.0 --port 8000

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, HTMLResponse
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os, sqlite3
from psycopg2.extras import DictCursor  # psycopg2는 db.get_conn에서 import
from db import get_conn, init_schema

app = FastAPI(title="TREEANT Mobile API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

# === helpers ===
IS_PG = os.getenv("DATABASE_URL","").startswith(("postgres://","postgresql://"))

def q(sql: str, params: tuple=()) -> List[Dict[str,Any]]:
    conn = None
    try:
        conn = get_conn()
        if IS_PG:
            sql = sql.replace("?", "%s")
            with conn:
                with conn.cursor(cursor_factory=DictCursor) as cur:
                    cur.execute(sql, params)
                    rows = cur.fetchall() if cur.description else []
                    return [dict(r) for r in rows]
        else:
            conn.row_factory = sqlite3.Row
            cur = conn.cursor(); cur.execute(sql, params)
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(500, f"Database error: {e}")
    finally:
        if conn: conn.close()

def x(sql: str, params: tuple=()) -> int:
    """DML 실행(INSERT/UPDATE/DELETE). 영향을 받은 행 수 리턴."""
    conn = None
    try:
        conn = get_conn()
        if IS_PG:
            sql = sql.replace("?", "%s")
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                    return cur.rowcount or 0
        else:
            cur = conn.cursor(); cur.execute(sql, params); conn.commit()
            return cur.rowcount or 0
    except Exception as e:
        raise HTTPException(500, f"DML error: {e}")
    finally:
        if conn: conn.close()

def prefix_from_boxid(boxid: str) -> str:
    return boxid.rsplit("-",1)[0] + "-" if "-" in boxid else boxid

# === inline move helpers (원래 location_utils에 있던 역할) ===
def assign_initial_location(boxid: str, to_loc: str, operator: str="", reason: str="INITIAL"):
    # 없으면 INSERT, 있으면 UPDATE
    affected = x("UPDATE boxid_log SET Location=?, UpdatedAt=CURRENT_TIMESTAMP WHERE BoxID=?",
                 (to_loc, boxid))
    if affected == 0:
        x("INSERT INTO boxid_log (BoxID, Location, Status, CreatedAt) VALUES (?,?, 'OK', CURRENT_TIMESTAMP)",
          (boxid, to_loc))
    # (선택) 이동 이력 테이블이 있으면 거기에 로그도 추가
    try:
        x("INSERT INTO move_log (BoxID, ToLocation, Operator, Reason, CreatedAt) VALUES (?,?,?,?,CURRENT_TIMESTAMP)",
          (boxid, to_loc, operator, reason))
    except: pass

def move_location(boxid: str, to_loc: str, operator: str="", reason: str="MOVE"):
    assign_initial_location(boxid, to_loc, operator, reason)

# === startup ===
@app.on_event("startup")
def _start():
    try: init_schema()
    except Exception as e: print(f"[WARN] init_schema skipped: {e}")

# === models ===
class MoveRangeIn(BaseModel):
    boxid: str; start: int; end: int; to_loc: str
    operator: str = ""; reason: str = "MOVE"

class MoveBulkIn(BaseModel):
    boxids: List[str]; to_loc: str
    operator: str = ""; reason: str = "MOVE"

# === routes ===
@app.get("/")
def root(): return {"ok": True, "try": ["/health","/docs","/app"]}

@app.get("/health")
def health():
    try: q("SELECT 1"); return {"ok": True, "db":"ok"}
    except Exception as e: return {"ok": False, "db":"fail", "err": str(e)}

@app.get("/api/locations")
def list_locations(limit: int=2000):
    rows = q("SELECT DISTINCT Location FROM boxid_log WHERE Location IS NOT NULL AND Location<>'' ORDER BY 1 LIMIT ?",
             (limit,))
    return {"locations": [r["Location"] for r in rows]}

@app.get("/api/box/by-id")
def box_by_id(boxid: str):
    rows = q("SELECT * FROM boxid_log WHERE BoxID=? LIMIT 1", (boxid,))
    if not rows: raise HTTPException(404,"Not found")
    return rows[0]

@app.get("/api/boxes/search")
def boxes_search(boxid: Optional[str]=None, location: Optional[str]=None, limit: int=10000):
    sql = ("SELECT BoxID, ItemCode, Qty, Location, Status, COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt "
           "FROM boxid_log WHERE 1=1")
    params: List[Any] = []
    if boxid:   sql += " AND BoxID LIKE ?";   params.append(f"%{boxid}%")
    if location:sql += " AND Location LIKE ?";params.append(f"{location}%")
    sql += " ORDER BY COALESCE(UpdatedAt, CreatedAt) DESC, BoxID DESC LIMIT ?"; params.append(limit)
    return {"rows": q(sql, tuple(params))}

@app.get("/api/boxes/by-scan")
def boxes_by_scan(boxid: str):
    if not boxid: raise HTTPException(400,"boxid required")
    prefix = prefix_from_boxid(boxid)
    rows = q(("SELECT BoxID, ItemCode, Qty, Location, Status, COALESCE(UpdatedAt, CreatedAt) AS UpdatedAt "
              "FROM boxid_log WHERE BoxID LIKE ? || '%' ORDER BY BoxID"), (prefix,))
    if not rows: raise HTTPException(404, f"No boxes for prefix {prefix}")
    for r in rows: r["Serial"] = r["BoxID"][-4:]
    return {"prefix": prefix, "count": len(rows), "boxes": rows}

@app.post("/api/move/by-range")
def move_by_range(body: MoveRangeIn):
    prefix = prefix_from_boxid(body.boxid)
    if IS_PG:
        rows = q(("SELECT BoxID FROM boxid_log WHERE BoxID LIKE %s || '%%' "
                  "AND CAST(RIGHT(BoxID,4) AS INTEGER) BETWEEN %s AND %s ORDER BY BoxID"),
                 (prefix, body.start, body.end))
    else:
        rows = q(("SELECT BoxID FROM boxid_log WHERE BoxID LIKE ? || '%' "
                  "AND CAST(substr(BoxID,-4) AS INTEGER) BETWEEN ? AND ? ORDER BY BoxID"),
                 (prefix, body.start, body.end))
    if not rows: raise HTTPException(404,"No boxes in range")
    for r in rows:
        cur = q("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (r["BoxID"],))
        if cur: move_location(r["BoxID"], body.to_loc, body.operator, body.reason)
        else:   assign_initial_location(r["BoxID"], body.to_loc, body.operator, "INITIAL")
    return {"moved": len(rows), "to_loc": body.to_loc, "range": [body.start, body.end]}

@app.post("/api/move/bulk")
def move_bulk(body: MoveBulkIn):
    if not body.boxids: raise HTTPException(400,"boxids empty")
    moved, fails = 0, []
    for b in body.boxids:
        try:
            cur = q("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (b,))
            if cur: move_location(b, body.to_loc, body.operator, body.reason)
            else:   assign_initial_location(b, body.to_loc, body.operator, "INITIAL")
            moved += 1
        except Exception as ex:
            fails.append({"boxid": b, "err": str(ex)})
    return {"moved": moved, "fails": fails}

# === /app 임시 HTML (wwwroot가 없을 때)
@app.get("/app")
def app_fallback():
    html = """
    <html><head><meta charset="utf-8"><title>TREEANT /app</title></head>
    <body style="font-family:sans-serif">
      <h2>/app 임시 페이지</h2>
      <p>배포 성공. 정적 파일을 쓰려면 리포에 <code>wwwroot/</code>를 추가하세요.</p>
      <ul>
        <li><a href="/health">/health</a></li>
        <li><a href="/docs">/docs</a></li>
      </ul>
    </body></html>
    """
    return HTMLResponse(html)
