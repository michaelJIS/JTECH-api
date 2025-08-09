# location_utils.py  ·  위치 지정/이동/조회 유틸 (Hybrid: SQLite 로컬 / Postgres Render)
from __future__ import annotations
import os
from datetime import datetime
from typing import List, Dict, Optional

# --- 백엔드 판별 ---
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
USE_PG = DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://")

# 공통 포맷 시간
def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# =========================
#  SQLite (로컬 개발용)
# =========================
if not USE_PG:
    import sqlite3
    from boxid_utils import DB_PATH  # 기존 로컬 경로 그대로 사용

    def _conn_sqlite():
        return sqlite3.connect(DB_PATH)

    def init_move_tables():
        """(로컬) 이동 이력 테이블 없으면 생성. 기존 스키마 유지."""
        with _conn_sqlite() as c:
            c.execute("""
            CREATE TABLE IF NOT EXISTS box_move_log(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                BoxID TEXT NOT NULL,
                FromLoc TEXT,
                ToLoc TEXT NOT NULL,
                MovedAt TEXT NOT NULL,
                Operator TEXT,
                Reason TEXT
            )
            """)
            # boxid_log는 기존에 이미 있음(컬럼: BoxID, Location, UpdatedAt 등)

    def get_current_location(boxid: str) -> Optional[str]:
        """(로컬) boxid_log.Location(마지막 위치) 리턴. 없으면 None."""
        with _conn_sqlite() as c:
            cur = c.execute(
                "SELECT Location FROM boxid_log WHERE BoxID=? ORDER BY rowid DESC LIMIT 1",
                (boxid,)
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None

    def get_move_history(boxid: str, limit: int = 20) -> List[Dict]:
        with _conn_sqlite() as c:
            cur = c.execute(
                "SELECT FromLoc, ToLoc, MovedAt, Operator, Reason "
                "FROM box_move_log WHERE BoxID=? ORDER BY id DESC LIMIT ?",
                (boxid, limit)
            )
            return [{"From": r[0], "To": r[1], "At": r[2], "By": r[3], "Reason": r[4]}
                    for r in cur.fetchall()]

    def assign_initial_location(boxid: str, to_loc: str, operator: str, reason: str = "INITIAL"):
        """(로컬) 최초 입고 위치 지정. 이력 남기고 boxid_log.Location 갱신."""
        now = _now_str()
        with _conn_sqlite() as c:
            # 기존 위치 조회
            cur = c.execute("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (boxid,))
            row = cur.fetchone()
            from_loc = row[0] if row and row[0] else None

            # 이력
            c.execute(
                "INSERT INTO box_move_log(BoxID, FromLoc, ToLoc, MovedAt, Operator, Reason) "
                "VALUES (?,?,?,?,?,?)",
                (boxid, from_loc, to_loc, now, operator, reason)
            )
            # 현재 위치 갱신
            c.execute(
                "UPDATE boxid_log SET Location=?, UpdatedAt=? WHERE BoxID=?",
                (to_loc, now, boxid)
            )

    def move_location(boxid: str, to_loc: str, operator: str, reason: str = ""):
        """(로컬) 위치 이동(이력 + 현재위치 갱신)."""
        now = _now_str()
        with _conn_sqlite() as c:
            cur = c.execute("SELECT Location FROM boxid_log WHERE BoxID=? LIMIT 1", (boxid,))
            row = cur.fetchone()
            from_loc = row[0] if row and row[0] else None

            c.execute(
                "INSERT INTO box_move_log(BoxID, FromLoc, ToLoc, MovedAt, Operator, Reason) "
                "VALUES (?,?,?,?,?,?)",
                (boxid, from_loc, to_loc, now, operator, reason)
            )
            c.execute(
                "UPDATE boxid_log SET Location=?, UpdatedAt=? WHERE BoxID=?",
                (to_loc, now, boxid)
            )

# =========================
#  Postgres (Render 운영용)
# =========================
else:
    import psycopg2
    from psycopg2.extras import DictCursor

    BOXES_TABLE = os.getenv("BOXES_TABLE", "boxes")
    BOXES_ID_COLUMN = os.getenv("BOXES_ID_COLUMN", "box_id")
    BOXES_LOC_COLUMN = os.getenv("BOXES_LOC_COLUMN", "location")

    def _conn_pg():
        return psycopg2.connect(DATABASE_URL, cursor_factory=DictCursor)

    def _table_exists(conn, table_name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
                LIMIT 1
            """, (table_name,))
            return cur.fetchone() is not None

    def _column_exists(conn, table_name: str, column_name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = %s AND column_name = %s
                LIMIT 1
            """, (table_name, column_name))
            return cur.fetchone() is not None

    def init_move_tables():
        """(운영) move_log 보장. boxes 테이블은 존재하면 사용, 없으면 위치갱신은 생략."""
        conn = _conn_pg()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS move_log (
                    id BIGSERIAL PRIMARY KEY,
                    box_id TEXT NOT NULL,
                    from_location TEXT,
                    to_location TEXT NOT NULL,
                    moved_by TEXT,
                    moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    note TEXT
                )
                """)
                # 인덱스
                cur.execute("CREATE INDEX IF NOT EXISTS idx_move_log_box_id ON move_log(box_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_move_log_moved_at ON move_log(moved_at DESC);")
            conn.commit()
        finally:
            conn.close()

    def _get_current_location_pg(conn, boxid: str) -> Optional[str]:
        # boxes 테이블이 있고 id/location 컬럼이 있을 때만 조회
        if not _table_exists(conn, BOXES_TABLE):
            return None
        if not (_column_exists(conn, BOXES_TABLE, BOXES_ID_COLUMN)
                and _column_exists(conn, BOXES_TABLE, BOXES_LOC_COLUMN)):
            return None
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {BOXES_LOC_COLUMN} FROM {BOXES_TABLE} "
                f"WHERE {BOXES_ID_COLUMN}=%s LIMIT 1",
                (boxid,)
            )
            row = cur.fetchone()
            return row[0] if row else None

    def _upsert_location_pg(conn, boxid: str, to_loc: str):
        # boxes 테이블이 있어야만 현재 위치 갱신 시도
        if not _table_exists(conn, BOXES_TABLE):
            return
        if not (_column_exists(conn, BOXES_TABLE, BOXES_ID_COLUMN)
                and _column_exists(conn, BOXES_TABLE, BOXES_LOC_COLUMN)):
            return
        with conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {BOXES_TABLE} ({BOXES_ID_COLUMN}, {BOXES_LOC_COLUMN})
                VALUES (%s, %s)
                ON CONFLICT ({BOXES_ID_COLUMN})
                DO UPDATE SET {BOXES_LOC_COLUMN} = EXCLUDED.{BOXES_LOC_COLUMN}
                """,
                (boxid, to_loc)
            )

    def get_current_location(boxid: str) -> Optional[str]:
        conn = _conn_pg()
        try:
            return _get_current_location_pg(conn, boxid)
        finally:
            conn.close()

    def get_move_history(boxid: str, limit: int = 20) -> List[Dict]:
        conn = _conn_pg()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT from_location, to_location, moved_at, moved_by, note "
                    "FROM move_log WHERE box_id=%s ORDER BY id DESC LIMIT %s",
                    (boxid, limit)
                )
                rows = cur.fetchall()
                return [{"From": r[0], "To": r[1], "At": r[2].isoformat(), "By": r[3], "Reason": r[4]}
                        for r in rows]
        finally:
            conn.close()

    def assign_initial_location(boxid: str, to_loc: str, operator: str, reason: str = "INITIAL"):
        now = _now_str()
        conn = _conn_pg()
        try:
            init_move_tables()
            from_loc = _get_current_location_pg(conn, boxid)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO move_log(box_id, from_location, to_location, moved_by, note) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (boxid, from_loc, to_loc, operator, reason)
                )
            _upsert_location_pg(conn, boxid, to_loc)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def move_location(boxid: str, to_loc: str, operator: str, reason: str = ""):
        now = _now_str()
        conn = _conn_pg()
        try:
            init_move_tables()
            from_loc = _get_current_location_pg(conn, boxid)
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO move_log(box_id, from_location, to_location, moved_by, note) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (boxid, from_loc, to_loc, operator, reason if reason else None)
                )
            _upsert_location_pg(conn, boxid, to_loc)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
