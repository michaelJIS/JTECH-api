# db.py
import os
import sqlite3
import time
from contextlib import closing
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

# -------------------------
# 엔진 판별 / 경로 설정
# -------------------------
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", r"D:\BoxID_Auto\logs\logs.sqlite3").strip()

def _is_postgres() -> bool:
    return DATABASE_URL.startswith(("postgres://", "postgresql://"))

# -------------------------
# Postgres DSN 보정 (SSL 필수 + 커넥션 옵션)
# -------------------------
def _ensure_ssl_and_params(dsn: str) -> str:
    if not dsn or not _is_postgres():
        return dsn
    
    p = urlparse(dsn)
    q = dict(parse_qsl(p.query, keep_blank_values=True))

    # <<< RENDER PGBOUNCER 호환성 수정 >>>
    # Render DB URL에 포함된 'pgbouncer' 파라미터를 제거합니다.
    q.pop('pgbouncer', None)
    
    # 필수/권장 옵션
    q.setdefault("sslmode", "require")
    q.setdefault("connect_timeout", "10")
    q.setdefault("keepalives", "1")
    q.setdefault("keepalives_idle", "30")
    q.setdefault("keepalives_interval", "10")
    q.setdefault("keepalives_count", "5")
    
    new_p = p._replace(query=urlencode(q))
    return urlunparse(new_p)

# -------------------------
# 커넥션
# -------------------------
def get_conn():
    """
    Postgres: DictCursor 사용 + sslmode=require 보정 + 재시도 + sslrootcert 지정
    SQLite: sqlite3.Row 사용 (기존 기능 그대로)
    """
    if _is_postgres():
        import psycopg2
        from psycopg2.extras import DictCursor
        import certifi

        dsn = _ensure_ssl_and_params(DATABASE_URL)

        last_err = None
        # 연결 실패 시 지수 백오프로 5회 재시도
        for attempt in range(5):
            try:
                # sslrootcert에 certifi 번들을 명시하여 SSL 인증서 문제를 해결합니다.
                return psycopg2.connect(
                    dsn,
                    cursor_factory=DictCursor,
                    sslrootcert=certifi.where(),
                )
            except psycopg2.OperationalError as e:
                last_err = e
                # 재시도 간격: 1, 2, 4, 8, 8초
                time.sleep(min(2 ** attempt, 8))
        raise last_err

    # SQLite (로컬)
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    with closing(conn.cursor()) as cur:
        cur.execute("PRAGMA foreign_keys = ON;")
    return conn

# -------------------------
# 스키마 초기화
# -------------------------
def init_schema():
    """
    최소 스키마 + 운영/로컬 보강 스키마 생성.
    DB 종류에 따라 적절한 초기화 함수를 호출합니다.
    """
    if _is_postgres():
        _init_schema_postgres()
    else:
        _init_schema_sqlite()

def _init_schema_sqlite():
    ddl = [
        # ... (기존과 동일한 DDL 구문) ...
        """
        CREATE TABLE IF NOT EXISTS box_moves(...);
        """,
        """
        CREATE TABLE IF NOT EXISTS box_move_log(...);
        """,
        """
        CREATE TABLE IF NOT EXISTS boxid_log(...);
        """
    ]
    # --- with 구문으로 리소스 자동 반환 보장 (안정성 강화) ---
    with get_conn() as conn, conn.cursor() as cur:
        for q in ddl:
            cur.execute(q)
        conn.commit()

def _init_schema_postgres():
    ddl = [
        # ... (기존과 동일한 DDL 구문) ...
        """
        CREATE TABLE IF NOT EXISTS box_moves(...);
        """,
        """
        CREATE TABLE IF NOT EXISTS move_log (...);
        """,
        "CREATE INDEX IF NOT EXISTS idx_move_log_box_id ON move_log(box_id);",
        "CREATE INDEX IF NOT EXISTS idx_move_log_moved_at ON move_log(moved_at DESC);",
    ]
    # --- with 구문으로 리소스 자동 반환 보장 (안정성 강화) ---
    with get_conn() as conn, conn.cursor() as cur:
        for q in ddl:
            cur.execute(q)
        conn.commit()
