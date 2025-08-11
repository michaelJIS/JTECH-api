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
        # 기존 유지 테이블: box_moves (SQLite 문법)
        """
        CREATE TABLE IF NOT EXISTS box_moves(
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            BoxID TEXT NOT NULL,
            Location TEXT NOT NULL,
            Operator TEXT,
            Warehouse TEXT,
            CreatedAt TEXT DEFAULT (datetime('now','localtime'))
        );
        """,
        # 기존 로컬 이력 테이블 (location_utils의 SQLite 분기에서 사용)
        """
        CREATE TABLE IF NOT EXISTS box_move_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            BoxID TEXT NOT NULL,
            FromLoc TEXT,
            ToLoc TEXT NOT NULL,
            MovedAt TEXT NOT NULL,
            Operator TEXT,
            Reason TEXT
        );
        """,
        # location_utils가 참조하는 현재 위치 테이블(없으면 최소 스키마)
        """
        CREATE TABLE IF NOT EXISTS boxid_log(
            BoxID TEXT PRIMARY KEY,
            ItemCode TEXT,
            Qty INTEGER,
            Status TEXT,
            Location TEXT,
            CreatedAt TEXT,
            UpdatedAt TEXT
        );
        """
    ]
    # with 구문으로 리소스 자동 반환 보장 (안정성 강화)
    with get_conn() as conn, conn.cursor() as cur:
        for q in ddl:
            cur.execute(q)
        conn.commit()

def _init_schema_postgres():
    # Postgres 문법 (SERIAL, TIMESTAMPTZ, now())
    ddl = [
        # boxid_log 테이블 (API가 직접 사용하는 주 테이블)
        """
        CREATE TABLE IF NOT EXISTS boxid_log (
            BoxID TEXT PRIMARY KEY,
            ItemCode TEXT,
            Qty INTEGER,
            Status TEXT,
            Location TEXT,
            CreatedAt TIMESTAMPTZ DEFAULT NOW(),
            UpdatedAt TIMESTAMPTZ
        );
        """,
        # 운영 이력 테이블: move_log (hybrid location_utils의 PG 분기에서 사용)
        """
        CREATE TABLE IF NOT EXISTS move_log (
            id BIGSERIAL PRIMARY KEY,
            box_id TEXT NOT NULL,
            from_location TEXT,
            to_location TEXT NOT NULL,
            moved_by TEXT,
            moved_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            note TEXT
        );
        """,
        # 성능 인덱스
        "CREATE INDEX IF NOT EXISTS idx_move_log_box_id ON move_log(box_id);",
        "CREATE INDEX IF NOT EXISTS idx_move_log_moved_at ON move_log(moved_at DESC);",
        "CREATE INDEX IF NOT EXISTS idx_boxid_log_location ON boxid_log(Location text_pattern_ops);"
    ]
    # with 구문으로 리소스 자동 반환 보장 (안정성 강화)
    with get_conn() as conn, conn.cursor() as cur:
        for q in ddl:
            cur.execute(q)
        conn.commit()
