# db.py
import os, sqlite3
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

def get_conn():
    if DATABASE_URL.startswith("postgres"):
        import psycopg2
        return psycopg2.connect(DATABASE_URL)
    # 로컬 테스트용 (PC에서 실행 시)
    return sqlite3.connect(r"D:\BoxID_Auto\logs\logs.sqlite3", check_same_thread=False)

def init_schema():
    # 최소 스키마(네가 쓰는 테이블 이름 맞춰 조정 가능)
    ddl = [
        """CREATE TABLE IF NOT EXISTS box_moves(
            Id SERIAL PRIMARY KEY,
            BoxID TEXT NOT NULL,
            Location TEXT NOT NULL,
            Operator TEXT,
            Warehouse TEXT,
            CreatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );"""
    ]
    conn = get_conn()
    cur = conn.cursor()
    for q in ddl:
        cur.execute(q)
    conn.commit()
    cur.close()
    conn.close()
