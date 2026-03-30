import os
import sqlite3

from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./dashboard.db")


def get_db_path() -> str:
    """Extract file path from sqlite:/// URL."""
    if DATABASE_URL.startswith("sqlite:///"):
        return DATABASE_URL.replace("sqlite:///", "")
    return "dashboard.db"


def get_connection() -> sqlite3.Connection:
    """Return a new SQLite connection with row factory enabled."""
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    """Initialize database from schema.sql."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    conn = get_connection()
    with open(schema_path) as f:
        conn.executescript(f.read())
    conn.close()
    print("Database initialized successfully.")


if __name__ == "__main__":
    init_db()
