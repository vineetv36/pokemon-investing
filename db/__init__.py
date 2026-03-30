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
    """Initialize database from schema.sql, with migration for existing DBs."""
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    conn = get_connection()

    # Check if cards table already exists without the unique constraint
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='cards'")
    if cursor.fetchone():
        # Check if UNIQUE(name, set_name, card_number) index exists
        cursor.execute("PRAGMA index_list(cards)")
        indexes = cursor.fetchall()
        # Check if any unique index covers (name, set_name, card_number)
        has_name_unique = False
        for idx in indexes:
            cursor.execute(f"PRAGMA index_info({idx['name']})")
            cols = [row["name"] for row in cursor.fetchall()]
            if "name" in cols and "set_name" in cols and "card_number" in cols:
                has_name_unique = True
                break
        if not has_name_unique:
            # Migrate: recreate cards table with unique constraint
            print("Migrating cards table to add unique constraint...")
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS cards_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    set_name TEXT,
                    card_number TEXT,
                    tcgplayer_id TEXT,
                    image_url TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(name, set_name, card_number)
                );
                INSERT OR IGNORE INTO cards_new (id, name, set_name, card_number, tcgplayer_id, image_url, is_active, created_at)
                    SELECT id, name, set_name, card_number, tcgplayer_id, image_url, is_active, created_at FROM cards;
                DROP TABLE cards;
                ALTER TABLE cards_new RENAME TO cards;
            """)
            print("Migration complete — duplicates removed.")

    with open(schema_path) as f:
        conn.executescript(f.read())
    conn.close()
    print("Database initialized successfully.")


if __name__ == "__main__":
    init_db()
