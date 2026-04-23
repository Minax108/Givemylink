import sqlite3
import os
import time
import logging

logger = logging.getLogger(__name__)

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_cache.db")

def init_db():
    """Initialize the SQLite database for MVP Step 1."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            # Table for caching reel links
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS reel_links (
                    shortcode TEXT PRIMARY KEY,
                    extracted_link TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            # Basic user tracking
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    requests_made INTEGER DEFAULT 0,
                    last_request_time REAL
                )
            """)
            conn.commit()
            logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error(f"Database initialization failed: {e}")

def get_cached_link(shortcode: str) -> str | None:
    """Check if we already have the link for this reel."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT extracted_link FROM reel_links WHERE shortcode = ?", (shortcode,))
            result = cursor.fetchone()
            if result:
                return result[0]
    except Exception as e:
        logger.error(f"Error fetching cached link for {shortcode}: {e}")
    return None

def save_cached_link(shortcode: str, extracted_link: str):
    """Save a successfully extracted link to the cache."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO reel_links (shortcode, extracted_link, created_at)
                VALUES (?, ?, ?)
            """, (shortcode, extracted_link, time.time()))
            conn.commit()
            logger.info(f"Saved link to cache for shortcode: {shortcode}")
    except Exception as e:
        logger.error(f"Error saving cached link for {shortcode}: {e}")

def track_user_request(user_id: int, username: str):
    """Track user activity."""
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO users (user_id, username, requests_made, last_request_time)
                VALUES (?, ?, 1, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    requests_made = requests_made + 1,
                    last_request_time = excluded.last_request_time,
                    username = excluded.username
            """, (user_id, username, time.time()))
            conn.commit()
    except Exception as e:
        logger.error(f"Error tracking user {user_id}: {e}")
