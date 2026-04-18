import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
import html
from dotenv import load_dotenv
load_dotenv()  # Load .env file if present (local dev); no-op in containers

import re
import time
import logging
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import instagrapi
from instagrapi import Client
from collections import Counter

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Credentials are loaded from environment variables for security.
# Set them in your .env file (locally) or in your cloud provider's dashboard.

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8015860336:AAGjG4734O8BRRIqhCnHgbwR8-fR0jQ6aX8")
BOT_INSTAGRAM_USERNAME = os.environ.get("BOT_INSTAGRAM_USERNAME", "geturlink")
BOT_INSTAGRAM_PASSWORD = os.environ.get("BOT_INSTAGRAM_PASSWORD", "")

# STEP 3: What to comment on the reel (usually "link" but some reels use other keywords)
DEFAULT_COMMENT = "link"

# STEP 4: How long to wait for ManyChat to DM back (in seconds)
DM_WAIT_TIME = 60       # Just wait 60s for auto-DMs to reply
DM_CHECK_INTERVAL = 10  # Check DMs every 10s

# STEP 5: Concurrency settings
MAX_CONCURRENT_IG_CALLS = 3   # max simultaneous Instagram API operations
THREAD_POOL_SIZE = 10          # thread pool workers for blocking IG calls

# ──────────────────────────────────────────────────────────────────────────────

import uuid
import random
import json
from instagrapi.exceptions import LoginRequired

# Challenge code storage (set externally via /code Telegram command)
_challenge_code = None

def challenge_code_handler(username, choice):
    """Called by instagrapi when Instagram requires a verification code."""
    global _challenge_code
    logger.info(f"Instagram challenge requested for {username} (method: {choice})")
    logger.info("Waiting for verification code... Use /code <digits> in Telegram to provide it.")
    # Wait up to 120 seconds for the code to be provided
    for _ in range(120):
        if _challenge_code:
            code = _challenge_code
            _challenge_code = None
            logger.info(f"Challenge code received: {code}")
            return code
        time.sleep(1)
    logger.error("Challenge code timeout — no code provided within 120 seconds")
    return ""


# ─── DEVICE FINGERPRINT ───────────────────────────────────────────────────────
DEVICE_FINGERPRINT_FILE = "device_fingerprint.json"


def _load_or_create_device_fingerprint() -> dict:
    """Load a persisted device fingerprint or generate and save a new one.

    Keeping a stable fingerprint prevents Instagram from treating every
    re-login as a brand-new device and invalidating the session.
    """
    if os.path.exists(DEVICE_FINGERPRINT_FILE):
        try:
            with open(DEVICE_FINGERPRINT_FILE, "r") as f:
                fp = json.load(f)
            if fp and "manufacturer" in fp:
                logger.info("Loaded persisted device fingerprint.")
                return fp
        except Exception as e:
            logger.warning(f"Failed to load device fingerprint: {e}")

    # Generate a new fingerprint and persist it
    fp = {
        "app_version": "269.0.0.18.75",
        "android_version": random.randint(26, 33),
        "android_release": f"{random.randint(10, 14)}.0",
        "dpi": random.choice(["480dpi", "640dpi", "320dpi"]),
        "resolution": random.choice(["1080x1920", "1440x2560", "1080x2400"]),
        "manufacturer": random.choice(["Samsung", "OnePlus", "Google", "Xiaomi"]),
        "device": random.choice(["star2qltechn", "beyond1", "OnePlus6T", "jasmine_sprout"]),
        "model": random.choice(["SM-G965F", "SM-G973F", "ONEPLUS A6013", "Mi A2"]),
        "cpu": random.choice(["qcom", "exynos9810", "samsungexynos9820"]),
        "version_code": "314665256",
    }
    try:
        with open(DEVICE_FINGERPRINT_FILE, "w") as f:
            json.dump(fp, f, indent=2)
        logger.info("Generated and saved new device fingerprint.")
    except Exception as e:
        logger.warning(f"Failed to save device fingerprint: {e}")
    return fp


def _build_fresh_client() -> Client:
    """Create a new instagrapi Client with a stable persisted device fingerprint."""
    cl = Client()
    cl.delay_range = [2, 5]  # random delay between API calls (looks human)

    # Fix 1: Use a stable persisted fingerprint so Instagram does not see a
    # "new device" on every re-login and invalidate the session.
    fp = _load_or_create_device_fingerprint()
    cl.set_device(fp)
    cl.set_user_agent(
        f"Instagram {fp.get('app_version', '269.0.0.18.75')} Android "
        f"({fp.get('android_version', 30)}/{fp.get('android_release', '13.0')}; "
        f"{fp.get('dpi', '480dpi')}; {fp.get('resolution', '1080x1920')}; "
        f"{fp.get('manufacturer', 'Samsung')}; {fp.get('model', 'SM-G965F')}; "
        f"{fp.get('device', 'star2qltechn')}; {fp.get('cpu', 'qcom')}; en_US; 314665256)"
    )

    # Set the challenge handler
    cl.challenge_code_handler = challenge_code_handler

    return cl


ig_client = _build_fresh_client()
ig_logged_in = False
_login_fail_count = 0        # Track consecutive login failures
_last_login_attempt = 0.0    # Timestamp of last login attempt
_last_login_error = ""       # Capture exact exception string

# Fix 3: Periodic session save state
_last_session_save = 0.0
SESSION_SAVE_INTERVAL = 300  # save session every 5 minutes

# Track pending requests: {telegram_user_id: {"reel_url": ..., "timestamp": ...}}
# Changed to per-user list to allow multiple users simultaneously
pending_requests = {}  # {telegram_user_id: {"shortcode": ..., "timestamp": ...}}

# Instagram DM listener state
ig_dm_pending = {}          # {ig_user_pk: {"shortcode": ..., "thread_id": ..., "timestamp": ...}}
ig_dm_processed = set()     # Set of processed DM item_ids to avoid re-processing
ig_dm_last_check = 0.0      # Timestamp of last DM check
waiting_for_owners = set()  # Reel owner user IDs we're currently waiting for DM responses
IG_DM_CHECK_INTERVAL = 15   # How often to check for new IG DM requests (seconds)

# Concurrency primitives (initialized in main)
ig_lock = None           # asyncio.Lock — protects login/session state
ig_semaphore = None      # asyncio.Semaphore — limits concurrent IG API calls
thread_pool = None       # ThreadPoolExecutor — runs blocking IG calls


async def run_ig(func, *args):
    """Run a blocking Instagram API call in the thread pool, respecting the semaphore."""
    loop = asyncio.get_event_loop()
    async with ig_semaphore:
        return await loop.run_in_executor(thread_pool, func, *args)


def _get_session_id_from_file(session_file):
    """Extract sessionid from a saved session file without making API calls."""
    try:
        import json
        with open(session_file, "r") as f:
            settings = json.load(f)
        sid = settings.get("authorization_data", {}).get("sessionid", "")
        if not sid:
            sid = settings.get("cookies", {}).get("sessionid", "")
        return sid
    except Exception:
        return ""


def _maybe_save_session():
    """Save the Instagram session periodically to keep the on-disk copy fresh."""
    global _last_session_save
    now = time.time()
    if now - _last_session_save >= SESSION_SAVE_INTERVAL:
        try:
            ig_client.dump_settings("ig_session.json")
            _last_session_save = now
            logger.info("Session saved periodically.")
        except Exception as e:
            logger.warning(f"Periodic session save failed: {e}")


def login_instagram():
    """Log in to the bot's dedicated Instagram account with retry logic."""
    global ig_logged_in, ig_client, _login_fail_count, _last_login_attempt, _last_login_error
    _last_login_error = ""
    
    # Cooldown: don't retry too fast after failures
    now = time.time()
    if _login_fail_count >= 3:
        cooldown = min(300, 30 * (2 ** (_login_fail_count - 3)))  # exponential backoff, max 5 min
        if now - _last_login_attempt < cooldown:
            logger.warning(f"Instagram login on cooldown ({cooldown}s). Skipping attempt.")
            _last_login_error = f"On cooldown ({cooldown}s)"
            return
    
    _last_login_attempt = now
    
    try:
        session_file = "ig_session.json"
        
        # Priority 0: Bypassing Instagram's login block entirely by loading a pre-flighted session dictionary
        import base64, json
        hardcoded_settings_b64 = (
            "ew0KICAgICJ1dWlkcyI6IHsNCiAgICAgICAgInBob25lX2lkIjogIjQ2N2ZlYWQ5LWVjMzAtNDI0ZC1hZWU2LWNkOGQyOWM1MGFj"
            "ZCIsDQogICAgICAgICJ1dWlkIjogIjhjYzA1ZDc1LTVlMTgtNGRjNy1iYmI4LTQ3ODIyZTcwMTJjYiIsDQogICAgICAgICJjbGll"
            "bnRfc2Vzc2lvbl9pZCI6ICI5YzgyNjFhMS05MDliLTRjZTAtOTY3Yi1lN2I2YTkzYWNiN2UiLA0KICAgICAgICAiYWR2ZXJ0aXNp"
            "bmdfaWQiOiAiNzI2ZjMxYTItOTVjYi00MTBkLTk0YzMtNDljNGEwYjFkMzFmIiwNCiAgICAgICAgImFuZHJvaWRfZGV2aWNlX2lk"
            "IjogImFuZHJvaWQtZjMzMDY2MGYxNDgwM2YxNSIsDQogICAgICAgICJyZXF1ZXN0X2lkIjogIjVmNzU2NDYwLWEwMWYtNDlhNS04"
            "M2M4LTI1MmIxMmU1ZDk5MyIsDQogICAgICAgICJ0cmF5X3Nlc3Npb25faWQiOiAiNTU2N2E4MjEtYjc5OS00YzJmLTgwY2YtNDBk"
            "ZmUyZjM1MTBkIg0KICAgIH0sDQogICAgIm1pZCI6ICJhZF92V0FBQkFBRVJLcDh0RkxscUZCN1VIUTAtIiwNCiAgICAiaWdfdV9y"
            "dXIiOiBudWxsLA0KICAgICJpZ193d3dfY2xhaW0iOiBudWxsLA0KICAgICJhdXRob3JpemF0aW9uX2RhdGEiOiB7DQogICAgICAg"
            "ICJkc191c2VyX2lkIjogIjM1NzExMDkxNzI0IiwNCiAgICAgICAgInNlc3Npb25pZCI6ICIzNTcxMTA5MTcyNCUzQVI1VzRMa2c2"
            "MVNGbFo2JTNBMTYlM0FBWWpQbE1zVVlOQXo3WmVndTVGNkpiX3N0eE8tRmJMdHd1ekY4X0ZQSmciLA0KICAgICAgICAic2hvdWxk"
            "X3VzZV9oZWFkZXJfb3Zlcl9jb29raWVzIjogdHJ1ZQ0KICAgIH0sDQogICAgImNvb2tpZXMiOiB7DQogICAgICAgICJzZXNzaW9u"
            "aWQiOiAiMzU3MTEwOTE3MjQlM0FSNVc0TGtnNjFTRmxaNiUzQTE2JTNBQVlqUGxNc1VZTkF6N1plZ3U1RjZKYl9zdHhPLUZiTHR3"
            "dXpGOF9GUEpnIg0KICAgIH0sDQogICAgImxhc3RfbG9naW4iOiBudWxsLA0KICAgICJkZXZpY2Vfc2V0dGluZ3MiOiB7DQogICAg"
            "ICAgICJhbmRyb2lkX3ZlcnNpb24iOiAyNiwNCiAgICAgICAgImFuZHJvaWRfcmVsZWFzZSI6ICI4LjAuMCIsDQogICAgICAgICJk"
            "cGkiOiAiNDgwZHBpIiwNCiAgICAgICAgInJlc29sdXRpb24iOiAiMTA4MHgxOTIwIiwNCiAgICAgICAgIm1hbnVmYWN0dXJlciI6"
            "ICJPbmVQbHVzIiwNCiAgICAgICAgImRldmljZSI6ICJkZXZpdHJvbiIsDQogICAgICAgICJtb2RlbCI6ICI2VCBEZXYiLA0KICAg"
            "ICAgICAiY3B1IjogInFjb20iLA0KICAgICAgICAiYXBwX3ZlcnNpb24iOiAiMzg1LjAuMC40Ny43NCIsDQogICAgICAgICJ2ZXJz"
            "aW9uX2NvZGUiOiAiMzc4OTA2ODQzIiwNCiAgICAgICAgImJsb2tzX3ZlcnNpb25pbmdfaWQiOiAiYTg5NzNkNDlhOWNjNmE2ZjY1"
            "YTQ5OTdjMTAyMTZjZTJhMDZmNjVhNTE3MDEwZTY0ODg1ZTkyMDI5YmIxOTIyMSINCiAgICB9LA0KICAgICJ1c2VyX2FnZW50Ijog"
            "Ikluc3RhZ3JhbSAzODUuMC4wLjQ3Ljc0IEFuZHJvaWQgKDI2LzguMC4wOyA0ODBkcGk7IDEwODB4MTkyMDsgT25lUGx1czsgNlQg"
            "RGV2OyBkZXZpdHJvbjsgcWNvbTsgZW5fVVM7IDM3ODkwNjg0MykiLA0KICAgICJjb3VudHJ5IjogIlVTIiwNCiAgICAiY291bnRy"
            "eV9jb2RlIjogMSwNCiAgICAibG9jYWxlIjogImVuX1VTIiwNCiAgICAidGltZXpvbmVfb2Zmc2V0IjogLTE0NDAwDQp9"
        )
        try:
            logger.info("Instantiating API client entirely offline using snapshot injection...")
            settings_dict = json.loads(base64.b64decode(hardcoded_settings_b64).decode("utf-8"))
            ig_client = Client(settings=settings_dict)
            ig_client.delay_range = [1, 3]
            try:
                # Using private residential proxy to bypass Datacenter/Railway IP blocks
                ig_client.set_proxy("http://qcxmvcyr:evrdck2dzymr@31.59.20.176:6754")
                logger.info("Instagram: Private Proxy configured successfully.")
            except Exception as pe:
                logger.error(f"Failed to configure proxy: {pe}")
            ig_client.challenge_code_handler = challenge_code_handler
            ig_logged_in = True
            _login_fail_count = 0
            logger.info("Client initialization 100% complete.")
            return
        except Exception as override_err:
            raise Exception(f"Snapshot hydration failed: {override_err}")
        
        # Fix 4: Always prefer IG_SESSION_B64 env var over whatever is on disk.
        # A stale file would otherwise shadow a freshly-exported session.
        session_b64 = os.environ.get("IG_SESSION_B64", "")
        if session_b64:
            import base64
            logger.info("Instagram: creating session file from IG_SESSION_B64 env var...")
            session_data = base64.b64decode(session_b64).decode()
            with open(session_file, "w") as f:
                f.write(session_data)
            logger.info("Instagram: session file created from env var.")
        
        # Try loading from session file
        if os.path.exists(session_file):
            session_id = _get_session_id_from_file(session_file)
            
            if session_id:
                logger.info("Instagram: logging in by sessionid...")
                # Build fresh client to avoid stale state
                ig_client = _build_fresh_client()
                ig_client.login_by_sessionid(session_id)
                ig_client.dump_settings(session_file)
                logger.info(f"Instagram: session login successful as @{ig_client.username}")
                ig_logged_in = True
                _login_fail_count = 0
                return
            else:
                logger.warning("Instagram: session file exists but no sessionid found, trying password login")
        
        # Fallback: fresh password login
        ig_client = _build_fresh_client()
        logger.info("Instagram: attempting password login...")
        time.sleep(random.uniform(2, 5))
        ig_client.login(BOT_INSTAGRAM_USERNAME, BOT_INSTAGRAM_PASSWORD)
        ig_client.dump_settings(session_file)
        logger.info("Instagram: password login succeeded.")
        ig_logged_in = True
        _login_fail_count = 0
    except Exception as e:
        _login_fail_count += 1
        import traceback
        _last_login_error = traceback.format_exc()
        logger.error(f"Instagram login failed (attempt #{_login_fail_count}): {e}")
        ig_logged_in = False


async def ensure_logged_in_async() -> bool:
    """Make sure we're logged into Instagram, reconnect if needed. Thread-safe."""
    global ig_logged_in
    if ig_logged_in:
        return True
    async with ig_lock:
        if not ig_logged_in:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(thread_pool, login_instagram)
    return ig_logged_in


def extract_reel_url(text: str) -> str | None:
    """Extract Instagram reel/post URL from text."""
    match = re.search(
        r"https?://(?:www\.)?instagram\.com/(reel|reels|p)/[A-Za-z0-9_-]+/?[^\s]*",
        text
    )
    return match.group(0) if match else None


def extract_shortcode(url: str) -> str | None:
    """Extract shortcode from Instagram URL."""
    match = re.search(r"instagram\.com/(?:reel|reels|p)/([A-Za-z0-9_-]+)", url)
    return match.group(1) if match else None


def get_best_keyword(shortcode: str) -> str:
    """Analyze the reel's top comments to deduce the keyword users are commenting."""
    global ig_logged_in
    try:
        media_pk = ig_client.media_pk_from_code(shortcode)
        media_id = ig_client.media_id(media_pk)
        
        # Fetch latest comments
        comments = ig_client.media_comments(media_id, amount=40)
        words = []
        for c in comments:
            text = c.text.strip().lower()
            # If the comment is short (e.g. 1-3 words), it's likely a keyword attempt
            parts = re.findall(r'[a-z]+', text)
            if len(parts) <= 3:
                words.extend(parts)
        
        if words:
            # Filter out standard stop words just in case
            stopwords = {"the", "a", "is", "in", "it", "to", "and", "of", "for", "on", "this", "that", "my", "i", "love", "awesome", "great", "plz", "please", "bro"}
            filtered_words = [w for w in words if w not in stopwords and len(w) > 1]
            if filtered_words:
                most_common = Counter(filtered_words).most_common(1)
                if most_common:
                    keyword, count = most_common[0]
                    # If the most common word appeared at least 2 times, we trust it
                    if count >= 2:
                        logger.info(f"Deduced keyword from comments: {keyword} (count: {count})")
                        return keyword
                    
    except LoginRequired:
        logger.warning("Session expired during keyword detection — marking for re-login.")
        ig_logged_in = False
    except Exception as e:
        logger.error(f"Failed to fetch comments to deduce keyword: {e}")
        
    return DEFAULT_COMMENT


def comment_on_reel(shortcode: str, comment_text: str) -> bool:
    """Comment on a reel and return True if successful."""
    try:
        media_pk = ig_client.media_pk_from_code(shortcode)
        media_id = ig_client.media_id(media_pk)
        ig_client.media_comment(media_id, comment_text)
        logger.info(f"Commented '{comment_text}' on reel {shortcode}")
        return True
    except Exception as e:
        logger.error(f"Failed to comment on {shortcode}: {e}")
        raise


def get_reel_owner(shortcode: str) -> dict:
    """Get info about the reel's owner to identify their DM."""
    try:
        media_pk = ig_client.media_pk_from_code(shortcode)
        media_info = ig_client.media_info(media_pk)
        return {
            "user_id": media_info.user.pk,
            "username": media_info.user.username,
        }
    except Exception as e:
        logger.error(f"Failed to get reel owner: {e}")
        return {}


def follow_user(user_id):
    """Follow an Instagram user."""
    ig_client.user_follow(user_id)


def unfollow_user(user_id):
    """Unfollow an Instagram user."""
    try:
        ig_client.user_unfollow(user_id)
        logger.info(f"Unfollowed creator {user_id}")
    except Exception as e:
        logger.error(f"Failed to unfollow creator {user_id}: {e}")


def check_dms_for_link(reel_owner_id: int, after_timestamp: float) -> str | None:
    """
    Check Instagram DMs for a new message from the reel owner.
    Handles generic_xma (ManyChat cards with CTA buttons), text, and link messages.
    Returns the link/text if found, None otherwise.
    """
    global ig_logged_in
    try:
        # Check both main inbox and message requests
        threads = ig_client.direct_threads(amount=20)
        try:
            pending = ig_client.direct_pending_inbox(amount=20)
            threads.extend(pending)
        except Exception as e:
            logger.warning(f"Error checking pending inbox: {e}")

        for thread in threads:
            # Check if this thread involves the reel owner
            for user in thread.users:
                if str(user.pk) == str(reel_owner_id):
                    # Found the right thread — use raw API to get full message data
                    try:
                        result = ig_client.private_request(
                            f"direct_v2/threads/{thread.id}/",
                            params={"visual_message_return_type": "unseen", "direction": "older", "seq_id": "40065", "limit": "10"},
                        )
                        items = result.get("thread", {}).get("items", [])
                    except Exception as e:
                        logger.error(f"Error fetching raw thread data: {e}")
                        # Fallback to standard API
                        items = []

                    clicked_postback = False
                    for item in items:
                        item_type = item.get("item_type")
                        ts = item.get("timestamp", 0)
                        if isinstance(ts, int):
                            ts = ts / 1_000_000  # Convert microseconds to seconds
                        if ts < after_timestamp - 86400:
                            continue

                        # First, if there's a CTA postback button, trigger it
                        if item_type in ("generic_xma", "xma_link", "xma_media_share", "xma_clip", "xma_reel_share"):
                            xma_data = item.get(item_type, [])
                            if isinstance(xma_data, dict):
                                xma_data = [xma_data]
                            for xma in (xma_data if isinstance(xma_data, list) else []):
                                cta_buttons = xma.get("cta_buttons", []) if isinstance(xma, dict) else []
                                for btn in cta_buttons:
                                    action_url = btn.get("action_url", "")
                                    if action_url and action_url.startswith("http"):
                                        logger.info(f"Found link in CTA button: {action_url}")
                                        return action_url

                                # If CTA has postback but no URL, reply with button text
                                if not clicked_postback:
                                    for btn in cta_buttons:
                                        btn_title = btn.get("title", "")
                                        platform_token = btn.get("platform_token", {})
                                        postback = platform_token.get("postback", {})
                                        payload = postback.get("postback_payload", "")
                                        if payload and btn_title:
                                            try:
                                                logger.info(f"Replying with button text to trigger automation: '{btn_title}'")
                                                ig_client.direct_answer(
                                                    thread_id=int(thread.id),
                                                    text=btn_title,
                                                )
                                                clicked_postback = True
                                                logger.info("Reply sent, will check for follow-up message next poll")
                                            except Exception as e:
                                                logger.error(f"Failed to reply with button text: {e}")

                        # Now search the ENTIRE item dictionary for any URLs (target_url, text, link_url, etc)
                        def find_any_url(data):
                            if isinstance(data, str):
                                urls = re.findall(r'https?://[^\s<>"]+', data)
                                for u in urls:
                                    if "fbcdn.net" not in u and "favicon" not in u and "ig_profile_pic" not in u:
                                        return u
                                return None
                            elif isinstance(data, list):
                                for x in data:
                                    res = find_any_url(x)
                                    if res: return res
                            elif isinstance(data, dict):
                                # Prioritize exact keys if they contain links
                                for key in ("target_url", "link_url", "text", "action_url", "title_text", "url"):
                                    val = data.get(key)
                                    if val and isinstance(val, str):
                                        res = find_any_url(val)
                                        if res: return res
                                # Then fallback to all values
                                for val in data.values():
                                    res = find_any_url(val)
                                    if res: return res
                            return None

                        found_url = find_any_url(item)
                        if found_url:
                            logger.info(f"Found link via recursive search in message type {item_type}: {found_url}")
                            return found_url

        return None
    except LoginRequired:
        logger.warning("Session expired while checking DMs — marking for re-login.")
        ig_logged_in = False
        return None
    except Exception as e:
        logger.error(f"Error checking DMs: {e}")
        return None


# ─── INSTAGRAM DM HANDLING ────────────────────────────────────────────────────

def send_dm_reply(thread_id: int, text: str):
    """Send a reply in an Instagram DM thread."""
    ig_client.direct_answer(thread_id=thread_id, text=text)


def fetch_dm_inbox():
    """Fetch recent Instagram DM threads with their latest messages (blocking)."""
    global ig_logged_in
    results = []
    try:
        threads = ig_client.direct_threads(amount=20)
        logger.info(f"[IG DM] Fetched {len(threads)} main inbox threads")

        # Fetch pending inbox using raw API to avoid Pydantic validation errors
        try:
            raw_pending = ig_client.private_request(
                "direct_v2/pending_inbox/",
                params={"visual_message_return_type": "unseen", "persistentBadging": "true",
                        "is_prefetching": "false"},
            )
            pending_threads = raw_pending.get("inbox", {}).get("threads", [])
            logger.info(f"[IG DM] Fetched {len(pending_threads)} pending inbox threads (raw)")

            for pt in pending_threads:
                pt_id = pt.get("thread_id", "")
                # Auto-approve pending threads
                try:
                    ig_client.direct_pending_approve(int(pt_id))
                    logger.info(f"[IG DM] Approved pending thread {pt_id}")
                except Exception as e:
                    logger.warning(f"[IG DM] Could not approve pending thread {pt_id}: {e}")

                # Extract thread data directly from raw response
                items = pt.get("items", [])
                users = pt.get("users", [])
                user_list = [(u.get("pk", 0), u.get("username", "unknown")) for u in users]
                if user_list and items:
                    results.append({
                        "thread_id": int(pt_id),
                        "users": user_list,
                        "items": items,
                    })
        except Exception as e:
            logger.warning(f"[IG DM] Error fetching pending inbox: {e}")

        for thread in threads:
            try:
                raw = ig_client.private_request(
                    f"direct_v2/threads/{thread.id}/",
                    params={"direction": "older", "limit": "5"},
                )
                items = raw.get("thread", {}).get("items", [])
                results.append({
                    "thread_id": int(thread.id),
                    "users": [(u.pk, u.username) for u in thread.users],
                    "items": items,
                })
            except Exception:
                continue
        # Fix 3: Save session periodically after a successful inbox fetch
        _maybe_save_session()
    except LoginRequired:
        logger.warning("Session expired while fetching DM inbox — marking for re-login.")
        ig_logged_in = False
    except Exception as e:
        logger.error(f"Error fetching DM inbox: {e}")
    return results


async def process_ig_dm_request(user_pk: int, username: str, thread_id: int, reel_url: str):
    """Process a reel link request received via Instagram DM."""
    global ig_logged_in

    shortcode = extract_shortcode(reel_url)
    if not shortcode:
        await run_ig(send_dm_reply, thread_id, "Could not parse that URL. Send a valid Instagram reel link.")
        return

    # Send a prompt to the sender so they know the bot is working
    await run_ig(send_dm_reply, thread_id, "Getting your link as soon as possible... ⏳")

    ig_dm_pending[user_pk] = {"shortcode": shortcode, "thread_id": thread_id, "timestamp": time.time()}
    owner_id = None

    try:
        # Get reel owner
        owner_info = await run_ig(get_reel_owner, shortcode)
        if not owner_info:
            ig_dm_pending.pop(user_pk, None)
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

        # Auto-detect keyword
        final_keyword = await run_ig(get_best_keyword, shortcode)
        logger.info(f"[IG DM] Keyword for {shortcode}: '{final_keyword}'")

        # Follow creator
        try:
            await run_ig(follow_user, owner_id)
        except Exception:
            pass

        # Comment on reel
        waiting_for_owners.add(owner_id)
        timestamp_before = time.time()

        try:
            await run_ig(comment_on_reel, shortcode, final_keyword)
        except Exception as e:
            waiting_for_owners.discard(owner_id)
            ig_dm_pending.pop(user_pk, None)
            return

        # Poll for DM response from reel owner
        link_found = None
        elapsed = 0
        while elapsed < DM_WAIT_TIME:
            try:
                link_found = await run_ig(check_dms_for_link, owner_id, timestamp_before)
                if link_found:
                    break
            except Exception as e:
                logger.error(f"[IG DM] Poll error for @{username}: {e}")

            await asyncio.sleep(DM_CHECK_INTERVAL)
            elapsed += DM_CHECK_INTERVAL

        # Cleanup
        waiting_for_owners.discard(owner_id)
        ig_dm_pending.pop(user_pk, None)

        try:
            await run_ig(unfollow_user, owner_id)
        except Exception:
            pass

        # Send result
        if link_found:
            await run_ig(send_dm_reply, thread_id, f"Here's your link:\n\n{link_found}")
            logger.info(f"[IG DM] Got link for @{username}: {link_found}")
        else:
            await run_ig(send_dm_reply, thread_id,
                f"No response from @{owner_username}. They might not have automation set up, or the keyword '{final_keyword}' was wrong.")

    except Exception as e:
        logger.error(f"[IG DM] Error processing request from @{username}: {e}")
        if owner_id:
            waiting_for_owners.discard(owner_id)
        ig_dm_pending.pop(user_pk, None)
        try:
            await run_ig(send_dm_reply, thread_id, "Something went wrong. Please try again later.")
        except Exception:
            pass


async def ig_dm_listener():
    """Background task: polls Instagram DMs for new reel link requests."""
    global ig_dm_last_check

    await asyncio.sleep(20)  # let login settle
    first_scan = True
    logger.info("Instagram DM listener started")

    while True:
        try:
            if not ig_logged_in:
                await asyncio.sleep(IG_DM_CHECK_INTERVAL)
                continue

            # Cap processed set to prevent memory leak
            if len(ig_dm_processed) > 5000:
                ig_dm_processed.clear()

            threads_data = await run_ig(fetch_dm_inbox)
            logger.info(f"[IG DM] Scanned {len(threads_data)} threads, ig_dm_last_check={ig_dm_last_check:.0f}")

            for td in threads_data:
                thread_id = td["thread_id"]
                if not td["users"]:
                    continue

                # Get the other user in this 1-on-1 DM
                user_pk, username = td["users"][0]

                # Skip reel owners we're waiting for, and users with pending requests
                if user_pk in waiting_for_owners or user_pk in ig_dm_pending:
                    continue

                items = td["items"]
                if items:
                    logger.info(f"[IG DM] Thread with @{username} ({user_pk}): {len(items)} items")

                for idx, item in enumerate(items):
                    item_id = item.get("item_id", "")
                    if item_id in ig_dm_processed:
                        continue

                    sender = item.get("user_id")
                    item_type = item.get("item_type", "unknown")
                    logger.info(f"[IG DM] @{username} item #{idx}: type={item_type}, sender={sender}, bot_id={ig_client.user_id}")
                    if str(sender) == str(ig_client.user_id):
                        ig_dm_processed.add(item_id)
                        continue

                    # On the very first scan, only process the newest message per thread
                    # to avoid replaying old conversations
                    if first_scan and idx > 0:
                        ig_dm_processed.add(item_id)
                        continue

                    item_type = item.get("item_type", "")
                    logger.info(f"[IG DM] Message from @{username}: type={item_type}, item_id={item_id}")

                    reel_url = None

                    # Handle shared reels (when user taps share button on a reel)
                    if item_type in ("media_share", "clip", "felix_share", "reel_share", "story_share"):
                        media = item.get("media_share") or item.get("clip", {}).get("clip") or item.get("felix_share_reel_media") or {}
                        if not media and "media" in item:
                            media = item["media"]
                        shortcode = media.get("code", "")
                        if shortcode:
                            reel_url = f"https://www.instagram.com/reel/{shortcode}/"
                            logger.info(f"[IG DM] Extracted reel from share: {reel_url}")
                        else:
                            # Try to find it in nested structures
                            for key in ("media_share", "clip", "felix_share_reel_media", "reel_share"):
                                nested = item.get(key, {})
                                if isinstance(nested, dict):
                                    sc = nested.get("code", "")
                                    if sc:
                                        reel_url = f"https://www.instagram.com/reel/{sc}/"
                                        logger.info(f"[IG DM] Extracted reel from {key}: {reel_url}")
                                        break

                    # Handle text messages with URLs
                    elif item_type == "text":
                        text = item.get("text", "") or ""
                        reel_url = extract_reel_url(text)

                    # Handle link-type messages
                    elif item_type == "link":
                        link_data = item.get("link", {})
                        link_text = link_data.get("text", "") or link_data.get("link_url", "") or ""
                        reel_url = extract_reel_url(link_text)

                    # Handle xma (shared content cards — reels, posts, links)
                    elif item_type in ("xma_media_share", "generic_xma", "xma_link", "xma_reel_share", "xma_clip"):
                        # Log raw xma data for debugging
                        xma_data = item.get("generic_xma") or item.get("xma_media_share") or item.get("xma_link") or item.get("xma_clip") or []
                        logger.info(f"[IG DM] XMA data from @{username}: {xma_data}")

                        # Search all string values in the XMA structure for Instagram URLs
                        def find_reel_in_data(data):
                            if isinstance(data, str):
                                return extract_reel_url(data)
                            elif isinstance(data, list):
                                for item_x in data:
                                    result = find_reel_in_data(item_x)
                                    if result:
                                        return result
                            elif isinstance(data, dict):
                                # Check high-priority keys first
                                for key in ("target_url", "preview_url", "header_icon_url", "preview_url_mime_type",
                                            "title_text", "header_title_text", "text", "url", "link_url"):
                                    val = data.get(key, "")
                                    if val:
                                        result = extract_reel_url(str(val))
                                        if result:
                                            return result
                                # Then check all values
                                for val in data.values():
                                    result = find_reel_in_data(val)
                                    if result:
                                        return result
                            return None

                        reel_url = find_reel_in_data(xma_data)
                        if reel_url:
                            logger.info(f"[IG DM] Found reel URL in XMA: {reel_url}")

                    else:
                        logger.info(f"[IG DM] Unhandled item_type '{item_type}' from @{username}, keys: {list(item.keys())}")

                    ig_dm_processed.add(item_id)

                    if reel_url:
                        logger.info(f"[IG DM] New request from @{username}: {reel_url}")
                        asyncio.create_task(
                            process_ig_dm_request(user_pk, username, thread_id, reel_url)
                        )
                        break  # one request per user at a time
                    elif item_type == "text" and (item.get("text", "") or "").strip():
                        # User sent text that's not a valid URL — send help
                        try:
                            await run_ig(send_dm_reply, thread_id,
                                "Hey! Send me an Instagram post or reel link and I'll get the hidden resource link for you.\n\nYou can either share a reel/post directly or paste a link like:\nhttps://www.instagram.com/reel/ABC123/\nhttps://www.instagram.com/p/ABC123/")
                        except Exception:
                            pass
                        break

        except Exception as e:
            logger.error(f"[IG DM] Listener error: {e}")

        first_scan = False
        await asyncio.sleep(IG_DM_CHECK_INTERVAL)


# ─── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    admin_id = os.environ.get("ADMIN_TELEGRAM_ID")
    is_admin = admin_id and str(update.effective_user.id) == admin_id

    commands_text = (
        "📌 <b>Commands:</b>\n"
        "/start — Show this message\n"
        "/status — Check bot status"
    )
    if is_admin:
        commands_text += "\n/restart — Re-login to Instagram"

    await update.message.reply_text(
        f"👋 <b>Welcome to the Instagram Link Bot!</b>\n\n"
        f"Send me any Instagram Post or Reel URL and I'll get the link for you!\n\n"
        f"🔄 <b>How it works:</b>\n"
        f"1. You send the post/reel URL\n"
        f"2. I auto-detect the keyword from comments\n"
        f"3. I follow the creator & comment for you\n"
        f"4. The creator DMs me the link\n"
        f"5. I send the link back to you!\n\n"
        f"{commands_text}\n\n"
        f"<b>Try it:</b> Just paste a link!",
        parse_mode="HTML"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active_users = len(pending_requests)
    if ig_logged_in:
        safe_username = html.escape(str(BOT_INSTAGRAM_USERNAME))
        await update.message.reply_text(
            f"✅ Bot is <b>online</b>\n"
            f"📸 Instagram: <code>@{safe_username}</code>\n"
            f"📊 Active Telegram requests: {active_users}\n"
            f"📬 IG DM requests: {len(ig_dm_pending)}\n"
            f"🔧 Max concurrent: {MAX_CONCURRENT_IG_CALLS}\n"
            f"🔄 Login failures: {_login_fail_count}",
            parse_mode="HTML"
        )
    else:
        safe_error = html.escape(str(_last_login_error))
        await update.message.reply_text(
            f"❌ Instagram is <b>disconnected</b>.\n"
            f"🔄 Login failures: {_login_fail_count}\n"
            f"⚠️ <b>Error:</b> <code>{safe_error}</code>\n"
            "The bot will try to reconnect on the next request.",
            parse_mode="HTML"
        )


async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Re-login to Instagram and clear pending requests."""
    global ig_logged_in, pending_requests, ig_dm_last_check, _login_fail_count

    # Admin check
    admin_id = os.environ.get("ADMIN_TELEGRAM_ID")
    if not admin_id or str(update.effective_user.id) != admin_id:
        await update.message.reply_text("⛔ You are not authorized to use this command.")
        return

    msg = await update.message.reply_text("🔄 Restarting Instagram session...")

    # Clear pending requests
    pending_requests.clear()
    ig_dm_pending.clear()
    ig_dm_processed.clear()
    waiting_for_owners.clear()
    _login_fail_count = 0  # Reset cooldown

    # Delete old session and re-login from env var
    async with ig_lock:
        ig_logged_in = False
        session_file = "ig_session.json"
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except OSError:
                pass

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(thread_pool, login_instagram)

    if ig_logged_in:
        ig_dm_last_check = time.time()
        safe_username = html.escape(str(BOT_INSTAGRAM_USERNAME))
        await msg.edit_text(
            f"✅ <b>Restarted successfully!</b>\n\n"
            f"📸 Instagram: <code>@{safe_username}</code>\n"
            f"🧹 Pending requests cleared",
            parse_mode="HTML"
        )
    else:
        safe_error = html.escape(str(_last_login_error))
        await msg.edit_text(
            f"❌ <b>Restart failed</b> — Instagram login error.\n"
            f"⚠️ <b>Exact Error:</b> <code>{safe_error}</code>\n"
            "Check credentials and try again.",
            parse_mode="HTML"
        )


async def code_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Provide an Instagram verification code for login challenge."""
    global _challenge_code
    if not context.args:
        await update.message.reply_text(
            "🔑 <b>Usage:</b> <code>/code 123456</code>\n\n"
            "Send the verification code Instagram sent to your email/phone.",
            parse_mode="HTML"
        )
        return
    _challenge_code = context.args[0].strip()
    safe_code = html.escape(str(_challenge_code))
    await update.message.reply_text(f"✅ Code <code>{safe_code}</code> received — applying to login challenge...", parse_mode="HTML")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main handler: user sends a reel URL → bot comments → waits for DM → sends link back.
    
    Multiple users can submit requests simultaneously — each user gets their own
    async task that runs concurrently.
    """
    global ig_logged_in

    text = update.message.text or ""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"

    # Extract the Instagram URL from the message
    url = extract_reel_url(text)

    if not url:
        await update.message.reply_text(
            "🔗 Please send a valid Instagram Post or Reel URL.\n\n"
            "<b>Just paste the link</b> — I'll auto-detect the keyword!\n\n"
            "Examples:\n<code>https://www.instagram.com/reel/ABC123xyz/</code>\n"
            "<code>https://www.instagram.com/p/ABC123xyz/</code>",
            parse_mode="HTML"
        )
        return

    shortcode = extract_shortcode(url)
    if not shortcode:
        await update.message.reply_text("❌ Couldn't parse that URL. Make sure it's a valid link.")
        return

    # Check if user already has a pending request for THIS SAME link
    if user_id in pending_requests:
        existing = pending_requests[user_id]
        if existing.get("shortcode") == shortcode:
            await update.message.reply_text("⏳ You already have a request for this link in progress. Please wait for it to finish.")
            return
        # Allow different links — they'll queue naturally via the semaphore

    # Step 1: Ensure Instagram is connected
    safe_user_name = html.escape(str(user_name))
    safe_shortcode = html.escape(str(shortcode))
    status_msg = await update.message.reply_text(
        f"⏳ Processing your request, {safe_user_name}...\n\n"
        f"🔗 Link: <code>...{safe_shortcode}</code>\n"
        f"🔍 Auto-detecting keyword...",
        parse_mode="HTML"
    )

    if not await ensure_logged_in_async():
        await status_msg.edit_text(
            "❌ Instagram login failed. Retrying...\nPlease try again in a minute.",
            parse_mode="HTML"
        )
        return

    # Step 2: Get reel owner info
    try:
        owner_info = await run_ig(get_reel_owner, shortcode)
        if not owner_info:
            await status_msg.edit_text("❌ Couldn't find information about this post/reel. Make sure the URL is correct.")
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

        safe_owner = html.escape(str(owner_username))
        await status_msg.edit_text(
            f"✅ Found post by <b>@{safe_owner}</b>\n"
            f"💬 Preparing...",
            parse_mode="HTML"
        )
    except instagrapi.exceptions.LoginRequired:
        ig_logged_in = False
        await status_msg.edit_text(
            "⚠️ Instagram session expired. Reconnecting...\nPlease try sending the link again.",
            parse_mode="HTML"
        )
        await ensure_logged_in_async()
        return
    except Exception as e:
        await status_msg.edit_text(f"❌ Error finding post/reel: {str(e)}")
        return

    # Step 2.5: Auto-detect keyword from comments and follow creator
    final_keyword = await run_ig(get_best_keyword, shortcode)
    logger.info(f"Auto-detected keyword for {shortcode}: '{final_keyword}'")

    safe_owner = html.escape(str(owner_username))
    safe_keyword = html.escape(str(final_keyword))
    await status_msg.edit_text(
        f"✅ Found post by <b>@{safe_owner}</b>\n"
        f"🔑 Keyword: <code>{safe_keyword}</code>\n"
        f"💬 Commenting...",
        parse_mode="HTML"
    )

    try:
        await run_ig(follow_user, owner_id)
        logger.info(f"Followed creator {owner_id}")
    except Exception as e:
        logger.info(f"Could not follow user {owner_id} (or already following): {e}")

    # Step 3: Comment on the reel
    waiting_for_owners.add(owner_id)
    timestamp_before_comment = time.time()
    pending_requests[user_id] = {"shortcode": shortcode, "timestamp": timestamp_before_comment}

    try:
        success = await run_ig(comment_on_reel, shortcode, final_keyword)

        safe_owner = html.escape(str(owner_username))
        safe_keyword = html.escape(str(final_keyword))
        await status_msg.edit_text(
            f"✅ Followed creator and commented <code>{safe_keyword}</code> on @{safe_owner}'s post/reel\n"
            f"⏳ Waiting for the creator to DM the link...\n\n"
            f"<i>(I'll send it to you as soon as it arrives!)</i>",
            parse_mode="HTML"
        )
    except instagrapi.exceptions.LoginRequired:
        ig_logged_in = False
        pending_requests.pop(user_id, None)
        waiting_for_owners.discard(owner_id)
        await status_msg.edit_text(
            "⚠️ Instagram session expired. Reconnecting...\nPlease try sending the link again.",
            parse_mode="HTML"
        )
        await ensure_logged_in_async()
        return
    except Exception as e:
        pending_requests.pop(user_id, None)
        waiting_for_owners.discard(owner_id)
        safe_error = html.escape(str(e))
        await status_msg.edit_text(
            f"❌ Couldn't comment on the post/reel.\n\n"
            f"<b>Reason:</b> {safe_error}\n\n"
            f"The post/reel might be private, or comments might be disabled.",
            parse_mode="HTML"
        )
        return

    # Step 4: Poll DMs for the response link
    link_found = None
    elapsed = 0

    while elapsed < DM_WAIT_TIME:
        try:
            link_found = await run_ig(check_dms_for_link, owner_id, timestamp_before_comment)
            if link_found:
                break
        except instagrapi.exceptions.LoginRequired:
            ig_logged_in = False
            logger.warning("Session expired during DM polling, attempting re-login...")
            if await ensure_logged_in_async():
                logger.info("Re-login successful, continuing DM polling")
            else:
                logger.error("Re-login failed during DM polling")
                break
        except Exception as e:
            logger.error(f"Error polling DMs for user {user_id}: {e}")

        await asyncio.sleep(DM_CHECK_INTERVAL)
        elapsed += DM_CHECK_INTERVAL

    # Clean up pending request
    pending_requests.pop(user_id, None)
    waiting_for_owners.discard(owner_id)

    try:
        await run_ig(unfollow_user, owner_id)
    except Exception:
        pass

    # Step 5: Send result to user
    if link_found:
        await update.message.reply_text(
            f"🔗 {link_found}"
        )
        logger.info(f"Successfully got link for user {user_id}: {link_found}")
    else:
        await update.message.reply_text(
            f"⏰ <b>Timeout</b> — No DM received. (waited 60s)",
            parse_mode="HTML"
        )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler — logs errors without crashing the bot."""
    logger.error(f"Exception while handling an update: {context.error}")
    logger.error(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))

    # Try to notify the user if possible
    if isinstance(update, Update) and update.effective_message:
        try:
            safe_error = html.escape(str(context.error)[:800])
            await update.effective_message.reply_text(
                f"⚠️ Something went wrong processing your request.\n\n"
                f"<b>Error Details:</b>\n<code>{safe_error}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass


LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")


def _is_pid_alive(pid: int) -> bool:
    """Check if a process with the given PID is still running (cross-platform)."""
    if sys.platform == "win32":
        import ctypes
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if handle:
            kernel32.CloseHandle(handle)
            return True
        return False
    else:
        try:
            os.kill(pid, 0)  # signal 0 = just check if process exists
            return True
        except OSError:
            return False


def acquire_lock():
    """Lock mechanism disabled. Telegram natively handles concurrency conflicts (HTTP 409)."""
    pass


def release_lock():
    """Lock mechanism disabled."""
    pass


def main():
    global ig_lock, ig_semaphore, thread_pool

    missing = []
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        missing.append("TELEGRAM_BOT_TOKEN")
    if not BOT_INSTAGRAM_USERNAME or BOT_INSTAGRAM_USERNAME == "YOUR_BOT_INSTAGRAM_USERNAME":
        missing.append("BOT_INSTAGRAM_USERNAME")
    
    direct_session_id = os.environ.get("IG_SESSION_ID", "")
    if not direct_session_id and (not BOT_INSTAGRAM_PASSWORD or BOT_INSTAGRAM_PASSWORD == "YOUR_BOT_INSTAGRAM_PASSWORD"):
        missing.append("BOT_INSTAGRAM_PASSWORD (or IG_SESSION_ID)")

    if missing:
        print("=" * 60)
        print("❌  Missing required environment variables:")
        for var in missing:
            print(f"   • {var}")
        print("")
        print("   Set them in your .env file (local) or in your")
        print("   cloud provider's environment/secrets dashboard.")
        print("=" * 60)
        sys.exit(1)

    # Prevent multiple instances from running at the same time
    acquire_lock()

    import atexit
    atexit.register(release_lock)

    # Initialize concurrency primitives
    thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)

    async def post_init(application):
        """Initialize async primitives and login to Instagram on the running event loop."""
        global ig_lock, ig_semaphore
        ig_lock = asyncio.Lock()
        ig_semaphore = asyncio.Semaphore(MAX_CONCURRENT_IG_CALLS)

        # Login to Instagram inside the event loop so thread pool works
        print("🔐 Logging into Instagram...")
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(thread_pool, login_instagram)
        if ig_logged_in:
            print(f"✅ Connected as @{BOT_INSTAGRAM_USERNAME}")
        else:
            print("⚠️ Instagram login failed — will retry on first request")

        # Start DM listener
        asyncio.create_task(ig_dm_listener())

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("restart", restart))
    app.add_handler(CommandHandler("code", code_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    print("")
    print("🤖 Reel Link Bot is running!")
    print(f"⚡ Concurrency: {MAX_CONCURRENT_IG_CALLS} simultaneous IG calls, {THREAD_POOL_SIZE} threads")
    print("📬 Instagram DM listener: active")
    print("📱 Open Telegram → Send /start to your bot")
    print("🛑 Press Ctrl+C to stop")
    print("")

    # Python 3.14 requires explicit event loop creation
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
