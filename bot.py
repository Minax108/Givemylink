import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os
from dotenv import load_dotenv
load_dotenv()  # Load .env file if present (local dev); no-op in containers

import re
import time
import logging
import asyncio
import traceback
import json
import html
import base64
import subprocess
import random
from urllib.parse import parse_qs, unquote, urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import instagrapi
from instagrapi import Client
from collections import Counter
import db_cache

try:
    from pw_engine import pw_intercept_manychat
except ImportError:
    pw_intercept_manychat = None
    # Playwright not available (e.g. on Termux/Android) — ManyChat button fallback disabled

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

URL_RE = re.compile(r'https?://[^\s<>"\')\]}]+', re.IGNORECASE)
INSTAGRAM_REDIRECT_HOSTS = {"l.instagram.com", "lm.instagram.com"}

# ─── CONFIG ───────────────────────────────────────────────────────────────────
# Credentials are loaded from environment variables for security.
# Set them in your .env file (locally) or in your cloud provider's dashboard.

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
BOT_INSTAGRAM_USERNAME = os.environ.get("BOT_INSTAGRAM_USERNAME", "")
BOT_INSTAGRAM_PASSWORD = os.environ.get("BOT_INSTAGRAM_PASSWORD", "")

# STEP 3: What to comment on the reel (usually "link" but some reels use other keywords)
DEFAULT_COMMENT = "link"

# STEP 4: How long to wait for ManyChat to DM back (in seconds)
DM_WAIT_TIME = 7200       # max seconds to wait for a DM reply (2 hours)
DM_CHECK_INTERVAL = 3    # Faster checking

# STEP 5: Concurrency settings
MAX_CONCURRENT_IG_CALLS = 6   # Increased for scaling (more simultaneous processes)
THREAD_POOL_SIZE = 20          # More threads for handling multiple accounts
PW_MANYCHAT_TIMEOUT = int(os.environ.get("PW_MANYCHAT_TIMEOUT", "90"))
IG_PROXY = os.environ.get("IG_PROXY", "")
ADMIN_USER_IDS = [int(x) for x in os.environ.get("ADMIN_USER_IDS", "").split(",") if x.strip()]
DEBUG_IG = os.environ.get("DEBUG_IG", "0").strip().lower() in {"1", "true", "yes", "on"}

# STEP 6: Playwright Relay (EC2 server for reliable button clicks)
# Set this to your EC2 server URL, e.g. "http://13.233.45.67:5123"
PW_RELAY_URL = os.environ.get("PW_RELAY_URL", "").strip()
PW_RELAY_API_KEY = os.environ.get("PW_RELAY_API_KEY", "givemylink-pw-secret-2026")

# ──────────────────────────────────────────────────────────────────────────────

ig_clients = []

# Track pending requests: {telegram_user_id: {"reel_url": ..., "timestamp": ...}}
pending_requests = {}

# Instagram DM listener state
ig_dm_pending = {}          # {ig_user_pk: {"shortcode": ..., "thread_id": ..., "timestamp": ...}}
clicked_postback_items = {}
ig_dm_processed = set()     # Set of processed DM item_ids to avoid re-processing
ig_dm_last_check = 0.0      # Timestamp of last DM check
waiting_for_owners = set()  # Reel owner user IDs we're currently waiting for DM responses
IG_DM_CHECK_INTERVAL = 8   # Faster scan for new requests
RESTART_DELAY = int(os.environ.get("BOT_RESTART_DELAY", "15"))
POSTBACK_RETRY_INTERVAL = int(os.environ.get("POSTBACK_RETRY_INTERVAL", "45"))
MAX_POSTBACK_ATTEMPTS = int(os.environ.get("MAX_POSTBACK_ATTEMPTS", "3"))
EARLY_MANYCHAT_FALLBACK_SECONDS = int(os.environ.get("EARLY_MANYCHAT_FALLBACK_SECONDS", "90"))

# Concurrency primitives (initialized in main)
ig_lock = None           # asyncio.Lock — protects login/session state
ig_semaphore = None      # asyncio.Semaphore — limits concurrent IG API calls
thread_pool = None       # ThreadPoolExecutor — runs blocking IG calls

# MVP Step 2: Action Queue State
action_queue = None
action_results = {}
action_events = {}


def dbg(message: str, *args):
    """Emit debug logs only when DEBUG_IG is enabled."""
    if DEBUG_IG:
        if args:
            logger.info("[IG DEBUG] " + message, *args)
        else:
            logger.info("[IG DEBUG] %s", message)


def get_postback_state(item_id: str) -> dict:
    return clicked_postback_items.get(item_id, {"attempts": 0, "last_attempt": 0.0})


def should_retry_postback(item_id: str) -> tuple[bool, int]:
    state = get_postback_state(item_id)
    attempts = int(state.get("attempts", 0))
    last_attempt = float(state.get("last_attempt", 0.0))

    if attempts >= MAX_POSTBACK_ATTEMPTS:
        return False, attempts

    if attempts == 0 or (time.time() - last_attempt) >= POSTBACK_RETRY_INTERVAL:
        return True, attempts + 1

    return False, attempts


def mark_postback_attempt(item_id: str, button_text: str, payload: str, attempt_number: int):
    clicked_postback_items[item_id] = {
        "attempts": attempt_number,
        "last_attempt": time.time(),
        "button_text": button_text,
        "payload": payload,
    }


def send_human_dm_text(client, thread_id: int, text: str, reply_to_item_id: str = None):
    """Send a DM with human-like pacing and presence signals."""
    import random
    import uuid

    try:
        client.direct_thread_mark_as_seen(thread_id)
    except Exception:
        pass

    time.sleep(random.uniform(1.2, 3.8))

    try:
        client.direct_thread_typing(thread_id, status=True)
        time.sleep(random.uniform(1.5, 3.6))
        client.direct_thread_typing(thread_id, status=False)
    except Exception:
        pass

    try:
        if reply_to_item_id:
            # Send a true reply to the specific item
            client_context = str(uuid.uuid4())
            data = {
                "text": text,
                "thread_ids": f"[{thread_id}]",
                "action": "send_item",
                "client_context": client_context,
                "reply_to": json.dumps({"item_id": reply_to_item_id})
            }
            client.private_request("direct_v2/threads/broadcast/text/", data=data)
        else:
            client.direct_answer(thread_id=thread_id, text=text)
    except Exception as e:
        logger.warning(f"Failed to send DM text '{text}': {e}")
        try:
            # Fallback to normal DM if reply fails
            if reply_to_item_id:
                client.direct_answer(thread_id=thread_id, text=text)
        except:
            pass


async def run_ig(client, func, *args):
    """Run a blocking Instagram API call in the thread pool, respecting the semaphore."""
    loop = asyncio.get_event_loop()
    async with ig_semaphore:
        return await loop.run_in_executor(thread_pool, func, client, *args)


def create_logged_task(coro, label: str):
    """Create a background task and log any exception it raises."""
    task = asyncio.create_task(coro)

    def _done_callback(t: asyncio.Task):
        try:
            t.result()
        except asyncio.CancelledError:
            logger.info("[%s] cancelled", label)
        except Exception:
            logger.exception("[%s] crashed", label)

    task.add_done_callback(_done_callback)
    return task


async def supervise_background_task(label: str, coro_factory, retry_delay: int = RESTART_DELAY):
    """Keep a background coroutine running even if it crashes."""
    while True:
        try:
            logger.info("[%s] started", label)
            await coro_factory()
            logger.warning("[%s] exited; restarting in %ss", label, retry_delay)
        except asyncio.CancelledError:
            logger.info("[%s] cancelled", label)
            raise
        except Exception:
            logger.exception("[%s] crashed; restarting in %ss", label, retry_delay)
        await asyncio.sleep(retry_delay)

async def get_random_client():
    global ig_clients
    async with ig_lock:
        if not ig_clients:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(thread_pool, login_instagram)
    if not ig_clients:
        raise Exception("No active Instagram clients available.")
    import random
    return random.choice(ig_clients)

def login_instagram():
    """Log in to all provided session IDs."""
    global ig_clients
    ig_clients.clear()
    session_str = os.environ.get("IG_SESSION_IDS", "")
    if not session_str:
        session_str = os.environ.get("IG_SESSION_ID", "")
    session_ids = [s.strip() for s in session_str.split(",") if s.strip()]

    for i, sid in enumerate(session_ids):
        client = Client()
        client.request_timeout = 45  # Prevent infinite hangs on bad network
        if IG_PROXY:
            client.set_proxy(IG_PROXY)
        try:
            client.login_by_sessionid(sid)
            ig_clients.append(client)
            logger.info(f"Instagram: client {i+1} logged in securely using provided SESSION_ID.")
        except Exception as e:
            logger.error(f"Instagram client {i+1} login failed: {type(e).__name__}: {e}")

async def ensure_logged_in_async() -> bool:
    """Make sure we have at least one logged in Instagram client, reconnect if needed. Thread-safe."""
    global ig_clients
    async with ig_lock:
        if not ig_clients:
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(thread_pool, login_instagram)
    return len(ig_clients) > 0


def get_active_ig_usernames() -> list[str]:
    """Return the usernames of the currently logged-in Instagram clients."""
    usernames = []
    for client in ig_clients:
        username = getattr(client, "username", None)
        if username:
            usernames.append(username)
    return usernames


async def retire_client(client, reason: str = "") -> int:
    """Remove a failing client from the active pool and return remaining count."""
    global ig_clients

    async with ig_lock:
        before = len(ig_clients)
        ig_clients = [c for c in ig_clients if c is not client]
        after = len(ig_clients)

    if before != after:
        username = getattr(client, "username", "unknown")
        if reason:
            logger.warning("Retired Instagram client @%s: %s", username, reason)
        else:
            logger.warning("Retired Instagram client @%s", username)

    return len(ig_clients)


async def handle_client_auth_failure(client, reason: str = "") -> bool:
    """Drop a bad client and re-login only if the pool becomes empty."""
    remaining = await retire_client(client, reason)
    if remaining > 0:
        return True
    return await restart_async_only()

async def get_next_client():
    """Get a random Instagram client from the active pool."""
    global ig_clients
    
    if not await ensure_logged_in_async():
        raise Exception("No Instagram clients available")
        
    async with ig_lock:
        if not ig_clients:
            raise Exception("No Instagram clients available")
        
        client = random.choice(ig_clients)
        
        logger.info(
            "[Rotation] Using random account @%s from %s active sessions",
            getattr(client, "username", "unknown"),
            len(ig_clients),
        )
        return client


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


def to_plain_data(value, depth: int = 0, max_depth: int = 20):
    """Convert instagrapi/Pydantic objects into JSON-ish dict/list/scalar data."""
    if depth > max_depth:
        return repr(value)
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(k): to_plain_data(v, depth + 1, max_depth) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_plain_data(v, depth + 1, max_depth) for v in value]
    if hasattr(value, "model_dump"):
        try:
            return to_plain_data(value.model_dump(), depth + 1, max_depth)
        except Exception:
            pass
    if hasattr(value, "dict"):
        try:
            return to_plain_data(value.dict(), depth + 1, max_depth)
        except Exception:
            pass
    if hasattr(value, "__dict__"):
        try:
            return {
                str(k): to_plain_data(v, depth + 1, max_depth)
                for k, v in vars(value).items()
                if not str(k).startswith("_")
            }
        except Exception:
            pass
    return repr(value)


def unwrap_instagram_redirect(url: str) -> str:
    """Return the destination for l.instagram.com/lm.instagram.com redirect URLs."""
    try:
        parsed = urlparse(url)
        if parsed.netloc.lower() in INSTAGRAM_REDIRECT_HOSTS:
            query = parse_qs(parsed.query)
            if query.get("u"):
                return unquote(query["u"][0])
    except Exception:
        pass
    return url


def extract_urls_from_text(text: str) -> list[str]:
    urls = []
    for match in URL_RE.findall(text or ""):
        cleaned = match.rstrip(".,;:!?)]}")
        urls.append(unwrap_instagram_redirect(cleaned))
    return urls


def extract_reel_url_from_shared_item(item: dict) -> str | None:
    """Extract a reel URL from Instagram shared media item payloads."""
    candidate_sources = []

    for key in ("media_share", "clip", "reel_share", "story_share", "xma_clip", "xma_media_share", "xma_link", "generic_xma"):
        value = item.get(key)
        if value:
            candidate_sources.append(value)

    if "media" in item and item["media"]:
        candidate_sources.append(item["media"])

    def _walk(value):
        if isinstance(value, str):
            url = extract_reel_url(value)
            if url:
                return url
            try:
                parsed = json.loads(value)
                return _walk(parsed)
            except Exception:
                return None

        if isinstance(value, list):
            for child in value:
                found = _walk(child)
                if found:
                    return found
            return None

        if isinstance(value, dict):
            for key in ("target_url", "url", "link_url", "text", "serialized_content_ref"):
                val = value.get(key)
                if val:
                    found = _walk(val)
                    if found:
                        return found

            shortcode = value.get("code", "")
            if shortcode:
                return f"https://www.instagram.com/reel/{shortcode}/"

            for child in value.values():
                found = _walk(child)
                if found:
                    return found

        return None

    for source in candidate_sources:
        found = _walk(source)
        if found:
            return found

    return None


def find_urls_deep(value, path: str = "$", found: list[tuple[str, str]] | None = None, seen=None):
    """Recursively search any nested structure for http(s) and Instagram redirect URLs."""
    if found is None:
        found = []
    if seen is None:
        seen = set()

    if value is None or isinstance(value, (int, float, bool)):
        return found

    obj_id = id(value)
    if isinstance(value, (dict, list, tuple, set)) or hasattr(value, "__dict__"):
        if obj_id in seen:
            return found
        seen.add(obj_id)

    if isinstance(value, str):
        for url in extract_urls_from_text(value):
            found.append((path, url))
        return found

    if not isinstance(value, (dict, list, tuple, set)):
        value = to_plain_data(value)

    if isinstance(value, dict):
        for key, child in value.items():
            find_urls_deep(child, f"{path}.{key}", found, seen)
    elif isinstance(value, (list, tuple, set)):
        for index, child in enumerate(value):
            find_urls_deep(child, f"{path}[{index}]", found, seen)

    return found


def first_deep_url(value, requested_shortcode: str = "") -> str | None:
    findings = find_urls_deep(value)
    priority_keys = ("action_url", "link_url", "target_url", "url", "text", "title")
    noisy_keys = (
        "thumbnail",
        "preview",
        "image",
        "video",
        "profile_pic",
        "header_icon",
        "display_url",
    )

    def score(finding: tuple[str, str]) -> tuple[int, int]:
        path, url = finding
        lowered_path = path.lower()
        lowered_url = url.lower()
        is_instagram_media_noise = any(key in lowered_path for key in noisy_keys)
        is_asset = any(
            host in lowered_url
            for host in ("fbcdn.net", "cdninstagram.com", "scontent", "fna.fbcdn.net")
        )
        if any(key in lowered_path for key in priority_keys) and not is_instagram_media_noise:
            return (0, len(path))
        if not is_asset and not is_instagram_media_noise:
            return (1, len(path))
        return (2, len(path))

    seen_urls = []
    for _, url in sorted(findings, key=score):
        if requested_shortcode and requested_shortcode in url and 'instagram.com' in url:
            continue
        if url not in seen_urls:
            seen_urls.append(url)
    return seen_urls[0] if seen_urls else None


def get_instagram_session_b64(client) -> str | None:
    """Return a base64-encoded instagrapi settings snapshot for Playwright."""
    return base64.b64encode(json.dumps(client.get_settings()).encode('utf-8')).decode('ascii')


def get_best_keyword(client, shortcode: str) -> str:
    """Analyze the reel's top comments to deduce the keyword users are commenting."""
    try:
        media_pk = client.media_pk_from_code(shortcode)
        media_id = client.media_id(media_pk)
        
        # Fetch latest comments
        comments = client.media_comments(media_id, amount=40)
        words = []
        for c in comments:
            text = c.text.strip().lower()
            # If the comment is short (e.g. 1-3 words), it's likely a keyword attempt
            parts = re.findall(r'[a-z]+', text)
            if len(parts) <= 3:
                words.extend(parts)
        
        if words:
            # Filter out standard stop words just in case
            stopwords = {"the", "a", "is", "in", "it", "to", "and", "of", "for", "on", "this", "that", "my", "i", "love", "awesome", "great", "send", "me", "plz", "please", "bro"}
            # keep "send" as keyword actually, don't put send in stopwords.
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
                    
    except Exception as e:
        logger.error(f"Failed to fetch comments to deduce keyword: {e}")
        
    return DEFAULT_COMMENT


def comment_on_reel(client, shortcode: str, comment_text: str) -> bool:
    """Comment on a reel with heavy stealth and human-like delays."""
    try:
        import random, time
        media_pk = client.media_pk_from_code(shortcode)
        media_id = client.media_id(media_pk)
        
        # 1. Simulate "Watching" the reel for a realistic but faster amount of time
        watch_time = random.uniform(4.5, 12.0)
        logger.info(f"[Stealth] Watching reel {shortcode} for {watch_time:.1f}s...")
        time.sleep(watch_time)
        
        # 2. Simulate "Interacting" like a human (Fetch comments first)
        try:
            client.media_comments(media_id, amount=random.randint(3, 8))
            time.sleep(random.uniform(1.5, 4.0))
        except Exception:
            pass

        # 3. Like the reel (Natural engagement)
        try:
             client.media_like(media_id)
             logger.info(f"[Stealth] Liked reel {shortcode}")
        except Exception as e:
             logger.warning(f"Failed to like: {e}")
             
        time.sleep(random.uniform(2.0, 5.0))
        
        # 4. Vary the comment text slightly to avoid spam detection
        variations = [
            comment_text,
            comment_text.capitalize(),
            f"{comment_text}!",
            f"{comment_text} please",
            f"{comment_text}!",
            f"{comment_text} pls",
            comment_text.lower()
        ]
        final_text = random.choice(variations)
        
        client.media_comment(media_id, final_text)
        logger.info(f"[Stealth] Commented '{final_text}' on {shortcode}")
        return True
    except Exception as e:
        logger.error(f"Failed to comment on {shortcode}: {e}")
        raise


def get_reel_owner(client, shortcode: str) -> dict:
    """Get info about the reel's owner to identify their DM, strictly using Mobile API to avoid bans."""
    try:
        media_pk = client.media_pk_from_code(shortcode)
        # Never use media_info() (GraphQL) as it flags session cookies. Force V1 Mobile API.
        media_info = client.media_info_v1(media_pk)
        return {
            "user_id": media_info.user.pk,
            "username": media_info.user.username,
        }
    except Exception as e:
        err_msg = f"{type(e).__name__}: {e}"
        return {"error": err_msg}


def follow_user(client, user_id):
    """Follow an Instagram user."""
    client.user_follow(user_id)


def unfollow_user(client, user_id):
    """Unfollow an Instagram user."""
    try:
        client.user_unfollow(user_id)
    except Exception as e:
        logger.warning(f"Could not unfollow user {user_id}: {e}")


def trigger_xma_postback(client, thread_id: str, item_id: str, payload: str, button_text: str = ""):
    """Trigger a ManyChat button click by trying multiple API endpoints.
    
    Instagram's postback mechanism is undocumented, so we try several
    known endpoints that the Instagram app might use internally.
    """
    import random
    import uuid

    success = False

    # Human-like pre-interaction
    delay = random.uniform(1.5, 4.0)
    logger.info(f"[Postback] Waiting {delay:.1f}s before clicking '{button_text}'...")
    time.sleep(delay)

    try:
        client.direct_thread_mark_as_seen(thread_id)
    except Exception:
        pass
    time.sleep(random.uniform(0.5, 1.2))

    try:
        client.direct_thread_typing(thread_id, status=True)
        time.sleep(random.uniform(1.0, 2.5))
        client.direct_thread_typing(thread_id, status=False)
    except Exception:
        pass

    client_context = str(uuid.uuid4())
    mutation_token = str(uuid.uuid4())

    # --- Endpoint 1: /payload/ (original approach) ---
    try:
        resp = client.private_request(
            f"direct_v2/threads/{thread_id}/items/{item_id}/payload/",
            data={
                "payload": payload,
                "client_context": client_context,
                "mutation_token": mutation_token,
            },
        )
        logger.info(f"[Postback EP1 /payload/] Success: {resp}")
        success = True
    except Exception as e:
        dbg("[Postback EP1 /payload/] Failed: %s", e)

    time.sleep(random.uniform(1.0, 2.0))

    # --- Endpoint 2: /broadcast/xma_cta_action/ ---
    if not success:
        try:
            resp = client.private_request(
                f"direct_v2/threads/{thread_id}/broadcast/xma_cta_action/",
                data={
                    "thread_ids": f"[{thread_id}]",
                    "item_id": item_id,
                    "payload": payload,
                    "action": "send_item",
                    "client_context": str(uuid.uuid4()),
                    "mutation_token": str(uuid.uuid4()),
                },
            )
            logger.info(f"[Postback EP2 /xma_cta_action/] Success: {resp}")
            success = True
        except Exception as e:
            dbg("[Postback EP2 /xma_cta_action/] Failed: %s", e)

    time.sleep(random.uniform(0.5, 1.5))

    # --- Endpoint 3: /payload/ without signature ---
    if not success:
        try:
            resp = client.private_request(
                f"direct_v2/threads/{thread_id}/items/{item_id}/payload/",
                data={"payload": payload},
                with_signature=False,
            )
            logger.info(f"[Postback EP3 /payload/ no-sig] Success: {resp}")
            success = True
        except Exception as e:
            dbg("[Postback EP3 /payload/ no-sig] Failed: %s", e)

    # --- Endpoint 4: React to the message (some flows trigger on reactions) ---
    try:
        client.private_request(
            f"direct_v2/threads/{thread_id}/items/{item_id}/reactions/",
            data={
                "reaction_status": "created",
                "reaction_type": "like",
                "client_context": str(uuid.uuid4()),
                "node_type": "item",
                "item_id": item_id,
            },
        )
        logger.info(f"[Postback EP4] Reacted to item {item_id}")
    except Exception as e:
        dbg("[Postback EP4 /reactions/] Failed: %s", e)

    if success:
        logger.info(f"[Postback] At least one endpoint succeeded for '{button_text}'")
    else:
        logger.warning(f"[Postback] All API endpoints failed for '{button_text}'")

    return success


def _quick_poll_for_link(client, thread_id, requested_shortcode: str = "", max_wait: int = 60, interval: int = 5) -> str | None:
    """After triggering a postback, immediately poll the thread for a follow-up link.
    
    ManyChat typically sends the link within 5-30 seconds of the button click.
    This inline poll catches it much faster than waiting for the next DM scan cycle.
    """
    import time
    start = time.time()
    seen_item_ids = set()
    logger.info(f"[QuickPoll] Polling thread {thread_id} for up to {max_wait}s...")

    while time.time() - start < max_wait:
        time.sleep(interval)
        try:
            result = client.private_request(
                f"direct_v2/threads/{thread_id}/",
                params={"visual_message_return_type": "unseen", "direction": "older", "limit": "10"},
            )
            items = result.get("thread", {}).get("items", [])

            for item in items:
                iid = item.get("item_id", "")
                if iid in seen_item_ids:
                    continue
                seen_item_ids.add(iid)

                item_data = to_plain_data(item)
                item_type = item.get("item_type", "")

                # Check XMA cards for CTA URLs
                if item_type in ("generic_xma", "xma_link", "xma_share", "xma_media_share"):
                    xma_list = item.get(item_type, [])
                    if isinstance(xma_list, dict):
                        xma_list = [xma_list]
                    for xma in xma_list:
                        for btn in xma.get("cta_buttons", []):
                            action_url = btn.get("action_url", "")
                            if action_url and action_url.startswith("http"):
                                found_url = unwrap_instagram_redirect(action_url)
                                if requested_shortcode and requested_shortcode in found_url and 'instagram.com' in found_url:
                                    continue
                                logger.info(f"[QuickPoll] Found link in follow-up CTA: {found_url}")
                                return found_url
                        # Check title text for URLs
                        title = xma.get("title_text", "") or ""
                        for u in extract_urls_from_text(title):
                            if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                continue
                            logger.info(f"[QuickPoll] Found link in follow-up title: {u}")
                            return u

                # Check text messages
                elif item_type == "text":
                    for u in extract_urls_from_text(item.get("text", "")):
                        if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                            continue
                        logger.info(f"[QuickPoll] Found link in follow-up text: {u}")
                        return u

                # Check link messages
                elif item_type == "link":
                    link_data = item.get("link", {})
                    link_url = link_data.get("text", "") or link_data.get("link_url", "")
                    for u in extract_urls_from_text(link_url):
                        if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                            continue
                        logger.info(f"[QuickPoll] Found link in follow-up link msg: {u}")
                        return u

                # Deep URL search
                fallback_url = first_deep_url(item_data, requested_shortcode)
                if fallback_url:
                    logger.info(f"[QuickPoll] Found link by deep scan: {fallback_url}")
                    return fallback_url

        except Exception as e:
            dbg("[QuickPoll] Error polling thread %s: %s", thread_id, e)

        elapsed = int(time.time() - start)
        dbg("[QuickPoll] No link yet after %ss", elapsed)

    logger.info(f"[QuickPoll] No follow-up link found after {max_wait}s")
    return None


def call_pw_relay(client, creator_username: str, thread_id: str) -> str | None:
    """Call the EC2 Playwright relay server to click a postback button via real browser.
    
    This is the nuclear option — when API-based postback triggers fail,
    we delegate to a cloud server running Playwright + Chromium that
    actually opens Instagram web and clicks the button.
    """
    if not PW_RELAY_URL:
        return None

    try:
        import base64
        import requests as http_requests

        # Get the session settings from the instagrapi client
        settings = client.get_settings()
        b64_settings = base64.b64encode(json.dumps(settings).encode()).decode()

        payload = {
            "b64_settings": b64_settings,
            "creator_username": creator_username,
            "thread_id": thread_id,
            "timeout": PW_MANYCHAT_TIMEOUT,
        }

        logger.info(f"[PW-Relay] Sending click request for @{creator_username} thread={thread_id}")
        
        resp = http_requests.post(
            f"{PW_RELAY_URL}/click",
            json=payload,
            headers={"X-API-Key": PW_RELAY_API_KEY},
            timeout=PW_MANYCHAT_TIMEOUT + 30,  # Give extra time for network
        )

        if resp.status_code == 200:
            data = resp.json()
            link = data.get("link")
            if link:
                logger.info(f"[PW-Relay] Got link from Playwright: {link}")
                return link
            else:
                logger.warning(f"[PW-Relay] Server returned no link: {data.get('error', 'unknown')}")
        else:
            logger.error(f"[PW-Relay] Server returned HTTP {resp.status_code}: {resp.text[:200]}")

    except Exception as e:
        logger.error(f"[PW-Relay] Failed to call relay server: {e}")

    return None


def check_dms_for_link(client, reel_owner_id: int, after_timestamp: float, requested_shortcode: str = '') -> str | None:
    """
    Check Instagram DMs for a new message from the reel owner.
    Handles generic_xma (ManyChat cards with CTA buttons), text, and link messages.
    Returns the link/text if found, None otherwise.
    """
    import random
    try:
        # Check both main inbox and message requests
        threads = client.direct_threads(amount=20)
        dbg("check_dms_for_link scanning %s main threads for owner_id=%s shortcode=%s", len(threads), reel_owner_id, requested_shortcode)
        try:
            pending = client.direct_pending_inbox(amount=20)
            threads.extend(pending)
            dbg("check_dms_for_link appended %s pending threads", len(pending))
        except Exception as e:
            logger.warning(f"Error checking pending inbox: {e}")
            dbg("check_dms_for_link pending inbox error: %s", e)

        for thread in threads:
            # Check if this thread involves the reel owner
            for user in thread.users:
                if str(user.pk) == str(reel_owner_id):
                    dbg("Matched owner_id=%s in thread_id=%s with users=%s", reel_owner_id, thread.id, [(u.pk, u.username) for u in thread.users])
                    # Found the right thread — use raw API to get full message data
                    try:
                        result = client.private_request(
                            f"direct_v2/threads/{thread.id}/",
                            params={"visual_message_return_type": "unseen", "direction": "older", "seq_id": "40065", "limit": "10"},
                        )
                        items = result.get("thread", {}).get("items", [])
                        dbg("Loaded %s items from raw thread_id=%s", len(items), thread.id)
                    except Exception as e:
                        logger.error(f"Error fetching raw thread data: {e}")
                        dbg("Raw thread fetch failed for thread_id=%s: %s", thread.id, e)
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

                        item_data = to_plain_data(item)
                        field_names = (
                            "text",
                            "link",
                            "xma_share",
                            "media_share",
                            "clip",
                            "reel_share",
                            "story_share",
                            "generic_xma",
                        )
                        logger.info(f"[DM inspect] item_type={item_type}")
                        for field_name in field_names:
                            logger.info(f"[DM inspect] {field_name}={json.dumps(item_data.get(field_name), ensure_ascii=False, default=str)}")

                        deep_urls = find_urls_deep(item_data)
                        for path, url in deep_urls:
                            logger.info(f"[DM inspect] extracted_url path={path} url={url}")

                        # Handle generic_xma (ManyChat/automation cards) and other XMA types
                        if item_type in ("generic_xma", "xma_link", "xma_share", "xma_media_share"):
                            xma_list = item.get(item_type, [])
                            if isinstance(xma_list, dict):
                                xma_list = [xma_list]
                            dbg("Inspecting generic_xma item_id=%s count=%s", item.get("item_id"), len(xma_list))
                            for xma in xma_list:
                                cta_buttons = xma.get("cta_buttons", [])
                                dbg("generic_xma title=%s cta_count=%s", xma.get("title_text", ""), len(cta_buttons))
                                for btn in cta_buttons:
                                    action_url = btn.get("action_url", "")
                                    if action_url and action_url.startswith("http"):
                                        found_url = unwrap_instagram_redirect(action_url)
                                        # Fix: don't return if the URL is just the exact same reel we requested!
                                        if requested_shortcode and requested_shortcode in found_url and 'instagram.com' in found_url:
                                            logger.info(f"Ignored CTA link because it loops back to requested reel: {found_url}")
                                            dbg("Ignored CTA url looping back to requested reel: %s", found_url)
                                            continue
                                        logger.info(f"Found link in CTA button: {found_url}")
                                        dbg("Returning CTA url: %s", found_url)
                                        return found_url

                                # Multi-strategy button trigger:
                                # Strategy 1: Try postback API (unreliable but worth trying)
                                # Strategy 2: Send button text as keyword DM (most reliable)
                                # Strategy 3: Send common trigger keywords as fallback
                                item_id = item.get("item_id")
                                if item_id and cta_buttons:
                                    should_attempt, attempt_number = should_retry_postback(item_id)
                                    if not should_attempt:
                                        state = get_postback_state(item_id)
                                        dbg(
                                            "Skipping retry for item_id=%s attempts=%s age=%.1fs",
                                            item_id,
                                            state.get("attempts", 0),
                                            time.time() - float(state.get("last_attempt", 0.0)),
                                        )
                                        continue

                                    # Pick the first button with a title
                                    btn = cta_buttons[0]
                                    btn_title = btn.get("title", "").strip()
                                    platform_token = btn.get("platform_token", {})
                                    if isinstance(platform_token, str):
                                        try:
                                            platform_token = json.loads(platform_token)
                                        except Exception:
                                            platform_token = {}
                                    postback = platform_token.get("postback", {})
                                    payload = postback.get("postback_payload", "")

                                    try:
                                        # Strategy 1: Try the postback API
                                        if payload:
                                            logger.info(f"[Strategy 1] Triggering postback for '{btn_title}' (attempt {attempt_number})")
                                            trigger_res = trigger_xma_postback(client, str(thread.id), item_id, payload, btn_title)
                                            if trigger_res:
                                                logger.info(f"[Strategy 1] Postback API returned success for '{btn_title}'")
                                            else:
                                                logger.warning(f"[Strategy 1] Postback API failed for '{btn_title}'")
                                            
                                            # Give ManyChat a moment to process
                                            time.sleep(random.uniform(3.0, 6.0))

                                        # Strategy 2: Send the button text as a keyword DM (as a direct reply to the card)
                                        # ManyChat bots often listen for exact button text as a fallback trigger.
                                        if btn_title and attempt_number == 1:
                                            logger.info(f"[Strategy 2] Replying with button text: '{btn_title}'")
                                            send_human_dm_text(client, int(thread.id), btn_title, reply_to_item_id=item_id)
                                            time.sleep(random.uniform(2.0, 4.0))

                                        # Strategy 3: Smarter Keyword Extraction & Fallback
                                        if attempt_number >= 2:
                                            keywords_to_try = []
                                            
                                            # Look for quoted text or ALL CAPS words in the card's title/subtitle
                                            title_text = xma.get("title_text", "") or ""
                                            subtitle_text = xma.get("subtitle_text", "") or ""
                                            combined_text = f"{title_text} {subtitle_text}"
                                            
                                            # 1. Words in quotes (e.g. Reply "YES")
                                            quotes = re.findall(r'["\']([^"\']+)["\']', combined_text)
                                            keywords_to_try.extend([q for q in quotes if len(q.split()) <= 3])
                                            
                                            # 2. Uppercase emphasis (e.g. comment LINK)
                                            uppercase = re.findall(r'\b[A-Z]{3,}\b', combined_text)
                                            keywords_to_try.extend(uppercase)
                                            
                                            # 3. Standard fallback dictionary
                                            common_keywords = ["link", "send", "yes", "access", "get"]
                                            
                                            # Find the first extracted keyword that isn't the button title
                                            extracted = [k for k in keywords_to_try if k.lower() != btn_title.lower()]
                                            
                                            if attempt_number == 2 and extracted:
                                                keyword_to_try = extracted[0]
                                                logger.info(f"[Strategy 3] Sending extracted keyword: '{keyword_to_try}'")
                                            else:
                                                keyword_to_try = common_keywords[(attempt_number) % len(common_keywords)]
                                                logger.info(f"[Strategy 4] Sending fallback keyword: '{keyword_to_try}'")
                                                
                                            time.sleep(random.uniform(5.0, 10.0))
                                            send_human_dm_text(client, int(thread.id), keyword_to_try, reply_to_item_id=item_id)

                                        mark_postback_attempt(item_id, btn_title, payload, attempt_number)
                                        logger.info(f"Automation triggered (attempt {attempt_number}), quick-polling for response...")

                                        # CRITICAL: Immediately poll the thread for the follow-up link
                                        quick_link = _quick_poll_for_link(
                                            client, str(thread.id),
                                            requested_shortcode=requested_shortcode,
                                            max_wait=60 if attempt_number <= 2 else 30,
                                            interval=5,
                                        )
                                        if quick_link:
                                            logger.info(f"[QuickPoll] Got link after postback trigger: {quick_link}")
                                            return quick_link

                                        # Strategy 5: PLAYWRIGHT RELAY (nuclear option)
                                        # If API strategies failed after enough attempts, use the EC2 browser
                                        if attempt_number >= 2 and PW_RELAY_URL:
                                            logger.info(f"[Strategy 5] Calling Playwright relay at {PW_RELAY_URL}")
                                            pw_link = call_pw_relay(
                                                client=client,
                                                creator_username=user.username if hasattr(user, 'username') else "",
                                                thread_id=str(thread.id),
                                            )
                                            if pw_link:
                                                logger.info(f"[Strategy 5] Playwright relay returned link: {pw_link}")
                                                return pw_link
                                            else:
                                                logger.warning(f"[Strategy 5] Playwright relay returned no link")

                                    except Exception as e:
                                        logger.error(f"Failed to trigger button: {e}")
                                        mark_postback_attempt(item_id, btn_title, payload, attempt_number)

                                # Also check the title text for URLs
                                title = xma.get("title_text", "") or ""
                                urls = extract_urls_from_text(title)
                                if urls:
                                    for u in urls:
                                        if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                            continue
                                        dbg("Returning URL from generic_xma title: %s", u)
                                        return u

                        # Handle plain text messages
                        elif item_type == "text":
                            text = item.get("text", "")
                            urls = extract_urls_from_text(text)
                            if urls:
                                for u in urls:
                                    if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                        continue
                                    logger.info(f"Found link in text message: {u}")
                                    dbg("Returning URL from text item_id=%s url=%s", item.get("item_id"), u)
                                    return u

                        # Handle link type messages
                        elif item_type == "link":
                            link_data = item.get("link", {})
                            link_url = link_data.get("text", "") or link_data.get("link_url", "")
                            urls = extract_urls_from_text(link_url)
                            if urls:
                                for u in urls:
                                    if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                        continue
                                    logger.info(f"Found link in link message: {u}")
                                    dbg("Returning URL from link item_id=%s url=%s", item.get("item_id"), u)
                                    return u

                        fallback_url = first_deep_url(item_data, requested_shortcode)
                        if fallback_url:
                            logger.info(f"Found link by recursive DM inspection: {fallback_url}")
                            dbg("Returning URL from recursive inspection item_id=%s url=%s", item.get("item_id"), fallback_url)
                            return fallback_url

        return None
    except Exception as e:
        logger.error(f"Error checking DMs: {e}")
        return None


# ─── INSTAGRAM DM HANDLING ────────────────────────────────────────────────────

def send_dm_reply(client, thread_id: int, text: str):
    """Send a reply in an Instagram DM thread."""
    dbg("Sending DM reply to thread %s: %s", thread_id, text[:200] if isinstance(text, str) else text)
    client.direct_answer(thread_id=thread_id, text=text)


def get_dm_thread_id_for_user(client, user_id: int) -> int | None:
    """Find the Instagram DM thread id that contains the target user."""
    try:
        threads = client.direct_threads(amount=30)
        for thread in threads:
            for user in getattr(thread, "users", []):
                if str(user.pk) == str(user_id):
                    return int(thread.id)
    except Exception as e:
        logger.warning(f"Could not find DM thread for {user_id}: {e}")
    return None


async def playwright_manychat_fallback(client, owner_username: str, owner_id: int) -> str | None:
    """Use Instagram Web to click ManyChat buttons and capture webview/chat links."""
    if pw_intercept_manychat is None:
        logger.info("[Playwright] pw_engine not available on this platform; skipping fallback")
        return None
    session_b64 = get_instagram_session_b64(client)
    if not session_b64:
        logger.warning("[Playwright] No IG session snapshot found; skipping fallback")
        return None

    creator_thread_id = await run_ig(client, get_dm_thread_id_for_user, owner_id)
    logger.info(
        f"[Playwright] Starting fallback for @{owner_username}, thread_id={creator_thread_id}"
    )
    try:
        return await pw_intercept_manychat(
            session_b64,
            owner_username,
            timeout_sec=PW_MANYCHAT_TIMEOUT,
            proxy=IG_PROXY or None,
            thread_id=creator_thread_id,
        )
    except Exception as e:
        logger.error(f"[Playwright] Fallback failed for @{owner_username}: {e}")
        return None


def fetch_dm_inbox(client):
    """Fetch recent Instagram DM threads with their latest messages (blocking)."""
    results = []
    try:
        threads = client.direct_threads(amount=20)
        logger.info(f"[IG DM] Fetched {len(threads)} main inbox threads")
        dbg("Main inbox thread ids: %s", [getattr(t, "id", None) for t in threads])

        # Fetch pending inbox using raw API to avoid Pydantic validation errors
        try:
            raw_pending = client.private_request(
                "direct_v2/pending_inbox/",
                params={"visual_message_return_type": "unseen", "persistentBadging": "true",
                        "is_prefetching": "false"},
            )
            logger.info(f"[DEBUG] Raw Pending Inbox: {json.dumps(raw_pending, default=str)[:1000]}")
            pending_threads = raw_pending.get("inbox", {}).get("threads", [])
            logger.info(f"[IG DM] Fetched {len(pending_threads)} pending inbox threads (raw)")
            dbg("Pending inbox thread ids: %s", [pt.get("thread_id") for pt in pending_threads])

            for pt in pending_threads:
                pt_id = pt.get("thread_id", "")
                # Auto-approve pending threads
                try:
                    client.private_request(
                        f"direct_v2/threads/{pt_id}/approve/",
                        data={},
                        with_signature=False,
                    )
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

        # Fetch hidden/spam message requests (Instagram's "Hidden Requests" folder)
        try:
            raw_spam = client.private_request(
                "direct_v2/spam_inbox/",
                params={"visual_message_return_type": "unseen", "persistentBadging": "true",
                        "is_prefetching": "false"},
            )
            logger.info(f"[DEBUG] Raw Spam Inbox: {json.dumps(raw_spam, default=str)[:1000]}")
            
            # Try filtered_inbox as another variation
            raw_filtered = client.private_request(
                "direct_v2/filtered_inbox/",
                params={"visual_message_return_type": "unseen", "persistentBadging": "true",
                        "is_prefetching": "false"},
            )
            logger.info(f"[DEBUG] Raw Filtered Inbox: {json.dumps(raw_filtered, default=str)[:1000]}")

            # Also try pending_inbox with folder=spam as a variation
            raw_pending_spam = client.private_request(
                "direct_v2/pending_inbox/",
                params={"visual_message_return_type": "unseen", "persistentBadging": "true",
                        "is_prefetching": "false", "folder": "spam"},
            )
            logger.info(f"[DEBUG] Raw Pending Spam Inbox: {json.dumps(raw_pending_spam, default=str)[:1000]}")

            spam_threads = raw_spam.get("inbox", {}).get("threads", [])
            if raw_filtered.get("inbox", {}).get("threads"):
                spam_threads.extend(raw_filtered.get("inbox", {}).get("threads"))
            if raw_pending_spam.get("inbox", {}).get("threads"):
                spam_threads.extend(raw_pending_spam.get("inbox", {}).get("threads"))
                
            logger.info(f"[IG DM] Fetched {len(spam_threads)} hidden/spam/filtered inbox threads")

            for st in spam_threads:
                st_id = st.get("thread_id", "")
                # Auto-approve hidden request threads
                try:
                    client.private_request(
                        f"direct_v2/threads/{st_id}/approve/",
                        data={},
                        with_signature=False,
                    )
                    logger.info(f"[IG DM] Approved hidden request thread {st_id}")
                except Exception as e:
                    logger.warning(f"[IG DM] Could not approve hidden request thread {st_id}: {e}")

                items = st.get("items", [])
                users = st.get("users", [])
                user_list = [(u.get("pk", 0), u.get("username", "unknown")) for u in users]
                if user_list and items:
                    results.append({
                        "thread_id": int(st_id),
                        "users": user_list,
                        "items": items,
                    })
                    dbg("Added hidden/spam thread %s users=%s item_count=%s", st_id, user_list, len(items))
        except Exception as e:
            logger.warning(f"[IG DM] Error fetching hidden/spam inbox: {e}")

        for thread in threads:
            try:
                raw = client.private_request(
                    f"direct_v2/threads/{thread.id}/",
                    params={"direction": "older", "limit": "5"},
                )
                items = raw.get("thread", {}).get("items", [])
                results.append({
                    "thread_id": int(thread.id),
                    "users": [(u.pk, u.username) for u in thread.users],
                    "items": items,
                })
                dbg("Added inbox thread %s users=%s item_count=%s", thread.id, [(u.pk, u.username) for u in thread.users], len(items))
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Error fetching DM inbox: {e}")
        err_lower = str(e).lower()
        if any(token in err_lower for token in ("loginrequired", "login_required", "challenge", "checkpoint")):
            raise
    return results


async def process_ig_dm_request(client, user_pk: int, username: str, thread_id: int, reel_url: str):
    """Process a reel link request received via Instagram DM."""
    global ig_clients

    dbg("Incoming IG DM request from @%s user_pk=%s thread_id=%s reel_url=%s", username, user_pk, thread_id, reel_url)

    shortcode = extract_shortcode(reel_url)
    if not shortcode:
        dbg("Could not parse shortcode from reel_url=%s", reel_url)
        await run_ig(client, send_dm_reply, thread_id, "Could not parse that URL. Send a valid Instagram reel link.")
        return

    ig_dm_pending[user_pk] = {"shortcode": shortcode, "thread_id": thread_id, "timestamp": time.time()}
    dbg("Marked user_pk=%s as pending with shortcode=%s", user_pk, shortcode)
    owner_id = None

    try:
        await run_ig(client, send_dm_reply, thread_id, "Got it, checking the reel now.")
        dbg("Sent processing reply to @%s", username)

        # Get reel owner
        owner_info = await run_ig(client, get_reel_owner, shortcode)
        dbg("Owner lookup result for shortcode=%s: %s", shortcode, owner_info)
        if not owner_info or "error" in owner_info:
            err = owner_info.get("error", "Unknown") if owner_info else "Unknown"
            err_lower = err.lower()
            if "loginrequired" in err_lower or "login_required" in err_lower or "challenge" in err_lower or "checkpoint" in err_lower:
                await run_ig(client, send_dm_reply, thread_id, "Instagram session expired. Attempting to reconnect...")
                ig_dm_pending.pop(user_pk, None)
                await handle_client_auth_failure(client, err)
                return

            await run_ig(client, send_dm_reply, thread_id, f"Couldn't find this reel. Check the URL and try again. Error: {html.escape(str(err))}")
            ig_dm_pending.pop(user_pk, None)
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]
        dbg("Resolved owner for shortcode=%s -> @%s (%s)", shortcode, owner_username, owner_id)

        # Auto-detect keyword
        final_keyword = await run_ig(client, get_best_keyword, shortcode)
        logger.info(f"[IG DM] Keyword for {shortcode}: '{final_keyword}'")
        dbg("Auto-detected keyword for shortcode=%s: %s", shortcode, final_keyword)

        # Follow creator
        try:
            await run_ig(client, follow_user, owner_id)
            dbg("Followed owner_id=%s (@%s)", owner_id, owner_username)
        except Exception:
            dbg("Follow failed for owner_id=%s (@%s)", owner_id, owner_username)
            pass

        # Comment on reel
        waiting_for_owners.add(owner_id)
        timestamp_before = time.time()

        try:
            await run_ig(client, comment_on_reel, shortcode, final_keyword)
            dbg("Commented on shortcode=%s with keyword=%s", shortcode, final_keyword)
            await run_ig(client, send_dm_reply, thread_id,
                f"Commented '{final_keyword}' on @{owner_username}'s reel. I’m checking the replies now.")
        except Exception as e:
            waiting_for_owners.discard(owner_id)
            ig_dm_pending.pop(user_pk, None)
            await run_ig(client, send_dm_reply, thread_id, f"Couldn't comment on the reel: {html.escape(str(e))}")
            return

        # Poll for DM response from reel owner
        link_found = None
        elapsed = 0
        fallback_tried = False
        while elapsed < DM_WAIT_TIME:
            try:
                link_found = await run_ig(client, check_dms_for_link, owner_id, timestamp_before, shortcode)
                dbg(
                    "Poll %s for owner_id=%s returned %s",
                    elapsed // DM_CHECK_INTERVAL,
                    owner_id,
                    link_found,
                )
                if link_found:
                    break
            except Exception as e:
                logger.error(f"[IG DM] Poll error for @{username}: {e}")
                dbg("Poll error for owner_id=%s: %s", owner_id, e)

            if (
                not link_found
                and not fallback_tried
                and elapsed >= EARLY_MANYCHAT_FALLBACK_SECONDS
            ):
                fallback_tried = True
                await run_ig(client, send_dm_reply, thread_id, "Trying another route to open the DM card...")
                dbg("Starting early Playwright fallback for owner_id=%s after %ss", owner_id, elapsed)
                link_found = await playwright_manychat_fallback(client, owner_username, owner_id)
                dbg("Early Playwright fallback returned %s for owner_id=%s", link_found, owner_id)
                if link_found:
                    break

            await asyncio.sleep(DM_CHECK_INTERVAL)
            elapsed += DM_CHECK_INTERVAL

        if not link_found:
            await run_ig(client, send_dm_reply, thread_id, "Still checking the reply path...")
            dbg("No DM link found for owner_id=%s, starting Playwright fallback", owner_id)
            link_found = await playwright_manychat_fallback(client, owner_username, owner_id)
            dbg("Playwright fallback returned %s for owner_id=%s", link_found, owner_id)

        # Cleanup
        waiting_for_owners.discard(owner_id)
        ig_dm_pending.pop(user_pk, None)

        # Send result
        if link_found:
            db_cache.save_cached_link(shortcode, link_found)
            await run_ig(client, send_dm_reply, thread_id, f"Here's your link:\n\n{link_found}")
            logger.info(f"[IG DM] Got link for @{username}: {link_found}")
            dbg("Sent final link reply to @%s: %s", username, link_found)
        else:
            await run_ig(client, send_dm_reply, thread_id,
                f"I couldn't catch a reply from @{owner_username} yet. If it shows up, I'll pick it up automatically.")
            dbg("No link found for owner_id=%s (@%s)", owner_id, owner_username)

    except Exception as e:
        logger.error(f"[IG DM] Error processing request from @{username}: {e}")
        if owner_id:
            waiting_for_owners.discard(owner_id)
        ig_dm_pending.pop(user_pk, None)
        try:
            await run_ig(client, send_dm_reply, thread_id, "Something went wrong. Please try again later.")
        except Exception:
            pass


async def ig_dm_listener():
    """Background task: polls Instagram DMs for new reel link requests."""
    global ig_dm_last_check

    await asyncio.sleep(20)  # let login settle
    first_scan = True
    logger.info("Instagram DM listener started")
    dbg("IG DM listener entered main loop")

    while True:
        try:
            if len(ig_clients) == 0:
                await asyncio.sleep(IG_DM_CHECK_INTERVAL)
                continue

            # Cap processed set to prevent memory leak
            if len(ig_dm_processed) > 5000:
                ig_dm_processed.clear()

            all_threads = []
            for client in ig_clients.copy():
                try:
                    tds = await run_ig(client, fetch_dm_inbox)
                    for td in tds:
                        td['client'] = client
                    all_threads.extend(tds)
                except Exception as e:
                    logger.error(f"[IG DM] Client fetch failed: {e}")
                    dbg("Client fetch failed for @%s: %s", getattr(client, "username", "unknown"), e)
                    err_lower = str(e).lower()
                    if any(token in err_lower for token in ("loginrequired", "login_required", "challenge", "checkpoint")):
                        await handle_client_auth_failure(client, str(e))
            threads_data = all_threads
            logger.info(f"[IG DM] Scanned {len(threads_data)} threads across {len(ig_clients)} clients, ig_dm_last_check={ig_dm_last_check:.0f}")
            dbg("Listener scan complete: threads=%s clients=%s", len(threads_data), len(ig_clients))

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
                    dbg("Inspecting thread_id=%s @%s user_pk=%s item_count=%s", thread_id, username, user_pk, len(items))

                for idx, item in enumerate(items):
                    item_id = item.get("item_id", "")
                    if item_id in ig_dm_processed:
                        continue

                    sender = item.get("user_id")
                    item_type = item.get("item_type", "unknown")
                    client_user_id = str(td['client'].user_id) if hasattr(td.get('client', None), 'user_id') else ''
                    logger.info(f"[IG DM] @{username} item #{idx}: type={item_type}, sender={sender}")
                    if str(sender) == client_user_id:
                        ig_dm_processed.add(item_id)
                        continue

                    # On the very first scan, only process the newest message per thread
                    # to avoid replaying old conversations
                    if first_scan and idx > 0:
                        ig_dm_processed.add(item_id)
                        continue

                    item_type = item.get("item_type", "")
                    logger.info(f"[IG DM] Message from @{username}: type={item_type}, item_id={item_id}")
                    dbg("Item detail thread_id=%s item_id=%s type=%s sender=%s keys=%s", thread_id, item_id, item_type, sender, list(item.keys()))

                    reel_url = None

                    # Handle shared reels (when user taps share button on a reel)
                    if item_type in ("media_share", "clip", "felix_share", "reel_share", "story_share", "xma_clip", "xma_reel_share", "xma_media_share"):
                        reel_url = extract_reel_url_from_shared_item(item)
                        if reel_url:
                            logger.info(f"[IG DM] Extracted reel from share: {reel_url}")
                            dbg("Shared media item resolved to reel_url=%s for item_type=%s", reel_url, item_type)

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
                    elif item_type in ("generic_xma", "xma_link"):
                        # Log raw xma data for debugging
                        xma_data = item.get("generic_xma") or item.get("xma_media_share") or item.get("xma_link") or []
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
                        dbg("Detected reel request from @%s -> %s", username, reel_url)
                        create_logged_task(
                            process_ig_dm_request(td['client'], user_pk, username, thread_id, reel_url),
                            f"IG DM request @{username}",
                        )
                        break  # one request per user at a time
                    elif item_type == "text" and (item.get("text", "") or "").strip():
                        # User sent text that's not a reel URL — send help
                        try:
                            await run_ig(td['client'], send_dm_reply, thread_id,
                                "Hey! Send me an Instagram reel link and I'll get the hidden resource link for you.\n\nYou can either share a reel directly or paste a link like:\nhttps://www.instagram.com/reel/ABC123/")
                        except Exception:
                            pass
                        break

        except Exception as e:
            logger.error(f"[IG DM] Listener error: {e}")

        first_scan = False
        await asyncio.sleep(IG_DM_CHECK_INTERVAL)


async def action_queue_worker():
    """Background task that pulls from action_queue and paces out Instagram comments."""
    logger.info("Action Queue Worker started")
    while True:
        try:
            if action_queue is None:
                await asyncio.sleep(1)
                continue
                
            task = await action_queue.get()
            shortcode = task["shortcode"]
            client = task["client"]
            owner_id = task["owner_id"]
            final_keyword = task["final_keyword"]
            status_msg = task["status_msg"]

            # Wait a random human-like delay before commenting
            # if the queue was backed up, pacing happens naturally
            delay = random.uniform(15.0, 45.0)
            
            try:
                await status_msg.edit_text(
                    f"🚶‍♂️ <b>Human Pacing...</b> Waiting {int(delay)}s to avoid spam filters.",
                    parse_mode="HTML"
                )
            except Exception:
                pass
                
            await asyncio.sleep(delay)

            try:
                await run_ig(client, follow_user, owner_id)
            except Exception:
                pass
                
            success = False
            error_msg = ""
            try:
                success = await run_ig(client, comment_on_reel, shortcode, final_keyword)
            except Exception as e:
                error_msg = str(e)
                
            action_results[shortcode] = {"success": success, "error": error_msg}
            
            # Unblock handle_message
            if shortcode in action_events:
                action_events[shortcode].set()
                
            action_queue.task_done()
        except Exception as e:
            logger.error(f"Action Queue Worker error: {e}")
            await asyncio.sleep(5)


# ─── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 <b>Welcome to the Reel Link Bot!</b>\n\n"
        "Send me any Instagram Reel URL and I'll get the link for you!\n\n"
        "🔄 <b>How it works:</b>\n"
        "1. You send the reel URL\n"
        "2. I auto-detect the keyword from comments\n"
        "3. I follow the creator & comment for you\n"
        "4. The creator DMs me the link\n"
        "5. I send the link back to you!\n\n"
        "📌 <b>Commands:</b>\n"
        "/start — Show this message\n"
        "/status — Check bot status\n"
        "/restart — Re-login to Instagram\n\n"
        "<b>Try it:</b> Just paste a reel URL!",
        parse_mode="HTML"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    active_usernames = get_active_ig_usernames()
    if len(ig_clients) > 0:
        await update.message.reply_text(
            f"✅ Bot is <b>online</b>\n"
            f"📸 Instagram: <code>@{', @'.join(active_usernames) if active_usernames else BOT_INSTAGRAM_USERNAME}</code>\n"
            f"🧪 IG debug: <b>{'on' if DEBUG_IG else 'off'}</b>\n"
            f"📊 Telegram requests: {len(pending_requests)}\n"
            f"📬 IG DM requests: {len(ig_dm_pending)}\n"
            f"🔧 Max concurrent: {MAX_CONCURRENT_IG_CALLS}",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            "❌ Instagram is <b>disconnected</b>.\n"
            "The bot will try to reconnect on the next request.",
            parse_mode="HTML"
        )


async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Re-login to Instagram and clear pending requests."""
    global ig_clients, pending_requests, ig_dm_last_check
    
    user_id = update.effective_user.id
    if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("⛔ You are not authorized to use this command.")
        return

    msg = await update.message.reply_text("🔄 Restarting Instagram session...")

    # Clear pending requests
    pending_requests.clear()
    ig_dm_pending.clear()
    ig_dm_processed.clear()
    clicked_postback_items.clear()
    waiting_for_owners.clear()

    # Delete old session and re-login
    async with ig_lock:
        ig_clients.clear()
        session_file = "ig_session.json"
        if os.path.exists(session_file):
            try:
                os.remove(session_file)
            except OSError:
                pass

        loop = asyncio.get_event_loop()
        await loop.run_in_executor(thread_pool, login_instagram)

    if len(ig_clients) > 0:
        ig_dm_last_check = time.time()
        await msg.edit_text(
            f"✅ <b>Restarted successfully!</b>\n\n"
            f"📸 Instagram: <code>@{BOT_INSTAGRAM_USERNAME}</code>\n"
            f"🧹 Pending requests cleared",
            parse_mode="HTML"
        )
    else:
        await msg.edit_text(
            "❌ <b>Restart failed</b> — Instagram login error.\n"
            "Check credentials and try again.",
            parse_mode="HTML"
        )


async def restart_async_only() -> bool:
    """Restart Instagram state without requiring a Telegram update object."""
    global ig_clients, ig_dm_last_check

    async with ig_lock:
        ig_clients.clear()
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(thread_pool, login_instagram)

    if len(ig_clients) > 0:
        ig_dm_last_check = time.time()
        logger.info("Instagram reconnected successfully")
        return True

    logger.warning("Instagram reconnect failed")
    return False


async def set_session_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Update the IG_SESSION_IDS in the .env file directly from Telegram."""
    user_id = update.effective_user.id
    if ADMIN_USER_IDS and user_id not in ADMIN_USER_IDS:
        await update.message.reply_text("⛔ You are not authorized to use this command.")
        return

    if not context.args:
        await update.message.reply_text(
            "⚠️ Please provide a session ID.\n"
            "Usage: `/set_session <new_session_id>`",
            parse_mode="Markdown"
        )
        return

    new_session = " ".join(context.args).strip()
    
    # Update .env file locally
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    env_lines = []
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            env_lines = f.readlines()
            
    found = False
    for i, line in enumerate(env_lines):
        if line.startswith("IG_SESSION_IDS=") or line.startswith("IG_SESSION_ID="):
            env_lines[i] = f"IG_SESSION_IDS={new_session}\n"
            found = True
            
    if not found:
        env_lines.append(f"\nIG_SESSION_IDS={new_session}\n")
        
    with open(env_path, "w") as f:
        f.writelines(env_lines)
        
    # Update process environment
    os.environ["IG_SESSION_IDS"] = new_session

    await update.message.reply_text("✅ Session ID saved! Restarting...")
    # Trigger restart to apply new session
    await restart(update, context)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main handler: user sends a reel URL → bot comments → waits for DM → sends link back."""
    global ig_clients

    text = update.message.text or ""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"

    # Extract the Instagram URL from the message
    url = extract_reel_url(text)

    if not url:
        await update.message.reply_text(
            "🔗 Please send a valid Instagram Reel URL.\n\n"
            "<b>Just paste the link</b> — I'll auto-detect the keyword!\n\n"
            "Example:\n<code>https://www.instagram.com/reel/ABC123xyz/</code>",
            parse_mode="HTML"
        )
        return

    shortcode = extract_shortcode(url)
    if not shortcode:
        await update.message.reply_text("❌ Couldn't parse that URL. Make sure it's a valid reel link.")
        return

    # MVP Step 1: Check Database Cache First
    cached_link = db_cache.get_cached_link(shortcode)
    if cached_link:
        await update.message.reply_text(
            f"⚡ <b>Instant Delivery!</b> I found this link in my cache:\n\n"
            f"🔗 {cached_link}\n\n"
            f"<i>(Saved you the wait!)</i>",
            parse_mode="HTML"
        )
        db_cache.track_user_request(user_id, user_name)
        return

    # Check if user already has a pending request
    if user_id in pending_requests:
        await update.message.reply_text("⏳ You already have a request in progress. Please wait for it to finish.")
        return

    # Step 1: Get next available client in rotation
    try:
        client = await get_next_client()
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {e}")
        return
        
    status_msg = await update.message.reply_text(
        f"⏳ <b>Checking the reel...</b>",
        parse_mode="HTML"
    )

    if not await ensure_logged_in_async():
        await status_msg.edit_text(
            "❌ Instagram login failed. The bot admin needs to check credentials.\n"
            "Try again later.",
            parse_mode="HTML"
        )
        return

    # Step 2: Get reel owner info
    try:
        owner_info = await run_ig(client, get_reel_owner, shortcode)
        if not owner_info or "error" in owner_info:
            err = owner_info.get("error", "Unknown") if owner_info else "Unknown"
            err_lower = err.lower()
            if "loginrequired" in err_lower or "login_required" in err_lower or "challenge" in err_lower or "checkpoint" in err_lower:
                await status_msg.edit_text(
                    "⚠️ Instagram session expired or IP blocked. Reconnecting...\nPlease try sending the link again.",
                    parse_mode="HTML"
                )
                await handle_client_auth_failure(client, err)
                return

            await status_msg.edit_text(
                f"❌ Couldn't find information about this reel.\n\nError: <code>{html.escape(str(err))}</code>",
                parse_mode="HTML"
            )
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {html.escape(str(e))}")
        return

    # Step 2.5: Auto-detect keyword from comments
    final_keyword = await run_ig(client, get_best_keyword, shortcode)
    logger.info(f"Auto-detected keyword for {shortcode}: '{final_keyword}'")
    
    # --- MVP Step 2: Queue the Action ---
    global action_queue
    queue_pos = action_queue.qsize() + 1
    
    try:
        await status_msg.edit_text(
            f"📝 <b>Added to Action Queue</b> (Position: #{queue_pos})\n\n"
            f"I'll comment on this reel shortly using the keyword: '<code>{final_keyword}</code>'.",
            parse_mode="HTML"
        )
    except Exception:
        pass

    action_events[shortcode] = asyncio.Event()
    
    await action_queue.put({
        "shortcode": shortcode,
        "client": client,
        "owner_id": owner_id,
        "final_keyword": final_keyword,
        "status_msg": status_msg
    })
    
    # Wait for the worker to process it
    await action_events[shortcode].wait()
    
    result = action_results.get(shortcode, {"success": False, "error": "Unknown"})
    
    # Cleanup events
    action_events.pop(shortcode, None)
    action_results.pop(shortcode, None)
    
    if not result["success"]:
        pending_requests.pop(user_id, None)
        waiting_for_owners.discard(owner_id)
        
        err = result["error"]
        if "loginrequired" in err.lower() or "challenge" in err.lower():
            await status_msg.edit_text(
                "⚠️ Instagram session expired. Reconnecting...\nPlease try sending the link again.",
                parse_mode="HTML"
            )
            await handle_client_auth_failure(client, err)
        else:
            await status_msg.edit_text(
                f"❌ Couldn't comment on the reel.\n\n"
                f"<b>Reason:</b> {html.escape(err)}\n\n"
                f"The reel might be private, or comments might be disabled.",
                parse_mode="HTML"
            )
        return
        
    # Comment succeeded!
    waiting_for_owners.add(owner_id)
    timestamp_before_comment = time.time()
    pending_requests[user_id] = {"shortcode": shortcode, "timestamp": timestamp_before_comment}
    
    await status_msg.edit_text(
        f"✅ <b>Done!</b> I’ve left the comment and I’m watching @{owner_username}.",
        parse_mode="HTML"
    )
    # --- End MVP Step 2 Queueing ---

    # Step 4: Poll DMs for the response link
    link_found = None
    elapsed = 0
    fallback_tried = False

    while elapsed < DM_WAIT_TIME:
        try:
            link_found = await run_ig(client, check_dms_for_link, owner_id, timestamp_before_comment, shortcode)
            if link_found:
                break
        except Exception as e:
            logger.error(f"Error polling DMs for user {user_id}: {e}")

        if (
            not link_found
            and not fallback_tried
            and elapsed >= EARLY_MANYCHAT_FALLBACK_SECONDS
        ):
            fallback_tried = True
            try:
                await status_msg.edit_text(
                    f"I’m trying another route with @{owner_username} now...",
                    parse_mode="HTML"
                )
            except Exception:
                pass
            link_found = await playwright_manychat_fallback(client, owner_username, owner_id)
            if link_found:
                break

        # Update status every ~10 minutes (600 sec) to avoid hitting Telegram rate limits
        if elapsed > 0 and elapsed % 600 < DM_CHECK_INTERVAL:
            try:
                await status_msg.edit_text(
                    f"✅ Commented <code>{final_keyword}</code> on @{owner_username}'s reel\n"
                    f"⏳ Still checking the replies...\n\n"
                    f"<i>I’ll send it over when it shows up.</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
                
        await asyncio.sleep(DM_CHECK_INTERVAL)
        elapsed += DM_CHECK_INTERVAL

        if not link_found:
            try:
                await status_msg.edit_text(
                    f"Still checking @{owner_username}. I’m trying another reply path now...",
                    parse_mode="HTML"
                )
            except Exception:
                pass
        link_found = await playwright_manychat_fallback(client, owner_username, owner_id)

    # Clean up pending request
    pending_requests.pop(user_id, None)
    waiting_for_owners.discard(owner_id)

    # Step 5: Send result to user and unfollow creator
    if link_found:
        db_cache.save_cached_link(shortcode, link_found)
        await status_msg.edit_text(
            f"✅ <b>Got it!</b> Here's your link:\n\n"
            f"🔗 {link_found}\n\n"
            f"<i>(Sent by @{owner_username})</i>",
            parse_mode="HTML"
        )
        logger.info(f"Successfully got link for user {user_id}: {link_found}")

        # Unfollow the creator after a long delay (2-5 mins) to look natural
        try:
            unfollow_delay = random.uniform(120, 300)
            logger.info(f"[Stealth] Will unfollow creator {owner_id} in {unfollow_delay:.1f}s")
            
            async def delayed_unfollow(cid, uid, delay):
                await asyncio.sleep(delay)
                try:
                    await run_ig(cid, unfollow_user, uid)
                    logger.info(f"[Stealth] Unfollowed creator {uid} (delayed)")
                except Exception:
                    pass
            
            create_logged_task(
                delayed_unfollow(client, owner_id, unfollow_delay),
                f"delayed unfollow {owner_id}",
            )
        except Exception as e:
            logger.warning(f"Error scheduling unfollow: {e}")
        else:
            await update.message.reply_text(
                f"⏰ <b>No reply yet</b> from @{owner_username}.\n\n"
                f"I’ll keep the thread ready, but this reel may not have an active automation reply.",
                parse_mode="HTML"
            )
        # Still unfollow after delay even on timeout
        try:
            unfollow_delay = random.uniform(60, 180)
            async def delayed_unfollow(cid, uid, delay):
                await asyncio.sleep(delay)
                try:
                    await run_ig(cid, unfollow_user, uid)
                except Exception:
                    pass
            create_logged_task(
                delayed_unfollow(client, owner_id, unfollow_delay),
                f"delayed unfollow {owner_id}",
            )
        except Exception:
            pass
        except Exception:
            pass


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    """Global error handler — logs errors without crashing the bot."""
    logger.error(f"Exception while handling an update: {context.error}")
    logger.error(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))

    # Try to notify the user if possible
    if isinstance(update, Update) and update.effective_message:
        try:
            await update.effective_message.reply_text(
                "⚠️ Something went wrong processing your request. Please try again.",
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


def _is_our_bot_process(pid: int) -> bool:
    """Return True if the PID appears to belong to this bot process."""
    if sys.platform == "win32":
        try:
            cmd = subprocess.check_output(
                [
                    "powershell",
                    "-NoProfile",
                    "-Command",
                    f"(Get-CimInstance Win32_Process -Filter \"ProcessId={pid}\").CommandLine",
                ],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
            return "bot.py" in cmd and "python" in cmd.lower()
        except Exception:
            return False

    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            cmdline = f.read().decode("utf-8", errors="ignore").replace("\x00", " ")
        return "bot.py" in cmdline and "python" in cmdline.lower()
    except Exception:
        return False


def _kill_other_bot_processes():
    """Kill ALL other python bot.py processes except ourselves."""
    my_pid = os.getpid()
    killed = []
    if sys.platform == "win32":
        return killed  # Windows: rely on lockfile only
    try:
        # Use /proc to find all python bot.py processes
        for entry in os.listdir("/proc"):
            if not entry.isdigit():
                continue
            pid = int(entry)
            if pid == my_pid:
                continue
            try:
                with open(f"/proc/{pid}/cmdline", "rb") as f:
                    cmdline = f.read().decode("utf-8", errors="ignore").replace("\x00", " ")
                if "bot.py" in cmdline and "python" in cmdline.lower():
                    logger.warning("Killing duplicate bot process PID %s: %s", pid, cmdline.strip())
                    os.kill(pid, 9)  # SIGKILL
                    killed.append(pid)
            except (OSError, PermissionError):
                continue
    except Exception as e:
        logger.warning("Error scanning for duplicate processes: %s", e)
    return killed


def acquire_lock():
    """Ensure only one bot instance runs at a time using a lockfile with PID check."""
    # Step 1: Always kill any other bot.py processes (belt-and-suspenders)
    killed = _kill_other_bot_processes()
    if killed:
        print(f"🔪 Killed {len(killed)} duplicate bot process(es): {killed}")
        time.sleep(2)  # Give OS time to clean up

    # Step 2: Check the lockfile
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
            if _is_pid_alive(old_pid) and old_pid != os.getpid():
                # The kill above should have handled this, but double-check
                if _is_our_bot_process(old_pid):
                    print(f"❌ Another bot instance is STILL running (PID {old_pid}) after kill attempt.")
                    print("   This should not happen. Force-killing again...")
                    try:
                        os.kill(old_pid, 9)
                        time.sleep(2)
                    except OSError:
                        pass
                else:
                    print(f"🧹 Removing stale lockfile (PID {old_pid} is not a bot process)")
            else:
                print(f"🧹 Removing stale lockfile (PID {old_pid} is gone)")
        except (ValueError, OSError):
            print("🧹 Removing invalid lockfile")

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))
    print(f"🔒 Lock acquired (PID {os.getpid()})")


def release_lock():
    """Remove the lockfile on clean exit."""
    try:
        os.remove(LOCK_FILE)
    except OSError:
        pass


def main():
    global ig_lock, ig_semaphore, thread_pool

    missing = []
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        missing.append("TELEGRAM_BOT_TOKEN")
    # Username/password not needed when using session IDs.
    ig_session_id = os.environ.get("IG_SESSION_ID", "").strip()
    ig_session_ids = os.environ.get("IG_SESSION_IDS", "").strip()
    if not ig_session_id and not ig_session_ids:
        if not BOT_INSTAGRAM_USERNAME or BOT_INSTAGRAM_USERNAME == "YOUR_BOT_INSTAGRAM_USERNAME":
            missing.append("BOT_INSTAGRAM_USERNAME")
        if not BOT_INSTAGRAM_PASSWORD or BOT_INSTAGRAM_PASSWORD == "YOUR_BOT_INSTAGRAM_PASSWORD":
            missing.append("BOT_INSTAGRAM_PASSWORD")

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

    # Initialize MVP Step 1 database
    db_cache.init_db()

    print("🔐 Logging into Instagram...")
    login_instagram()
    if len(ig_clients) > 0:
        active_usernames = ", @".join(get_active_ig_usernames()) or BOT_INSTAGRAM_USERNAME
        print(f"✅ Connected as @{active_usernames}")
    else:
        print("⚠️ No Instagram clients logged in — will retry on first request")

    async def post_init(application):
        """Start the background tasks."""
        global action_queue
        action_queue = asyncio.Queue()
        
        create_logged_task(
            supervise_background_task("Instagram DM listener", ig_dm_listener),
            "Instagram DM listener supervisor",
        )
        create_logged_task(
            supervise_background_task("Action Queue Worker", action_queue_worker),
            "Action Queue Worker supervisor",
        )

    def build_app():
        app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("status", status_cmd))
        app.add_handler(CommandHandler("restart", restart))
        app.add_handler(CommandHandler("set_session", set_session_cmd))
        app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        app.add_error_handler(error_handler)
        return app

    print("")
    print("🤖 Reel Link Bot is running!")
    print(f"⚡ Concurrency: {MAX_CONCURRENT_IG_CALLS} simultaneous IG calls, {THREAD_POOL_SIZE} threads")
    print("📬 Instagram DM listener: active")
    print("📱 Open Telegram → Send /start to your bot")
    print("🛑 Press Ctrl+C to stop")
    print("")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Initialize async primitives on the event loop
    ig_lock = asyncio.Lock()
    ig_semaphore = asyncio.Semaphore(MAX_CONCURRENT_IG_CALLS)

    app = build_app()
    try:
        app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)
    except KeyboardInterrupt:
        raise
    except Exception:
        logger.exception("Telegram polling loop crashed. Exiting to allow start_bot.sh to restart cleanly.")
        sys.exit(1)


if __name__ == "__main__":
    main()
