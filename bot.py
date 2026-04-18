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
from urllib.parse import parse_qs, unquote, urlparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
import instagrapi
from instagrapi import Client
from collections import Counter
from pw_engine import pw_intercept_manychat

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
DM_CHECK_INTERVAL = 10   # check DMs every N seconds

# STEP 5: Concurrency settings
MAX_CONCURRENT_IG_CALLS = 3   # max simultaneous Instagram API operations
THREAD_POOL_SIZE = 10          # thread pool workers for blocking IG calls
PW_MANYCHAT_TIMEOUT = int(os.environ.get("PW_MANYCHAT_TIMEOUT", "90"))
IG_PROXY = os.environ.get("IG_PROXY", "")

# ──────────────────────────────────────────────────────────────────────────────

ig_clients = []

# Track pending requests: {telegram_user_id: {"reel_url": ..., "timestamp": ...}}
pending_requests = {}

# Instagram DM listener state
ig_dm_pending = {}          # {ig_user_pk: {"shortcode": ..., "thread_id": ..., "timestamp": ...}}
clicked_postback_items = set()
ig_dm_processed = set()     # Set of processed DM item_ids to avoid re-processing
ig_dm_last_check = 0.0      # Timestamp of last DM check
waiting_for_owners = set()  # Reel owner user IDs we're currently waiting for DM responses
IG_DM_CHECK_INTERVAL = 15   # How often to check for new IG DM requests (seconds)

# Concurrency primitives (initialized in main)
ig_lock = None           # asyncio.Lock — protects login/session state
ig_semaphore = None      # asyncio.Semaphore — limits concurrent IG API calls
thread_pool = None       # ThreadPoolExecutor — runs blocking IG calls


async def run_ig(client, func, *args):
    """Run a blocking Instagram API call in the thread pool, respecting the semaphore."""
    loop = asyncio.get_event_loop()
    async with ig_semaphore:
        return await loop.run_in_executor(thread_pool, func, client, *args)

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
    """Comment on a reel and return True if successful."""
    try:
        import random, time
        media_pk = client.media_pk_from_code(shortcode)
        media_id = client.media_id(media_pk)
        
        # Human-like behavior: Wait randomly
        delay = random.uniform(3, 8)
        logger.info(f"Watching reel for {delay:.1f} seconds...")
        time.sleep(delay)
        
        # Human-like behavior: Like the reel before commenting
        try:
             client.media_like(media_id)
             logger.info(f"Liked reel {shortcode}")
        except Exception as e:
             logger.warning(f"Failed to like reel: {e}")
             
        time.sleep(random.uniform(1, 4))
        
        client.media_comment(media_id, comment_text)
        logger.info(f"Commented '{comment_text}' on reel {shortcode}")
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


def check_dms_for_link(client, reel_owner_id: int, after_timestamp: float, requested_shortcode: str = '') -> str | None:
    """
    Check Instagram DMs for a new message from the reel owner.
    Handles generic_xma (ManyChat cards with CTA buttons), text, and link messages.
    Returns the link/text if found, None otherwise.
    """
    try:
        # Check both main inbox and message requests
        threads = client.direct_threads(amount=20)
        try:
            pending = client.direct_pending_inbox(amount=20)
            threads.extend(pending)
        except Exception as e:
            logger.warning(f"Error checking pending inbox: {e}")

        for thread in threads:
            # Check if this thread involves the reel owner
            for user in thread.users:
                if str(user.pk) == str(reel_owner_id):
                    # Found the right thread — use raw API to get full message data
                    try:
                        result = client.private_request(
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

                        # Handle generic_xma (ManyChat/automation cards)
                        if item_type == "generic_xma":
                            xma_list = item.get("generic_xma", [])
                            if isinstance(xma_list, dict):
                                xma_list = [xma_list]
                            for xma in xma_list:
                                cta_buttons = xma.get("cta_buttons", [])
                                for btn in cta_buttons:
                                    action_url = btn.get("action_url", "")
                                    if action_url and action_url.startswith("http"):
                                        found_url = unwrap_instagram_redirect(action_url)
                                        # Fix: don't return if the URL is just the exact same reel we requested!
                                        if requested_shortcode and requested_shortcode in found_url and 'instagram.com' in found_url:
                                            logger.info(f"Ignored CTA link because it loops back to requested reel: {found_url}")
                                            continue
                                        logger.info(f"Found link in CTA button: {found_url}")
                                        return found_url

                                # If CTA has postback but no URL, reply with
                                # the button title text to trigger ManyChat.
                                # (The old xma_postback endpoint was deprecated
                                #  by Instagram and returns 404.)
                                item_id = item.get("item_id")
                                if item_id and item_id not in clicked_postback_items:
                                    for btn in cta_buttons:
                                        btn_title = btn.get("title", "")
                                        platform_token = btn.get("platform_token", {})
                                        postback = platform_token.get("postback", {})
                                        payload = postback.get("postback_payload", "")
                                        if payload and btn_title:
                                            try:
                                                logger.info(f"Replying with button text to trigger automation: '{btn_title}'")
                                                client.direct_answer(
                                                    thread_id=int(thread.id),
                                                    text=btn_title,
                                                )
                                                clicked_postback_items.add(item_id)
                                                logger.info("Reply sent, will check for follow-up message next poll")
                                                break
                                            except Exception as e:
                                                logger.error(f"Failed to reply with button text: {e}")

                                # Also check the title text for URLs
                                title = xma.get("title_text", "") or ""
                                urls = extract_urls_from_text(title)
                                if urls:
                                    for u in urls:
                                        if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                            continue
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
                                    return u

                        fallback_url = first_deep_url(item_data, requested_shortcode)
                        if fallback_url:
                            logger.info(f"Found link by recursive DM inspection: {fallback_url}")
                            return fallback_url

        return None
    except Exception as e:
        logger.error(f"Error checking DMs: {e}")
        return None


# ─── INSTAGRAM DM HANDLING ────────────────────────────────────────────────────

def send_dm_reply(client, thread_id: int, text: str):
    """Send a reply in an Instagram DM thread."""
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

        # Fetch pending inbox using raw API to avoid Pydantic validation errors
        try:
            raw_pending = client.private_request(
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
            except Exception:
                continue
    except Exception as e:
        logger.error(f"Error fetching DM inbox: {e}")
    return results


async def process_ig_dm_request(client, user_pk: int, username: str, thread_id: int, reel_url: str):
    """Process a reel link request received via Instagram DM."""
    global ig_clients

    shortcode = extract_shortcode(reel_url)
    if not shortcode:
        await run_ig(client, send_dm_reply, thread_id, "Could not parse that URL. Send a valid Instagram reel link.")
        return

    ig_dm_pending[user_pk] = {"shortcode": shortcode, "thread_id": thread_id, "timestamp": time.time()}
    owner_id = None

    try:
        await run_ig(client, send_dm_reply, thread_id, "Processing your reel link request...")

        # Get reel owner
        owner_info = await run_ig(client, get_reel_owner, shortcode)
        if not owner_info or "error" in owner_info:
            err = owner_info.get("error", "Unknown") if owner_info else "Unknown"
            err_lower = err.lower()
            if "loginrequired" in err_lower or "login_required" in err_lower or "challenge" in err_lower or "checkpoint" in err_lower:
                ig_clients.clear()
                await run_ig(client, send_dm_reply, thread_id, "Instagram session expired. Attempting to reconnect...")
                ig_dm_pending.pop(user_pk, None)
                await ensure_logged_in_async()
                return

            await run_ig(client, send_dm_reply, thread_id, f"Couldn't find this reel. Check the URL and try again. Error: {html.escape(str(err))}")
            ig_dm_pending.pop(user_pk, None)
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

        # Auto-detect keyword
        final_keyword = await run_ig(client, get_best_keyword, shortcode)
        logger.info(f"[IG DM] Keyword for {shortcode}: '{final_keyword}'")

        # Follow creator
        try:
            await run_ig(client, follow_user, owner_id)
        except Exception:
            pass

        # Comment on reel
        waiting_for_owners.add(owner_id)
        timestamp_before = time.time()

        try:
            await run_ig(client, comment_on_reel, shortcode, final_keyword)
            await run_ig(client, send_dm_reply, thread_id,
                f"Commented '{final_keyword}' on @{owner_username}'s reel. Waiting for the link...")
        except Exception as e:
            waiting_for_owners.discard(owner_id)
            ig_dm_pending.pop(user_pk, None)
            await run_ig(client, send_dm_reply, thread_id, f"Couldn't comment on the reel: {html.escape(str(e))}")
            return

        # Poll for DM response from reel owner
        link_found = None
        elapsed = 0
        while elapsed < DM_WAIT_TIME:
            try:
                link_found = await run_ig(client, check_dms_for_link, owner_id, timestamp_before, shortcode)
                if link_found:
                    break
            except Exception as e:
                logger.error(f"[IG DM] Poll error for @{username}: {e}")

            await asyncio.sleep(DM_CHECK_INTERVAL)
            elapsed += DM_CHECK_INTERVAL

        if not link_found:
            await run_ig(client, send_dm_reply, thread_id, "Trying the Instagram Web button flow...")
            link_found = await playwright_manychat_fallback(client, owner_username, owner_id)

        # Cleanup
        waiting_for_owners.discard(owner_id)
        ig_dm_pending.pop(user_pk, None)

        # Send result
        if link_found:
            await run_ig(client, send_dm_reply, thread_id, f"Here's your link:\n\n{link_found}")
            logger.info(f"[IG DM] Got link for @{username}: {link_found}")
        else:
            await run_ig(client, send_dm_reply, thread_id,
                f"No response from @{owner_username}. They might not have automation set up, or the keyword '{final_keyword}' was wrong.")

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
            threads_data = all_threads
            logger.info(f"[IG DM] Scanned {len(threads_data)} threads across {len(ig_clients)} clients, ig_dm_last_check={ig_dm_last_check:.0f}")

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
                    elif item_type in ("xma_media_share", "generic_xma", "xma_link", "xma_reel_share"):
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
                        asyncio.create_task(
                            process_ig_dm_request(td['client'], user_pk, username, thread_id, reel_url)
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
    if len(ig_clients) > 0:
        await update.message.reply_text(
            f"✅ Bot is <b>online</b>\n"
            f"📸 Instagram: <code>@{BOT_INSTAGRAM_USERNAME}</code>\n"
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

    msg = await update.message.reply_text("🔄 Restarting Instagram session...")

    # Clear pending requests
    pending_requests.clear()
    ig_dm_pending.clear()
    ig_dm_processed.clear()
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

    # Check if user already has a pending request
    if user_id in pending_requests:
        await update.message.reply_text("⏳ You already have a request in progress. Please wait for it to finish.")
        return

    # Step 1: Ensure Instagram is connected
    try:
        client = await get_random_client()
    except Exception as e:
        await update.message.reply_text("❌ No Instagram clients available.")
        return
        
    status_msg = await update.message.reply_text(
        f"⏳ Processing your request...\n\n"
        f"🔗 Reel: <code>...{shortcode}</code>\n"
        f"🔍 Auto-detecting keyword...",
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
                ig_clients.clear()
                await status_msg.edit_text(
                    "⚠️ Instagram session expired or IP blocked. Reconnecting...\nPlease try sending the link again.",
                    parse_mode="HTML"
                )
                await ensure_logged_in_async()
                return

            await status_msg.edit_text(
                f"❌ Couldn't find information about this reel.\n\nError: <code>{html.escape(str(err))}</code>",
                parse_mode="HTML"
            )
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

        await status_msg.edit_text(
            f"✅ Found reel by <b>@{owner_username}</b>\n"
            f"💬 Preparing...",
            parse_mode="HTML"
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ Error finding reel: {html.escape(str(e))}")
        return

    # Step 2.5: Auto-detect keyword from comments and follow creator
    final_keyword = await run_ig(client, get_best_keyword, shortcode)
    logger.info(f"Auto-detected keyword for {shortcode}: '{final_keyword}'")

    await status_msg.edit_text(
        f"✅ Found reel by <b>@{owner_username}</b>\n"
        f"🔑 Keyword: <code>{final_keyword}</code>\n"
        f"💬 Commenting...",
        parse_mode="HTML"
    )

    try:
        await run_ig(client, follow_user, owner_id)
        logger.info(f"Followed creator {owner_id}")
    except Exception as e:
        logger.info(f"Could not follow user {owner_id} (or already following): {e}")

    # Step 3: Comment on the reel
    waiting_for_owners.add(owner_id)
    timestamp_before_comment = time.time()
    pending_requests[user_id] = {"shortcode": shortcode, "timestamp": timestamp_before_comment}

    try:
        success = await run_ig(client, comment_on_reel, shortcode, final_keyword)

        await status_msg.edit_text(
            f"✅ Followed creator and commented <code>{final_keyword}</code> on @{owner_username}'s reel\n"
            f"⏳ Waiting for the creator to DM the link...\n\n"
            f"<i>(I'll send it to you as soon as it arrives!)</i>",
            parse_mode="HTML"
        )
    except instagrapi.exceptions.LoginRequired:
        ig_clients.clear()
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
        await status_msg.edit_text(
            f"❌ Couldn't comment on the reel.\n\n"
            f"<b>Reason:</b> {html.escape(str(e))}\n\n"
            f"The reel might be private, or comments might be disabled.",
            parse_mode="HTML"
        )
        return

    # Step 4: Poll DMs for the response link
    link_found = None
    elapsed = 0

    while elapsed < DM_WAIT_TIME:
        try:
            link_found = await run_ig(client, check_dms_for_link, owner_id, timestamp_before_comment, shortcode)
            if link_found:
                break
        except Exception as e:
            logger.error(f"Error polling DMs for user {user_id}: {e}")

        # Update status every ~10 minutes (600 sec) to avoid hitting Telegram rate limits
        if elapsed > 0 and elapsed % 600 < DM_CHECK_INTERVAL:
            remaining = DM_WAIT_TIME - elapsed
            try:
                await status_msg.edit_text(
                    f"✅ Followed creator and commented <code>{final_keyword}</code> on @{owner_username}'s reel\n"
                    f"⏳ Waiting for DM response... ({int(remaining/60)}m remaining)\n\n"
                    f"<i>(We will message you when it arrives!)</i>",
                    parse_mode="HTML"
                )
            except Exception:
                pass
                
        await asyncio.sleep(DM_CHECK_INTERVAL)
        elapsed += DM_CHECK_INTERVAL

    if not link_found:
        try:
            await status_msg.edit_text(
                f"Still waiting on @{owner_username}. Trying the Instagram Web button flow...",
                parse_mode="HTML"
            )
        except Exception:
            pass
        link_found = await playwright_manychat_fallback(client, owner_username, owner_id)

    # Clean up pending request
    pending_requests.pop(user_id, None)
    waiting_for_owners.discard(owner_id)

    # Step 5: Send result to user
    if link_found:
        await status_msg.edit_text(
            f"✅ <b>Got it!</b> Here's your link:\n\n"
            f"🔗 {link_found}\n\n"
            f"<i>(Sent by @{owner_username})</i>",
            parse_mode="HTML"
        )
        logger.info(f"Successfully got link for user {user_id}: {link_found}")
    else:
        await update.message.reply_text(
            f"⏰ <b>Timeout</b> — No DM received from @{owner_username}.\n\n"
            f"<b>Possible reasons:</b>\n"
            f"• The creator doesn't have an automation set up\n"
            f"• The keyword <code>{final_keyword}</code> might be wrong\n"
            f"• The automation is broken on their end\n",
            parse_mode="HTML"
        )


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


def acquire_lock():
    """Ensure only one bot instance runs at a time using a lockfile with PID check."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
            if _is_pid_alive(old_pid):
                print(f"❌ Another bot instance is already running (PID {old_pid}).")
                print("   Kill it first, or delete bot.lock if it's stale.")
                sys.exit(1)
            else:
                print(f"🧹 Removing stale lockfile (PID {old_pid} is gone)")
        except (ValueError, OSError):
            print("🧹 Removing invalid lockfile")

    with open(LOCK_FILE, "w") as f:
        f.write(str(os.getpid()))


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

    print("🔐 Logging into Instagram...")
    login_instagram()
    if len(ig_clients) > 0:
        print(f"✅ Connected as @{BOT_INSTAGRAM_USERNAME}")
    else:
        print("⚠️ No Instagram clients logged in — will retry on first request")

    async def post_init(application):
        """Start the Instagram DM listener background task."""
        asyncio.create_task(ig_dm_listener())

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("restart", restart))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

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

    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
