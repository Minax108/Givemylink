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

# ──────────────────────────────────────────────────────────────────────────────

ig_client = Client()
ig_logged_in = False

# Track pending requests: {telegram_user_id: {"reel_url": ..., "timestamp": ...}}
pending_requests = {}

# Concurrency primitives (initialized in main)
ig_lock = None           # asyncio.Lock — protects login/session state
ig_semaphore = None      # asyncio.Semaphore — limits concurrent IG API calls
thread_pool = None       # ThreadPoolExecutor — runs blocking IG calls


async def run_ig(func, *args):
    """Run a blocking Instagram API call in the thread pool, respecting the semaphore."""
    loop = asyncio.get_event_loop()
    async with ig_semaphore:
        return await loop.run_in_executor(thread_pool, func, *args)


def login_instagram():
    """Log in to the bot's dedicated Instagram account."""
    global ig_logged_in
    try:
        session_file = "ig_session.json"
        if os.path.exists(session_file):
            ig_client.load_settings(session_file)
            ig_client.login(BOT_INSTAGRAM_USERNAME, BOT_INSTAGRAM_PASSWORD)
            logger.info("Instagram: logged in using saved session.")
        else:
            ig_client.login(BOT_INSTAGRAM_USERNAME, BOT_INSTAGRAM_PASSWORD)
            ig_client.dump_settings(session_file)
            logger.info("Instagram: fresh login, session saved.")
        ig_logged_in = True
    except Exception as e:
        logger.error(f"Instagram login failed: {e}")
        ig_logged_in = False


async def ensure_logged_in_async() -> bool:
    """Make sure we're logged into Instagram, reconnect if needed. Thread-safe."""
    global ig_logged_in
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


def check_dms_for_link(reel_owner_id: int, after_timestamp: float) -> str | None:
    """
    Check Instagram DMs for a new message from the reel owner.
    Handles generic_xma (ManyChat cards with CTA buttons), text, and link messages.
    Returns the link/text if found, None otherwise.
    """
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

                        # Handle generic_xma (ManyChat/automation cards)
                        if item_type == "generic_xma":
                            xma_list = item.get("generic_xma", [])
                            for xma in xma_list:
                                cta_buttons = xma.get("cta_buttons", [])
                                for btn in cta_buttons:
                                    action_url = btn.get("action_url", "")
                                    if action_url and action_url.startswith("http"):
                                        logger.info(f"Found link in CTA button: {action_url}")
                                        return action_url

                                # If CTA has postback but no URL, reply with
                                # the button title text to trigger ManyChat.
                                # (The old xma_postback endpoint was deprecated
                                #  by Instagram and returns 404.)
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

                                # Also check the title text for URLs
                                title = xma.get("title_text", "") or ""
                                urls = re.findall(r'https?://[^\s<>"]+', title)
                                if urls:
                                    return urls[0]

                        # Handle plain text messages
                        elif item_type == "text":
                            text = item.get("text", "")
                            urls = re.findall(r'https?://[^\s<>"]+', text)
                            if urls:
                                logger.info(f"Found link in text message: {urls[0]}")
                                return urls[0]

                        # Handle link type messages
                        elif item_type == "link":
                            link_data = item.get("link", {})
                            link_url = link_data.get("text", "") or link_data.get("link_url", "")
                            urls = re.findall(r'https?://[^\s<>"]+', link_url)
                            if urls:
                                logger.info(f"Found link in link message: {urls[0]}")
                                return urls[0]

        return None
    except Exception as e:
        logger.error(f"Error checking DMs: {e}")
        return None


# ─── TELEGRAM HANDLERS ─────────────────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 **Welcome to the Reel Link Bot!**\n\n"
        "Send me any Instagram Reel URL and I'll get the link for you!\n\n"
        "🔄 **How it works:**\n"
        "1. You send the reel URL\n"
        "2. I auto-detect the keyword from comments\n"
        "3. I follow the creator & comment for you\n"
        "4. The creator DMs me the link\n"
        "5. I send the link back to you!\n\n"
        "📌 **Commands:**\n"
        "/start — Show this message\n"
        "/status — Check bot status\n"
        "/restart — Re-login to Instagram\n\n"
        "**Try it:** Just paste a reel URL!",
        parse_mode="Markdown"
    )


async def status_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if ig_logged_in:
        await update.message.reply_text(
            f"✅ Bot is **online**\n"
            f"📸 Instagram: `@{BOT_INSTAGRAM_USERNAME}`\n"
            f"📊 Active requests: {len(pending_requests)}\n"
            f"🔧 Max concurrent: {MAX_CONCURRENT_IG_CALLS}",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "❌ Instagram is **disconnected**.\n"
            "The bot will try to reconnect on the next request.",
            parse_mode="Markdown"
        )


async def restart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Re-login to Instagram and clear pending requests."""
    global ig_logged_in, pending_requests

    msg = await update.message.reply_text("🔄 Restarting Instagram session...")

    # Clear pending requests
    pending_requests.clear()

    # Delete old session and re-login
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
        await msg.edit_text(
            f"✅ **Restarted successfully!**\n\n"
            f"📸 Instagram: `@{BOT_INSTAGRAM_USERNAME}`\n"
            f"🧹 Pending requests cleared",
            parse_mode="Markdown"
        )
    else:
        await msg.edit_text(
            "❌ **Restart failed** — Instagram login error.\n"
            "Check credentials and try again.",
            parse_mode="Markdown"
        )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Main handler: user sends a reel URL → bot comments → waits for DM → sends link back."""
    global ig_logged_in

    text = update.message.text or ""
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"

    # Extract the Instagram URL from the message
    url = extract_reel_url(text)

    if not url:
        await update.message.reply_text(
            "🔗 Please send a valid Instagram Reel URL.\n\n"
            "**Just paste the link** — I'll auto-detect the keyword!\n\n"
            "Example:\n`https://www.instagram.com/reel/ABC123xyz/`",
            parse_mode="Markdown"
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
    status_msg = await update.message.reply_text(
        f"⏳ Processing your request...\n\n"
        f"🔗 Reel: `...{shortcode}`\n"
        f"🔍 Auto-detecting keyword...",
        parse_mode="Markdown"
    )

    if not await ensure_logged_in_async():
        await status_msg.edit_text(
            "❌ Instagram login failed. The bot admin needs to check credentials.\n"
            "Try again later.",
            parse_mode="Markdown"
        )
        return

    # Step 2: Get reel owner info
    try:
        owner_info = await run_ig(get_reel_owner, shortcode)
        if not owner_info:
            await status_msg.edit_text("❌ Couldn't find information about this reel. Make sure the URL is correct.")
            return

        owner_username = owner_info.get("username", "unknown")
        owner_id = owner_info["user_id"]

        await status_msg.edit_text(
            f"✅ Found reel by **@{owner_username}**\n"
            f"💬 Preparing...",
            parse_mode="Markdown"
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ Error finding reel: {str(e)}")
        return

    # Step 2.5: Auto-detect keyword from comments and follow creator
    final_keyword = await run_ig(get_best_keyword, shortcode)
    logger.info(f"Auto-detected keyword for {shortcode}: '{final_keyword}'")

    await status_msg.edit_text(
        f"✅ Found reel by **@{owner_username}**\n"
        f"🔑 Keyword: `{final_keyword}`\n"
        f"💬 Commenting...",
        parse_mode="Markdown"
    )

    try:
        await run_ig(follow_user, owner_id)
        logger.info(f"Followed creator {owner_id}")
    except Exception as e:
        logger.info(f"Could not follow user {owner_id} (or already following): {e}")

    # Step 3: Comment on the reel
    timestamp_before_comment = time.time()
    pending_requests[user_id] = {"shortcode": shortcode, "timestamp": timestamp_before_comment}

    try:
        success = await run_ig(comment_on_reel, shortcode, final_keyword)

        await status_msg.edit_text(
            f"✅ Followed creator and commented `{final_keyword}` on @{owner_username}'s reel\n"
            f"⏳ Waiting for the creator to DM the link...\n\n"
            f"_(I'll send it to you as soon as it arrives!)_",
            parse_mode="Markdown"
        )
    except instagrapi.exceptions.LoginRequired:
        ig_logged_in = False
        pending_requests.pop(user_id, None)
        await status_msg.edit_text(
            "⚠️ Instagram session expired. Reconnecting...\nPlease try sending the link again.",
            parse_mode="Markdown"
        )
        await ensure_logged_in_async()
        return
    except Exception as e:
        pending_requests.pop(user_id, None)
        await status_msg.edit_text(
            f"❌ Couldn't comment on the reel.\n\n"
            f"**Reason:** {str(e)}\n\n"
            f"The reel might be private, or comments might be disabled.",
            parse_mode="Markdown"
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
        except Exception as e:
            logger.error(f"Error polling DMs for user {user_id}: {e}")

        # Update status every ~10 minutes (600 sec) to avoid hitting Telegram rate limits
        if elapsed > 0 and elapsed % 600 < DM_CHECK_INTERVAL:
            remaining = DM_WAIT_TIME - elapsed
            try:
                await status_msg.edit_text(
                    f"✅ Followed creator and commented `{final_keyword}` on @{owner_username}'s reel\n"
                    f"⏳ Waiting for DM response... ({int(remaining/60)}m remaining)\n\n"
                    f"_(We will message you when it arrives!)_",
                    parse_mode="Markdown"
                )
            except Exception:
                pass
                
        await asyncio.sleep(DM_CHECK_INTERVAL)
        elapsed += DM_CHECK_INTERVAL

    # Clean up pending request
    pending_requests.pop(user_id, None)

    # Step 5: Send result to user
    if link_found:
        await status_msg.edit_text(
            f"✅ **Got it!** Here's your link:\n\n"
            f"🔗 {link_found}\n\n"
            f"_(Sent by @{owner_username})_",
            parse_mode="Markdown"
        )
        logger.info(f"Successfully got link for user {user_id}: {link_found}")
    else:
        await update.message.reply_text(
            f"⏰ **Timeout** — No DM received from @{owner_username}.\n\n"
            f"**Possible reasons:**\n"
            f"• The creator doesn't have an automation set up\n"
            f"• The keyword `{final_keyword}` might be wrong\n"
            f"• The automation is broken on their end\n",
            parse_mode="Markdown"
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
                parse_mode="Markdown"
            )
        except Exception:
            pass


LOCK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.lock")


def acquire_lock():
    """Ensure only one bot instance runs at a time using a lockfile with PID check."""
    if os.path.exists(LOCK_FILE):
        try:
            with open(LOCK_FILE, "r") as f:
                old_pid = int(f.read().strip())
            # Check if that PID is still alive
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x1000, False, old_pid)  # PROCESS_QUERY_LIMITED_INFORMATION
            if handle:
                kernel32.CloseHandle(handle)
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

    if TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN":
        print("=" * 55)
        print("⚠️  Set your TELEGRAM_BOT_TOKEN in bot.py (line 22)")
        print("   → Get one from @BotFather on Telegram")
        print("=" * 55)
        return

    if BOT_INSTAGRAM_USERNAME == "YOUR_BOT_INSTAGRAM_USERNAME":
        print("=" * 55)
        print("⚠️  Set your BOT_INSTAGRAM_USERNAME in bot.py (line 26)")
        print("   → Create a dedicated Instagram account for this bot")
        print("=" * 55)
        return

    # Prevent multiple instances from running at the same time
    acquire_lock()

    import atexit
    atexit.register(release_lock)

    # Initialize concurrency primitives
    thread_pool = ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE)

    print("🔐 Logging into Instagram...")
    login_instagram()
    if ig_logged_in:
        print(f"✅ Connected as @{BOT_INSTAGRAM_USERNAME}")
    else:
        print("⚠️ Instagram login failed — will retry on first request")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("status", status_cmd))
    app.add_handler(CommandHandler("restart", restart))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    app.add_error_handler(error_handler)

    print("")
    print("🤖 Reel Link Bot is running!")
    print(f"⚡ Concurrency: {MAX_CONCURRENT_IG_CALLS} simultaneous IG calls, {THREAD_POOL_SIZE} threads")
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
