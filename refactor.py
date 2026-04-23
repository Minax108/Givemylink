import re
import sys

def main():
    with open("bot.py", "r", encoding="utf-8") as f:
        content = f.read()

    # 1. Update function signatures entirely that used to rely on global ig_client
    replacements = [
        ("def get_instagram_session_b64() -> str | None:", "def get_instagram_session_b64(client) -> str | None:\n    import json, base64\n    return base64.b64encode(json.dumps(client.get_settings()).encode('utf-8')).decode('ascii')\n#"),
        ("def get_best_keyword(shortcode: str) -> str:", "def get_best_keyword(client, shortcode: str) -> str:"),
        ("def comment_on_reel(shortcode: str, comment_text: str) -> bool:", "def comment_on_reel(client, shortcode: str, comment_text: str) -> bool:"),
        ("def get_reel_owner(shortcode: str) -> dict:", "def get_reel_owner(client, shortcode: str) -> dict:"),
        ("def follow_user(user_id):", "def follow_user(client, user_id):"),
        ("def check_dms_for_link(reel_owner_id: int, after_timestamp: float) -> str | None:", "def check_dms_for_link(client, reel_owner_id: int, after_timestamp: float, requested_shortcode: str = '') -> str | None:"),
        ("def send_dm_reply(thread_id: int, text: str):", "def send_dm_reply(client, thread_id: int, text: str):"),
        ("def get_dm_thread_id_for_user(user_id: int) -> int | None:", "def get_dm_thread_id_for_user(client, user_id: int) -> int | None:"),
        ("async def playwright_manychat_fallback(owner_username: str, owner_id: int) -> str | None:", "async def playwright_manychat_fallback(client, owner_username: str, owner_id: int) -> str | None:"),
        ("def fetch_dm_inbox():", "def fetch_dm_inbox(client):"),
        ("async def process_ig_dm_request(user_pk: int, username: str, thread_id: int, reel_url: str):", "async def process_ig_dm_request(client, user_pk: int, username: str, thread_id: int, reel_url: str):"),
    ]

    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            print(f"Replaced signature: {old.split('(')[0]}")
        else:
            print(f"Warning: Could not find {old}")

    # Process all internal ig_client bounds
    # Replace ig_client. with client. in the body of those functions
    # Actually, ig_client was removed from global state. We can safely replace 'ig_client.' with 'client.' universally.
    content = content.replace("ig_client.", "client.")

    # 2. Add delays and "liking" behavior for human-like interaction.
    comment_old = '''    try:
        media_pk = client.media_pk_from_code(shortcode)
        media_id = client.media_id(media_pk)
        client.media_comment(media_id, comment_text)'''
    comment_new = '''    try:
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
        
        client.media_comment(media_id, comment_text)'''
    content = content.replace(comment_old, comment_new)

    # 3. Fix the URL extractor (issue 1) in check_dms_for_link
    fallback_old = '''                                    if action_url and action_url.startswith("http"):
                                        found_url = unwrap_instagram_redirect(action_url)
                                        logger.info(f"Found link in CTA button: {found_url}")
                                        return found_url'''
    fallback_new = '''                                    if action_url and action_url.startswith("http"):
                                        found_url = unwrap_instagram_redirect(action_url)
                                        # Fix: don't return if the URL is just the exact same reel we requested!
                                        if requested_shortcode and requested_shortcode in found_url and 'instagram.com' in found_url:
                                            logger.info(f"Ignored CTA link because it loops back to requested reel: {found_url}")
                                            continue
                                        logger.info(f"Found link in CTA button: {found_url}")
                                        return found_url'''
    content = content.replace(fallback_old, fallback_new)

    # Text message fallback
    text_old = '''                            if urls:
                                logger.info(f"Found link in text message: {urls[0]}")
                                return urls[0]'''
    text_new = '''                            if urls:
                                for u in urls:
                                    if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                        continue
                                    logger.info(f"Found link in text message: {u}")
                                    return u'''
    content = content.replace(text_old, text_new)

    # Link message fallback
    link_old = '''                            if urls:
                                logger.info(f"Found link in link message: {urls[0]}")
                                return urls[0]'''
    link_new = '''                            if urls:
                                for u in urls:
                                    if requested_shortcode and requested_shortcode in u and 'instagram.com' in u:
                                        continue
                                    logger.info(f"Found link in link message: {u}")
                                    return u'''
    content = content.replace(link_old, link_new)

    # Handle process_ig_dm_request
    content = content.replace("await run_ig(send_dm_reply, thread_id", "await run_ig(client, send_dm_reply, thread_id")
    content = content.replace("await run_ig(get_reel_owner, shortcode)", "await run_ig(client, get_reel_owner, shortcode)")
    content = content.replace("await run_ig(get_best_keyword, shortcode)", "await run_ig(client, get_best_keyword, shortcode)")
    content = content.replace("await run_ig(follow_user, owner_id)", "await run_ig(client, follow_user, owner_id)")
    content = content.replace("await run_ig(comment_on_reel, shortcode, final_keyword)", "await run_ig(client, comment_on_reel, shortcode, final_keyword)")
    content = content.replace("await run_ig(check_dms_for_link, owner_id, timestamp_before)", "await run_ig(client, check_dms_for_link, owner_id, timestamp_before, shortcode)")
    content = content.replace("await playwright_manychat_fallback(owner_username, owner_id)", "await playwright_manychat_fallback(client, owner_username, owner_id)")
    content = content.replace("global ig_logged_in", "global ig_clients")

    # Handle handle_message
    handle_msg_old = '''    # Step 1: Ensure Instagram is connected
    status_msg = await update.message.reply_text('''
    handle_msg_new = '''    # Step 1: Ensure Instagram is connected
    try:
        client = await get_random_client()
    except Exception as e:
        await update.message.reply_text("❌ No Instagram clients available.")
        return
        
    status_msg = await update.message.reply_text('''
    content = content.replace(handle_msg_old, handle_msg_new)

    # In handle_message, replace run_ig calls with client parameter
    content = content.replace("await run_ig(check_dms_for_link, owner_id, timestamp_before_comment)", "await run_ig(client, check_dms_for_link, owner_id, timestamp_before_comment, shortcode)")

    # Handle ig_dm_listener
    listener_old = '''            threads_data = await run_ig(fetch_dm_inbox)
            logger.info(f"[IG DM] Scanned {len(threads_data)} threads, ig_dm_last_check={ig_dm_last_check:.0f}")'''
    listener_new = '''            all_threads = []
            for client in ig_clients.copy():
                try:
                    tds = await run_ig(client, fetch_dm_inbox)
                    for td in tds:
                        td['client'] = client
                    all_threads.extend(tds)
                except Exception as e:
                    logger.error(f"[IG DM] Client fetch failed: {e}")
            threads_data = all_threads
            logger.info(f"[IG DM] Scanned {len(threads_data)} threads across {len(ig_clients)} clients, ig_dm_last_check={ig_dm_last_check:.0f}")'''
    content = content.replace(listener_old, listener_new)
    
    content = content.replace("sender = item.get(\"user_id\")\n                    item_type = item.get(\"item_type\", \"unknown\")\n                    logger.info(f\"[IG DM] @{username} item #{idx}: type={item_type}, sender={sender}, bot_id={client.user_id}\")\n                    if str(sender) == str(client.user_id):",
    "sender = item.get(\"user_id\")\n                    item_type = item.get(\"item_type\", \"unknown\")\n                    client_user_id = str(td['client'].user_id) if hasattr(td.get('client', None), 'user_id') else ''\n                    logger.info(f\"[IG DM] @{username} item #{idx}: type={item_type}, sender={sender}\")\n                    if str(sender) == client_user_id:")
    
    request_spawn_old = '''                        asyncio.create_task(
                            process_ig_dm_request(user_pk, username, thread_id, reel_url)
                        )'''
    request_spawn_new = '''                        asyncio.create_task(
                            process_ig_dm_request(td['client'], user_pk, username, thread_id, reel_url)
                        )'''
    content = content.replace(request_spawn_old, request_spawn_new)
    content = content.replace("await run_ig(send_dm_reply, thread_id,", "await run_ig(td['client'], send_dm_reply, thread_id,")

    # In Restart function, replace ig_logged_in checks
    content = content.replace("global ig_logged_in, pending_requests, ig_dm_last_check", "global ig_clients, pending_requests, ig_dm_last_check")
    content = content.replace("ig_logged_in = False", "ig_clients.clear()")
    content = content.replace("if ig_logged_in:", "if len(ig_clients) > 0:")

    # In status_cmd
    content = content.replace("if ig_logged_in:", "if len(ig_clients) > 0:")
    
    # In main
    content = content.replace("if ig_logged_in:", "if len(ig_clients) > 0:")
    content = content.replace('print("⚠️ Instagram login failed — will retry on first request")', 'print("⚠️ No Instagram clients logged in — will retry on first request")')

    # Remove get_instagram_session_b64 calls looking for session_b64.txt, etc
    old_b64_func = '''def get_instagram_session_b64(client) -> str | None:
    import json, base64
    return base64.b64encode(json.dumps(client.get_settings()).encode('utf-8')).decode('ascii')
#
    session_b64 = os.environ.get("IG_SESSION_B64", "").strip()
    if session_b64:
        return session_b64

    if os.path.exists("session_b64.txt"):
        try:
            with open("session_b64.txt", "r", encoding="utf-8") as f:
                session_b64 = f.read().strip()
            if session_b64:
                return session_b64
        except Exception as e:
            logger.warning(f"Could not read session_b64.txt for Playwright: {e}")

    if os.path.exists("ig_session.json"):
        try:
            with open("ig_session.json", "rb") as f:
                return base64.b64encode(f.read()).decode("ascii")
        except Exception as e:
            logger.warning(f"Could not encode ig_session.json for Playwright: {e}")

    return None'''
    new_b64_func = '''def get_instagram_session_b64(client) -> str | None:
    """Load a session snapshot for Playwright cookie injection."""
    import json, base64
    return base64.b64encode(json.dumps(client.get_settings()).encode('utf-8')).decode('ascii')'''
    content = content.replace(old_b64_func, new_b64_func)

    with open("bot.py", "w", encoding="utf-8") as f:
        f.write(content)
    print("Done refactoring bot.py")

if __name__ == "__main__":
    main()
