import asyncio
import json
import base64
import time
import re
import logging
import random
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

logger = logging.getLogger(__name__)

URL_RE = re.compile(r'https?://[^\s<>"\')\]}]+', re.IGNORECASE)


def is_external_url(url: str) -> bool:
    lowered = (url or "").lower()
    if not lowered.startswith("http"):
        return False
    # Only return true for links that are NOT internal Instagram/Facebook/Meta links
    return not any(host in lowered for host in ("instagram.com", "facebook.com", "fb.com", "meta.com"))

async def inject_cookies_from_base64(context, b64_settings: str):
    try:
        settings_dict = json.loads(base64.b64decode(b64_settings).decode("utf-8"))
    except Exception as e:
        logger.error(f"[Playwright] Failed to decode settings: {e}")
        return

    cookies = []
    
    # 1. Standard cookies
    if "cookies" in settings_dict:
        for name, value in settings_dict["cookies"].items():
            cookies.append({"name": name, "value": str(value), "domain": ".instagram.com", "path": "/"})
            
    # 2. Extract User ID and CSRF from Authorization Data if missing
    if "authorization_data" in settings_dict:
        auth = settings_dict["authorization_data"]
        if "ds_user_id" in auth:
            cookies.append({"name": "ds_user_id", "value": str(auth["ds_user_id"]), "domain": ".instagram.com", "path": "/"})
        if "csrftoken" in auth:
            cookies.append({"name": "csrftoken", "value": str(auth["csrftoken"]), "domain": ".instagram.com", "path": "/"})
            
    # 3. Extract MID (Machine ID)
    if "mid" in settings_dict:
        cookies.append({"name": "mid", "value": str(settings_dict["mid"]), "domain": ".instagram.com", "path": "/"})
        
    # 4. Extract ig_did
    if "ig_did" in settings_dict:
        cookies.append({"name": "ig_did", "value": str(settings_dict["ig_did"]), "domain": ".instagram.com", "path": "/"})
        
    # Remove duplicates by name
    unique_cookies = []
    seen = set()
    for c in cookies:
        if c["name"] not in seen:
            seen.add(c["name"])
            unique_cookies.append(c)

    if unique_cookies:
        await context.add_cookies(unique_cookies)
        logger.info(f"[Playwright] Injected {len(unique_cookies)} cookies for full web auth.")
    else:
        logger.warning("[Playwright] No valid cookies found in provided settings.")

async def pw_intercept_manychat(
    b64_settings: str,
    creator_username: str,
    timeout_sec: int = 60,
    proxy: str = None,
    thread_id: str | int | None = None,
) -> str | None:
    """
    Opens browser, visits the DM thread, and uses human-like interactions to click CTA buttons.
    """
    
    logger.info(f"[Playwright] Starting stealth interception for @{creator_username} (T: {timeout_sec}s)")
    
    async with async_playwright() as p:
        launch_args = {
            "headless": True,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox"
            ]
        }
        
        if proxy:
            # Parse proxy http://user:pass@ip:port
            match = re.match(r"http://([^:]+):([^@]+)@([^:]+:\d+)", proxy)
            if match:
                user, pwd, serverport = match.groups()
                launch_args["proxy"] = {
                    "server": f"http://{serverport}",
                    "username": user,
                    "password": pwd
                }
            else:
                launch_args["proxy"] = {"server": proxy}

        browser = await p.chromium.launch(**launch_args)
        
        # User agents for rotation or a stable modern one
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        
        context = await browser.new_context(
            user_agent=ua,
            viewport={"width": 1280, "height": 800},
            device_scale_factor=1,
        )
        
        await inject_cookies_from_base64(context, b64_settings)
        
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        
        # Human simulation: random mouse jitter starting position
        await page.mouse.move(random.randint(0, 100), random.randint(0, 100))
        
        thread_url = (
            f"https://www.instagram.com/direct/t/{thread_id}/"
            if thread_id
            else f"https://www.instagram.com/direct/inbox/"
        )
        
        logger.info(f"[Playwright] Navigating to {thread_url}")
        
        try:
            # Random delay before navigation
            await asyncio.sleep(random.uniform(1.0, 3.0))
            await page.goto(thread_url, wait_until="domcontentloaded", timeout=45000)
            
            # Wait for main content
            await page.wait_for_selector('main', timeout=30000)
            
            if not thread_id:
                # Search for username in inbox
                logger.info(f"[Playwright] Clicking inbox item for @{creator_username}")
                user_match = page.get_by_text(creator_username, exact=False).first
                await user_match.click(delay=random.randint(50, 150))
                await asyncio.sleep(random.uniform(2.0, 4.0))

            logger.info(f"[Playwright] Thread loaded: {page.url}")
        except Exception as e:
            try:
                await page.screenshot(path="pw_error.png")
                logger.error(f"[Playwright] Error saved to pw_error.png")
            except: pass
            logger.error(f"[Playwright] Navigation failure: {e}")
            await browser.close()
            return None
            
        elapsed = 0
        link_found = None
        clicked_button_text = set()
        poll_interval = 1.0 # Fast polling, but human-like actions
        
        async def human_jitter(pg):
            """Perform subtle human-like movements."""
            try:
                # Random tiny scroll
                if random.random() > 0.5:
                    await pg.mouse.wheel(0, random.randint(-150, 150))
                # Random mouse move
                await pg.mouse.move(random.randint(200, 1000), random.randint(200, 600), steps=5)
            except: pass

        while elapsed < timeout_sec:
            # Check for popups/new pages
            for open_page in context.pages:
                if is_external_url(open_page.url):
                    logger.info(f"[Playwright] External popup detected: {open_page.url}")
                    link_found = open_page.url
                    break
            if link_found: break

            # 1. Scan for raw links/buttons
            chat_container = await page.query_selector('main')
            if chat_container:
                # Human simulation during scan
                await human_jitter(page)
                
                # Check for URLs in text
                text_content = await chat_container.inner_text()
                urls = URL_RE.findall(text_content)
                if urls:
                    valid = [u for u in urls if is_external_url(u)]
                    if valid:
                        link_found = valid[-1]
                        logger.info(f"[Playwright] Extracted URL from text: {link_found}")
                        break

                # 2. High-reliability button detection
                # Look for ANYTHING that might be a button: role="button", <button>, or specifically "access" text
                buttons = await page.locator('main [role="button"], main button, main div[tabindex="0"]').all()
                for btn in buttons:
                    try:
                        btn_text = (await btn.inner_text()).strip()
                        if not btn_text: continue
                        
                        low_text = btn_text.lower()
                        # Exclude standard junk
                        if any(junk in btn_text for junk in ["Turn On", "Not Now", "Report", "Block", "Send", "Like", "Voice clip"]):
                            continue
                        
                        if btn_text not in clicked_button_text:
                            # Heuristic for automation: "access", "link", "get", "send", "click", "yes", "want", "link"
                            is_action = any(k in low_text for k in ["access", "link", "get", "send", "click", "yes", "i want", "give me"])
                            
                            logger.info(f"[Playwright] Found Potential Action: '{btn_text}' (Priority={is_action})")
                            
                            if is_action:
                                # Target specifically!
                                box = await btn.bounding_box()
                                if box:
                                    await page.mouse.move(box["x"] + box["width"]/2, box["y"] + box["height"]/2, steps=10)
                                    await asyncio.sleep(random.uniform(0.3, 0.7))
                                
                                logger.info(f"[Playwright] CLICKING: '{btn_text}'")
                                await btn.click(delay=random.randint(100, 250))
                                clicked_button_text.add(btn_text)
                                # Post-click wait for automation response
                                await asyncio.sleep(4.0)
                                break # Scan again for new messages
                    except: continue

            if link_found: break
            
            wait_time = poll_interval + random.uniform(0.5, 1.5)
            await asyncio.sleep(wait_time)
            elapsed += wait_time
            
        await browser.close()
        return link_found
