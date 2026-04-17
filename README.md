# Instagram Auto-Link Extractor Bot 🤖🚀

A high-speed, fully autonomous Telegram bot that interacts with Instagram DMs to retrieve hidden links from creators who use "ManyChat" or automated comment-to-DM workflows.

## 🎯 What it Does
Have you ever seen an Instagram Reel where the creator says "Comment 'LINK' and I'll DM it to you"? 
This bot handles that entire process for you automatically:
1. **Receive:** You send the bot an Instagram Reel or Post URL on Telegram.
2. **Comment:** The bot automatically figures out the correct keyword, follows the creator, and comments on the reel from a dedicated scraper account.
3. **Intercept:** It silently polls the scraper account's Instagram DMs to wait for the creator's automated ManyChat system to send the hidden URL.
4. **Deliver:** The bot immediately intercepts the hidden URL from the DM and forwards it directly to your Telegram.

## ✨ Features
- **Smart Keyword Detection:** Analyzes recent comments on a Reel to dynamically guess the right keyword to comment if "link" isn't the trigger.
- **ManyChat & XMA Parsing:** Built to parse complex Instagram DM structures, including automation cards, CTA buttons, and `generic_xma` object types.
- **Stealth & Anti-Ban Architecture:** 
  - Uses static Android device fingerprinting to prevent API flags.
  - Implements Base64 session hydration to rapidly deploy without manual password logins.
  - Proxy-enabled (Residential IP rotation) to bypass Datacenter IP blocking (fixes AWS/Railway IP bans).
- **Concurrent Polling:** Can handle multiple Telegram users requesting links simultaneously using Python's `asyncio` and thread-pooling mechanics.
- **Auto-Approval:** Silently auto-approves pending Message Requests so automation bots can reach the inbox.

## 🛠️ Technology Stack
- **Python 3.10+** (Async Event Loop)
- **Telegram:** `python-telegram-bot`
- **Instagram API:** custom-patched `instagrapi` library

## ⚙️ Environment Variables (`.env`)
To run securely, the bot relies on standard environment variables:
```ini
TELEGRAM_BOT_TOKEN="your_telegram_bot_token"
BOT_INSTAGRAM_USERNAME="your_bot_ig_username"
BOT_INSTAGRAM_PASSWORD="your_bot_ig_password" # Or injected via base64
```

## 🚀 Deployment instructions
This bot is designed to comfortably run on instances like **AWS EC2** or **Railway**.

### Creating a Session Snapshot
Because Instagram blocks standard logins from headless servers, you can create a localized session file (`login_test.py`) from your browser's persistent cookies.
1. Log into your scraper Instagram account on your browser.
2. Grab the `sessionid` cookie value.
3. Run `login_test.py` to bake an `ig_session.json` which bypasses future blocks.

### AWS EC2 (`systemd`)
The bot runs permanently as a `systemd` daemon.
* Start: `sudo systemctl start igbot.service`
* Stop: `sudo systemctl stop igbot.service`
* Live Logs: `sudo journalctl -u igbot.service -f`

## 🧩 Patches
- Includes native inline patching for `instagrapi` (`extract_broadcast_channel` error) to accommodate unannounced API changes in Instagram's profile payloads.
