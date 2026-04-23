"""
Playwright Button-Click Relay Server
=====================================
Runs on an EC2 instance. Receives postback button click requests from the
Termux bot, uses a real Chromium browser to click the button in Instagram
DMs, and returns the resulting link.

Usage:
    python pw_server.py

Endpoints:
    POST /click   - Click a postback button and return the link
    GET  /health  - Health check
"""

import asyncio
import json
import logging
import os
import sys
import time
import traceback

from flask import Flask, request, jsonify

# Import the existing Playwright engine
from pw_engine import pw_intercept_manychat, inject_cookies_from_base64

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Simple API key for security (set via environment variable)
API_KEY = os.environ.get("PW_API_KEY", "givemylink-pw-secret-2026")


def require_api_key(f):
    """Simple API key middleware."""
    from functools import wraps
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get("X-API-Key", "")
        if key != API_KEY:
            return jsonify({"error": "unauthorized"}), 401
        return f(*args, **kwargs)
    return decorated


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "pw-relay"})


@app.route("/click", methods=["POST"])
@require_api_key
def click_button():
    """
    Click a postback button in Instagram DMs using Playwright.
    
    Request JSON:
    {
        "b64_settings": "...",        # base64-encoded instagrapi session settings
        "creator_username": "...",     # Instagram username of the reel owner
        "thread_id": "...",           # DM thread ID (optional but recommended)
        "timeout": 90                  # Max seconds to wait (optional, default 90)
    }
    
    Response JSON:
    {
        "link": "https://..."         # The extracted link, or null if not found
        "error": "..."                # Error message if failed
    }
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "no JSON body"}), 400

        b64_settings = data.get("b64_settings")
        creator_username = data.get("creator_username", "unknown")
        thread_id = data.get("thread_id")
        timeout = int(data.get("timeout", 90))

        if not b64_settings:
            return jsonify({"error": "b64_settings is required"}), 400

        logger.info(
            f"[PW-Relay] Click request: @{creator_username} thread={thread_id} timeout={timeout}s"
        )

        # Run the async Playwright function in a new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            link = loop.run_until_complete(
                pw_intercept_manychat(
                    b64_settings=b64_settings,
                    creator_username=creator_username,
                    timeout_sec=timeout,
                    thread_id=thread_id,
                )
            )
        finally:
            loop.close()

        if link:
            logger.info(f"[PW-Relay] SUCCESS: Found link {link}")
            return jsonify({"link": link})
        else:
            logger.warning(f"[PW-Relay] No link found for @{creator_username}")
            return jsonify({"link": None, "error": "no link found after clicking"})

    except Exception as e:
        logger.error(f"[PW-Relay] Error: {traceback.format_exc()}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PW_PORT", "5123"))
    logger.info(f"[PW-Relay] Starting on port {port}...")
    app.run(host="0.0.0.0", port=port, threaded=True)
