"""Login using sessionid from browser."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import os, base64
from dotenv import load_dotenv
load_dotenv()
from instagrapi import Client

SESSION_ID = "35711091724%3AUoRdIsUEWw8oIx%3A27%3AAYhNaKzK0M4YbISVV9z5WMhNxPKbREOoqFE-813-Uw"

print(f"Using session ID: {SESSION_ID[:20]}...")

cl = Client()
cl.delay_range = [2, 5]

try:
    print("Logging in by session ID...")
    cl.login_by_sessionid(SESSION_ID)
    print(f"[OK] Logged in as @{cl.username} (user_id: {cl.user_id})")
    
    cl.dump_settings("ig_session.json")
    print("[OK] Session saved to ig_session.json")
    
    with open("ig_session.json", "r") as f:
        session_data = f.read()
    b64 = base64.b64encode(session_data.encode()).decode()
    with open("session_b64.txt", "w") as f:
        f.write(b64)
    print(f"[OK] Base64 session saved ({len(b64)} chars)")
    print("\nReady! Run: python bot.py")
    
except Exception as e:
    print(f"[FAIL] {e}")
    import traceback
    traceback.print_exc()
