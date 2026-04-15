import os, json, base64
from dotenv import load_dotenv
load_dotenv()
from instagrapi import Client

session_id = "34690420527%3AUUXsZPJeV0Eetr%3A26%3AAYi1LI1vgLi7Ry56RwFKtgvpsIP5vQ_WRAg7lGoacg"

print("Logging in with sessionid...")
cl = Client()
cl.delay_range = [2, 5]

try:
    cl.login_by_sessionid(session_id)
    print(f"SUCCESS! Logged in as: @{cl.username} (user_id: {cl.user_id})")
    
    cl.dump_settings("ig_session.json")
    print("Session saved to ig_session.json!")
    
    # Export for Railway
    with open("ig_session.json", "r") as f:
        data = f.read()
    encoded = base64.b64encode(data.encode()).decode()
    with open("session_b64.txt", "w") as f:
        f.write(encoded)
    print(f"Base64 session exported to session_b64.txt ({len(encoded)} chars)")
    print("\nDone! Now we need to:")
    print("1. Update bot.py to support loading session from env var")
    print("2. Add IG_SESSION_B64 to Railway")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
