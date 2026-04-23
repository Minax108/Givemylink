from instagrapi import Client
import json
import logging

logging.basicConfig(level=logging.INFO)

def main():
    cli = Client()
    cli.set_proxy("http://qcxmvcyr:evrdck2dzymr@23.26.71.145:5628")
    
    try:
        print("Attempting fresh login with user credentials...")
        cli.login("pratwik097", "vssut009")
        print("Login successful!")
        
        session_id = cli.get_settings().get("authorization_data", {}).get("sessionid")
        print(f"\nNEW_SESSION_ID: {session_id}\n")
        
        with open("ig_session_pratwik.json", "w") as f:
            json.dump(cli.get_settings(), f)
            
    except Exception as e:
        print(f"Failed to login: {e}")

if __name__ == "__main__":
    main()
