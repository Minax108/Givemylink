import paramiko
import urllib.parse

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

new_sid_raw = '27939375597%3A0ANhqGPoRVR4R0%3A4%3AAYjHQmO8eGScN_zofxSp4aPMhcC1nxCfX1pREBs1Eg'
new_sid = urllib.parse.unquote(new_sid_raw)

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password, timeout=10)
    print(f"Updating SESSION_ID to: {new_sid}")
    
    # Update .env using sed
    # Use | as delimiter in sed to avoid issues with / in the sid
    ssh.exec_command(f"sed -i 's|^IG_SESSION_IDS=.*|IG_SESSION_IDS={new_sid}|' .env")
    
    # Restart bot
    print("Restarting bot...")
    ssh.exec_command('pkill -9 -f "python bot.py"')
    
    ssh.close()
    print("DONE! Bot is restarting with the new ID.")
except Exception as e:
    print("Error:", e)
