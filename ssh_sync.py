import paramiko
import os

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password)
    sftp = ssh.open_sftp()
    
    base = os.path.dirname(os.path.abspath(__file__))
    
    files_to_sync = ['bot.py', 'db_cache.py', '.env']
    
    for fname in files_to_sync:
        local_path = os.path.join(base, fname)
        if os.path.exists(local_path):
            print(f"Uploading {fname} to ~/{fname}...")
            sftp.put(local_path, fname)
        else:
            print(f"Skipping {fname} (not found locally)")
    
    print("Upload complete!")
    
    sftp.close()
    ssh.close()
except Exception as e:
    print("Error:", e)
