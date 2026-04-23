import paramiko
import sys
import os

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    print(f"Connecting to {host}:{port}...")
    ssh.connect(host, port, username, password)
    
    print("Uploading bot.py...")
    sftp = ssh.open_sftp()
    
    bot_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.py")
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    
    sftp.put(bot_path, 'bot.py')
    print("Done uploading bot.py")
    
    # Always upload latest .env
    print("Uploading local .env...")
    if os.path.exists(env_path):
        sftp.put(env_path, '.env')
        print("Done uploading .env")
            
    print("Restarting bot...")
    # Kill any existing python process
    ssh.exec_command('pkill -f "python bot.py"')
    
    # Start the bot with nohup to keep it running
    stdin, stdout, stderr = ssh.exec_command('nohup python bot.py > bot.log 2>&1 &')
    print("Sent start command.")
    
    sftp.close()
    ssh.close()
    print("SUCCESS")
except Exception as e:
    print("Error:", e)
