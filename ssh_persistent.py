import paramiko
import time

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password, timeout=10)
    
    print("Force cleanup: killing all Python and Bash bot processes...")
    ssh.exec_command('pkill -9 -f "python bot.py"')
    ssh.exec_command('pkill -9 -f "start_bot.sh"')
    time.sleep(2)
    ssh.exec_command('rm -f ~/bot.lock')
    
    # Create the persistent runner script
    # It checks if bot.py is already running before starting it
    runner_script = """#!/bin/bash
while true; do
  if ! pgrep -f "python bot.py" > /dev/null; then
    echo "[$(date)] Starting Reel Link Bot..." >> bot_restart.log
    rm -f ~/bot.lock
    python bot.py >> bot.log 2>&1
  fi
  sleep 5
done
"""
    print("Updating persistent runner script...")
    ssh.exec_command(f"echo '{runner_script}' > start_bot.sh && chmod +x start_bot.sh")
    
    print("Starting persistent runner...")
    ssh.exec_command('setsid nohup ./start_bot.sh > /dev/null 2>&1 &')
    
    print("Enabling Wake Lock...")
    ssh.exec_command('termux-wake-lock')
    
    time.sleep(5)
    stdin, stdout, stderr = ssh.exec_command('ps aux | grep -E "python bot.py|start_bot.sh" | grep -v grep')
    print("=== Final status ===")
    print(stdout.read().decode('utf-8', errors='ignore'))
    
    ssh.close()
    print("Cleanup complete. Single instance running in persistent mode.")
except Exception as e:
    print("Error:", e)
