import paramiko
import time
import os
import sys

# Force UTF-8 on Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password, timeout=10)
    
    # Step 1: Aggressively kill ALL bot processes
    print("[*] Stopping ALL existing bot processes...")
    ssh.exec_command('pkill -9 -f "python bot.py"')
    ssh.exec_command('pkill -9 -f "python3 bot.py"')
    time.sleep(2)
    
    # Step 2: Verify they're actually dead - retry kill if needed
    stdin, stdout, stderr = ssh.exec_command('ps aux | grep "[p]ython.*bot\\.py"')
    survivors = stdout.read().decode('utf-8', errors='ignore').strip()
    if survivors:
        print("[!] Survivors found, force-killing by PID...")
        for line in survivors.splitlines():
            parts = line.split()
            if len(parts) >= 2:
                pid = parts[1]
                ssh.exec_command(f'kill -9 {pid}')
        time.sleep(2)
        
        # Final verification
        stdin, stdout, stderr = ssh.exec_command('ps aux | grep "[p]ython.*bot\\.py"')
        still_alive = stdout.read().decode('utf-8', errors='ignore').strip()
        if still_alive:
            print(f"[FAIL] Could not kill all bot processes:\n{still_alive}")
            print("   Please manually kill them on the device.")
            ssh.close()
            exit(1)
    
    print("[OK] All old bot processes killed")
    
    # Step 3: Clean up stale lockfile
    ssh.exec_command('rm -f ~/bot.lock')
    time.sleep(1)
    
    # Step 4: Start fresh bot
    print("[*] Starting bot in background...")
    ssh.exec_command('setsid nohup python bot.py > bot.log 2>&1 &')
    
    # Step 5: Wait and verify it started successfully
    time.sleep(6)
    
    stdin, stdout, stderr = ssh.exec_command('ps aux | grep "[p]ython.*bot\\.py"')
    ps_out = stdout.read().decode('utf-8', errors='ignore').strip()
    
    if ps_out:
        lines = ps_out.strip().splitlines()
        if len(lines) > 1:
            print(f"[WARN] {len(lines)} bot processes detected (should be 1)!")
            print(ps_out)
        else:
            print("[OK] Bot is running (single instance)!")
            print(ps_out)
    else:
        print("[FAIL] Bot failed to start. Checking log for errors...")
        stdin, stdout, stderr = ssh.exec_command('tail -20 bot.log')
        print(stdout.read().decode('utf-8', errors='ignore'))
    
    ssh.close()
except Exception as e:
    print("Error:", e)
