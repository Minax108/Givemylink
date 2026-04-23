import paramiko
import sys

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password, timeout=10)
    
    # Check process
    stdin, stdout, stderr = ssh.exec_command('ps aux | grep "python bot.py" | grep -v grep')
    ps_out = stdout.read().decode('utf-8', errors='ignore').strip()
    print("=== Process ===")
    print(ps_out if ps_out else "NOT RUNNING")
    
    # Check log
    print("\n=== Log (tail -n 50) ===")
    stdin, stdout, stderr = ssh.exec_command('tail -n 200 ~/bot.log 2>&1')
    log_out = stdout.read().decode('utf-8', errors='replace').strip()
    # Write to a file instead of printing if it might cause console errors
    print(log_out.encode('ascii', 'replace').decode())
    
    ssh.close()
except Exception as e:
    print("Error:", e)
