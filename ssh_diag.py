import paramiko

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('192.168.1.18', 8022, 'u0_a438', 'baby009', timeout=10)

# Check if bot process is running
stdin, stdout, stderr = ssh.exec_command('ps aux | grep python | grep -v grep')
ps = stdout.read().decode('utf-8', 'replace').strip()
print("=== PROCESS ===")
print(ps if ps else "NOT RUNNING")

# Check last 30 lines of bot.log for crash info
print("\n=== LAST 30 LINES OF LOG ===")
stdin, stdout, stderr = ssh.exec_command('tail -n 30 ~/bot.log 2>&1')
print(stdout.read().decode('utf-8', 'replace').strip())

# Check if lock file exists
print("\n=== LOCK FILE ===")
stdin, stdout, stderr = ssh.exec_command('cat ~/bot.lock 2>&1; echo "---"; ls -la ~/bot.lock 2>&1')
print(stdout.read().decode('utf-8', 'replace').strip())

ssh.close()
