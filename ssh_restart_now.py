import paramiko
import time

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect('192.168.1.18', 8022, 'u0_a438', 'baby009', timeout=10)

# Kill frozen bot
print("Killing frozen bot process...")
ssh.exec_command('pkill -9 -f "python bot.py"')
time.sleep(2)

# Remove stale lock file
ssh.exec_command('rm -f ~/bot.lock')

# Verify it's dead
stdin, stdout, stderr = ssh.exec_command('ps aux | grep "python bot.py" | grep -v grep')
ps = stdout.read().decode('utf-8', 'replace').strip()
print("After kill:", ps if ps else "CONFIRMED DEAD")

# Restart with nohup
print("Restarting bot...")
ssh.exec_command('cd ~ && nohup python bot.py > bot.log 2>&1 &')
time.sleep(5)

# Verify it started
stdin, stdout, stderr = ssh.exec_command('ps aux | grep "python bot.py" | grep -v grep')
ps = stdout.read().decode('utf-8', 'replace').strip()
print("After restart:", ps if ps else "FAILED TO START")

# Check first few log lines
print("\n=== Initial log output ===")
stdin, stdout, stderr = ssh.exec_command('tail -n 10 ~/bot.log 2>&1')
print(stdout.read().decode('utf-8', 'replace').strip())

ssh.close()
print("\nDone!")
