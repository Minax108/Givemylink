import paramiko

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password, timeout=10)
    print("Force killing all bot instances...")
    ssh.exec_command('pkill -9 -f "python bot.py"')
    ssh.exec_command('pkill -9 -f "bash start_bot.sh"')
    ssh.exec_command('rm -f ~/bot.lock')
    print("Done. All instances stopped.")
    ssh.close()
except Exception as e:
    print("Error:", e)
