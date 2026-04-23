import paramiko

host = '192.168.1.18'
port = 8022
username = 'u0_a438'
password = 'baby009'

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
try:
    ssh.connect(host, port, username, password)
    
    # Check latest logs
    print("=== Last 40 lines of bot.log ===")
    stdin, stdout, stderr = ssh.exec_command('tail -40 ~/bot.log 2>&1')
    print(stdout.read().decode('utf-8', errors='replace').strip())
    
    ssh.close()
except Exception as e:
    print("Error:", e)
