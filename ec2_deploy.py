"""
Deploy pw_server.py and pw_engine.py to EC2 instance.

Usage:
    python ec2_deploy.py <EC2_IP>
    
Example:
    python ec2_deploy.py 13.233.45.67
"""

import paramiko
import os
import sys

if len(sys.argv) < 2:
    print("Usage: python ec2_deploy.py <EC2_IP>")
    print("Example: python ec2_deploy.py 13.233.45.67")
    sys.exit(1)

EC2_IP = sys.argv[1]
EC2_PORT = 22
EC2_USER = "ubuntu"  # Default for Ubuntu AMIs; change to "ec2-user" for Amazon Linux
KEY_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "ec2key.pem")

FILES_TO_UPLOAD = [
    ("pw_server.py", "~/pw-relay/pw_server.py"),
    ("pw_engine.py", "~/pw-relay/pw_engine.py"),
    ("ec2_setup.sh", "~/pw-relay/ec2_setup.sh"),
]

ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    print(f"Connecting to {EC2_IP}:{EC2_PORT} as {EC2_USER}...")
    pkey = paramiko.RSAKey.from_private_key_file(KEY_PATH)
    ssh.connect(EC2_IP, EC2_PORT, EC2_USER, pkey=pkey, timeout=15)
    print("✅ SSH connected\n")

    # Create directory
    ssh.exec_command("mkdir -p ~/pw-relay")

    sftp = ssh.open_sftp()
    base_dir = os.path.dirname(os.path.abspath(__file__))

    for local_name, remote_path in FILES_TO_UPLOAD:
        local_path = os.path.join(base_dir, local_name)
        # Expand ~ in remote path
        remote_expanded = remote_path.replace("~", f"/home/{EC2_USER}")
        print(f"Uploading {local_name} -> {remote_expanded}...")
        sftp.put(local_path, remote_expanded)

    sftp.close()
    print("\n✅ Files uploaded!")

    # Check if setup has been run
    _, stdout, _ = ssh.exec_command("test -d ~/pw-relay/venv && echo EXISTS || echo MISSING")
    venv_status = stdout.read().decode().strip()

    if venv_status == "MISSING":
        print("\n⚠️  Virtual environment not found. Running setup script...")
        print("This will take a few minutes (installing Playwright + Chromium)...\n")
        _, stdout, stderr = ssh.exec_command("cd ~/pw-relay && chmod +x ec2_setup.sh && bash ec2_setup.sh 2>&1")
        for line in stdout:
            print(line.strip())
        err = stderr.read().decode()
        if err:
            print(f"STDERR: {err}")
    else:
        print("\n✅ Virtual environment already exists. Restarting service...")
        ssh.exec_command("sudo systemctl restart pw-relay 2>/dev/null || true")

    # Start/restart service
    print("\nStarting pw-relay service...")
    _, stdout, stderr = ssh.exec_command(
        "sudo systemctl daemon-reload && sudo systemctl enable pw-relay && sudo systemctl restart pw-relay && sleep 2 && sudo systemctl status pw-relay --no-pager"
    )
    status = stdout.read().decode()
    print(status)

    print(f"\n🎉 Done! Your relay server should be running at http://{EC2_IP}:5123")
    print(f"   Health check: curl http://{EC2_IP}:5123/health")
    print(f"\n⚠️  Make sure port 5123 is open in your EC2 Security Group!")

    ssh.close()

except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
