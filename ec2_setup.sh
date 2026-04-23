#!/bin/bash
# ============================================================
# EC2 Setup Script for Playwright Button-Click Relay
# Run this ONCE on your EC2 instance to set everything up.
# 
# Usage:
#   chmod +x ec2_setup.sh && ./ec2_setup.sh
# ============================================================

set -e

echo "=== [1/6] Updating system ==="
sudo apt-get update -y
sudo apt-get install -y python3 python3-pip python3-venv git

echo "=== [2/6] Creating virtual environment ==="
cd ~
mkdir -p pw-relay
cd pw-relay

python3 -m venv venv
source venv/bin/activate

echo "=== [3/6] Installing Python packages ==="
pip install --upgrade pip
pip install flask playwright playwright-stealth

echo "=== [4/6] Installing Chromium browser ==="
playwright install chromium
playwright install-deps chromium

echo "=== [5/6] Creating systemd service ==="
sudo tee /etc/systemd/system/pw-relay.service > /dev/null <<EOF
[Unit]
Description=Playwright Button-Click Relay
After=network.target

[Service]
Type=simple
User=$USER
WorkingDirectory=$HOME/pw-relay
Environment=PATH=$HOME/pw-relay/venv/bin:/usr/local/bin:/usr/bin:/bin
Environment=PW_API_KEY=givemylink-pw-secret-2026
Environment=PW_PORT=5123
ExecStart=$HOME/pw-relay/venv/bin/python pw_server.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

echo "=== [6/6] Setup complete! ==="
echo ""
echo "Next steps:"
echo "  1. Copy pw_server.py and pw_engine.py to ~/pw-relay/"
echo "  2. Run: sudo systemctl daemon-reload"
echo "  3. Run: sudo systemctl enable pw-relay"
echo "  4. Run: sudo systemctl start pw-relay"
echo "  5. Check: sudo systemctl status pw-relay"
echo ""
echo "Make sure port 5123 is open in your EC2 security group!"
