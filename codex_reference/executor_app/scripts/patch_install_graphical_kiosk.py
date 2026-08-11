from pathlib import Path

service_path = Path('/etc/systemd/system/signalmaker-kiosk.service')
service = '''[Unit]
Description=SignalMaker Browser Kiosk
After=graphical.target raspberry-executor.service
Wants=graphical.target raspberry-executor.service

[Service]
Type=simple
User=pi
WorkingDirectory=/home/pi/Desktop/SignalMaker
Environment=DISPLAY=:0
Environment=XAUTHORITY=/home/pi/.Xauthority
Environment=SIGNALMAKER_KIOSK_URL=http://127.0.0.1:8080/positions.html
ExecStart=/bin/bash /home/pi/Desktop/SignalMaker/scripts/start_kiosk_browser.sh
Restart=always
RestartSec=10

[Install]
WantedBy=graphical.target
'''

try:
    service_path.write_text(service)
    print('written', service_path)
except PermissionError:
    print('permission denied writing service directly; raspberry_update_all.sh will install it with sudo fallback')
