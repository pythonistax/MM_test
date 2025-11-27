# MM DepRec Server Setup Guide

This guide will help you set up the MM DepRec system on a Linux server.

## Prerequisites

- Linux server (Ubuntu/Debian or CentOS/RHEL recommended)
- Python 3.8 or higher
- Root/sudo access for installing system packages

## Quick Setup (Automated)

Run the automated setup script:

```bash
chmod +x server_setup.sh
./server_setup.sh
```

## Manual Setup

### 1. Install System Dependencies

#### Ubuntu/Debian:
```bash
sudo apt-get update
sudo apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    xvfb \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2
```

#### CentOS/RHEL/Fedora:
```bash
sudo yum install -y \
    python3 \
    python3-pip \
    xorg-x11-server-Xvfb \
    nss \
    nspr \
    atk \
    cups-libs \
    libdrm \
    dbus \
    libxkbcommon \
    libXcomposite \
    libXdamage \
    libXfixes \
    libXrandr \
    mesa-libgbm \
    alsa-lib \
    glib2
```

### 2. Install Python Dependencies

```bash
# Create virtual environment (recommended)
python3 -m venv venv
source venv/bin/activate

# Install requirements
pip install --upgrade pip
pip install -r MM_requirements_production.txt  # Minimal production requirements
# OR
pip install -r MM_requirements.txt  # Full requirements (includes Jupyter)
```

### 3. Install Playwright Browsers

```bash
playwright install chromium
```

### 4. Set Up Virtual Display (Xvfb)

The system uses `headless=False` mode for Playwright, so you need a virtual display.

#### Option A: Run Xvfb manually
```bash
# Start Xvfb on display :99
Xvfb :99 -screen 0 1024x768x24 &

# Set DISPLAY environment variable
export DISPLAY=:99
```

#### Option B: Use xvfb-run (recommended)
```bash
# Run commands with xvfb-run
xvfb-run -a python3 chat_logger.py
```

#### Option C: Create systemd service (for production)
Create `/etc/systemd/system/xvfb.service`:
```ini
[Unit]
Description=Virtual Framebuffer X Server
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/Xvfb :99 -screen 0 1024x768x24
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl enable xvfb
sudo systemctl start xvfb
export DISPLAY=:99
```

### 5. Verify Installation

```bash
python3 -c "import pandas, numpy, playwright, telegram, anthropic, rapidfuzz, xlsxwriter, openpyxl, xlrd; print('✅ All packages installed')"
```

## Running the Application

### Start the Telegram Bot

```bash
# With xvfb-run (if Xvfb not running as service)
xvfb-run -a python3 chat_logger.py

# Or if Xvfb is running as service
export DISPLAY=:99
python3 chat_logger.py
```

### Required Files

Make sure these files are in the working directory:
- `chat_logger.py`
- `bank_statement_retriever_and_organizer.py`
- `CRM_report_integrator_with_bank_statements.py`
- `MM_DEPREC_playwright.py`
- `GWID_MM.xls`
- `MM_requirements.txt` or `MM_requirements_production.txt`

## Environment Variables

Set these if needed:
```bash
export DISPLAY=:99  # For Playwright
export BOT_TOKEN=your_telegram_bot_token  # Optional, can be hardcoded
export ANTHROPIC_API_KEY=your_anthropic_api_key  # For AI features
```

## Troubleshooting

### Playwright browser not found
```bash
playwright install chromium
```

### Xvfb not working
```bash
# Check if Xvfb is running
ps aux | grep Xvfb

# Test Xvfb
Xvfb :99 -screen 0 1024x768x24 &
export DISPLAY=:99
xclock  # Should open a clock window (if you have X11 forwarding)
```

### Permission errors
```bash
# Make sure scripts are executable
chmod +x *.py
chmod +x server_setup.sh
```

### Import errors
```bash
# Verify all packages are installed
pip list | grep -E "pandas|numpy|playwright|telegram|anthropic|rapidfuzz|xlsxwriter|openpyxl|xlrd"
```

## Production Deployment

For production, consider:
1. Using a process manager (systemd, supervisor, PM2)
2. Setting up log rotation
3. Using environment variables for sensitive data
4. Setting up monitoring/alerting
5. Running as a non-root user

### Example systemd service for chat_logger.py

Create `/etc/systemd/system/mm-deprec-bot.service`:
```ini
[Unit]
Description=MM DepRec Telegram Bot
After=network.target xvfb.service

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/MM/test
Environment="DISPLAY=:99"
Environment="PATH=/path/to/venv/bin:/usr/bin"
ExecStart=/path/to/venv/bin/python3 chat_logger.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl enable mm-deprec-bot
sudo systemctl start mm-deprec-bot
sudo systemctl status mm-deprec-bot
```

