#!/bin/bash
# MM DepRec Server Setup Script
# Run this script on a fresh Linux server to set up all dependencies

set -e  # Exit on error

echo "=========================================="
echo "MM DepRec Server Setup"
echo "=========================================="
echo ""

# Detect Linux distribution
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
else
    echo "❌ Cannot detect Linux distribution"
    exit 1
fi

echo "Detected OS: $OS"
echo ""

# Update package manager
echo "📦 Updating package manager..."
if [ "$OS" = "ubuntu" ] || [ "$OS" = "debian" ]; then
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
        libasound2 \
        libxshmfence1 \
        libglib2.0-0
elif [ "$OS" = "centos" ] || [ "$OS" = "rhel" ] || [ "$OS" = "fedora" ]; then
    if command -v dnf &> /dev/null; then
        sudo dnf install -y \
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
    else
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
    fi
else
    echo "⚠️  Unsupported OS. Please install dependencies manually."
    echo "Required packages:"
    echo "  - Python 3.x"
    echo "  - pip"
    echo "  - Xvfb (X Virtual Framebuffer)"
    echo "  - Playwright system dependencies (libnss3, libnspr4, etc.)"
fi

echo ""
echo "✅ System dependencies installed"
echo ""

# Create virtual environment (optional but recommended)
read -p "Create Python virtual environment? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv venv
    source venv/bin/activate
    echo "✅ Virtual environment created and activated"
    echo "   To activate later: source venv/bin/activate"
    echo ""
fi

# Install Python dependencies
echo "📚 Installing Python dependencies..."
if [ -f "MM_requirements_production.txt" ]; then
    pip install --upgrade pip
    pip install -r MM_requirements_production.txt
    echo "✅ Production requirements installed"
else
    echo "⚠️  MM_requirements_production.txt not found, using full requirements..."
    pip install --upgrade pip
    pip install -r MM_requirements.txt
    echo "✅ Full requirements installed"
fi

echo ""

# Install Playwright browsers
echo "🌐 Installing Playwright browsers..."
playwright install chromium
echo "✅ Playwright browsers installed"
echo ""

# Verify installation
echo "🔍 Verifying installation..."
python3 -c "import pandas, numpy, playwright, telegram, anthropic, rapidfuzz, xlsxwriter, openpyxl, xlrd; print('✅ All core packages imported successfully')"
echo ""

# Set up Xvfb service (optional - for running Xvfb as a service)
echo "📝 Setup complete!"
echo ""
echo "=========================================="
echo "Next Steps:"
echo "=========================================="
echo "1. Make sure Xvfb is running:"
echo "   Xvfb :99 -screen 0 1024x768x24 &"
echo ""
echo "2. Set DISPLAY environment variable:"
echo "   export DISPLAY=:99"
echo ""
echo "3. Run your scripts:"
echo "   python3 chat_logger.py"
echo ""
echo "Or use xvfb-run to run commands:"
echo "   xvfb-run -a python3 chat_logger.py"
echo ""
echo "=========================================="

