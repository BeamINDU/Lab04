# =============================================================================
# Unified Dockerfile for Siamtemp HVAC Chatbot
# This single Dockerfile handles both the main service and optional proxy
# =============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    curl \
    supervisor \
    && rm -rf /var/lib/apt/lists/*

# Create requirements.txt with all necessary dependencies
COPY <<'REQUIREMENTS_EOF' requirements.txt
# Core web framework
fastapi==0.104.1
uvicorn[standard]==0.24.0

# Database connectivity
asyncpg==0.29.0
psycopg2-binary==2.9.7

# HTTP client for AI and external services
httpx==0.25.2
aiohttp==3.9.0
requests==2.31.0

# Data handling and validation
pydantic==2.5.0
python-dotenv==1.0.0

# Process management
supervisor==4.2.5

# Optional: Enhanced logging
structlog==23.2.0
REQUIREMENTS_EOF

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy all application files
COPY enhanced_multi_agent_service.py .
COPY openwebui_proxy.py .

# Copy agents directory if it exists
COPY agents/ ./agents/

# Create necessary directories
RUN mkdir -p /app/logs /app/cache /etc/supervisor/conf.d /var/log/supervisor

# Create supervisor configuration for managing multiple processes
COPY <<'SUPERVISOR_EOF' /etc/supervisor/conf.d/siamtemp.conf
[supervisord]
nodaemon=true
user=siamtemp
logfile=/var/log/supervisor/supervisord.log
pidfile=/var/run/supervisord.pid

[program:main-service]
command=python enhanced_multi_agent_service.py
directory=/app
user=siamtemp
autostart=true
autorestart=true
startretries=3
redirect_stderr=true
stdout_logfile=/app/logs/main-service.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=3
environment=PYTHONPATH="/app",PYTHONUNBUFFERED="1"

[program:proxy-service]
command=python openwebui_proxy.py
directory=/app
user=siamtemp
autostart=%(ENV_ENABLE_PROXY)s
autorestart=true
startretries=3
redirect_stderr=true
stdout_logfile=/app/logs/proxy-service.log
stdout_logfile_maxbytes=10MB
stdout_logfile_backups=3
environment=PYTHONPATH="/app",PYTHONUNBUFFERED="1",PORT="%(ENV_PROXY_PORT)s"
SUPERVISOR_EOF

# Create comprehensive health check script
COPY <<'HEALTH_EOF' /app/healthcheck.py
#!/usr/bin/env python3
"""
Unified health check for both main service and proxy (if enabled)
"""
import requests
import sys
import os
import time
import json

def check_service(port, name, required=True):
    """Check if a service is healthy"""
    try:
        response = requests.get(f"http://localhost:{port}/health", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ {name} service healthy on port {port}")
            if 'version' in data:
                print(f"   Version: {data['version']}")
            return True
        else:
            print(f"❌ {name} service unhealthy: HTTP {response.status_code}")
            return False
            
    except requests.exceptions.ConnectionError:
        if required:
            print(f"❌ Cannot connect to {name} service on port {port}")
            return False
        else:
            print(f"ℹ️ {name} service not running on port {port} (may be disabled)")
            return True
    except Exception as e:
        print(f"❌ {name} health check error: {e}")
        return False

def main():
    print("🏥 Unified Health Check for Siamtemp HVAC Chatbot")
    print("=" * 50)
    
    # Wait for services to be ready
    time.sleep(3)
    
    # Check main service (required)
    main_port = os.getenv('PORT', '5000')
    main_healthy = check_service(main_port, "Main HVAC AI", required=True)
    
    # Check proxy service (optional)
    proxy_enabled = os.getenv('ENABLE_PROXY', 'false').lower() == 'true'
    proxy_healthy = True  # Default to True since it's optional
    
    if proxy_enabled:
        proxy_port = os.getenv('PROXY_PORT', '8001')
        proxy_healthy = check_service(proxy_port, "OpenWebUI Proxy", required=False)
    else:
        print("ℹ️ Proxy service disabled")
    
    # Overall health assessment
    overall_healthy = main_healthy and proxy_healthy
    
    if overall_healthy:
        print("\n🎉 Overall system health: EXCELLENT")
        sys.exit(0)
    else:
        print("\n⚠️ Overall system health: NEEDS ATTENTION")
        sys.exit(1)

if __name__ == "__main__":
    main()
HEALTH_EOF

# Create startup script that handles different run modes
COPY <<'STARTUP_EOF' /app/startup.py
#!/usr/bin/env python3
"""
Unified startup script that can run in different modes:
- main: Only main service
- proxy: Only proxy service  
- both: Both services (default)
- supervisor: Use supervisor to manage both
"""
import os
import sys
import subprocess
import signal
import time
from concurrent.futures import ThreadPoolExecutor

def run_main_service():
    """Run the main HVAC AI service"""
    print("🚀 Starting Main HVAC AI Service...")
    os.execvp('python', ['python', 'enhanced_multi_agent_service.py'])

def run_proxy_service():
    """Run the OpenWebUI proxy service"""
    print("🌐 Starting OpenWebUI Proxy Service...")
    os.execvp('python', ['python', 'openwebui_proxy.py'])

def run_both_services():
    """Run both services concurrently"""
    print("🚀 Starting Both Services...")
    
    def start_main():
        subprocess.run(['python', 'enhanced_multi_agent_service.py'])
    
    def start_proxy():
        subprocess.run(['python', 'openwebui_proxy.py'])
    
    # Use ThreadPoolExecutor to run both services
    with ThreadPoolExecutor(max_workers=2) as executor:
        main_future = executor.submit(start_main)
        proxy_future = executor.submit(start_proxy)
        
        # Wait for both services
        try:
            main_future.result()
            proxy_future.result()
        except KeyboardInterrupt:
            print("\n🛑 Shutting down services...")
            sys.exit(0)

def run_with_supervisor():
    """Run services using supervisor"""
    print("👥 Starting Services with Supervisor...")
    os.execvp('supervisord', ['supervisord', '-c', '/etc/supervisor/conf.d/siamtemp.conf'])

def main():
    mode = os.getenv('RUN_MODE', 'main').lower()
    
    print(f"🎯 Siamtemp HVAC Chatbot - Run Mode: {mode}")
    
    if mode == 'main':
        run_main_service()
    elif mode == 'proxy':
        run_proxy_service()
    elif mode == 'both':
        run_both_services()
    elif mode == 'supervisor':
        run_with_supervisor()
    else:
        print(f"❌ Unknown run mode: {mode}")
        print("Available modes: main, proxy, both, supervisor")
        sys.exit(1)

if __name__ == "__main__":
    main()
STARTUP_EOF

# Make scripts executable
RUN chmod +x /app/healthcheck.py /app/startup.py

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1
ENV PORT=5000
ENV PROXY_PORT=8001
ENV RUN_MODE=main
ENV ENABLE_PROXY=false

# Expose both potential ports
EXPOSE 5000 8001

# Health check that adapts to the run mode
HEALTHCHECK --interval=30s --timeout=15s --start-period=60s --retries=3 \
    CMD python /app/healthcheck.py

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash siamtemp && \
    chown -R siamtemp:siamtemp /app /var/log/supervisor /etc/supervisor

USER siamtemp

# Default command runs the startup script
CMD ["python", "startup.py"]

# =============================================================================
# Updated docker-compose.yml for unified container
# =============================================================================