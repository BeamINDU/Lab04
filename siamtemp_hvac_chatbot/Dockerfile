# Dockerfile
FROM python:3.10-slim

WORKDIR /app

# Install essentials
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

# Run
CMD ["python", "enhanced_multi_agent_service.py"]