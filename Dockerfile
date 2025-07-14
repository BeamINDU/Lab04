FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install fastapi uvicorn aiohttp pydantic boto3 psycopg2-binary

# Copy files
COPY . .

# Default port
EXPOSE 8000

# Can run any service
CMD ["python", "app.py"]