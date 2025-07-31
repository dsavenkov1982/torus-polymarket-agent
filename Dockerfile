# Use the official Python 3.11 image
FROM python:3.11-slim

# Set the working directory
WORKDIR /polymarket-agent

# Copy the requirements file into the working directory
COPY requirements.txt .

# Install system dependencies (minimal)
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set environment variables
ENV PYTHONPATH=/polymarket-agent
ENV PYTHONUNBUFFERED=1

# Expose the port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Set default command
CMD ["uvicorn", "http_api:app", "--host", "0.0.0.0", "--port", "8000"]