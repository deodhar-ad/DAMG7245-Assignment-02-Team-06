FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements first for layer caching
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Set environment variables
ENV PORT=8082

# Command to run the application
CMD ["uvicorn", "backend.fastapi_backend:app", "--host", "0.0.0.0", "--port", "8082"]