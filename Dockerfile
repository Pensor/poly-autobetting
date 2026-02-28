FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for security
RUN useradd -m -u 1000 botuser

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir -r requirements.txt

# Optionally install relayer dependencies for gasless redemptions
# To use: docker build --build-arg INSTALL_RELAYER=1 .
ARG INSTALL_RELAYER=0
COPY requirements-relayer.txt .
RUN if [ "$INSTALL_RELAYER" = "1" ]; then \
    pip install --no-cache-dir -r requirements-relayer.txt; \
    fi

# Copy project code and set ownership
COPY . .
RUN chown -R botuser:botuser /app

# Switch to non-root user
USER botuser

# Default command: run the simple 45c bot
CMD ["python", "scripts/place_45.py"]
