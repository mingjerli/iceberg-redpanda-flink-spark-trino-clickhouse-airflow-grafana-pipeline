# =============================================================================
# Data Generator Container
# =============================================================================
# Generates synthetic webhook data for Shopify, Stripe, and HubSpot.
# Used for testing and demo purposes.
# =============================================================================

FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command - generate data
ENTRYPOINT ["python", "generator.py"]
CMD ["--help"]
