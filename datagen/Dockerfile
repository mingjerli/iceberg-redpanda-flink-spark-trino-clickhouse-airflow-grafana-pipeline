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

# Default command - run webhook simulator in continuous mode
ENTRYPOINT ["python", "simulate_webhooks.py"]
CMD ["--source", "all", "--continuous", "--interval", "2"]
