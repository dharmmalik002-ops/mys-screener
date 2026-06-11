FROM python:3.11-slim

WORKDIR /code

# Copy the requirements file and install dependencies
COPY backend/requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

# Copy the whole backend directory into the container
COPY backend /code/backend

# Create empty top-level data folder inside container for scraping cache 
# if backend needs it relative to root
RUN mkdir -p /code/backend/data

# Change working directory so Python can correctly find 'app' module
WORKDIR /code/backend

# Run the FastAPI application on port 7860 (Hugging Face default).
# --workers 2 matches the cpu-basic Space (2 vCPUs). The handlers are
# `async def` but call CPU-bound pandas/sorting work that blocks the event
# loop, so a single worker serialises parallel page-load requests and
# Vercel's edge proxy 500s the slowest ones. Two workers let the dashboard
# + scan-counts + groups + ribbon calls run in parallel.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860", "--workers", "2"]

# Redeploy marker: ships data/price_bands.json committed by the bhavcopy workflow.
