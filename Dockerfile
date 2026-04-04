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

# Run the FastAPI application on port 7860 (Hugging Face default)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]
