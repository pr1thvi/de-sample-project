FROM python:3.12.9

# Set working directory
WORKDIR /app

# Copy files and install dependencies
COPY . .
RUN pip install "dlt[duckdb]" alive-progress

# Command to run on container start
CMD ["python", "main.py"]