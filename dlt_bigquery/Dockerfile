FROM python:3.12.9

# Set working directory
WORKDIR /app

# Copy files and install dependencies
COPY . .
RUN pip install dlt[bigquery]>=0.4.7 alive-progress google-cloud-bigquery-storage

# Command to run on container start
CMD ["python", "main.py"]