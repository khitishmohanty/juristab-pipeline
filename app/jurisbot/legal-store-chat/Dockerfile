# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container
COPY . .

# Make port 8080 available to the world outside this container
EXPOSE 8080

# Define the command to run the app using Gunicorn.
# Gunicorn is a production-ready web server.
# We run 1 worker with 4 threads. The server is bound to 0.0.0.0:8080.
CMD ["gunicorn", "--workers=1", "--threads=4", "--bind", "0.0.0.0:8080", "main:server"]
