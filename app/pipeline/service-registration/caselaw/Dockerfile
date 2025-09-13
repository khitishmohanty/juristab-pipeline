# Use an official Python runtime as a parent image
# Using a slim version to keep the image size smaller
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
# --no-cache-dir ensures that pip doesn't store the cache, keeping the image size down
# --trusted-host pypi.python.org is sometimes needed in strict network environments
RUN pip install --no-cache-dir --trusted-host pypi.python.org -r requirements.txt

# Copy the rest of the application's code into the container at /app
# This includes main.py and the src, utils, and config directories
COPY . .

# Define the command to run your application
# This will be executed when the container starts
CMD ["python", "main.py"]
