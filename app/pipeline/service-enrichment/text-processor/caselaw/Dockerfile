# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application's code into the container at /app
# This assumes your application code is in a directory named 'app' and you have a main.py
COPY . .

# Make port 80 available to the world outside this container (if needed for a web service)
# For a batch job like this, it's not strictly necessary but good practice.
EXPOSE 80

# Define environment variables (can be overridden by Fargate task definition)
ENV PYTHONUNBUFFERED=1

# Run main.py when the container launches
CMD ["python", "main.py"]