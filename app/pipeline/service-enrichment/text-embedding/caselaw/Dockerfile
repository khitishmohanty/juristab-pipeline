# Dockerfile

# Use a RunPod base image with Python, PyTorch, and CUDA
FROM runpod/pytorch:2.1.0-py3.10-cuda11.8.0-devel

# Set the working directory
WORKDIR /app

# Copy dependency file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all your application code into the image
# This includes the handler, src, utils, and config folders
COPY . .

# Command to run the RunPod handler
CMD ["python", "-u", "main.py"] 