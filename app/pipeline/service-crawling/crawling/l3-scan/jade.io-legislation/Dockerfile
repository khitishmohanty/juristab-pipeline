# Dockerfile

# Use the modern python:3.9-slim-bullseye base image.
FROM python:3.9-slim-bullseye

# Set the working directory.
WORKDIR /app

# Install dependencies required for Chrome and chromedriver.
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    wget \
    gnupg \
    --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

# Add Google's official repository and install stable Chrome.
# This ensures all dependencies are correctly handled by the package manager.
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Manually install the matching Chromedriver for the installed Chrome version.
# This ensures a fixed, working version inside the container.
RUN CHROME_VERSION=$(google-chrome-stable --product-version | cut -d. -f1) && \
    DRIVER_VERSION=$(curl -s "https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_${CHROME_VERSION}") && \
    curl -Lo "/tmp/chromedriver.zip" "https://storage.googleapis.com/chrome-for-testing-public/${DRIVER_VERSION}/linux64/chromedriver-linux64.zip" && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    rm -rf /tmp/chromedriver.zip /usr/local/bin/chromedriver-linux64

# Copy requirements.txt first to leverage Docker cache.
COPY requirements.txt .

# Install Python packages.
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code.
COPY . .

# Set the final command.
CMD ["python", "main.py"]
