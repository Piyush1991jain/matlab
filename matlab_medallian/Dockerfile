FROM python:3.10-slim

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install NEW Databricks CLI (NOT pip version)
RUN curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh

# Copy project
COPY . /app

# Install Python deps (DO NOT install old databricks-cli)
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["bash", "entrypoint.sh"]