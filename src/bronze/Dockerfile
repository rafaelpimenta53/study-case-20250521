FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN apt-get install -y tzdata
ENV TZ="America/Sao_Paulo"

# Copy poetry files
COPY pyproject.toml poetry.lock* ./

# Install poetry
RUN pip install poetry

# Configure poetry to not create virtual environment
RUN poetry config virtualenvs.create false

# Install dependencies (use --no-root to skip installing the current project)
RUN poetry install --with bronze --no-root

# Copy source code
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1

# Run the bronze pipeline
CMD ["python", "src/bronze/main.py"]
