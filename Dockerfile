FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y \
    git \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip
COPY pyproject.toml .
RUN pip install .[dev]

COPY . .

ENTRYPOINT ["bash"]