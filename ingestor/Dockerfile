FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml .
COPY mqtt_consumer.py .
RUN uv sync --no-dev
CMD ["uv", "run", "python", "mqtt_consumer.py"]
