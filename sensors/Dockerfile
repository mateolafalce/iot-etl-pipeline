FROM python:3.11-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml .
COPY simulator.py .
RUN uv sync --no-dev
CMD ["uv", "run", "python", "simulator.py"]
