# syntax=docker/dockerfile:1

FROM python:3.12-slim AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv

WORKDIR /app

# uv is copied from the official image instead of installed with pip, keeping uv
# as the only Python dependency-management tool used by the Docker build.
COPY --from=ghcr.io/astral-sh/uv:0.9.30 /uv /uvx /usr/local/bin/

# Copy dependency metadata first so Docker can reuse the dependency layer when
# only application code changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --extra dev

FROM python:3.12-slim AS runtime

ENV PATH="/opt/venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# The runtime image only receives the already-created virtualenv and app code;
# build tooling and uv stay out of the final image.
COPY --from=builder /opt/venv /opt/venv
COPY app ./app
COPY alembic ./alembic
COPY alembic.ini ./alembic.ini
COPY tests ./tests
COPY pytest.ini ./pytest.ini

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
