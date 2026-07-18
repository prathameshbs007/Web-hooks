FROM python:3.12-slim

WORKDIR /app

# Install deps first so source edits don't bust the layer cache.
COPY pyproject.toml ./
RUN pip install --no-cache-dir .

COPY alembic.ini ./
COPY alembic ./alembic
COPY relay ./relay
COPY flaky_endpoint ./flaky_endpoint
RUN pip install --no-cache-dir --no-deps .

CMD ["uvicorn", "relay.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
