#!/usr/bin/env sh
# CI entrypoint: lint then test. Fails fast on the first error.
set -e

echo "== ruff =="
ruff check .

echo "== pytest =="
pytest -q
