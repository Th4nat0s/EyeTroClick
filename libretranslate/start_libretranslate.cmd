#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

exec "$SCRIPT_DIR/.venv/bin/libretranslate" --host 127.0.0.1 --port 5050 
# --force-update-model
