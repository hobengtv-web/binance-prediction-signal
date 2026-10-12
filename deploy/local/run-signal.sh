#!/usr/bin/env bash
# Jalankan binance-prediction-signal LOKAL di Mac (dipakai oleh pm2).
# Data persisten: ./ledger (ledger+engine session) & ./models.
cd "$(dirname "$0")/../.." || exit 1
APP_DIR="$PWD"
set -a
. "$APP_DIR/deploy/oracle/signal.env"
set +a
# Override path utk Mac (env VM memakai /data):
export LEDGER_DIR="$APP_DIR/ledger"
export DATA_DIR="$APP_DIR/ledger"
export ENGINE_SESSION_FILE="$APP_DIR/ledger/engine_session.json"
export PORT="${PORT:-8000}"
exec node server.js
