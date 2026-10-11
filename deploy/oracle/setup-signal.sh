#!/usr/bin/env bash
# Provision Oracle Cloud Ubuntu VM -> jalankan binance-prediction-signal (Node 22) via pm2.
# Jalankan DI VM (mis. sebagai user `ubuntu` yang punya sudo):
#   REPO_URL=https://github.com/<user>/binance-prediction-signal.git bash setup-signal.sh
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/hobengtv-web/binance-prediction-signal.git}"
APP_DIR="${APP_DIR:-$HOME/binance-prediction-signal}"
DATA_DIR="${DATA_DIR:-/data}"
PORT="${PORT:-8000}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "== 1) Node 22 + pm2 =="
if ! command -v node >/dev/null 2>&1 || [ "$(node -v | sed 's/v\([0-9]*\).*/\1/')" -lt 20 ]; then
  curl -fsSL https://deb.nodesource.com/setup_22.x | sudo -E bash -
  sudo apt-get install -y nodejs git
fi
sudo npm i -g pm2 || npm i -g pm2

echo "== 2) ambil repo =="
if [ -d "$APP_DIR/.git" ]; then (cd "$APP_DIR" && git pull --ff-only) ; else git clone "$REPO_URL" "$APP_DIR" ; fi
cd "$APP_DIR"

echo "== 3) direktori data persisten ($DATA_DIR) =="
sudo mkdir -p "$DATA_DIR/ledger" "$DATA_DIR/models"
sudo chown -R "$USER":"$USER" "$DATA_DIR"

echo "== 4) env =="
mkdir -p "$HOME/.config"
cp -f "$HERE/signal.env" "$HOME/.config/signal.env"
# PORT bisa di-override
sed -i "s/^export PORT=.*/export PORT=$PORT/" "$HOME/.config/signal.env" || true

echo "== 5) start pm2 (muat env) =="
# shellcheck disable=SC1090
set -a; . "$HOME/.config/signal.env"; set +a
pm2 delete signal 2>/dev/null || true
pm2 start server.js --name signal --update-env
pm2 save
sudo env PATH="$PATH" pm2 startup systemd -u "$USER" --hp "$HOME" | tail -1 | bash || true

echo "== 6) firewall (bila ufw aktif) =="
sudo ufw allow "${PORT}/tcp" 2>/dev/null || true

echo "== selesai. Cek: curl -s localhost:${PORT}/api/signal?tf=5m | head -c 200 =="
echo "== Untuk HTTPS publik pakai Cloudflare Tunnel:"
echo "   curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 -o ~/cloudflared && chmod +x ~/cloudflared"
echo "   ~/cloudflared tunnel --url http://localhost:${PORT}"
