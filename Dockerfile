FROM node:22-bookworm-slim

# App Node murni (tanpa dependency npm) — tidak perlu npm install.
ENV NODE_ENV=production
WORKDIR /app

COPY . .

# Data persisten (ledger + models) di-mount ke /data oleh docker-compose.
ENV PORT=8000 \
    LEDGER_DIR=/data/ledger \
    DATA_DIR=/data/ledger \
    ENGINE_SESSION_FILE=/data/ledger/engine_session.json
RUN mkdir -p /data/ledger /data/models

EXPOSE 8000

# Entry: node server.js (lihat package.json "start")
CMD ["node", "server.js"]
