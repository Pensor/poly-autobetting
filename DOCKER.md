# Docker Deployment Guide

This guide explains how to build and run the poly-autobetting bot in Docker.

## Quick Start

### Using docker-compose (Recommended)

1. **Set up environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your Polymarket credentials
   nano .env
   ```

2. **Start the bot:**
   ```bash
   docker-compose up -d
   ```

3. **View logs:**
   ```bash
   docker-compose logs -f bot
   ```

4. **Stop the bot:**
   ```bash
   docker-compose down
   ```

## Manual Docker Commands

### Build the image
```bash
docker build -t poly-autobetting .
```

### Run with environment variables
```bash
docker run -d \
  --name poly-autobetting \
  --restart unless-stopped \
  -e POLYMARKET_PRIVATE_KEY=0x... \
  -e POLYMARKET_FUNDER=0x... \
  -e POLYMARKET_API_KEY=... \
  -e POLYMARKET_API_SECRET=... \
  -e POLYMARKET_PASSPHRASE=... \
  -e POLYMARKET_BUILDER_API_KEY=... \
  -e POLYMARKET_BUILDER_SECRET=... \
  -e POLYMARKET_BUILDER_PASSPHRASE=... \
  -v $(pwd)/state:/app/state \
  poly-autobetting
```

### Run with .env file
```bash
docker run -d \
  --name poly-autobetting \
  --restart unless-stopped \
  --env-file .env \
  -v $(pwd)/state:/app/state \
  poly-autobetting
```

### View logs
```bash
docker logs -f poly-autobetting
```

### Stop and remove container
```bash
docker stop poly-autobetting
docker rm poly-autobetting
```

## Gasless Redemptions (Optional)

To enable auto-redemption of resolved positions via gasless relayer, you need builder relayer credentials.

**Enable in docker-compose.yml:**
```yaml
services:
  bot:
    build:
      context: .
      args:
        INSTALL_RELAYER: 1  # Change to 1 to install relayer packages
```

Then set these in `.env`:
```bash
POLYMARKET_BUILDER_API_KEY=...
POLYMARKET_BUILDER_SECRET=...
POLYMARKET_BUILDER_PASSPHRASE=...
```

**Or build manually:**
```bash
docker build --build-arg INSTALL_RELAYER=1 -t poly-autobetting .
```

If you don't have builder relayer credentials, the bot will still work but won't auto-redeem (you'll redeem manually).

## Configuration

### Environment Variables

All Polymarket credentials and bot configuration can be passed via environment variables:

**Required (for bot to work):**
- `POLYMARKET_PRIVATE_KEY` — Your wallet private key
- `POLYMARKET_API_KEY` — CLOB API key (derived from private key if not set)
- `POLYMARKET_API_SECRET` — CLOB API secret
- `POLYMARKET_PASSPHRASE` — CLOB API passphrase

**Optional:**
- `POLYMARKET_FUNDER` — Funder/proxy address
- `POLYMARKET_BUILDER_API_KEY` — Builder relayer API key (for gasless redemptions)
- `POLYMARKET_BUILDER_SECRET` — Builder relayer secret
- `POLYMARKET_BUILDER_PASSPHRASE` — Builder relayer passphrase

**Bot Configuration (all optional, use defaults if not set):**
- `BOT_SESSION_CAPITAL_LIMIT=300.0` — Capital limit per session
- `BOT_SHARES_PER_ORDER=10.0` — Shares per order
- `BOT_STRATEGY=active` — Trading strategy (active/passive/trend)
- See `src/bot/bot_config.py` for all available `BOT_*` variables

### Mount State Directory (Optional)

To persist bot state across restarts:
```bash
docker run -d ... -v $(pwd)/state:/app/state poly-autobetting
```

## Running Different Entry Points

### Simple 45c Bot (Default)
The docker-compose.yml and Dockerfile default to running `scripts/place_45.py`.

### Complex Market-Making Bot
To run the full bot with advanced strategies, override the command:

**With docker-compose:**
```yaml
services:
  bot:
    # ... rest of config ...
    command: python -m src.bot.runner
```

**With docker run:**
```bash
docker run -d \
  --env-file .env \
  poly-autobetting \
  python -m src.bot.runner
```

## Troubleshooting

### Container exits immediately
Check logs for errors:
```bash
docker logs poly-autobetting
```

Common issues:
- Missing or invalid `POLYMARKET_PRIVATE_KEY` in `.env`
- Missing CLOB API credentials
- Network connectivity issues

### Rebuilding the image after code changes
```bash
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

### Accessing the container for debugging
```bash
docker exec -it poly-autobetting bash
```

### Checking state files
```bash
docker exec poly-autobetting ls -la /app/state
```

## Production Considerations

1. **Use secrets management** — Don't hardcode credentials; use Docker secrets or environment variable management tools (Vault, AWS Secrets Manager, etc.)

2. **Set resource limits:**
   ```yaml
   services:
     bot:
       deploy:
         resources:
           limits:
             cpus: '1'
             memory: 512M
   ```

3. **Use a reverse proxy** — If exposing any services, use nginx or similar

4. **Monitor logs** — Integrate with centralized logging (ELK stack, CloudWatch, etc.)

5. **Backup state** — Regularly backup the `/app/state` volume

6. **Network isolation** — Run on a dedicated network if using multiple containers

## Docker Image Optimization

The image uses `python:3.11-slim` to reduce size (~150MB). For production deployments on resource-constrained systems, consider:

- Using `python:3.11-alpine` (smaller, but requires more build dependencies)
- Multi-stage builds if adding additional tools

## Health Monitoring

The docker-compose.yml includes a basic healthcheck. For production, consider:
- Adding metrics export (Prometheus)
- Using container orchestration (Kubernetes, Docker Swarm)
- Implementing alerting based on container status
