#!/usr/bin/env bash
# Bootstrap Relay on a fresh Oracle Cloud "Always Free" Ampere (ARM) VM.
# Ubuntu 22.04. Run as the default 'ubuntu' user:
#
#   curl -fsSL https://raw.githubusercontent.com/prathameshbs007/Web-hooks/main/scripts/deploy-oracle.sh | bash
#   # ...or clone the repo and run: bash scripts/deploy-oracle.sh
#
# Before running, in the Oracle console open ingress for TCP 80 and 443 in the
# instance's subnet Security List (or an NSG). This script opens the *instance*
# firewall (iptables), which Ubuntu Oracle images ship locked down — that second
# firewall is the usual reason "it won't connect" after the cloud rules look right.
set -euo pipefail

REPO="${RELAY_REPO:-https://github.com/prathameshbs007/Web-hooks.git}"
DIR="${RELAY_DIR:-$HOME/relay}"

echo "==> Installing Docker (official convenience script)"
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker "$USER" || true
fi

echo "==> Opening the instance firewall for 80/443 (Oracle images block these by default)"
# Insert accept rules before the default REJECT, then persist.
sudo iptables -I INPUT -p tcp --dport 80 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 443 -j ACCEPT
sudo netfilter-persistent save 2>/dev/null || {
  sudo apt-get update -y && sudo apt-get install -y iptables-persistent
  sudo netfilter-persistent save
}

echo "==> Cloning the repo into $DIR"
if [ -d "$DIR/.git" ]; then
  git -C "$DIR" pull --ff-only
else
  git clone "$REPO" "$DIR"
fi
cd "$DIR"

if [ ! -f .env ]; then
  echo "==> Creating .env from the example (fill in the placeholders below)"
  cp .env.example .env
  # Generate strong secrets so the defaults are never shipped to prod.
  ADMIN=$(openssl rand -hex 24)
  GFPW=$(openssl rand -hex 16)
  sed -i "s/^ADMIN_TOKEN=.*/ADMIN_TOKEN=${ADMIN}/" .env
  grep -q '^GF_SECURITY_ADMIN_PASSWORD=' .env \
    && sed -i "s/^GF_SECURITY_ADMIN_PASSWORD=.*/GF_SECURITY_ADMIN_PASSWORD=${GFPW}/" .env \
    || echo "GF_SECURITY_ADMIN_PASSWORD=${GFPW}" >> .env
  grep -q '^RELAY_DOMAIN=' .env || echo "RELAY_DOMAIN=CHANGE-ME.duckdns.org" >> .env

  cat <<EOF

  .env created with generated secrets. You MUST still set two values:

    RELAY_DOMAIN=<your-name>.duckdns.org   # the public hostname (DNS -> this VM's IP)
    LLM_API_KEY=<your gemini key>          # optional, enables real agent diagnoses

  Edit it:   nano $DIR/.env
  Then re-run:  bash scripts/deploy-oracle.sh

EOF
  exit 0
fi

DOMAIN=$(grep '^RELAY_DOMAIN=' .env | cut -d= -f2-)
if [ -z "$DOMAIN" ] || [ "$DOMAIN" = "CHANGE-ME.duckdns.org" ]; then
  echo "!! Set RELAY_DOMAIN in $DIR/.env to your real hostname first."; exit 1
fi

echo "==> Bringing up the stack (prod overlay) for $DOMAIN"
sg docker -c "docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build" \
  || docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

cat <<EOF

==> Up. Caddy is fetching a TLS cert (needs DNS for $DOMAIN pointing here + ports 80/443 open).

  Dashboard : https://$DOMAIN/
  API docs  : https://$DOMAIN/docs
  Grafana   : private - reach it with an SSH tunnel:
              ssh -L 3000:localhost:3000 ubuntu@<this-vm-ip>   then open http://localhost:3000

  Logs:   docker compose logs -f caddy api agent
EOF
