#!/bin/sh
# Provision/upgrade dot-bridge inside the dot-bridge container. Run as root in the
# container from a directory containing dot_bridge/ and deploy/. Idempotent.
set -eu
SRC=$(cd "$(dirname "$0")/.." && pwd)

# Same approved public resolvers the egress ACL allows.
if [ -L /etc/resolv.conf ] || ! grep -q '^nameserver 223.5.5.5' /etc/resolv.conf; then
  rm -f /etc/resolv.conf
  printf 'nameserver 223.5.5.5\nnameserver 1.1.1.1\noptions timeout:2 attempts:2\n' > /etc/resolv.conf
fi
if systemctl is-enabled systemd-resolved >/dev/null 2>&1; then
  systemctl disable --now systemd-resolved
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -y -q --no-install-recommends ca-certificates python3 python3-aiohttp python3-qrcode python3-cryptography

id dot-bridge >/dev/null 2>&1 || useradd --system --home-dir /var/lib/dot-bridge \
  --no-create-home --shell /usr/sbin/nologin dot-bridge
chown dot-bridge:dot-bridge /var/lib/dot-bridge
chmod 0700 /var/lib/dot-bridge

rm -rf /opt/dot-bridge.new
install -d -m 0755 /opt/dot-bridge.new/dot_bridge
install -m 0644 "$SRC"/dot_bridge/*.py /opt/dot-bridge.new/dot_bridge/
rm -rf /opt/dot-bridge.old
[ -d /opt/dot-bridge ] && mv /opt/dot-bridge /opt/dot-bridge.old
mv /opt/dot-bridge.new /opt/dot-bridge

install -m 0755 "$SRC"/deploy/dot-bridge.sh /usr/local/bin/dot-bridge
install -m 0644 "$SRC"/deploy/dot-bridge.service /etc/systemd/system/dot-bridge.service
install -d -m 0755 /etc/systemd/system/dot-bridge.service.d
install -m 0644 "$SRC"/deploy/zzzz-agentbox-hardening.conf \
  /etc/systemd/system/dot-bridge.service.d/zzzz-agentbox-hardening.conf

systemctl daemon-reload
systemd-analyze verify /etc/systemd/system/dot-bridge.service
systemctl enable dot-bridge.service
systemctl restart dot-bridge.service
