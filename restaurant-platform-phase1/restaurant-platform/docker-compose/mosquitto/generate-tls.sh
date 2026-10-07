#!/bin/sh
# Makes the broker's certificate for the Compose stack, once, and keeps it. Run by the `mqtt-tls-init` service before
# Mosquitto starts; nothing here is secret except the private key, which goes only to the volume the broker (and this
# service) can see. The certificate is self-signed and is itself what the clients trust (the same arrangement as
# k8s/mosquitto-tls): it names the broker as `mosquitto` (its Compose service name) and `localhost`.
#
# A certificate that is there and has more than 30 days left is kept, so a restart never changes what clients trust.
# One that is missing or about to expire is replaced; running clients keep the old copy until they restart.
set -eu
umask 077

public=/tls-public/tls.crt
private=/tls-private/tls.key

if [ -s "$public" ] && [ -s "$private" ] && openssl x509 -in "$public" -noout -checkend 2592000 >/dev/null 2>&1; then
  echo "the broker's certificate is present and valid for more than 30 days: keeping it"
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout "$private" -out "$public" -subj "/CN=mosquitto" \
  -addext "subjectAltName=DNS:mosquitto,DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1
chmod 0600 "$private"
chmod 0644 "$public"   # the certificate is public: every client reads it to know whom to trust
echo "made a new certificate for the broker (valid 10 years)"
