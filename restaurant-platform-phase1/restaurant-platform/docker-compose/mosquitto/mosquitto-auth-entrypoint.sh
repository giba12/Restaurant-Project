#!/bin/ash
# Builds Mosquitto's password file from the environment, then starts the broker the way the image normally does.
# Each user's password is MQTT_PASSWORD_<USER>, the user name upper-cased with '-' as '_'; unset means the local-development
# placeholder (the same convention as the database passwords in docker-compose.yml). Nothing is kept on disk between runs.
set -eu

# Everything created here is private from the start: mosquitto_passwd itself warns about a password file others can read.
umask 077

mkdir -p /mosquitto/auth
chmod 0755 /mosquitto/auth  # the broker, as its own user, must be able to enter it; only the files inside are private
passwd=/mosquitto/auth/passwd
: > "$passwd"

for user in sim-plate-cam-01 sim-pos-01 sim-ticket-timer-01 sim-staffing-sensor-01 rp-mqtt-kafka-bridge edge-operator; do
  variable="MQTT_PASSWORD_$(echo "$user" | tr 'a-z-' 'A-Z_')"
  password="$(printenv "$variable" || true)"
  mosquitto_passwd -b "$passwd" "$user" "${password:-changeme-local-dev-only}"
done

# The ACL comes from the repository (mounted read-only, owned by whoever checked it out); copy it beside the password file.
cp /etc/mosquitto/acl.source /mosquitto/auth/acl

# The broker drops to this user, and warns that a future version will refuse a password or ACL file it does not own
# or that others can read.
chown mosquitto:mosquitto "$passwd" /mosquitto/auth/acl
chmod 0600 "$passwd" /mosquitto/auth/acl

exec /docker-entrypoint.sh /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
