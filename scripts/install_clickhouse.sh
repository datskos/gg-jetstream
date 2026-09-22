#!/usr/bin/env bash
# Provision a NEW Debian/Ubuntu systemd instance with a dedicated ClickHouse disk.
# Usage: sudo bash scripts/install_clickhouse.sh PASSWORD /dev/nvme1n1
# Formats the specified blank disk as ext4 and mounts it at /var/lib/clickhouse.
# Refuses existing disk signatures, partitions, mounts, and ClickHouse data.
# EC2 instance-store data is lost on instance stop/termination; use EBS for durability.
# Official install instructions: https://clickhouse.com/docs/install/debian_ubuntu
set -euo pipefail

if (( $# != 2 )) || [[ -z "$1" || -z "$2" ]]; then
    echo "Usage: sudo bash $0 PASSWORD /dev/DATA_DISK" >&2
    echo "A non-empty password and a blank, dedicated data disk are required." >&2
    exit 1
fi
jetstreamer_password=$1
data_disk=$(readlink -f -- "$2")
data_dir=/var/lib/clickhouse

die() {
    echo "Error: $*" >&2
    exit 1
}

if (( EUID != 0 )); then
    echo "Run this script as root (sudo bash $0 PASSWORD /dev/DATA_DISK)." >&2
    exit 1
fi

[[ -d /run/systemd/system ]] || die "A running systemd instance is required."
for command in lsblk wipefs blkid findmnt mountpoint mkfs.ext4; do
    command -v "$command" > /dev/null || die "Required command not found: $command"
done

# Validate everything before installing packages or touching the disk. Never
# infer the data disk from NVMe numbering, which can change between boots.
[[ -b "$data_disk" ]] || die "$data_disk is not a block device."
[[ $(lsblk -dnro TYPE "$data_disk") == disk ]] || die "Specify a whole disk."
[[ $(lsblk -dnro RO "$data_disk") == 0 ]] || die "The disk is read-only."
[[ $(lsblk -nrpo NAME "$data_disk" | wc -l) -eq 1 ]] || die "The disk has partitions or dependent devices."
[[ -z $(lsblk -nrpo MOUNTPOINTS "$data_disk" | tr -d '[:space:]') ]] || die "The disk is mounted."
[[ -z $(wipefs --no-act --noheadings --output TYPE "$data_disk") ]] || die "The disk already contains filesystem/partition signatures."
if mountpoint -q "$data_dir"; then
    die "$data_dir is already a mount point."
fi
[[ ! -L "$data_dir" ]] || die "$data_dir must not be a symlink."
if [[ -d "$data_dir" ]] && [[ -n $(find "$data_dir" -mindepth 1 -maxdepth 1 -print -quit) ]]; then
    die "$data_dir contains existing data; this installer is for new instances."
fi
if findmnt --fstab --mountpoint "$data_dir" > /dev/null; then
    die "$data_dir already has an fstab entry."
fi
if systemctl is-active --quiet clickhouse-server; then
    die "ClickHouse is already running; stop and migrate it separately."
fi

echo "Formatting $data_disk and mounting it at $data_dir..."
mkfs.ext4 -m 0 -L clickhouse "$data_disk"
install -d -m 0755 "$data_dir"
mount "$data_disk" "$data_dir"
disk_uuid=$(blkid -s UUID -o value "$data_disk")
[[ -n "$disk_uuid" ]] || die "Could not determine the data disk UUID."
cp -a /etc/fstab "/etc/fstab.before-clickhouse.$(date +%s)"
printf 'UUID=%s %s ext4 defaults,noatime,nofail 0 2\n' "$disk_uuid" "$data_dir" >> /etc/fstab

# Install the dependency before apt can start ClickHouse. If storage disappears,
# the service must fail instead of silently writing to the root filesystem.
install -d -m 0755 /etc/systemd/system/clickhouse-server.service.d
cat > /etc/systemd/system/clickhouse-server.service.d/storage.conf <<'UNIT'
[Unit]
RequiresMountsFor=/var/lib/clickhouse

[Service]
ExecStartPre=/usr/bin/mountpoint -q /var/lib/clickhouse
UNIT
systemctl daemon-reload

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y apt-transport-https ca-certificates curl gnupg

install -d -m 0755 /usr/share/keyrings /etc/apt/sources.list.d
curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
    | gpg --batch --yes --dearmor -o /usr/share/keyrings/clickhouse-keyring.gpg
chmod 0644 /usr/share/keyrings/clickhouse-keyring.gpg
arch=$(dpkg --print-architecture)
printf 'deb [signed-by=/usr/share/keyrings/clickhouse-keyring.gpg arch=%s] https://packages.clickhouse.com/deb stable main\n' "$arch" \
    > /etc/apt/sources.list.d/clickhouse.list

apt-get update
apt-get install -y clickhouse-server clickhouse-client
chown clickhouse:clickhouse "$data_dir"

# Allow the default admin account to create SQL-managed users and grant access.
install -d -m 0755 /etc/clickhouse-server/users.d
cat > /etc/clickhouse-server/users.d/jetstreamer-access-management.xml <<'XML'
<clickhouse>
    <users>
        <default>
            <access_management>1</access_management>
        </default>
    </users>
</clickhouse>
XML
chmod 0644 /etc/clickhouse-server/users.d/jetstreamer-access-management.xml

systemctl enable clickhouse-server
systemctl restart clickhouse-server

admin_client=(clickhouse-client --host 127.0.0.1 --user default)
ready=false
for (( attempt = 0; attempt < 60; attempt++ )); do
    if "${admin_client[@]}" --connect_timeout 2 --receive_timeout 2 --query 'SELECT 1' > /dev/null 2>&1; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    echo "ClickHouse did not become ready. Check its service logs and admin credentials." >&2
    exit 1
fi

# Only interpolate the hex digest into SQL so quotes and backslashes in the
# supplied password cannot change the query.
password_hash=$(printf '%s' "$jetstreamer_password" | sha256sum)
password_hash=${password_hash%% *}
"${admin_client[@]}" --multiquery <<SQL
CREATE DATABASE IF NOT EXISTS jetstreamer;
CREATE USER IF NOT EXISTS jetstreamer IDENTIFIED WITH sha256_hash BY '$password_hash';
ALTER USER jetstreamer IDENTIFIED WITH sha256_hash BY '$password_hash' DEFAULT DATABASE jetstreamer;
GRANT ALL ON jetstreamer.* TO jetstreamer;
SQL

clickhouse-client --host 127.0.0.1 --user jetstreamer --password "$jetstreamer_password" \
    --database jetstreamer --query 'SELECT currentUser(), currentDatabase()'
echo "ClickHouse is ready: database=jetstreamer user=jetstreamer"
df -h "$data_dir"
cat <<'INFO'

Use JETSTREAMER_CLICKHOUSE_MODE=remote even when connecting to localhost.
This uses the installed service instead of extracting a bundled ClickHouse onto /.

JETSTREAMER_THREADS=128 \
JETSTREAMER_CLICKHOUSE_MODE=remote \
JETSTREAMER_CLICKHOUSE_DSN='http://jetstreamer:URL_ENCODED_PASSWORD@127.0.0.1:8123/?database=jetstreamer' \
./jetstreamer 1038 --with-plugin tx-metadata --tui

Replace URL_ENCODED_PASSWORD with your URL-encoded password and 1038 with your epoch/range.
INFO
