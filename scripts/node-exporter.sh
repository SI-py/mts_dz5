set -eu
mkdir -p /tmp/node-proc
for entry in /proc/*; do
    ln -sfn "$entry" "/tmp/node-proc/${entry##*/}"
done
ln -sfn /proc/1/net /tmp/node-proc/net
exec "$@"
