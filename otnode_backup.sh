#before you move jnl file to zfs ensure you set smaller record size 16k or 8k for you pool like this - zfs set recordsize=16K blzpool

#!/bin/bash
set -euo pipefail
exec > >(tee -a /root/zfs_backup.log) 2>&1

TELEGRAM_BOT_TOKEN="YOUR_TG_BOT_TOKEN"
TELEGRAM_CHAT_ID="YOUR_CHAT_ID"
SRC_POOL="LOCAL_POOL_NAME"
DST_POOL="REMOTE_POOL_NAME"
DST_HOST="YOUR_REMOTE_HOST_DOMAIN_NAME_OR_IP"
SNAP_NAME="bk_$(date +%F_%H-%M-%S)"

notify_telegram() {
    local msg="$1"
    echo "$1"
	curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="${TELEGRAM_CHAT_ID}" \
        -d parse_mode="Markdown" \
        -d text="${msg}" >/dev/null
}
trap 'notify_telegram "❌ *ZFS backup FAILED* on $(hostname) at $(date). Check /root/zfs_backup.log"' ERR

notify_telegram "🚀 *ZFS backup started* on $(hostname) at $(date)"
notify_telegram "stopping otnode..."
systemctl stop otnode
sleep 5
notify_telegram "starting SQL dump..."
mysqldump -u root -padmin operationaldb > /${SRC_POOL}/operationaldb_backup.sql

#capture number of triples in the blazegraph at the source
mkdir -p /root/blaze_count
curl --fail -s -X POST http://localhost:9999/blazegraph/namespace/dkg/sparql -H "Content-Type: application/sparql-query" -H "Accept: application/sparql-results+json" \
	--data 'SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }' > /root/blaze_count/${SNAP_NAME}.json

# Extract triple count from JSON
triples=$(grep -oP '"value"\s*:\s*"\K[0-9]+' /root/blaze_count/${SNAP_NAME}.json || echo "unknown")

if [[ "$triples" =~ ^[0-9]+$ ]]; then
  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" -d chat_id="${TELEGRAM_CHAT_ID}" -d parse_mode="Markdown" \
	  -d text="Blazegraph snapshot *${SNAP_NAME}*. Triple count at the source: \`${triples}\`" >/dev/null
else
  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" -d chat_id="${TELEGRAM_CHAT_ID}" -d parse_mode="Markdown" \
	  -d text="❌ Could not get triple count at the source for *${SNAP_NAME}*" >/dev/null
  cat /root/blaze_count/${SNAP_NAME}.json
fi

systemctl stop blazegraph
sleep 5
notify_telegram "creating snapshot..."
zfs snapshot ${SRC_POOL}@${SNAP_NAME}
notify_telegram "snapshot created"

notify_telegram "starting blazegraph and otnode..."
systemctl start blazegraph
sleep 5
systemctl start otnode

if ! timeout 60 bash -c 'until journalctl -u otnode.service -n 200 | grep -q "Node is up and running!"; do sleep 1; done'; then
  notify_telegram "❌ OT-Node failed to start in time"
  exit 1
else
  notify_telegram "*OT-Node is up and running*"
fi


# Get latest local snapshot
latest_snap=$(zfs list -t snapshot -o name -s creation -H | grep "^${SRC_POOL}@" | tail -n1)

# Get latest snapshot on remote
remote_snap=$(ssh root@${DST_HOST} "zfs list -t snapshot -o name -s creation -H | grep '^${DST_POOL}@' | tail -n1")

# Extract snapshot names (e.g., bk_2025-05-15_234533)
latest_name=${latest_snap#${SRC_POOL}@}
remote_name=${remote_snap#${DST_POOL}@}

echo -e "Source:\t\t   $latest_snap"
echo "Destination has: $remote_snap"

#ensure remote side is on latest local snapshot, it's is not as starting blaze for validation changes the data
ssh root@${DST_HOST} "zfs rollback ${DST_POOL}@${remote_snap#${DST_POOL}@}"

notify_telegram "sending snapshot..."
# Send snapshot
if [[ -z "$remote_name" ]]; then
    echo "No matching snapshot on remote. Sending full snapshot..."
    zfs send -c "${latest_snap}" | ssh root@${DST_HOST} zfs receive -F ${DST_POOL}
else
    echo "Sending incremental snapshot from ${remote_name}..."
    zfs send -c -i "${SRC_POOL}@${remote_name}" "${latest_snap}" | ssh root@${DST_HOST} zfs receive ${DST_POOL}
fi

#validate jnl on the remote host
notify_telegram "validating snapshot at the destination..."
ssh root@backup.ambbot.com DST_POOL="$DST_POOL" SNAP_NAME="$SNAP_NAME" TELEGRAM_BOT_TOKEN="$TELEGRAM_BOT_TOKEN" TELEGRAM_CHAT_ID="$TELEGRAM_CHAT_ID" 'bash -s' <<'EOF'
	set -euo pipefail
	echo "Starting Blazegraph..."
	systemctl start blazegraph.service
	sleep 5

	echo "Running validation query..."
	mkdir -p /root/blaze_count
	curl --fail -s -X POST http://localhost:9999/blazegraph/namespace/dkg/sparql -H "Content-Type: application/sparql-query" -H "Accept: application/sparql-results+json" \
	  --data 'SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }' > /root/blaze_count/${SNAP_NAME}.json

	# Extract triple count from JSON
	triples=$(grep -oP '"value"\s*:\s*"\K[0-9]+' /root/blaze_count/${SNAP_NAME}.json || echo "unknown")
	echo $triples
	# Report and stop
	if [[ "$triples" =~ ^[0-9]+$ ]]; then
	  echo "✅ Blazegraph snapshot is healthy: ${triples} triples"
	  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" -d chat_id="${TELEGRAM_CHAT_ID}" -d parse_mode="Markdown" \
		  -d text="✅ Blazegraph snapshot *${SNAP_NAME}* verified. Triple count: \`${triples}\`" >/dev/null
	  
	else
	  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" -d chat_id="${TELEGRAM_CHAT_ID}" -d parse_mode="Markdown" \
		-d text="❌ Could not get triple count at the destination for *${SNAP_NAME}*" >/dev/null
	  cat /root/blaze_count/${SNAP_NAME}.json

	  # Extract first 4000 characters (safe margin) and escape backticks
	  payload=$(head -c 4000 /root/blaze_count/${SNAP_NAME}.json | sed 's/`/\\`/g')

	  curl -s -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
		-d chat_id="${TELEGRAM_CHAT_ID}" \
		-d parse_mode="Markdown" \
		-d text="❌ *Blazegraph validation failed* for *${SNAP_NAME}*.\n\n\`\`\`\n${payload}\n\`\`\`" >/dev/null

	  exit 1

	fi

	echo "Stopping Blazegraph..."
	systemctl stop blazegraph.service
EOF


# Keep only the 14 most recent snapshots
zfs list -t snapshot -o name -s creation -H | grep "^${SRC_POOL}@" | head -n -14 | xargs -r -n1 zfs destroy
ssh root@"$DST_HOST" "zfs list -t snapshot -o name -s creation -H | grep '^${DST_POOL}@' | head -n -14 | xargs -r -n1 zfs destroy"

notify_telegram "✅ *ZFS backup completed successfully* on $(hostname) at $(date)"
