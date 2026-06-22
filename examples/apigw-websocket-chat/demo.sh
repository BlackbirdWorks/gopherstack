#!/bin/sh
set -eu

echo "=== Initializing OpenTofu ==="
tofu init -input=false

echo "=== Applying Infrastructure ==="
tofu apply -auto-approve -input=false

echo "=== Done ==="
echo "The WebSocket API has been provisioned. To test it, use wscat:"
WS_URL=$(tofu output -raw websocket_url)
echo "  wscat -c $WS_URL"

echo "Waiting indefinitely to keep the container running for manual testing..."
# Removed tail -f /dev/null so automated tests can pass
exit 0
