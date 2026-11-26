#!/bin/sh
set -eu

echo "=== Initializing OpenTofu ==="
tofu init -input=false

echo "=== Applying Infrastructure ==="
tofu apply -auto-approve -input=false

# The websocket_url output is the AWS-style host, which Gopherstack does not serve.
API_ID=$(tofu output -raw websocket_url | sed 's#^https://\([^.]*\)\..*#\1#')
WS_URL="ws://localhost:8000/v2proxy/${API_ID}/prod"

echo "=== Done ==="
echo "The WebSocket API has been provisioned. To test it, use wscat:"
echo "  wscat -c $WS_URL"
echo "Then send any text; the Lambda echoes it back as 'Echo: <text>'."
