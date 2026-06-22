#!/usr/bin/env bash
set -e

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "=== Initializing OpenTofu ==="
tofu init

echo ""
echo "=== Deploying Cognito API Auth Stack to Local Gopherstack ==="
tofu apply -auto-approve

USER_POOL_ID=$(tofu output -raw user_pool_id)
CLIENT_ID=$(tofu output -raw client_id)
API_URL=$(tofu output -raw api_endpoint)

echo ""
echo "=== Creating a new User in Cognito ==="
# Use admin-create-user to bypass sign-up confirmation email
aws --endpoint-url http://localhost:8000 --region us-east-1 cognito-idp admin-create-user \
  --user-pool-id "$USER_POOL_ID" \
  --username "testuser" \
  --user-attributes Name=email,Value=testuser@example.com \
  --message-action SUPPRESS || true

echo "=== Setting Password for User ==="
aws --endpoint-url http://localhost:8000 --region us-east-1 cognito-idp admin-set-user-password \
  --user-pool-id "$USER_POOL_ID" \
  --username "testuser" \
  --password "Password123!" \
  --permanent

echo ""
echo "=== Authenticating User ==="
AUTH_RESULT=$(aws --endpoint-url http://localhost:8000 --region us-east-1 cognito-idp initiate-auth \
  --client-id "$CLIENT_ID" \
  --auth-flow USER_PASSWORD_AUTH \
  --auth-parameters USERNAME=testuser,PASSWORD=Password123! \
  --query 'AuthenticationResult' \
  --output json)

ID_TOKEN=$(echo "$AUTH_RESULT" | grep -o '"IdToken": "[^"]*' | cut -d'"' -f4)

echo "Got ID Token: ${ID_TOKEN:0:20}..."

echo ""
echo "=== Testing API without Token (Should fail) ==="
curl -s -i -X GET "$API_URL" || true

echo ""
echo ""
echo "=== Testing API with Token (Should succeed) ==="
curl -s -i -X GET "$API_URL" \
  -H "Authorization: $ID_TOKEN"
echo ""
