# cognito-api-auth

A REST API method protected by a Cognito user pool authorizer. A request without a token gets 401. A request with an ID token reaches a Lambda that reads the claims.

## What it shows

- OpenTofu (`main.tf`) creates a Cognito user pool `demo-user-pool` and app client `demo-app-client` (`ALLOW_USER_PASSWORD_AUTH`).
- A `nodejs20.x` Lambda `protected-api-backend` behind a REST API `protected-api` with a `GET /data` method and a `COGNITO_USER_POOLS` authorizer on the `Authorization` header. Stage `prod`.
- `demo.sh` then:
  1. Creates user `testuser` (`admin-create-user`) and sets a permanent password.
  2. Signs in with `initiate-auth` (`USER_PASSWORD_AUTH`) and takes the `IdToken`.
  3. Calls the API without a token, then with the token in `Authorization`.
- The Lambda returns `Hello <cognito:username>!` plus the `email` claim.

## Prerequisites

- Docker, OpenTofu (`tofu`), AWS CLI v2, `curl`.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda container.

> [!NOTE]
> `main.tf` and `demo.sh` use `http://localhost:8000`, so Gopherstack must listen there.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

On Docker Desktop, start the stack with `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`. The default is `172.17.0.1`, the Linux Docker bridge gateway.

## Expected output

After the `tofu apply` output and the created user:

```
Got ID Token: eyJhbGciOiJSUzI1NiIs...

=== Testing API without Token (Should fail) ===
HTTP/1.1 401 Unauthorized
...
{"Message":"Unauthorized","message":"Unauthorized"}

=== Testing API with Token (Should succeed) ===
HTTP/1.1 200 OK
...
{"message":"Hello testuser!","email":"testuser@example.com","status":"Success","secretData":"This is protected data that requires a valid Cognito token."}
```

The script does not delete anything. Re-running it reuses the OpenTofu state; `admin-create-user` fails harmlessly for the existing user.

## Files

| File | Purpose |
|------|---------|
| `main.tf` | User pool, client, Lambda, REST API, authorizer, deployment |
| `src/index.js` | Handler that reads `requestContext.authorizer.claims` |
| `demo.sh` | Deploy, create user, sign in, call the API |

## Clean up

```bash
tofu destroy -auto-approve
docker compose down -v
```
