# azure-keyvault-app

A Go app reads its config from a Key Vault secret, signs it with a Key Vault key (RS256), and verifies the signature.

## What it shows

- Real `azsecrets` and `azkeys` SDK clients against the Key Vault listener (HTTPS, port `10004`).
- Vaults are created on first use at `https://localhost:10004/vaults/<name>`.
- The signature is checked through Key Vault and locally with the public key. A tampered payload must be rejected.

## Prerequisites

- Docker, Go, `curl`, `openssl`.

## Run

```bash
docker compose up -d --build
./demo.sh
```

Without Docker, run `go run .` in the repo root instead of the first command.

| Variable | Default |
|----------|---------|
| `KEYVAULT_URL` | `https://localhost:10004/vaults/demo-vault` |
| `KEYVAULT_CA_FILE` | Unset. `demo.sh` fetches the listener certificate into a temp file. |

## Expected output

```
config secret: service=billing payload="invoice-42:total=199.00"
signed 32-byte digest with key app-signing-key (256-byte signature)
signature verified by Key Vault and locally; tampered payload rejected
PASS: azure-keyvault-app (secret read, payload signed, signature verified)
```

Failures print `FAIL:` and exit non-zero. Re-running is safe: each run adds a new secret version and key version.

> [!WARNING]
> With `KEYVAULT_CA_FILE` unset, `demo.sh` pins whatever certificate the listener presents on first connect. That is trust-on-first-use. For a stricter setup, start Gopherstack with `--azure-keyvault-tls-cert` and `--azure-keyvault-tls-key`, and point `KEYVAULT_CA_FILE` at that certificate.

> [!NOTE]
> Keep `localhost` in `KEYVAULT_URL`. The generated certificate covers `localhost`, `127.0.0.1` and `::1`. The listener accepts any bearer token unless started with `--azure-keyvault-validate-tokens`.

## Clean up

```bash
docker compose down -v
```
