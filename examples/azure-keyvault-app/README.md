# azure-keyvault-app

A small Go app that behaves like a real Azure workload: it reads its configuration from a Key Vault **secret**, signs the payload from that config with a Key Vault **key** (RS256, the private key never leaves the vault), then verifies the signature both through Key Vault and locally with the public key. A tampered payload is checked to be rejected.

Uses the real `azsecrets` and `azkeys` SDKs against gopherstack's Key Vault listener (HTTPS, default port `10004`). Vaults are auto-created on first use, addressed as `https://localhost:10004/vaults/<name>`.

## Prerequisites

- A Go toolchain (the app has its own `go.mod`; dependencies are the same versions as the root module).
- `curl`.
- Gopherstack running with the Key Vault port reachable: `docker-compose up -d` here, or `go run ../..` from the repo root.

## Running

```bash
docker-compose up -d
./demo.sh
```

Override the endpoint with `KEYVAULT_URL` (default `https://localhost:10004/vaults/demo-vault`).

## Expected output

```
config secret: service=billing payload="invoice-42:total=199.00"
signed 32-byte digest with key app-signing-key (256-byte signature)
signature verified by Key Vault and locally; tampered payload rejected
PASS: azure-keyvault-app (secret read, payload signed, signature verified)
```

The script exits non-zero with a `FAIL:` line otherwise. Re-running is safe: each run stores a new secret version and key version.

## Notes

- TLS is verified. The app trusts only the PEM certificate in `KEYVAULT_CA_FILE` (used as the sole root pool). The generated self-signed listener certificate covers `localhost`, `127.0.0.1` and `::1`, so keep `localhost` in `KEYVAULT_URL`.
- If `KEYVAULT_CA_FILE` is unset, `demo.sh` fetches the certificate the listener presents on first connect (`openssl s_client`) into a temp file and pins it. That is trust-on-first-use, acceptable only for a local emulator. To avoid it, start gopherstack with your own certificate (`--azure-keyvault-tls-cert` / `--azure-keyvault-tls-key`) and set `KEYVAULT_CA_FILE` to that CA or certificate PEM.
- The credential is a fixed dev token; the emulator does not validate bearer tokens.

## Teardown

```bash
docker-compose down -v
```
