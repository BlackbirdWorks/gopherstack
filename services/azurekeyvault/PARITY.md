---
service: azurekeyvault
sdk_module: azure-sdk-for-go/sdk/security/keyvault/azkeys@v1.5.0 # azsecrets and azcertificates are pinned at the same v1.5.0
last_audit_commit: d52fe4ac6
last_audit_date: 2026-10-18
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  SecretCRUD: {wire: ok, errors: ok, state: ok, persist: ok, note: "SetSecret/GetSecret (latest + version)/UpdateSecretProperties/ListSecrets/ListSecretVersions with default page size 25 and nextLink/$skiptoken paging. Wire verified by the real azsecrets v1.5.0 client in secrets_sdk_test.go. Disabled secrets 403 on get."}
  SecretSoftDelete: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteSecret/GetDeletedSecret/ListDeletedSecrets/RecoverDeletedSecret/PurgeDeletedSecret. Retention comes from the vault (ARM softDeleteRetentionInDays, default 90); expired deleted secrets are swept on the next write; purge protection makes purge 403; re-creating a deleted name is 409 Conflict."}
  SecretBackupRestore: {wire: partial, errors: ok, state: ok, persist: ok, note: "BackupSecret/RestoreSecret round-trip every version. The blob is an opaque gopherstack format (prefix + JSON), not Azure's real encrypted blob, so backups are not portable to or from real Key Vault."}
  KeyCreateImportGet: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateKey RSA/RSA-HSM 2048/3072/4096, EC/EC-HSM P-256/P-384/P-521, oct/oct-HSM 128/192/256 with real key generation; ImportKey RSA/EC/oct (private part required); GetKey latest/version, UpdateKey (attributes, tags, key_ops), ListKeys, ListKeyVersions. Public JWK (n/e or crv/x/y) is verified against the generated key in keys_sdk_test.go. P-256K/ES256K is rejected with 400: Go has no secp256k1 and no module providing it is in go.mod (none may be added)."}
  KeyCrypto: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Encrypt/Decrypt/WrapKey/UnwrapKey: RSA-OAEP, RSA-OAEP-256, RSA1_5 for RSA keys; A128/192/256GCM, A128/192/256CBCPAD and A128/192/256KW (RFC 3394) for oct keys. Sign/Verify: RS256/384/512, PS256/384/512, ES256/384/512 over a caller-supplied digest. All real crypto; signatures are checked against the returned public key with crypto/rsa and crypto/ecdsa in tests. key_ops, enabled, nbf and exp are enforced (403). Not implemented: A*CBC (unpadded), CKM_AES_KEY_WRAP, HS*, ES256K."}
  KeySoftDeleteBackupRestore: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteKey/GetDeletedKey/ListDeletedKeys/RecoverDeletedKey/PurgeDeletedKey/BackupKey/RestoreKey, same semantics and backup-blob caveat as secrets."}
  KeyRotation: {wire: ok, errors: ok, state: ok, persist: ok, note: "RotateKey creates a new version with fresh material and the same kty/size/curve; Get/UpdateKeyRotationPolicy store the policy (default is a Notify 30 days before expiry). A janitor (30s tick, stops with the worker context) executes Rotate lifetime actions on the latest version of non-managed, enabled keys (timeAfterCreate / timeBeforeExpiry, ISO-8601 Y/M/W/D/T durations) and RotateKey/auto-rotation stamp the new version's exp from the policy's expiryTime. A trigger at or before a version's creation is ignored so a fresh version cannot re-trigger itself. Notify actions are recorded only: there is no Event Grid or mail transport, so no notification is ever sent."}
  GetRandomBytes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "POST /rng, 1..128 bytes from crypto/rand."}
  CertificateCreateSelfSigned: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateCertificate with issuer Self generates a real X.509 certificate (RSA or EC key, subject, SANs, key usage, EKUs, validity_months) and returns 202 with a completed CertificateOperation; GetCertificateOperation/Update/Delete pending are served. UPN subject alternative names are encoded as otherName (OID 1.3.6.1.4.1.311.20.2.3) by hand-building the SAN extension. AutoRenew lifetime actions (lifetime_percentage / days_before_expiry) re-issue Self-issued certificates as a new version from the janitor (reuse_key honoured); EmailContacts is recorded only. DigiCert, GlobalSign and any other issuer name are 400 BadParameter."}
  CertificateUnknownIssuerCSR: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateCertificate with issuer Unknown returns 202 with an inProgress CertificateOperation carrying a real PKCS#10 CSR (subject, DNS/email/URI/IP/UPN SANs) for a freshly generated key; GetCertificate/GetCertificatePolicy serve the pending bundle (no cer); MergeCertificate takes x5c entries as base64 DER or PEM bundles, matches the leaf to the pending key (any order), stores the rest as the chain and completes the certificate under the pending version id (201). Get/Cancel (cancellation_requested; merge then 409)/DeleteCertificateOperation behave per azcertificates. Pending state is in the vault snapshot."}
  CertificateGetListUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "GetCertificate latest/version (cer, x5t, kid, sid, policy), ListCertificates, ListCertificateVersions, UpdateCertificate (attributes, tags), Get/UpdateCertificatePolicy. Each certificate also owns a same-named managed key and secret (managed=true) whose version ids match the certificate version."}
  CertificateImport: {wire: partial, errors: ok, state: ok, persist: ok, note: "ImportCertificate accepts a PEM bundle (certificate + PKCS#8/PKCS#1/EC private key) or a base64 PKCS#12 PFX (optional password, key + chain); a wrong password is a 400 BadParameter. Backing secrets honour the policy contentType (PEM default, base64 password-less PFX for application/x-pkcs12)."}
  CertificateSoftDeleteBackupRestore: {wire: ok, errors: ok, state: ok, persist: ok, note: "Delete/GetDeleted/ListDeleted/Recover/Purge/Backup/Restore; the managed key and secret follow the certificate."}
  CertificateIssuersContacts: {wire: ok, errors: ok, state: ok, persist: ok, note: "Set/Get/Update/Delete/List issuers (credentials password is write-only) and Set/Get/Delete certificate contacts. Stored issuers are not consulted when issuing: the policy issuer name alone selects Self or Unknown."}
  ArmVaults: {wire: ok, errors: ok, state: ok, persist: partial, note: "Microsoft.KeyVault/vaults PUT/GET/DELETE/LIST and vaults/accessPolicies add|remove|replace in services/azurearm/rp_keyvault.go. A PUT registers the vault (retention, purge protection) in the data plane; vaultUri is https://<vault>.<host>:10004/. The ARM-side resource metadata is not in the ARM snapshot (same gap as the Storage and ServiceBus providers); the data-plane vault contents do persist."}
families:
  addressing: {status: ok, note: "Vault is the first label of a dotted Host (<vault>.vault.azure.net, <vault>.localhost) or a /vaults/<name> path prefix; otherwise the vault named default. IDs always use https://<Host>/<type>/<name>/<version> without the prefix because the Azure SDK ID parsers assume that shape."}
  auth: {status: partial, note: "A request with no Bearer token gets 401 plus WWW-Authenticate: Bearer authorization=.., resource=..; when the Host has a vault label the resource is the remaining domain so the SDK challenge-resource check passes unmodified, otherwise clients need DisableChallengeResourceVerification. Any bearer token is then accepted; --azure-keyvault-validate-tokens only requires a well-formed, unexpired JWT (the signature is not verified, there is no token endpoint here)."}
  tls: {status: ok, note: "HTTPS only (pkgs/devtls self-signed, or --azure-keyvault-tls-cert/--azure-keyvault-tls-key), because azcore refuses to send bearer tokens over HTTP."}
  routing_isolation: {status: ok, note: "Own listener on fixed port 10004 (--azure-keyvault-port), never matches the shared AWS router, port reserved in the shared pool in cli.go."}
  persistence: {status: ok, note: "backendSnapshot holds every vault with its secrets, keys (including private material), certificates, issuers and contacts; Restore rejects null vaults and discards unknown snapshot versions."}
items_still_open:
  - "Third-party certificate issuers (DigiCert, GlobalSign) are rejected; AutoRenew of Unknown-issuer certificates (which would open a new pending CSR) is not executed."
  - "Key release (ReleaseKey, release_policy), GetKeyAttestation and ES256K/P-256K (no secp256k1 in go.mod) are not implemented."
  - "Notify and EmailContacts lifetime actions are recorded only; nothing is delivered."
  - "ARM Microsoft.KeyVault: deletedVaults, checkNameAvailability and vault purge are not implemented; ARM vault metadata is not persisted."
  - "Backups use an opaque gopherstack blob, not Azure's format."
deferred: []
leaks: {status: clean, note: "One listener goroutine plus one lifetime-action janitor goroutine group, both stopped by Shutdown or context cancel; leak_test.go runs the package under pkgs/testleak. Expired soft-deleted entities are also swept on each janitor tick."}
---

## Notes

### Why path-prefix IDs omit the prefix
`azsecrets.ID.Name()/Version()` (internal/parse.go) split the URL path and take indexes 1 and 2, so an ID such as `/vaults/x/secrets/n/v` yields name `secrets`. Returning prefix-less IDs keeps the helpers correct in both addressing modes; follow-up calls still go through the client's own vault URL.

### Certificates own a key and a secret
As in Key Vault, creating a certificate also creates a key and a secret with the same name and version. They are stored as ordinary entities flagged managed, cannot be deleted or overwritten on their own (409), are hidden from the deleted-secrets and deleted-keys listings, and are soft-deleted, recovered and purged together with the certificate.

### Time
All attribute timestamps are Unix epoch seconds, as the azsecrets/azkeys/azcertificates `datetime.Unix` deserializers expect.
