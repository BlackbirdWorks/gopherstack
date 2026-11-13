---
service: ocivault
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 8533cce14
last_audit_date: 2026-10-25
overall: B
ops:
  CreateVault: {wire: ok, errors: partial, state: ok, persist: ok, note: "DEFAULT or VIRTUAL_PRIVATE (EXTERNAL rejected). Returns CREATING; the vault is ACTIVE on the next read. managementEndpoint/cryptoEndpoint are http://<prefix>-management.localhost:<port> and -crypto.localhost, with the port taken from the request Host."}
  GetVault: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; DELETED vaults stay readable."}
  ListVaults: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; sortBy TIMECREATED (default, DESC) or DISPLAYNAME; paged with opc-next-page."}
  UpdateVault: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName and tags; If-Match gives 412 NoEtagMatch."}
  ScheduleVaultDeletion: {wire: ok, errors: partial, state: ok, persist: ok, note: "timeOfDeletion 7 to 30 days (default 30) against the injectable clock; response reports SCHEDULING_DELETION, vault is PENDING_DELETION, then DELETED when due. Keys and secrets are not cascaded."}
  CancelVaultDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Response reports CANCELLING_DELETION; vault returns to ACTIVE."}
  ChangeVaultCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "Moves the vault record only; keys and secrets keep their own compartment."}
  GetVaultUsage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Key and key-version counts, software subset."}
  CreateKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "AES 16/24/32, RSA 256/384/512, ECDSA 32/48/66 with matching curveId; protectionMode HSM (default) or SOFTWARE stored (both are in-process software keys); EXTERNAL rejected. Auto-rotation interval 60 to 1095 days. Returns ENABLING."}
  GetKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Scoped to the vault addressed by the endpoint host."}
  ListKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Filters protectionMode, algorithm, length, curveId; sort and paging."}
  UpdateKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName, tags, auto-rotation settings; If-Match."}
  EnableKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "DISABLED to ENABLED, else 409 IncorrectState."}
  DisableKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "ENABLED to DISABLED, else 409 IncorrectState."}
  ScheduleKeyDeletion: {wire: ok, errors: partial, state: ok, persist: ok, note: "7 to 30 days; the key later becomes DELETED and its material is erased."}
  CancelKeyDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Restores the state the key had before scheduling."}
  ChangeKeyCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Fresh material becomes the current version; earlier versions keep decrypting and verifying."}
  GetKeyVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "publicKey (PKIX PEM) for RSA and ECDSA."}
  ListKeyVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Paged."}
  ScheduleKeyVersionDeletion: {wire: ok, errors: partial, state: ok, persist: ok, note: "The current version cannot be scheduled (409 IncorrectState)."}
  CancelKeyVersionDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ImportKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "Key material wrapped with the vault wrapping key: RSA_OAEP_AES_SHA256 (RSA-OAEP of a temporary AES key followed by AES-KWP of the material, PKCS#11 style) or RSA_OAEP_SHA256. AES material is raw bytes, RSA and ECDSA are PKCS#8 DER; length and curve are checked against keyShape."}
  ImportKeyVersion: {wire: ok, errors: partial, state: ok, persist: ok, note: "Same wrapping rules; the imported material becomes the current version."}
  GetWrappingKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "Per-vault RSA 4096 key pair (docs importingkeys.htm) generated on first use and persisted; publicKey is PKIX PEM."}
  Encrypt: {wire: ok, errors: partial, state: ok, persist: n/a, note: "AES-GCM with associatedData bound as AAD; ciphertext is opaque base64 embedding the key version id. RSA keys use raw RSA-OAEP (SHA-256 default, SHA-1 selectable). plaintext is base64, at most 4096 bytes."}
  Decrypt: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Uses the embedded key version so rotated keys still decrypt; RSA tries keyVersionId or every enabled version. Wrong key, AAD or tampering is 400 InvalidParameter."}
  GenerateDataEncryptionKey: {wire: ok, errors: partial, state: ok, persist: n/a, note: "AES data keys of 16/24/32 bytes wrapped by the master key; includePlaintextKey controls plaintext and plaintextChecksum."}
  Sign: {wire: ok, errors: partial, state: ok, persist: n/a, note: "RSA PSS (salt = hash length) and PKCS1 v1.5 with SHA-224/256/384/512, ECDSA SHA-256/384/512 (DER signature); messageType RAW (max 4096 bytes) or DIGEST (length checked)."}
  Verify: {wire: ok, errors: ok, state: ok, persist: n/a, note: "keyVersionId required; mismatches return isSignatureValid false, not an error."}
  ExportKey: {wire: ok, errors: partial, state: ok, persist: n/a, note: "SOFTWARE keys only; RSA_OAEP_AES_SHA256 and RSA_OAEP_SHA256 to a caller RSA public key (PEM, at least 2048 bits). Exports raw AES bytes or PKCS#8 DER."}
  CreateSecret: {wire: ok, errors: partial, state: ok, persist: ok, note: "BASE64 content up to 25 KB encrypted with the keyId AES key (must be ENABLED, same vault); names unique per vault (409 Conflict); secretRules SECRET_EXPIRY_RULE and SECRET_REUSE_RULE stored and enforced; rotationConfig, secretGenerationContext and enableAutoGeneration are accepted (see RotateSecret and the generation family); replicationConfig is rejected with 400."}
  GetSecret: {wire: ok, errors: ok, state: ok, persist: n/a, note: "currentVersionNumber, rules, metadata, timeOfCurrentVersionExpiry."}
  ListSecrets: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; name, vaultId, lifecycleState filters; sortBy NAME or TIMECREATED."}
  UpdateSecret: {wire: ok, errors: partial, state: ok, persist: ok, note: "New secretContent creates a version (stage CURRENT or PENDING); currentVersionNumber promotes or rolls back; metadata, rules and tags; If-Match."}
  ScheduleSecretDeletion: {wire: ok, errors: partial, state: ok, persist: ok, note: "1 to 30 days (default 30); 204; DELETED when due and the name is reusable."}
  CancelSecretDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: "204."}
  ChangeSecretCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ListSecretVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Stages CURRENT, PENDING, LATEST, PREVIOUS, DEPRECATED computed from the version chain."}
  GetSecretVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ScheduleSecretVersionDeletion: {wire: ok, errors: partial, state: ok, persist: ok, note: "The current version cannot be scheduled; due versions are purged."}
  CancelSecretVersionDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  GetSecretBundle: {wire: ok, errors: partial, state: ok, persist: n/a, note: "By versionNumber, secretVersionName or stage (default CURRENT; at most one selector). Content is decrypted with the secret's KMS key, which must be usable. Secrets pending deletion, versions pending deletion and expired versions with retrieval blocked answer 404."}
  GetSecretBundleByName: {wire: ok, errors: ok, state: ok, persist: n/a, note: "secretName and vaultId; accepts POST (SDK) and GET."}
  ListSecretBundleVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "VERSION_NUMBER ascending by default."}
  BackupVault: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Body backupLocation BUCKET (namespace, bucketName, objectName) or PRE_AUTHENTICATED_REQUEST_URI (uri); writes an AES-GCM encrypted vault-metadata blob into in-process Object Storage (PAR write access is checked). Only virtual private vaults can be backed up (docs backingupvaultsandkeys.htm; 400 here, code unverified). The vault is BACKUP_IN_PROGRESS until the transition delay elapses. Response is the Vault (keymanagement/backup_vault_request_response.go); no work request (the SDK has no GetWorkRequest). 200 and the 400 for an unwritable location are unverified."}
  BackupKey: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Same locations; the blob holds every key version and the vault metadata, encrypted under a per-emulator wrapping key so plaintext material never leaves in clear. Key must be ENABLED or DISABLED (BACKUP_IN_PROGRESS, then back) and HSM-protected: SOFTWARE keys are refused with 400 (same docs page, code unverified)."}
  RestoreVaultFromFile: {wire: ok, errors: partial, state: ok, persist: ok, note: "Binary body, compartmentId query, optional content-md5 verified (400 InvalidDigest on mismatch), opc-content-md5 echoed. The vault keeps its OCID and prefix (docs: same-region restore keeps the OCID), restoredFromVaultId is set, state RESTORING then ACTIVE. 409 when the vault exists and is not DELETED. Corrupt, tampered or foreign blobs are 400."}
  RestoreVaultFromObjectStore: {wire: ok, errors: partial, state: ok, persist: ok, note: "Same as RestoreVaultFromFile, reading the blob from the BUCKET or PAR location (404 when absent)."}
  RestoreKeyFromFile: {wire: ok, errors: partial, state: ok, persist: ok, note: "Recreates the key and all versions in its original vault (404 when the vault is gone, 409 when the key exists and is not DELETED); restoredFromKeyId set; RESTORING then the state it had at backup. Deleting a vault now erases its keys when the deletion time passes, so a vault restore needs key restores."}
  RestoreKeyFromObjectStore: {wire: ok, errors: partial, state: ok, persist: ok, note: "Reads the blob from the backup location."}
  CreateVaultReplica: {wire: ok, errors: partial, state: ok, persist: ok, note: "202. Virtual private vaults only (400 otherwise), replicaRegion must be a region id other than the vault's own, repeat for the same region is a no-op, a second region is 409. Replica CREATING then CREATED; existing keys get a replication operation."}
  DeleteVaultReplica: {wire: ok, errors: partial, state: ok, persist: ok, note: "202. DELETING then removed from the list. ScheduleVaultDeletion is 409 while a replica exists."}
  ListVaultReplicas: {wire: ok, errors: ok, state: ok, persist: n/a, note: "region, status, and per-replica endpoints <prefix>-<region>-management|crypto.localhost; those hosts reach the same vault, serve reads and crypto, and reject key writes with 400."}
  GetReplicationStatus: {wire: ok, errors: partial, state: ok, persist: ok, note: "replicationId from vault or key replicaDetails; REPLICATING until the transition delay passes, then REPLICATED, per replica region. Operations are retained 24 hours."}
  RotateSecret: {wire: ok, errors: partial, state: ok, persist: ok, note: "202. Needs rotationConfig. Runs VERIFY_CONNECTION, CREATE_PENDING_VERSION, UPDATE_TARGET_SYSTEM, PROMOTE_PENDING_VERSION against a FUNCTION target (OCI Functions in-process; body {secretId, step, versionNo}, the SecretRotationInput POJO of docs secret-management manage-secrets.htm); the service creates the pending version from the generation context when the function did not, and promotes PENDING to CURRENT after the last step. ADB targets run the stage mechanics only. A failed step leaves the pending version and the current one. The four step names and the payload field names are from that page; its prose says currentVersionNo while the POJO and sample use versionNo, the POJO is followed."}
  CancelSecretRotation: {wire: ok, errors: partial, state: ok, persist: ok, note: "Disables scheduled rotation (CANCELLING then NOT_ENABLED); 409 when nothing is scheduled. Success code unverified."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys; accept-any otherwise, --oci-vault-enforce-auth rejects unsigned and unknown keys."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; Conflict for a duplicate secret name and CannotParseRequest are unverified."}
  vault_endpoints: {status: ok, note: "The SDK forbids a path in the endpoint, so each vault gets <prefix>-management.localhost and <prefix>-crypto.localhost on the one listener; the prefix in the Host header selects the vault. Without a prefix the only existing vault is used; with several, 400. Raw HTTP clients can use /_vault/<vault ocid>/20180608/... instead."}
  crypto: {status: ok, note: "Real crypto/aes, crypto/rsa and crypto/ecdsa; every key type round-trips through the SDK and is cross-checked with local verification and local OAEP."}
  lifecycle: {status: ok, note: "Create, enable, disable, schedule and cancel deletion pass through CREATING, ENABLING, DISABLING, SCHEDULING_DELETION, CANCELLING_DELETION (also BACKUP_IN_PROGRESS and RESTORING) for TransitionDelay (2 s, SetTransitionDelay) on the injectable clock; ops in a transitional state get 409 IncorrectState. Deletion, auto-rotation, scheduled secret rotation and expiry follow the same clock, applied on every request and by a one-minute sweeper. The SDK exposes no work requests for these services."}
  generation: {status: ok, note: "secretGenerationContext PASSPHRASE (SECRETS_DEFAULT_PASSWORD 8-128, DBAAS_DEFAULT_PASSWORD 12-30 chars, default 16, upper+lower+digit guaranteed), SSH_KEY (RSA_2048/3072/4096, PEM public/private JSON) and BYTES (512/1024) with secretTemplate placeholders from the SDK model docs; length limits and character sets are unverified."}
  secrets_at_rest: {status: ok, note: "Secret versions are stored only as AES-GCM ciphertext under the secret KMS key version; snapshots never contain plaintext."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-vault-port / OCI_VAULT_PORT (default 8151); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "HSM clusters, EKMS private endpoints, external vaults and external keys, and UpdateRegisteredVaultForMtls are not implemented."
  - "Secret replication (replicationConfig) is not implemented and is rejected; ADB rotation targets are not contacted (no ADB password is changed); rotation failures are logged only because the SDK has no work requests."
  - "HSM and SOFTWARE protection modes are both in-process software keys; HSM only differs by being non-exportable."
  - "Key version states are not transitional, and replica endpoints share the primary state: a key is readable there before its replication operation reports REPLICATED."
  - "The plaintextChecksum algorithm (SHA-256 hex here) and the ECDSA signature encoding (ASN.1 DER here) are unverified."
  - "Several error codes and statuses are unverified against the live service: 409 Conflict for a duplicate secret name, 404 for pending-deletion or expired secret retrieval, 409 IncorrectState for a disabled key."
  - "Backup and restore: the 200 statuses, the BackupLocation required rule, the error codes for refused backups and the 409 for existing resources are unverified against the live service; key backups still include key versions that are deleted or pending deletion, which Oracle excludes, and restores are not limited to the original compartment."
  - "Service limits (keys per vault, secrets per tenancy) and IAM policy checks are not applied."
  - "Per-vault endpoints rely on *.localhost resolving to loopback; hosts without that resolution need an /etc/hosts entry or the /_vault/<ocid> path form with a raw HTTP client."
  - "Resource principal and instance principal tokens are not understood; only API-key signatures are verified."
