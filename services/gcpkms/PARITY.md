---
service: gcpkms
sdk_module: cloud.google.com/go/kms@v1.35.0
last_audit_commit: d0dad7f73
last_audit_date: 2026-10-19
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateKeyRing: {wire: ok, errors: ok, state: ok, persist: ok, note: "Id [a-zA-Z0-9_-]{1,63}; ALREADY_EXISTS on duplicate. Key rings cannot be deleted (as in Cloud KMS)."}
  GetKeyRing: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListKeyRings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Sorted by name, paged via pkgs/page, total_size. filter (fields name, createTime; =, !=, :, relational on timestamps; AND/OR/parentheses) and order_by (name, createTime, asc/desc, comma list); unsupported syntax or fields are INVALID_ARGUMENT."}
  CreateCryptoKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "Purposes ENCRYPT_DECRYPT, ASYMMETRIC_SIGN, ASYMMETRIC_DECRYPT, MAC, RAW_ENCRYPT_DECRYPT; version_template algorithm validated against the purpose; SOFTWARE and HSM levels (HSM is emulated in software). Rotation (period 24h-876000h needs a future next_rotation_time), labels, import_only, skip_initial_version_creation, destroy_scheduled_duration (24h-120d, default 30d). The initial version is generated immediately and is primary for ENCRYPT_DECRYPT keys."}
  GetCryptoKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Embeds the primary version."}
  ListCryptoKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "filter fields: name, createTime, purpose, importOnly, labels.KEY, versionTemplate.algorithm/protectionLevel; order_by name, createTime."}
  UpdateCryptoKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask required; labels, rotation_period, next_rotation_time and version_template(.algorithm/.protection_level) are mutable. Setting only rotation_period derives next_rotation_time from now."}
  UpdateCryptoKeyPrimaryVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "ENCRYPT_DECRYPT keys only; the target must be ENABLED."}
  CreateCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Real key material from the key's template; the first version of an ENCRYPT_DECRYPT key without a primary becomes primary. Import-only keys are FAILED_PRECONDITION."}
  GetCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListCryptoKeyVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Ascending version order, paged."}
  UpdateCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "update_mask must be state; ENABLED and DISABLED only; scheduled or destroyed versions are FAILED_PRECONDITION."}
  DestroyCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Schedules destruction after the key's destroy_scheduled_duration; the primary version is refused. Due versions become DESTROYED lazily and by the janitor, and their material is wiped."}
  RestoreCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "DESTROY_SCHEDULED to DISABLED."}
  CreateImportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "RSA_OAEP_3072/4096_SHA256, RSA_OAEP_3072/4096_SHA256_AES_256 and RSA_OAEP_3072/4096_SHA1_AES_256; a real RSA wrapping key is generated, public_key.pem returned, expires after 3 days (state EXPIRED)."}
  GetImportJob: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListImportJobs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "filter fields: name, createTime, expireTime, state, importMethod, protectionLevel; order_by name, createTime."}
  DeleteCryptoKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "Returns a done long-running operation (metadata.retired_resource, Empty response). Every non-primary version must be DESTROYED or deleted (the primary goes with the key); records a RetiredResource, drops the key IAM policy; the name cannot be recreated (ALREADY_EXISTS). GetOperation is not served (operations are born done)."}
  DeleteCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Only DESTROYED, non-primary versions; key material is wiped, the version disappears from Get/List and its id is never reused (tombstone persisted as versionSnap.deleted)."}
  GetRetiredResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListRetiredResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Per location, sorted by name, paged, total_size; records persist in backendSnapshot.retired."}
  ImportCryptoKeyVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "Unwraps RSA-OAEP(+AES-KWP) material and validates its shape against the algorithm; symmetric keys are raw bytes, asymmetric keys PKCS#8 DER. Re-import into a DESTROYED imported version is supported."}
  Encrypt: {wire: ok, errors: ok, state: ok, persist: n/a, note: "AES-256-GCM with a random nonce; the ciphertext embeds the version id. AAD and plaintext/AAD crc32c verification; 64 KiB limits; accepts a key (primary) or a version name."}
  Decrypt: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Routes to the embedded version; wrong AAD or tampering is INVALID_ARGUMENT; disabled, scheduled or destroyed versions are FAILED_PRECONDITION; used_primary reported."}
  RawEncrypt: {wire: ok, errors: ok, state: ok, persist: n/a, note: "AES-128/256 GCM (16-byte tag appended), CBC (block-aligned input, no padding) and CTR; IV generated when absent and returned; AAD only for GCM."}
  RawDecrypt: {wire: ok, errors: ok, state: ok, persist: n/a}
  AsymmetricSign: {wire: ok, errors: ok, state: ok, persist: n/a, note: "RSA PKCS1/PSS 2048/3072/4096 SHA256/SHA512, RSA_SIGN_RAW_PKCS1_2048/3072/4096 (data only, no hashing or DigestInfo, at most key bytes - 11), EC P-256/P-384 (ASN.1 DER) and Ed25519; digest or data accepted, digest type/length checked, crc32c verified."}
  AsymmetricDecrypt: {wire: ok, errors: ok, state: ok, persist: n/a, note: "RSA-OAEP SHA256/SHA512 and SHA1 (2048/3072/4096)."}
  GetPublicKey: {wire: ok, errors: ok, state: ok, persist: n/a, note: "PEM (with pem_crc32c) or DER (public_key ChecksummedData) PKIX output."}
  MacSign: {wire: ok, errors: ok, state: ok, persist: n/a, note: "HMAC SHA1/224/256/384/512."}
  MacVerify: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Constant-time comparison; verified_success_integrity always true."}
  GenerateRandomBytes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "8-1024 bytes from crypto/rand; SOFTWARE and HSM levels accepted."}
  SetIamPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "google.iam.v1.IAMPolicy on key rings, crypto keys and import jobs; stale etag is ABORTED. Not enforced."}
  GetIamPolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  TestIamPermissions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Every requested permission is reported granted."}
families:
  rotation: {status: ok, note: "Automatic rotation at next_rotation_time creates a version (promoted for ENCRYPT_DECRYPT) and advances by rotation_period; without a period it is one-shot. Applied lazily on every RPC and by a 30s janitor; one rotation per evaluation."}
  routing_isolation: {status: ok, note: "Own gRPC server bound in StartWorker to --gcp-kms-port / GCP_KMS_PORT (default 8087, fails fast if taken); never registered on the shared AWS router. cli.go reserves the port in the shared PortAlloc pool. The official client has no emulator variable: use option.WithEndpoint with insecure credentials."}
  key_material: {status: partial, note: "Key material is real but stored unencrypted in the persistence snapshot; this is an emulator, not a secure key store. HSM and EXTERNAL protection are not real."}
  auth: {status: partial, note: "No request is authenticated or authorised; IAM policies are stored but not enforced."}
  observability: {status: partial, note: "One lockmetrics.RWMutex guards all state; RPC failures are logged at debug. gRPC calls are not fed to the shared telemetry operation metrics."}
items_still_open:
  - "ImportTrustedKeyWrappedCryptoKeyVersion, ExportTrustedKeyWrappedCryptoKeyVersion and Decapsulate are UNIMPLEMENTED."
  - "EkmService, Autokey, AutokeyAdmin and HsmManagement services are not served; EKM connections and crypto_key_backend are rejected."
  - "EC_SIGN_SECP256K1_SHA256 (no stdlib curve), post-quantum and KEM algorithms, AES_256_KWP (AES_WRAPPING purpose has no RPC that uses it) and EXTERNAL_SYMMETRIC_ENCRYPTION are rejected as unsupported."
  - "Attestation, key_access_justifications_policy and the version view parameter are not populated."
  - "The google.longrunning Operations service is not served; DeleteCryptoKey and DeleteCryptoKeyVersion return already-done operations."
leaks: {status: clean, note: "Background resources are the grpc.Server and the rotation janitor goroutine, both stopped and joined by Shutdown."}
---

Initial implementation (2026-10-18): KeyManagementService gRPC verified against cloud.google.com/go/kms v1.35.0 through the official client in the package tests.
