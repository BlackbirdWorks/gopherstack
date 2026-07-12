---
service: kms
sdk_module: aws-sdk-go-v2/service/kms@v1.54.0
last_audit_commit: eb94f3c3
last_audit_date: 2026-07-11
overall: A-           # re-audit; local surface unchanged since last sweep (sdk bumped
                       # 1.53.6->1.54.0, additive-only: request serialization snapshot
                       # tests, no new ops/fields). Found + fixed 2 missing errCodeLookup
                       # entries and 1 made-up default exception name.
ops:
  CreateKey: {wire: ok, errors: fixed, state: ok, persist: ok, note: "invalid KeySpec now classifies as ValidationException (400), not InternalServiceError (500); tags now validated before the key is created (was: orphan-leak on bad tag)"}
  DescribeKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ListKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  Encrypt: {wire: ok, errors: fixed, state: ok, persist: ok, note: "real AES-256-GCM / RSA-OAEP-SHA-256, AAD-bound encryption context, grant-token constraint check already present; expired imported material now classifies as ExpiredImportTokenException (400), not 500"}
  Decrypt: {wire: ok, errors: fixed, state: ok, persist: ok, note: "key ID embedded in blob prefix; mismatched context fails AES-GCM auth -> InvalidCiphertextException; history fallback for post-rotation ciphertexts; expired imported material now classifies as ExpiredImportTokenException (400), not 500"}
  ReEncrypt: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateDataKey: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateDataKeyWithoutPlaintext: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateDataKeyPair: {wire: fixed, errors: ok, state: ok, persist: ok, note: "GrantTokens field + EncryptionContext size validation + grant-constraint enforcement were all missing; added"}
  GenerateDataKeyPairWithoutPlaintext: {wire: fixed, errors: ok, state: ok, persist: ok, note: "delegates to GenerateDataKeyPair; GrantTokens now threaded through"}
  Sign: {wire: fixed, errors: ok, state: ok, persist: ok, note: "GrantTokens field + grant-token validity check were missing (disguised stub: op is a valid grant operation but the token was silently dropped)"}
  Verify: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same GrantTokens gap as Sign"}
  GetPublicKey: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same GrantTokens gap as Sign"}
  GenerateMac: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same GrantTokens gap as Sign"}
  VerifyMac: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same GrantTokens gap as Sign"}
  DeriveSharedSecret: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same GrantTokens gap as Sign; real ECDH via crypto/ecdh conversion"}
  GenerateRandom: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableKeyRotation: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableKeyRotation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKeyRotationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  RotateKeyOnDemand: {wire: ok, errors: ok, state: ok, persist: ok, note: "10-per-24h on-demand rate limit enforced"}
  ListKeyRotations: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableKey: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ScheduleKeyDeletion: {wire: ok, errors: ok, state: ok, persist: ok, note: "7-30 day window enforced; janitor purges past DeletionDate"}
  CancelKeyDeletion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGrant: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGrants: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeGrant: {wire: ok, errors: ok, state: ok, persist: ok}
  RetireGrant: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRetirableGrants: {wire: ok, errors: ok, state: ok, persist: ok}
  PutKeyPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKeyPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListKeyPolicies: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetParametersForImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "real RSA-2048/3072/4096 wrapping keypair generated per call"}
  ImportKeyMaterial: {wire: ok, errors: ok, state: ok, persist: ok, note: "real RSA-OAEP unwrap of caller material"}
  DeleteImportedKeyMaterial: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplicateKey: {wire: fixed, errors: ok, state: ok, persist: ok, note: "tag validation moved before replica creation (was: orphan-leak on bad tag, and tags on ReplicateKey bypassed validateTag entirely)"}
  UpdateKeyDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrimaryRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomKeyStores: {wire: ok, errors: ok, state: ok, persist: ok}
  ConnectCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DisconnectCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKeyLastUsage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "not a real AWS KMS op; internal telemetry accessor kept from a prior pass"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "tags stored via pkgs/tags, not in backendSnapshot (see gaps)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListResourceTags: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  crypto_core: {status: ok, note: "AES-256-GCM (real Seal/Open, AAD = keyID + sorted encryption-context pairs), RSA-OAEP-SHA-256/SHA-1 fallback, RSASSA-PSS/PKCS1v15, ECDSA P-256/384/521, ECDH (crypto/ecdh), HMAC-SHA-256/384/512 — all real crypto/*, no mock byte-flipping anywhere in crypto.go"}
  error_classification: {status: fixed, note: "kmsErrorTable was missing entries for ErrExpiredKeyMaterial and ErrKeyMaterialUnavailable (raised by checkKeyMaterialExpiry/requireKeyMaterial, reachable from every crypto op: Encrypt, Decrypt, ReEncrypt, Sign, Verify, GetPublicKey, GenerateMac, VerifyMac, DeriveSharedSecret, GenerateDataKeyPair(WithoutPlaintext)), so both fell through to the generic 500 default. Also, that generic default itself emitted the type string \"InternalServiceError\", which is not a real KMS exception name (the real SDK's unclassified-server-error type is KMSInternalException) -- a caller's errors.As(&types.KMSInternalException{}) would never match. Fixed: added both sentinels to the table (ExpiredImportTokenException/400 client-fault, KeyUnavailableException/500 server-fault per the real SDK's ErrorFault), and changed the default-branch type string to KMSInternalException."}
  key_state_machine: {status: ok, note: "Enabled/Disabled/PendingDeletion/PendingImport transitions all gated; keyStateError() maps Disabled->DisabledException, everything else->KMSInvalidStateException"}
  multi_region: {status: ok, note: "ReplicateKey/UpdatePrimaryRegion primary<->replica promotion verified by existing TestUpdatePrimaryRegion_RoleSwap; DescribeKey MultiRegionConfiguration built correctly for both primary and replica sides"}
gaps:
  - "GrantConstraints has no SourceArn field (real SDK: GrantConstraints.SourceArn); no operation in this mock threads a caller/resource ARN through crypto calls to check against it, and no other service adapter currently supplies one either — deferred, needs cross-cutting request-context plumbing, not a KMS-local fix (bd: gopherstack-w3k)"
  - "CreateGrantInput has no GrantTokens field (real SDK: authorizes the CreateGrant call itself via an existing not-yet-consistent grant). No IAM/authorization layer exists anywhere in this mock, so this field would currently be a no-op; deferred, consistent with the rest of the codebase's scope"
  - "GranteeServicePrincipal / RetiringServicePrincipal (AWS-service grantees) not modeled on CreateGrantInput; same no-IAM-layer scope reasoning as above"
deferred:
  - Custom key store cryptographic connection/HSM simulation (ConnectCustomKeyStore is a pure state-machine transition; no CloudHSM cluster or XKS proxy is modeled, matching pre-existing scope)
  - GetKeyLastUsage (not a real AWS KMS operation; left as-is from a prior pass, out of scope for this sweep)
leaks: {status: found, note: "janitor.purgeKey deleted a purged key's grants from the canonical grants/grantsByToken maps but left the grantsByKey[region][keyID] secondary-index submap allocated forever (unreachable after purge, since the keyID can never resolve again) — fixed by dropping the submap in purgeKey. Verified by Test_KMS_Janitor_PurgeKey_CleansGrantByKeyIndex, which fails without the fix. All other maps already bounded (keyMaterialHistory capped at 100 entries/key, janitor sweeps PendingDeletion keys, resolution cache cleared via evictAliasesFromCache)."}
---

## Notes

Freeform findings from the 2026-07-05 sweep (bd: gopherstack-42s), for the next auditor.

### Fixed this pass

1. **Grant-token wire gap on 7 operations (severe, disguised stub).** `SignInput`,
   `VerifyInput`, `GetPublicKeyInput`, `DeriveSharedSecretInput`, `GenerateDataKeyPairInput`,
   `GenerateDataKeyPairWithoutPlaintextInput`, `GenerateMacInput`, and `VerifyMacInput` had no
   `GrantTokens` field at all, even though `isValidGrantOperation` in backend.go already lists
   `Sign`, `Verify`, `GetPublicKey`, `GenerateMac`, `VerifyMac`, `DeriveSharedSecret`,
   `GenerateDataKeyPair`, and `GenerateDataKeyPairWithoutPlaintext` as grantable operations.
   Since dispatch does a bare `json.Unmarshal(body, &input)`, a caller-supplied `GrantTokens`
   array was silently dropped on the floor — the grant system modeled these operations as
   grantable but never actually validated a grant token for them. Confirmed against
   `aws-sdk-go-v2/service/kms` that all 8 real `*Input` structs carry `GrantTokens []string`.
   Fixed by adding the field to all 8 structs and wiring validation into the backend:
   - Per AWS docs (`kms/types.GrantConstraints` doc comment), `EncryptionContextEquals`/
     `EncryptionContextSubset` constraints apply ONLY to operations that support an
     encryption context (Encrypt, Decrypt, GenerateDataKey(WithoutPlaintext),
     GenerateDataKeyPair(WithoutPlaintext), ReEncryptFrom/To) — NOT to Sign, Verify,
     GetPublicKey, GenerateMac, VerifyMac, or DeriveSharedSecret. So the fix adds two
     different helpers: `validateGrantTokenPresence` (token must exist + be unexpired; no
     constraint check) for the six non-context ops, and reuses the existing
     `validateGrantTokenConstraints` (token + TTL + EncryptionContext match) for
     GenerateDataKeyPair(WithoutPlaintext), matching the pattern already used by
     Encrypt/Decrypt/GenerateDataKey/ReEncrypt.
   - **Trap for the next auditor:** do not "simplify" by reusing
     `validateGrantTokenConstraints` with a nil encryption context for Sign/Verify/etc. — if
     a grant happens to have `EncryptionContextEquals` set (unusual but not rejected at
     CreateGrant time), that would spuriously reject Sign/Verify calls that AWS would allow,
     since AWS never evaluates that constraint for those operations.
   - Also added `validateEncryptionContextSize` to `GenerateDataKeyPair`, which was missing
     it (Encrypt/Decrypt/GenerateDataKey/ReEncrypt already had it).

2. **Invalid KeySpec/KeyPairSpec misclassified as 500, not 400 (moderate).**
   `generateKeyMaterial`'s `default:` case returned an error wrapping only
   `errUnsupportedKeySpec`, never `ErrValidation`. `classifyKMSError` only matches via
   `errors.Is` against the sentinels in `kmsErrorTable()`, so `CreateKey` with a garbage
   `KeySpec` (e.g. a typo) and `GenerateDataKeyPair` with a garbage `KeyPairSpec` both fell
   through to the default `InternalServiceError` / 500 branch instead of
   `ValidationException` / 400 — exactly bug class #2 from `parity-principles.md`
   ("missing errCodeLookup entries"). Fixed with a single-point fix: wrap with both
   `ErrValidation` and `errUnsupportedKeySpec` (Go 1.20+ multi-`%w`) at the source, so every
   current and future caller of `generateKeyMaterial` gets correct classification for free.
   Proven with an HTTP-level test (`Test_KMS_InvalidKeySpec_Returns400ValidationException`)
   that exercises the full `Handler().Handler()` echo path and checks both status code and
   `ErrorResponse.Type`.

3. **`purgeKey` leaked the `grantsByKey` secondary-index submap (moderate, matches the
   "unbounded key/grant maps" pattern called out in the audit brief).** When the janitor
   permanently purges a key past its `ScheduleKeyDeletion` window, it deleted the key's
   grants from `grants` and `grantsByToken` but never removed
   `grantsByKey[region][keyID]`. Since the purged keyID can never be looked up again, that
   submap (and any residual per-grant map beneath it) is unreachable for the remainder of
   the process's lifetime — a genuine memory leak in any long-running instance that
   repeatedly creates keys with grants and lets them expire. Fixed by dropping the submap in
   `purgeKey`; `grantsByKeyStore` lazily recreates it on next access if the key ID is ever
   reused (it won't be, since key IDs are UUIDs, but the accessor is already written to
   tolerate that). Note `rebuildGrantIndexesLocked` (used by `Restore`) already rebuilds
   `grantsByKey` from scratch, confirming it's a pure derived index safe to drop.

4. **`CreateKey` and `ReplicateKey` created a real, permanent resource before validating
   tags (moderate, orphan-resource leak).** `createKeyAction` called
   `Backend.CreateKey` (allocating a UUID, real key material, and a backend map entry)
   and only validated `input.Tags` afterward via `applyInputTags`. If a tag was malformed
   (empty key, reserved `aws:` prefix, over-length), the handler returned an error to the
   caller — who never receives a `KeyId` — while the key remained permanently resident in
   the backend, discoverable only via `ListKeys`, with no route to ever tag, use, or delete
   it by the caller that "created" it. `ReplicateKey`'s dispatch closure was worse: it never
   validated tags at all (`copyTagsToReplica` calls `setTags` directly, bypassing
   `validateTag` entirely), so a malformed replica tag would just silently apply. Real AWS
   validates the whole request shape before creating any resource. Fixed by extracting a
   shared `validateTags` helper and calling it before `Backend.CreateKey` /
   `Backend.ReplicateKey` in both dispatch paths (the `ReplicateKey` closure was also
   extracted to `replicateKeyAction` to keep `buildReplicationAndMaintenanceActions` under
   the gocognit complexity gate).

### Traps / already-correct patterns confirmed (do not re-flag)

- `Decrypt`/`ReEncrypt` returning an error via `decryptWithHistory` after the primary
  `decryptData` attempt fails is NOT a stub — it's the real post-rotation fallback path that
  tries prior key-material versions (capped at `maxKeyMaterialHistoryEntries` = 100) before
  giving up with `InvalidCiphertextException`.
- `GetKeyLastUsage` is not a real AWS KMS API (AWS doesn't expose per-key last-usage via a
  named operation); it was added in an earlier pass as an internal telemetry accessor and is
  kept as deferred/non-blocking scope.
- `errCodeLookup`-equivalent (`kmsErrorTable()` + `classifyKMSError`) returns HTTP 400 for
  every matched sentinel including `NotFoundException` — this matches real AWS KMS
  (a `json-1.1` protocol service), which returns 400 Bad Request for `NotFoundException`
  too, not 404.
- `CreateGrant`'s own `GrantTokens` field (authorizing the CreateGrant call via an existing
  grant) is intentionally NOT modeled — there is no IAM/authorization layer anywhere in this
  mock, so it would be a no-op; this is a scope boundary, not a bug.

### Verification method

Every fix in this pass was proven with a negative control: the corresponding fix commit was
reverted locally, the new test was confirmed to fail (or fail to compile, for the wire-shape
field additions) against the reverted code, then the fix was reapplied and the full suite
re-run green. See `parity_sweep3_test.go`.

## 2026-07-11 re-audit (bd: none filed yet — see report)

Per the re-audit protocol: `git diff ede7169a..eb94f3c3 -- services/kms/` showed only the
sweep-3 commit itself touching this service (no drift since the ledger above was written),
and `aws-sdk-go-v2/service/kms` bumped v1.53.6 -> v1.54.0 with only "Add request
serialization snapshot tests" in the changelog (no new ops/fields). So this pass audited the
`kmsErrorTable()` / `classifyKMSError` machinery specifically (an area the sweep-3 notes
already flagged as a recurring bug class) rather than re-walking every already-`ok` row.

### Fixed this pass

1. **Two sentinel errors missing from `kmsErrorTable()` (moderate, same bug class as
   sweep 3's `ValidationException` gap).** `ErrExpiredKeyMaterial` (raised by
   `checkKeyMaterialExpiry`, reachable from `Encrypt`/`Decrypt`/etc. on a key whose
   imported material has passed its `ValidTo`) and `ErrKeyMaterialUnavailable` (raised by
   `requireKeyMaterial` when key material is absent, e.g. after restoring an old-format
   snapshot) both had no entry in the table, so `classifyKMSError` fell through to the
   generic default branch and returned a 500 for both — even though `ErrExpiredKeyMaterial`
   is a genuine 400 client-fault scenario (bad/expired import, not a server problem). Fixed
   by adding both to the table: `ErrExpiredKeyMaterial` -> `ExpiredImportTokenException`
   (400, confirmed a client-fault exception in the real SDK), `ErrKeyMaterialUnavailable`
   -> `KeyUnavailableException` (500, confirmed `ErrorFault: Server` in the real SDK).
   Required extending `kmsErrorEntry` with an optional `httpStatus` field (0 = default 400)
   since this is the first table entry that isn't a plain client-fault 400.
2. **The default/unclassified-error branch emitted a type string that isn't a real KMS
   exception (minor but structural).** `classifyKMSError`'s fallback returned
   `"InternalServiceError"`, which does not appear anywhere in
   `aws-sdk-go-v2/service/kms/types/errors.go` — the real type for an unclassified
   server-side failure is `KMSInternalException`. A caller doing
   `errors.As(err, &types.KMSInternalException{})` (a real, documented pattern for
   distinguishing retryable server errors) would never match against this emulator's
   output. Fixed by changing the fallback's type string to `"KMSInternalException"`.
   Verified no test asserted the literal string `"InternalServiceError"` (only comments
   did) before making the change.

Both fixes proven with the same negative-control method as sweep 3: see
`Test_KMS_ErrorClassification_MissingTableEntries` in `parity_sweep3_test.go` — reverting
`handler.go` alone (via `git stash`) was confirmed to fail both subtests with
`InternalServiceError` in place of the expected exception type, then the fix was reapplied
and the full suite re-run green.

No other gaps found this pass. The three deferred items and the previously-fixed leak in the
`ops`/`gaps`/`leaks` block above are unchanged and still accurate.
