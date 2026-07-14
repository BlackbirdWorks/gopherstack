---
service: kms
sdk_module: aws-sdk-go-v2/service/kms@v1.54.0
last_audit_commit: db25aeabef5bf3f7a33c8ed328247641b3ffcc15
last_audit_date: 2026-07-12
overall: A            # Terraform-lifecycle-focused re-audit (full apply/plan/destroy
                       # cycle with no drift). Found + fixed 1 CreateKey-Policy-class
                       # regression on ReplicateKey (aws_kms_replica_key would have hit
                       # the same 10-minute GetKeyPolicy poll hang the already-fixed
                       # CreateKey bug caused) and 1 real persistence gap (KMS resource
                       # tags live in a Handler-level side map, not the backend, and were
                       # never included in Snapshot/Restore -- silently dropped across any
                       # process restart with persistence enabled, which is exactly the
                       # perpetual-plan-drift bug class this sweep was hunting for).
ops:
  CreateKey: {wire: ok, errors: fixed, state: ok, persist: ok, note: "invalid KeySpec now classifies as ValidationException (400), not InternalServiceError (500); tags now validated before the key is created (was: orphan-leak on bad tag)"}
  DescribeKey: {wire: fixed, errors: ok, state: ok, persist: ok, note: "DescribeKeyInput was missing the GrantTokens field entirely (real SDK: aws-sdk-go-v2/service/kms@v1.54.0 DescribeKeyInput carries GrantTokens []string; DescribeKey is a valid grant operation and declares InvalidGrantTokenException in its error set). Added the field + validateGrantTokenPresence (existence+TTL, no encryption-context check -- consistent with Sign/Verify/GetPublicKey/DeriveSharedSecret). Empty tokens is a no-op (the only case Terraform exercises)."}
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
  CreateGrant: {wire: ok, errors: ok, state: fixed, persist: ok, note: "now resolves KeyId against the key's own region (ARN-embedded region for an ARN input) via resolveKeyAndRegion, so a grant created via a cross-region ARN is stored in the key's region -- was: stored in the request region, so ListGrants/RevokeGrant addressing the same ARN could not find it (root cause was resolveKeyID's resolution cache returning the request region on cache hits instead of the ARN's embedded region; fixed at source)"}
  ListGrants: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same region-resolution fix as CreateGrant"}
  RevokeGrant: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same region-resolution fix as CreateGrant"}
  RetireGrant: {wire: ok, errors: ok, state: fixed, persist: ok, note: "GrantId+KeyId path now uses the key's own region; GrantId-only (no KeyId, no region hint) now searches all regions instead of only the request region"}
  ListRetirableGrants: {wire: ok, errors: ok, state: ok, persist: ok}
  PutKeyPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same region-resolution fix as CreateGrant -- policy now stored in the key's own region so a cross-region ARN round-trips through GetKeyPolicy"}
  GetKeyPolicy: {wire: ok, errors: ok, state: fixed, persist: ok, note: "same region-resolution fix as CreateGrant -- reads the policy from the key's own region (ARN-embedded region for an ARN input)"}
  ListKeyPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "already region-aware (routes through lookupKey); confirmed no change needed"}
  GetParametersForImport: {wire: ok, errors: ok, state: ok, persist: ok, note: "real RSA-2048/3072/4096 wrapping keypair generated per call"}
  ImportKeyMaterial: {wire: ok, errors: ok, state: ok, persist: ok, note: "real RSA-OAEP unwrap of caller material"}
  DeleteImportedKeyMaterial: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplicateKey: {wire: fixed, errors: ok, state: ok, persist: ok, note: "tag validation moved before replica creation (was: orphan-leak on bad tag, and tags on ReplicateKey bypassed validateTag entirely); ReplicateKeyInput was ALSO missing the Policy field entirely (confirmed against aws-sdk-go-v2/service/kms@v1.54.0's api_op_ReplicateKey.go) -- an inline replica policy was silently dropped, so GetKeyPolicy on the replica always returned the synthesized default, the exact same bug class (and same Terraform symptom: aws_kms_replica_key's post-apply GetKeyPolicy poll never converges) as the already-fixed CreateKey Policy bug. Fixed by adding Policy (+ BypassPolicyLockoutSafetyCheck, unused like CreateKey's own copy since there is no IAM layer) to ReplicateKeyInput and persisting it into the replica's region-scoped policiesStore, mirroring CreateKey exactly."}
  UpdateKeyDescription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrimaryRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCustomKeyStores: {wire: ok, errors: ok, state: ok, persist: ok}
  ConnectCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DisconnectCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomKeyStore: {wire: ok, errors: ok, state: ok, persist: ok}
  GetKeyLastUsage: {wire: ok, errors: ok, state: ok, persist: n/a, note: "not a real AWS KMS op; internal telemetry accessor kept from a prior pass"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: fixed, note: "tags stored via pkgs/tags in a Handler-level side map (Handler.tags, keyed by KeyID), NOT in InMemoryBackend.backendSnapshot -- Handler.Snapshot previously delegated straight to Backend.Snapshot and never serialized Handler.tags at all, so a process restart with persistence enabled silently dropped every key's tags (ListResourceTags stayed correct within a single running process, masking the gap). Fixed: Handler.Snapshot/Restore now wrap the backend snapshot together with a tags map (see persistence.go's handlerSnapshot); a handlerFormat marker distinguishes the new wrapped shape from a legacy pre-fix snapshot (raw backend bytes) so old on-disk snapshots still restore backend state cleanly, just without tags (no worse than before)."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: fixed, note: "same Handler.tags persistence fix as TagResource"}
  ListResourceTags: {wire: ok, errors: ok, state: ok, persist: fixed, note: "same Handler.tags persistence fix as TagResource"}
families:
  crypto_core: {status: ok, note: "AES-256-GCM (real Seal/Open, AAD = keyID + sorted encryption-context pairs), RSA-OAEP-SHA-256/SHA-1 fallback, RSASSA-PSS/PKCS1v15, ECDSA P-256/384/521, ECDH (crypto/ecdh), HMAC-SHA-256/384/512 — all real crypto/*, no mock byte-flipping anywhere in crypto.go"}
  error_classification: {status: fixed, note: "kmsErrorTable was missing entries for ErrExpiredKeyMaterial and ErrKeyMaterialUnavailable (raised by checkKeyMaterialExpiry/requireKeyMaterial, reachable from every crypto op: Encrypt, Decrypt, ReEncrypt, Sign, Verify, GetPublicKey, GenerateMac, VerifyMac, DeriveSharedSecret, GenerateDataKeyPair(WithoutPlaintext)), so both fell through to the generic 500 default. Also, that generic default itself emitted the type string \"InternalServiceError\", which is not a real KMS exception name (the real SDK's unclassified-server-error type is KMSInternalException) -- a caller's errors.As(&types.KMSInternalException{}) would never match. Fixed: added both sentinels to the table (ExpiredImportTokenException/400 client-fault, KeyUnavailableException/500 server-fault per the real SDK's ErrorFault), and changed the default-branch type string to KMSInternalException."}
  key_state_machine: {status: ok, note: "Enabled/Disabled/PendingDeletion/PendingImport transitions all gated; keyStateError() maps Disabled->DisabledException, everything else->KMSInvalidStateException"}
  multi_region: {status: ok, note: "ReplicateKey/UpdatePrimaryRegion primary<->replica promotion verified by existing TestUpdatePrimaryRegion_RoleSwap; DescribeKey MultiRegionConfiguration built correctly for both primary and replica sides"}
gaps:
  - "GrantConstraints has no SourceArn field (real SDK: GrantConstraints.SourceArn); no operation in this mock threads a caller/resource ARN through crypto calls to check against it, and no other service adapter currently supplies one either — deferred, needs cross-cutting request-context plumbing, not a KMS-local fix (bd: gopherstack-w3k)"
  - "CreateGrantInput has no GrantTokens field (real SDK: authorizes the CreateGrant call itself via an existing not-yet-consistent grant). No IAM/authorization layer exists anywhere in this mock, so this field would currently be a no-op; deferred, consistent with the rest of the codebase's scope"
  - "GranteeServicePrincipal / RetiringServicePrincipal (AWS-service grantees) not modeled on CreateGrantInput; same no-IAM-layer scope reasoning as above"
  - "RESOLVED 2026-07-12: DescribeKeyInput was missing the GrantTokens field -- added + wired validateGrantTokenPresence (see the DescribeKey op row and describe_key_grant_tokens_test.go). Unlike the CreateGrant/CreateGrantInput GrantTokens gap above (which authorizes the CreateGrant call itself and has nothing to validate without an IAM layer), DescribeKey's GrantTokens resolve to real existing grants, so validation is meaningful and AWS-accurate here (DescribeKey declares InvalidGrantTokenException)."
  - "RESOLVED 2026-07-12: the region-scoped KeyId resolution inconsistency (GetKeyPolicy/PutKeyPolicy/CreateGrant/ListGrants/RevokeGrant/RetireGrant indexed their policiesStore/grantsRegion using the request region instead of an ARN's embedded region). Root cause was two-fold and both fixed at source: (1) these ops discarded the region resolveKeyID returned and re-used getRegion(ctx) -- fixed by adding a shared resolveKeyAndRegion helper (lookupKey now delegates to it too) that returns the key's actual region, and routing all six ops through it; (2) resolveKeyID's resolution cache stored only the resolved UUID and returned the REQUEST region on every cache hit, so even the region resolveKeyID returned was wrong for any ARN resolved more than once -- fixed by caching a {keyID, region} pair (region=\"\" sentinel for aliases means 'derive from request context', so alias behavior is unchanged; ARN caches its own embedded region, which is safe because the region is part of the ARN cache key). Verified by region_scoped_resolution_test.go (cross-region ReplicateKey -> replica-ARN Put/GetKeyPolicy round-trip + full grant lifecycle by ARN, all while ctx defaults to the primary's region)."
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

## 2026-07-12 Terraform-lifecycle-focused re-audit (bd: none filed yet — see report)

Per the re-audit protocol: `git diff eb94f3c3..HEAD -- services/kms/` showed two commits
since the last ledger entry: `d9cb5d10` (the error-classification fix already recorded
above) and `42cff5ce` ("fix(kms): CreateKey now honors inline Policy (Terraform
GetKeyPolicy hang)" — CreateKey dropped the inline `Policy` field entirely, so
`GetKeyPolicy` always returned the synthesized default and Terraform's `aws_kms_key`
polled to a 10-minute timeout; already fixed before this pass started, per the task
background). This pass's brief: hunt for *more* bugs in the same two classes (AWS wire
parity, and LocalStack/Terraform read-after-write behavioral parity) that would break a
full `terraform apply`/`plan`/`destroy` cycle, since the CreateKey bug proved more exist.

### Fixed this pass

1. **`ReplicateKeyInput` was missing the `Policy` field entirely — the exact same bug
   class as the already-fixed CreateKey regression (severe, confirmed Terraform-breaking).**
   Confirmed against the real `aws-sdk-go-v2/service/kms@v1.54.0` vendored source
   (`api_op_ReplicateKey.go`, found via the Go module cache): the real `ReplicateKeyInput`
   carries `Policy *string`, and the field's doc comment is explicit that "The key policy
   is not a shared property of multi-Region keys... KMS does not synchronize this
   property" — i.e. a replica does NOT inherit the primary's policy; it needs its own,
   supplied via this field or defaulted. gopherstack's `ReplicateKeyInput` had no `Policy`
   field at all, so `Backend.ReplicateKey` silently dropped any inline policy a caller
   supplied, and `GetKeyPolicy` on the replica always returned the synthesized default.
   Since Terraform's `aws_kms_replica_key` resource sets an inline `policy` argument via
   `ReplicateKeyInput.Policy` and then polls `GetKeyPolicy` after apply until the
   configured policy propagates (identical read-after-write pattern to `aws_kms_key`),
   this would have caused the **exact same 10-minute poll-timeout hang** as the
   already-fixed CreateKey bug, just on the replica-key resource instead of the primary.
   Fixed by adding `Policy` (plus `BypassPolicyLockoutSafetyCheck`, present on the real
   input but a no-op here just like CreateKey's own copy of that field, since there is no
   IAM layer) to `ReplicateKeyInput`, validating it with the same `validKeyPolicyDoc`
   helper CreateKey/PutKeyPolicy already share (rejecting a malformed policy with
   `MalformedPolicyDocumentException` *before* creating the replica, consistent with the
   orphan-resource-leak fix from sweep 3), and persisting it into the replica's
   region-scoped `policiesStore` exactly like CreateKey does. See
   `backend.go`'s `ReplicateKey` and `models.go`'s `ReplicateKeyInput`.
   Proven by `Test_ReplicateKey_InlinePolicy` in `replicate_key_policy_test.go`, routed
   through the full HTTP handler with per-region requests (the replica lives in a
   different region than the primary) — table-driven: policy round-trips verbatim,
   defaults correctly when omitted, is rejected as malformed before any replica is
   created, and does not leak onto the primary's own policy (proving the "not a shared
   property" semantics).

2. **KMS resource tags were never included in `Handler.Snapshot`/`Restore` — a real
   persistence gap, exactly the perpetual-`terraform-plan`-drift bug class this sweep's
   brief called out as highest priority (moderate; only manifests across a process
   restart with persistence enabled).** Unlike most other gopherstack services, which
   embed `*tags.Tags` directly in their resource struct (see
   `.claude/memories/pkgs-catalog.md`'s tags entry), KMS applies tags at the *handler*
   layer: `createKeyAction`/`tagResource`/`replicateKeyAction` all write into
   `Handler.tags`, a side map keyed by KeyID, entirely separate from
   `InMemoryBackend`'s own state. `Handler.Snapshot` previously delegated straight to
   `Backend.Snapshot(ctx)` and returned those bytes verbatim — `Handler.tags` was never
   serialized at all. `ListResourceTags` kept returning tags correctly within a single
   running process (masking the gap in every same-process test, including the existing
   `create_key_policy_test.go`-style tests), but any gopherstack restart with persistence
   enabled would silently drop every KMS key's tags — the next `terraform plan` after a
   restart would show a permanent diff on the `tags` attribute of every `aws_kms_key`/
   `aws_kms_replica_key` resource, forever, since there'd be no way to reconcile it short
   of a real `TagResource` call. This is precisely the kind of gap the audit brief
   highlighted ("verify these newly-touched fields are included in the snapshot/restore
   round trip") applied retroactively to an *existing*, previously undetected gap rather
   than a field I was actively adding. Fixed: `Handler.Snapshot` now wraps the backend's
   own snapshot bytes together with a tags map into a small `handlerSnapshot` envelope
   (`persistence.go`), stamped with a `handlerFormat` marker. `Handler.Restore` peeks that
   marker to distinguish the new wrapped shape from a *legacy* snapshot (raw backend
   bytes with no wrapper at all — exactly what `Handler.Snapshot` produced before this
   fix) so an existing on-disk snapshot taken before this fix still restores backend
   state cleanly instead of erroring out; it just won't have tags to restore (no worse
   than the pre-fix status quo). Proven by `TestHandlerSnapshotRestore_TagsRoundTrip`,
   `TestHandlerSnapshotRestore_EmptyTagsOmitted`, and
   `TestHandlerRestore_LegacyBackendOnlySnapshot` in `handler_tags_persistence_test.go`.

### Audited and confirmed already correct (do not re-check next pass)

- **CreateKey's inline `Policy` fix (the task's background bug) is solid**: verified the
  fix persists the policy into `policiesStore` before returning, and that `GetKeyPolicy`
  returns it verbatim on every subsequent call (not regenerated), including across two
  consecutive `DescribeKey`/`GetKeyPolicy`/`ListResourceTags`/`GetKeyRotationStatus`
  calls in the new `TestTerraformLifecycle_KMSKey` integration test (`assert.JSONEq`
  on the raw HTTP response bytes of back-to-back calls) — no drift.
- **The synthesized default key policy is deterministic**: built from a static string
  template (`policiesStore` miss branch in `GetKeyPolicy`), not regenerated with
  randomized/timestamped content — confirmed identical across repeated calls.
- **Tags round-trip immediately after CreateKey** (within a single process, the common
  case): `createKeyAction` validates tags *before* creating the key (sweep-3 fix, still
  correct) and applies them synchronously in the same request; `ListResourceTags`
  reflects them immediately, no async lag.
- **Aliases**: full `CreateAlias -> ListAliases -> UpdateAlias -> DeleteAlias` round trip
  confirmed correct. `ListAliases` does NOT synthesize `alias/aws/*` AWS-managed entries
  — confirmed intentional (no AWS-managed-key simulation anywhere in this mock, a
  pre-existing scope boundary, not a gap) rather than an oversight. `DeleteAlias` is
  correctly non-idempotent (`NotFoundException` on double-delete), matching real AWS.
- **Grants**: `CreateGrant` returns `GrantId`+`GrantToken`; `ListGrants` (with its
  `GrantId` filter, matching the real SDK's `ListGrantsInput.GrantId`) shows the new
  grant immediately, matching how `aws_kms_grant`'s refresh re-reads a specific grant;
  `RetireGrant`/`RevokeGrant` remove it immediately (verified via the pre-existing
  `GrantIndexesConsistent` test helper).
- **ScheduleKeyDeletion/CancelKeyDeletion/EnableKey/DisableKey**: all state transitions
  (`KeyState`, `Enabled`, `DeletionDate`, `PendingDeletionWindowInDays`) apply
  synchronously with no async lag; `DescribeKey` reflects them on the very next call.
  Proven end-to-end (not just at the backend layer) by
  `TestTerraformLifecycle_KMSKey`'s `ScheduleKeyDeletion -> DescribeKey` sequence.
- **EnableKeyRotation/DisableKeyRotation/GetKeyRotationStatus**: round-trip exactly;
  default `KeyRotationEnabled` is `false` and stable across repeated
  `GetKeyRotationStatus` calls (byte-identical JSON, asserted via `assert.JSONEq` in the
  new lifecycle test).
- **Replica keys (`ReplicateKey`/`DescribeKey`)**: `MultiRegionConfiguration`
  (`PRIMARY`/`REPLICA` role + `PrimaryKey`/`ReplicaKeys` list) already correct on both
  sides prior to this pass (per sweep-3's `TestUpdatePrimaryRegion_RoleSwap`); this
  pass's only replica-side finding was the missing `Policy` field (fixed above).
- **DescribeKey's `KeyMetadata` shape**: `KeyState`, `KeyManager` (hardcoded `"CUSTOMER"`,
  correct — this mock never creates AWS-managed keys), `MultiRegion`,
  `MultiRegionConfiguration`, `PendingDeletionWindowInDays`, `DeletionDate`, `ValidTo`,
  `Origin`, `KeySpec` AND the deprecated `CustomerMasterKeySpec` alias (always mirrors
  `KeySpec`), `EncryptionAlgorithms`, `SigningAlgorithms`, `Enabled` — all present and
  correct; cross-checked field-by-field against the real vendored
  `aws-sdk-go-v2/service/kms@v1.54.0/types.KeyMetadata`.
- **Tag/grant/policy wire shapes** (`TagResource`/`UntagResource`/`ListResourceTags`
  inputs+outputs, `ListGrantsInput`/`CreateGrantInput`/`GrantListEntry`) — all
  field-for-field checked against the vendored real SDK; no casing or shape gaps. Note
  the real SDK's `GrantListEntry` (the `ListGrants` response entry type) has NO
  `GrantToken` field at all (tokens are only ever returned once, at `CreateGrant` time);
  gopherstack's shared `Grant` struct does include `GrantToken` in `ListGrants` output
  too, which is a harmless superset (unknown extra JSON fields are ignored by the real
  SDK's non-strict deserializer) rather than a functional bug — not fixed, noted for
  awareness only.

### Cross-service KMS integration punch-list (Step 4, report-only — no edits made)

Searched the full non-KMS codebase (read-only) for `KmsKeyId`/`KMSMasterKeyId`/
`SSEKMSKeyId`-style fields. **Already wired in `cli.go` (no gap):**

- **SSM** (`wireSSMKMS` in `cli.go:3542`): `ssm.InMemoryBackend.WithKMS` +
  `ssm.KMSEncryptor` interface + a `cli.go`-local `ssmKMSAdapter` calling
  `kms.InMemoryBackend.Encrypt`/`Decrypt` directly. SecureString `Parameter`s whose
  `KeyId` is set get *real* KMS encryption already. No action needed.
- **Resource Groups Tagging API** (`wireTaggingKMS` in `cli.go:5218`): uses
  `Handler.TaggedKeys`/`TagKeyByARN`/`UntagKeyByARN` (already exported on
  `kms.Handler` specifically for this). No action needed.
- **CloudFormation** (`services/cloudformation/resources.go`,
  `resources_phase5.go`/`resources_phase6.go`): `AWS::KMS::Key`/`AWS::KMS::Alias`/
  `AWS::KMS::ReplicaKey` CFN resource types call `rc.backends.KMS.Backend.CreateKey`/
  `CreateAlias`/`DeleteAlias`/`ReplicateKey`/`ScheduleKeyDeletion` directly via the
  already-exported `Handler.Backend` (`StorageBackend`) field. This is CFN driving KMS
  natively (not really a "consumer" relationship) but confirms the integration point
  already works end-to-end. No action needed.

**Not wired (gap, but confirmed pro-tier-enforcement-only, not needed for a basic
`terraform apply`/`plan`/`destroy` to succeed):** none of the below call into the `kms`
package at all today; each just stores/echoes the KMS key ID string on its own resource
with no existence check, no alias resolution beyond what the field already is, and no
real encrypt/decrypt. For every one of these, **Terraform's own resource lifecycle only
needs the field to round-trip on the owning service's Create/Describe/Update calls**
(that service's own attribute-round-trip correctness is that service's PARITY.md
concern, not KMS's) — actually calling into KMS is additive Pro-tier realism, not a
correctness requirement for `apply`/`plan`/`destroy` to succeed:

  - **S3** (`aws_s3_bucket_server_side_encryption_configuration`,
    `kms_master_key_id`/`SSEKMSKeyId` in `backend_memory.go`/`multipart_ops.go`/
    `object_ops.go`/`types.go`): stored per-object/version, never validated or used to
    actually encrypt object bytes at rest. (a) existence check: no. (b) alias
    resolution: no (stores whatever string was given). (c) real encrypt/decrypt: no. (d)
    needed for basic apply: no.
  - **SQS** (`aws_sqs_queue`, `KmsMasterKeyId`/`KmsDataKeyReusePeriodSeconds` queue
    attributes in `types.go`/`backend.go`): stored/echoed only, format-unvalidated. Same
    (a)-(d) answers as S3.
  - **SNS** (`aws_sns_topic`, `KmsMasterKeyId` topic attribute in `backend.go`): DOES
    format-validate the value (alias name / alias ARN / key ID / key ARN shape) but
    never checks it against a live KMS key and never actually encrypts messages. (a) no
    existence check (format-only). (b)/(c) no. (d) not needed for basic apply.
  - **Secrets Manager** (`aws_secretsmanager_secret`, `KmsKeyID` in `backend.go`/
    `models.go`): stored/echoed on the secret and its replicas, never validated or used
    to encrypt `SecretString`/`SecretBinary` at rest. Same (a)-(d) answers as S3. This is
    arguably the highest-value future wire-up (mirrors the SSM precedent almost exactly
    — Secrets Manager's `SecretString` is conceptually identical to SSM's SecureString
    `Value`), but still not required for `terraform apply` to succeed today.
  - **DynamoDB** (`aws_dynamodb_table`, `SSESpecification.KMSMasterKeyId` ->
    `SSEKMSMasterKeyArn` in `table_ops.go`): stored/echoed only. Same (a)-(d) as S3.
  - **RDS** (`aws_db_instance`/`aws_db_snapshot`, `KmsKeyId` throughout
    `handler.go`/`batch1.go`): stored/echoed only. Same (a)-(d) as S3.
  - **EC2/EBS** (`aws_ebs_volume`/`aws_ebs_default_kms_key`, `KmsKeyID` in
    `backend_accuracy.go`/`backend_batch1.go`/`backend_batch3.go`): stored/echoed only,
    plus an account-level default-KMS-key setting (`GetEbsDefaultKmsKeyID`/
    `ResetEbsDefaultKmsKeyID`) that's also just a stored string. Same (a)-(d) as S3.
  - **CloudWatch Logs** (`aws_cloudwatch_log_group`, `KmsKeyId` in
    `backend.go`/`handler.go`/`models.go`): stored/echoed only. Same (a)-(d) as S3.

**No new KMS-side export is needed for any future cli.go wiring of the services above.**
The integration surface already exists and is already exercised by the SSM/CloudFormation
precedents: `kms.Handler.Backend` is already a public field typed as the `StorageBackend`
interface (exactly how `wireSSMKMS` and the CloudFormation resource handlers obtain a
concrete `*kms.InMemoryBackend` today), and that interface's already-public
`DescribeKey`/`Encrypt`/`Decrypt` methods are sufficient for a future adapter to (a)
check key existence (`DescribeKey` + `errors.Is(err, kms.ErrKeyNotFound)`, both exported),
(b) resolve an alias or ARN to a key ID (`DescribeKey` already accepts `alias/...`/
`arn:...`/bare-UUID interchangeably in its `KeyId` field and returns the canonical
`KeyMetadata.KeyID`), and (c) perform real encrypt/decrypt passthrough (`Encrypt`/
`Decrypt`), exactly mirroring `ssmKMSAdapter`'s three-method shape. Wiring any of the
services above is pure `cli.go` + that service's own backend work, per
`PARITY_PHASE4_KICKOFF.md`'s "cross-service interconnect wired in cli.go, main-thread
work" rule — out of scope for this KMS-only pass.

## 2026-07-12 gap-closure pass (in-scope follow-ups from the lifecycle re-audit)

Closed the two remaining in-scope gaps the Terraform-lifecycle re-audit above had
identified and deferred. Both are now `RESOLVED` in the `gaps` block; the op rows carry
the details.

1. **`DescribeKeyInput.GrantTokens` added (wire parity + AWS-accurate validation).** The
   real `aws-sdk-go-v2/service/kms@v1.54.0` `DescribeKeyInput` carries `GrantTokens
   []string` and the op declares `InvalidGrantTokenException` in its error set, so
   validation here is meaningful (unlike the still-deferred `CreateGrantInput.GrantTokens`,
   which authorizes the CreateGrant call itself and has nothing to check without an IAM
   layer). Wired `validateGrantTokenPresence` (existence + TTL, no encryption-context
   check), consistent with Sign/Verify/GetPublicKey/DeriveSharedSecret. Empty tokens is a
   no-op — the only case Terraform exercises. Tests: `describe_key_grant_tokens_test.go`.

2. **Region-scoped KeyId resolution made consistent across all region-partitioned ops.**
   `GetKeyPolicy`/`PutKeyPolicy`/`CreateGrant`/`ListGrants`/`RevokeGrant`/`RetireGrant` now
   index their `policiesStore`/`grantsRegion` using the key's OWN region (an ARN's embedded
   region for an ARN input), the same region-awareness `DescribeKey`/`Encrypt`/`Decrypt`
   already had via `lookupKey`. Two root causes, both fixed at source in `backend.go`:
   - Added `resolveKeyAndRegion(ctx, keyID) (*Key, region, error)` — resolves the key AND
     reports the region it actually lives in (with the same bare-UUID all-region fallback
     `lookupKey` had). `lookupKey` now delegates to it (returns just the key). The six ops
     route through it instead of `getRegion(ctx)` + `resolveKeyID` + `keysStore(region)`.
   - **Deeper bug the cross-region test surfaced:** `resolveKeyID`'s resolution cache stored
     only the resolved UUID and returned the *request* region on every cache hit — so the
     2nd+ resolution of any ARN lost the ARN's embedded region even for callers that did
     use it. Fixed by caching a `{keyID, region}` pair: `region=""` sentinel for aliases
     (means "derive from request context" — alias behavior unchanged, and safe because a
     bare `alias/name` cache key carries no region), and the ARN's own embedded region for
     ARN inputs (safe because the region is part of the ARN cache key). This is the piece
     that makes the fix hold across repeated calls, which is exactly the read-after-write
     pattern Terraform drives.
   Alias ops (`CreateAlias`/`UpdateAlias`/`DeleteAlias`/`ListAliases`) were audited and
   deliberately left request-region-scoped: an AWS alias must target a key in its own
   region, so resolving the target/filter against the request region is correct AWS
   behavior, not the same bug. Tests: `region_scoped_resolution_test.go` (cross-region
   `ReplicateKey` → replica-ARN policy round-trip + full grant lifecycle by ARN, table-
   driven over Revoke/Retire-by-ARN/Retire-by-token/Retire-by-GrantId, all while ctx
   defaults to the primary's region).

**Only remaining KMS-related follow-up is external (out of this service's scope):** the
cross-service Secrets Manager → KMS wiring (real `SecretString`/`SecretBinary` encryption
via a `secretsmanager.KMSEncryptor`-style adapter, mirroring the existing SSM precedent).
That is `cli.go` + Secrets Manager backend work — main-thread / cross-service territory
per `PARITY_PHASE4_KICKOFF.md`, and needs no new KMS-side export (`Handler.Backend`'s
`Encrypt`/`Decrypt`/`DescribeKey` already suffice, as documented in the cross-service
punch-list above). No KMS-local gaps remain.
