---
service: cognitoidp
sdk_module: aws-sdk-go-v2/service/cognitoidentityprovider@1.59.1
last_audit_commit: ee7d2bae
last_audit_date: 2026-07-12
overall: A                # ~17 LOC genuine fix (backend.go) + 107 LOC new tests this pass (PreventUserExistenceErrors masking gap closed); no local drift since ce30166a (prior sweep 3), SDK pinned at same v1.59.1
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
ops:
  InitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "USER_PASSWORD_AUTH/ADMIN_USER_PASSWORD_AUTH/USER_SRP_AUTH(simplified)/REFRESH_TOKEN_AUTH real; PreventUserExistenceErrors masking added prior pass (gopherstack-2sp); PreTokenGeneration trigger now fires on token issuance (this pass, gopherstack-8fw)"}
  AdminInitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "never masks UserNotFoundException, matching AWS (admin API); PreTokenGeneration trigger now fires (this pass)"}
  RespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "SOFTWARE_TOKEN_MFA now real RFC6238 TOTP (was disguised stub accepting any 6 digits); SMS_MFA/EMAIL_OTP now require the generated one-time code (was also any 6 digits); PASSWORD_VERIFIER/NEW_PASSWORD_REQUIRED unchanged, real; PreTokenGeneration trigger now fires on token issuance (this pass)"}
  AdminRespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as RespondToAuthChallenge (shared backend method); PreTokenGeneration trigger now fires (this pass)"}
  AssociateSoftwareToken: {wire: ok, errors: ok, state: ok, persist: ok}
  VerifySoftwareToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "now verifies a real RFC 6238 TOTP code against the associated secret (was: any 6 digits accepted) — gopherstack-2sp"}
  SetUserMFAPreference: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminSetUserMFAPreference: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUserPool: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUserPool: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUserPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "PasswordPolicy, MfaConfiguration, LambdaConfig(stored only, see gaps), AccountRecoverySetting, DeletionProtection all settable"}
  ListUserPools: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUserPool: {wire: ok, errors: ok, state: ok, persist: ok, note: "now refuses to delete when DeletionProtection=ACTIVE (was: silently deleted regardless) — gopherstack-2sp"}
  CreateUserPoolClient: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors field added this pass (was entirely unimplemented); OAuth flows/scopes/callback URLs/token validity units/secret generation all real"}
  UpdateUserPoolClient: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors now updatable"}
  DescribeUserPoolClient: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUserPoolClients: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUserPoolClient: {wire: ok, errors: ok, state: ok, persist: ok}
  AddUserPoolClientSecret: {wire: ok, errors: ok, state: ok, persist: ok}
  SignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "password policy enforced, real confirm code generated; PreSignUp trigger now fires and applies autoConfirmUser/autoVerifyEmail/autoVerifyPhone, CustomMessage trigger now fires (this pass, gopherstack-8fw)"}
  ConfirmSignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "expiring codes, CodeMismatchException/ExpiredCodeException; PostConfirmation trigger now fires fire-and-observe (this pass) -- invocation errors surface but do not roll back confirmation, matching AWS"}
  AdminConfirmSignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "PostConfirmation trigger now fires (this pass), same source/semantics as ConfirmSignUp"}
  ResendConfirmationCode: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors=ENABLED now masks unknown-user UserNotFoundException as a fabricated success (prior pass, closes gopherstack-aib); CustomMessage trigger now fires (this pass)"}
  AdminCreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreSignUp trigger now fires (source PreSignUp_AdminCreateUser); only autoVerifyEmail/autoVerifyPhone applied, autoConfirmUser has no target state for admin-created users (this pass)"}
  AdminSetUserPassword: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminGetUser: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminDeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminResetUserPassword: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminUpdateUserAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminDeleteUserAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminDisableUser: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminEnableUser: {wire: ok, errors: ok, state: ok, persist: ok}
  AdminUserGlobalSignOut: {wire: ok, errors: ok, state: ok, persist: ok, note: "revokes refresh tokens + stamps tokenRevokedBefore so already-issued access tokens are rejected too"}
  GlobalSignOut: {wire: ok, errors: ok, state: ok, persist: ok, note: "same revocation mechanism as AdminUserGlobalSignOut"}
  RevokeToken: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "pkgs/page-style pagination"}
  ListUsersInGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ForgotPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors=ENABLED masks unknown-user UserNotFoundException as a fabricated success (prior pass, closes gopherstack-aib); CustomMessage trigger now fires (this pass, gopherstack-8fw)"}
  ConfirmForgotPassword: {wire: ok, errors: ok, state: ok, persist: ok}
  ChangePassword: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUserAttributes/AdminDeleteUserAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  VerifyUserAttribute/GetUserAttributeVerificationCode: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroup/DeleteGroup/GetGroup/ListGroups/UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "precedence respected"}
  AdminAddUserToGroup/AdminRemoveUserFromGroup/AdminListGroupsForUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "cognito:groups claim reflected in ID/access tokens"}
  CreateResourceServer/DescribeResourceServer/ListResourceServers/UpdateResourceServer/DeleteResourceServer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIdentityProvider/DescribeIdentityProvider/ListIdentityProviders/UpdateIdentityProvider/DeleteIdentityProvider/GetIdentityProviderByIdentifier: {wire: ok, errors: ok, state: ok, persist: ok, note: "audited at a family level, not re-walked line by line this pass — unchanged since prior sweep"}
  TagResource/UntagResource/ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "pkgs/tags"}
  GetSigningCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "deterministic self-signed X.509 wrapping the pool's real RSA key"}
  GetUserPoolMfaConfig/SetUserPoolMfaConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  jwks_well_known: {wire: ok, errors: ok, state: ok, persist: ok, note: "RS256, real RSA-2048 per pool, JWKS + GetSigningCertificate both derive from the same key"}
families:
  user_import_jobs: {status: ok, note: "CreateUserImportJob/StartUserImportJob/StopUserImportJob/DescribeUserImportJob/ListUserImportJobs/GetCSVHeader — audited at family level, unchanged since prior sweep"}
  devices: {status: ok, note: "ConfirmDevice/ForgetDevice/AdminForgetDevice/GetDevice/AdminGetDevice/ListDevices/AdminListDevices/UpdateDeviceStatus/AdminUpdateDeviceStatus — family level, unchanged since prior sweep"}
  webauthn: {status: ok, note: "StartWebAuthnRegistration/CompleteWebAuthnRegistration/ListWebAuthnCredentials/DeleteWebAuthnCredential — family level, unchanged since prior sweep"}
  managed_login_branding: {status: ok, note: "Create/Describe/Update/Delete + DescribeManagedLoginBrandingByClient, UICustomization — family level"}
  risk_config: {status: ok, note: "DescribeRiskConfiguration/SetRiskConfiguration/AdminListUserAuthEvents/AdminUpdateAuthEventFeedback/GetUserAuthFactors — family level"}
  domains: {status: ok, note: "CreateUserPoolDomain/DescribeUserPoolDomain/DeleteUserPoolDomain — family level"}
  terms: {status: ok, note: "CreateTerms/DescribeTerms/ListTerms/UpdateTerms/DeleteTerms — family level"}
  log_delivery: {status: ok, note: "GetLogDeliveryConfiguration/SetLogDeliveryConfiguration — family level"}
gaps:
  - "USER_SRP_AUTH does not implement real SRP-6a: InitiateAuth requires AuthParameters[PASSWORD] directly (server-side bcrypt check), then returns a PASSWORD_VERIFIER challenge that RespondToAuthChallenge completes without any zero-knowledge proof exchange. A real SRP client never sends PASSWORD and cannot authenticate here. Investigated in depth this pass; not fixed because a byte-perfect implementation of Cognito's SRP variant (3072-bit N, HKDF-SHA256 with the \"Caldera Derived Key\" info string, HMAC-SHA256 M1 proof) could not be verified against a real client/reference vectors in this session, and a subtly-wrong crypto implementation would be worse than the current honestly-documented simplification. (bd: gopherstack-p8i)"
  - "LambdaConfig trigger invocation: PreSignUp (SignUp + AdminCreateUser), PostConfirmation (ConfirmSignUp + AdminConfirmSignUp), PreTokenGeneration (InitiateAuth/AdminInitiateAuth/RespondToAuthChallenge/AdminRespondToAuthChallenge + REFRESH_TOKEN_AUTH), and CustomMessage (SignUp/ForgotPassword/ResendConfirmationCode) now fire for real via a new `LambdaTriggerInvoker` interface (services/cognitoidp/lambda_triggers.go) that cli.go wires to the lambda service's Invoke; nil/unwired-invoker or unset LambdaConfig preserves prior no-op behavior exactly. PreAuthentication, PostAuthentication, DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse, and UserMigration are still stored/returned but never invoked — their invocation points/response contracts were not verified against the AWS custom-auth-challenge state machine this pass; each needs its own bd follow-up. (bd: gopherstack-8fw, now partially closed)"
  - "PreventUserExistenceErrors=ENABLED still only masks user-existence at InitiateAuth/ForgotPassword/ResendConfirmationCode (this pass closed the latter two — see 'What this pass fixed'). ConfirmSignUp and ConfirmForgotPassword do not mask an unknown username behind CodeMismatchException the way AWS does; not fixed this pass to keep the change scoped to the ledger's existing gap (gopherstack-aib) — worth a follow-up bd issue if stricter parity is wanted."
deferred:
  - "Identity providers, resource servers, user import jobs, devices, WebAuthn, managed login branding, risk config, domains, terms, log delivery: verified at a family/smoke level (dispatch wired, backend mutates real maps, persisted in backendSnapshot, no bare stubs found), not re-walked op-by-op field-by-field this pass — unchanged since the prior two sweeps (parity sweep 1 & 2) which already covered these in depth."
  - "handler.go retains dead, shadowed implementations of ~15 ops (ForgotPassword, ConfirmForgotPassword, ResendConfirmationCode, SignUp, ConfirmSignUp, InitiateAuth, AdminInitiateAuth, CreateUserPool[Client], UpdateUserPool[Client], DescribeUserPool[Client], ListUserPoolClients, GetUser) whose dispatchTable() registrations are unconditionally overwritten by accuracy_handler.go's accurate/SecretHash-validating versions via maps.Copy(table, h.accuracyDispatchTable()) in dispatchTable(). Confirmed genuinely unreachable (not a routing bug — the accurate version is correct and live), but it is dead code that could mislead a future auditor reading handler.go in isolation (as it briefly did this pass). Not deleted this pass to stay scoped to the PreventUserExistenceErrors fix; candidate for a follow-up de-stub-hygiene cleanup bd issue."
leaks: {status: clean, note: "janitor.go sweeps expired refresh tokens/mfa sessions/confirm codes/attr verification codes on a bounded interval (WithJanitor); ctx cancellation observed via StartWorker; no new goroutines/unbounded maps introduced this pass"}
---

## Notes

### What this pass fixed (see report for full detail)

1. **MFA challenge codes were a disguised stub (highest-severity finding).**
   `RespondToMFAChallenge` / `VerifySoftwareToken` validated only that the supplied code was
   6 ASCII digits and then always succeeded — SOFTWARE_TOKEN_MFA, SMS_MFA, and EMAIL_OTP were
   all bypassable with any random 6-digit string, regardless of the secret returned by
   `AssociateSoftwareToken`. Fixed with a real RFC 6238 TOTP implementation
   (`totp.go`: HMAC-SHA1, 30s step, ±1 step clock-skew tolerance, RFC 4226 dynamic
   truncation) verified against the **official RFC 4226 Appendix D and RFC 6238 Appendix B
   test vectors** (two independently published vector sets that cross-check each other —
   RFC 4226 counter=1 and RFC 6238 T=59 both yield `287082`). SMS_MFA/EMAIL_OTP now require
   the one-time code generated by `newMFASession` (mirroring the existing
   ForgotPassword/ConfirmSignUp confirmation-code pattern) instead of accepting anything.
   A wrong code no longer consumes the MFA session, so the caller can retry until it
   expires — matching real Cognito.

2. **PreventUserExistenceErrors was completely unimplemented** on `UserPoolClient` (not
   stored, not exposed on Create/Update/Describe, not enforced anywhere) despite being
   explicitly part of the client model. Added the field (default `"LEGACY"`, matching AWS),
   wired it through Create/UpdateUserPoolClient input/output, and enforced it in the
   non-admin `InitiateAuth` path: `ENABLED` now masks an unknown username behind the exact
   same `NotAuthorizedException` text a wrong password produces (proven in tests: the two
   error strings are asserted equal). `AdminInitiateAuth` intentionally never masks — AWS
   only applies this to the non-admin, unauthenticated API.

3. **DeletionProtection was stored but never enforced.** `DeleteUserPool` deleted pools
   unconditionally even when `DeletionProtection: "ACTIVE"`. Now returns
   `InvalidParameterException` with AWS's documented remediation message, and the pool can
   still be deleted after `UpdateUserPool` flips it back to `"INACTIVE"`.

4. **(2026-07-12 re-audit) `ForgotPassword`/`ResendConfirmationCode` did not honor
   `PreventUserExistenceErrors` (previously flagged gap `gopherstack-aib`).** Both ops
   unconditionally returned `UserNotFoundException` for an unknown username regardless of
   the app client's setting, letting a caller enumerate valid usernames even with
   `PreventUserExistenceErrors=ENABLED` — the exact vulnerability that setting exists to
   close, and the same masking `InitiateAuth` already got in a prior sweep. Fixed in
   `backend.go`'s `ForgotPassword`/`ResendConfirmationCode`: when the client masks
   existence errors, an unknown username now returns a fabricated success (same
   `CodeDeliveryDetails` shape a real account gets) instead of erroring, and the fabricated
   code is never stored so it can't actually be redeemed. New table-driven tests in
   `prevent_user_existence_test.go` (`Test_ForgotPassword_PreventUserExistenceErrors`,
   `Test_ResendConfirmationCode_PreventUserExistenceErrors`) lock in both the masked and
   unmasked (LEGACY) behavior.

### Traps for the next auditor

- **USER_SRP_AUTH is not a stub in the "returns fake data" sense** — it does real bcrypt
  password verification and issues real signed JWTs. It's a *simplified* SRP: the server
  never receives an SRP_A value and never proves anything in zero-knowledge; it just
  requires the plaintext password in `AuthParameters["PASSWORD"]` and fakes the
  `PASSWORD_VERIFIER` challenge/response shape around a direct password check. This means
  the wire *shape* (ChallengeName, Session, ChallengeParameters) matches AWS's
  `cognitoidentityprovider` types, but the *protocol* does not — any client that actually
  performs SRP math (rather than a test harness that knows to pass PASSWORD directly) will
  fail to authenticate. Do not "fix" this without either (a) real reference test vectors
  from a genuine Cognito SRP client, or (b) accepting the risk of shipping unverifiable
  crypto — get a second opinion before attempting bit-perfect SRP-6a here.
- **`RespondToMFAChallenge` is now challenge-type-aware** (`verifyMFAChallengeCode` in
  backend.go): SOFTWARE_TOKEN_MFA verifies against `user.TOTPSecret` via `verifyTOTPCode`;
  SMS_MFA/EMAIL_OTP verify against `mfaSessionEntry.Code`. If you add a new MFA challenge
  type, it must be added to this switch explicitly — the `default` case now denies rather
  than silently accepting (previously everything funneled through one format-only check).
- **`GenerateTOTPCode` is exported** specifically so integration/SDK-driven tests can
  compute the code a real authenticator app would produce for a secret returned by
  `AssociateSoftwareToken`, without needing a TOTP library dependency.
- Protocol is `json-1.1` (`X-Amz-Target: AWSCognitoIdentityProviderService.<Op>`), not
  XML — confirmed via `handler.go`'s `service.HandleTarget` wiring; no XML wrapper traps
  apply to this service the way they do to EC2/S3-family query/XML services.
- Token timestamps (`iat`/`exp`/`auth_time`/`UserCreateDate`/etc.) are epoch-seconds JSON
  numbers throughout — already correct, no `awstime.Epoch` gap found.
- **`handler.go`'s `dispatchTable()` and `accuracy_handler.go`'s `accuracyDispatchTable()`
  overlap on ~15 op names** (see `deferred` above for the full list). `dispatchTable()`
  does `maps.Copy(table, h.accuracyDispatchTable())` *after* populating its own entries, so
  the accuracy version always wins and the `handler.go` version of those 15 ops is dead
  code — do not assume editing `handler.go`'s `handleForgotPassword` (etc.) has any
  runtime effect; the live implementation is the `*Accurate` twin in `accuracy_handler.go`.
  This is what almost caused a wasted fix this pass (this branch's `PreventUserExistenceErrors`
  gap was correctly fixed in the shared `backend.go` methods, so it applies regardless, but
  reading only `handler.go` in isolation would give a false read on op behavior).
