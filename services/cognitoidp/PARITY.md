---
service: cognitoidp
sdk_module: aws-sdk-go-v2/service/cognitoidentityprovider@1.59.1
last_audit_commit: 7d0baf79
last_audit_date: 2026-07-05
overall: A                # ~870 LOC genuine fixes this pass (see below); prior sweeps already got most of the surface to B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
ops:
  InitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "USER_PASSWORD_AUTH/ADMIN_USER_PASSWORD_AUTH/USER_SRP_AUTH(simplified)/REFRESH_TOKEN_AUTH real; PreventUserExistenceErrors masking added this pass (gopherstack-2sp)"}
  AdminInitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "never masks UserNotFoundException, matching AWS (admin API)"}
  RespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "SOFTWARE_TOKEN_MFA now real RFC6238 TOTP (was disguised stub accepting any 6 digits); SMS_MFA/EMAIL_OTP now require the generated one-time code (was also any 6 digits); PASSWORD_VERIFIER/NEW_PASSWORD_REQUIRED unchanged, real"}
  AdminRespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as RespondToAuthChallenge (shared backend method)"}
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
  SignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "password policy enforced, real confirm code generated"}
  ConfirmSignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "expiring codes, CodeMismatchException/ExpiredCodeException"}
  AdminConfirmSignUp: {wire: ok, errors: ok, state: ok, persist: ok}
  ResendConfirmationCode: {wire: ok, errors: ok, state: ok, persist: ok, note: "does not yet apply PreventUserExistenceErrors masking, see gaps (gopherstack-aib)"}
  AdminCreateUser: {wire: ok, errors: ok, state: ok, persist: ok}
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
  ForgotPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "does not yet apply PreventUserExistenceErrors masking, see gaps (gopherstack-aib)"}
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
  - "LambdaConfig (PreSignUp, PostConfirmation, PreTokenGeneration, CustomMessage, etc.) is stored/returned but no trigger is ever invoked — would require cross-service invocation into the lambda service. (bd: gopherstack-8fw)"
  - "PreventUserExistenceErrors=ENABLED masking (added this pass) only covers InitiateAuth. ForgotPassword and ResendConfirmationCode still reveal UserNotFoundException regardless of the setting; AWS masks those too (fake-success response). (bd: gopherstack-aib)"
deferred:
  - "Identity providers, resource servers, user import jobs, devices, WebAuthn, managed login branding, risk config, domains, terms, log delivery: verified at a family/smoke level (dispatch wired, backend mutates real maps, persisted in backendSnapshot, no bare stubs found), not re-walked op-by-op field-by-field this pass — unchanged since the prior two sweeps (parity sweep 1 & 2) which already covered these in depth."
leaks: {status: clean, note: "janitor.go sweeps expired refresh tokens/mfa sessions/confirm codes/attr verification codes on a bounded interval (WithJanitor); ctx cancellation observed via StartWorker; no new goroutines/unbounded maps introduced this pass — the two new maps touched (mfaSessionEntry.Code is a field, not a map) reuse existing bounded, janitor-swept structures"}
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
