---
service: cognitoidp
sdk_module: aws-sdk-go-v2/service/cognitoidentityprovider@1.67.4
last_audit_commit: pending (uncommitted this pass -- see git log at merge time)
last_audit_date: 2026-08-08
overall: B                # 2026-08-08 (gopherstack-n7gh, follow-up pass): downgraded from A to B
                       # this pass -- terms/ (below) is a real, non-structural gap: its entire
                       # wire model is fictional, and no genuine aws-sdk-go-v2 client can
                       # successfully call CreateTerms against this backend today (client-side
                       # request validation itself rejects the call before it's even sent).
                       # Every other family this pass touched came back clean or was fixed;
                       # this is the one confirmed real op family a real client cannot use.
                       # Completed the rest of
                       # gopherstack-n7gh's stated scope. UserMigration_ForgotPassword trigger
                       # source implemented (user_migration.go/lambda_triggers.go); domain
                       # AWSAccountId/ManagedLoginVersion/S3Bucket now populated (domains.go).
                       # Op-by-op re-walk of user_import_jobs/devices/webauthn/
                       # managed_login_branding/risk_config/terms/log_delivery plus a full
                       # field diff of identity_providers/resource_servers found and fixed real
                       # bugs: webauthn's response used the wrong wire key ("FriendlyName"
                       # instead of "FriendlyCredentialName" -- silently unreadable by any real
                       # SDK client) and dropped the required AuthenticatorTransports field;
                       # managed_login_branding completely discarded Settings/Assets/
                       # UseCognitoProvidedValues -- the entire payload of the feature -- on
                       # Create/Update; SetLogDeliveryConfiguration was a disguised stub that
                       # hardcoded nil regardless of the client's LogConfigurations; CreateUserImportJob
                       # silently dropped CloudWatchLogsRoleArn (a required field) and
                       # PasswordHashingAlgorithm. All fixed this pass -- see gaps below for
                       # detail and see deferred below for what was found but NOT fixed:
                       # terms/ is built on an entirely fictional wire model (not just missing
                       # fields) and needs a full redesign, not a bounded fix.
                       # SRP-6a itself was already completed earlier the same day in commit
                       # 041c16c75 (see the CLOSED gap entry below) -- this pass did not touch it.
                       #
                       # --- prior (2026-07-25, parity-4) history, kept for context ---
                       # 7 new SDK ops (CreateUserPoolReplica/ListUserPoolReplicas/UpdateUserPoolReplica/DeleteUserPoolReplica/AdminGetUserAuthFactors/GetProvisionedLimit/UpdateProvisionedLimit) implemented for real against a bumped SDK, closing TestSDKCompleteness -- all field-diffed against the installed SDK's types/serializers/deserializers, all backed by real state (no notImplemented additions). Two explicit, documented modeling assumptions (replica's initial Status; provisioned limits' account-level-max ceiling) -- see families.user_pool_replicas/provisioned_limits below. Everything from the 2026-07-23 pass (CUSTOM_AUTH state machine, UserMigration/PreAuthentication/PostAuthentication triggers, PreventUserExistenceErrors on ConfirmSignUp/ConfirmForgotPassword, DescribeUserPoolDomain CustomDomainConfig, dead-code deletion) carries forward unchanged, not re-walked this pass. 0 golangci-lint issues, 0 banned nolints, race-clean
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
ops:
  InitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "USER_PASSWORD_AUTH/ADMIN_USER_PASSWORD_AUTH/CUSTOM_AUTH/REFRESH_TOKEN_AUTH all real; PreventUserExistenceErrors masking (prior pass, gopherstack-2sp); PreTokenGeneration trigger fires on token issuance (prior pass, gopherstack-8fw); PreAuthentication/PostAuthentication/UserMigration triggers fire (prior pass); CUSTOM_AUTH a real Lambda-driven state machine (prior pass). THIS PASS (gopherstack-n7gh): USER_SRP_AUTH is now real SRP-6a (see srp.go) -- previously it required AuthParameters[PASSWORD] directly (a real SRP client never sends this, so every real-client USER_SRP_AUTH call had always failed with NotAuthorizedException). Routes to InitiateAuthSRP given AuthParameters[SRP_A]."}
  AdminInitiateAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "never masks UserNotFoundException, matching AWS (admin API); PreTokenGeneration trigger fires (prior pass); PreAuthentication/PostAuthentication/UserMigration/CUSTOM_AUTH real (prior pass). THIS PASS (gopherstack-n7gh): ADMIN_USER_SRP_AUTH is now real SRP-6a via AdminInitiateAuthSRP -- previously this flow name was not even in authenticate()'s accepted-flow switch (only \"USER_SRP_AUTH\" was), so a real client's AdminInitiateAuth SRP attempt got InvalidUserPoolConfigurationException outright, a second bug on top of the plaintext-password one."}
  RespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "SOFTWARE_TOKEN_MFA real RFC6238 TOTP; SMS_MFA/EMAIL_OTP require the generated one-time code; NEW_PASSWORD_REQUIRED real; PreTokenGeneration trigger fires on token issuance; CUSTOM_CHALLENGE handled for real (prior pass). THIS PASS (gopherstack-n7gh): PASSWORD_VERIFIER now verifies a real SRP-6a zero-knowledge password-claim signature (PASSWORD_CLAIM_SECRET_BLOCK/PASSWORD_CLAIM_SIGNATURE/TIMESTAMP against server-held (A,b,B,v) session state) instead of unconditionally issuing tokens for any session token that merely existed. Also fixed: success now runs the same FORCE_CHANGE_PASSWORD/MFA gate USER_PASSWORD_AUTH runs (postCredentialCheckLocked) -- previously RespondToSRPChallenge always issued tokens directly, bypassing NEW_PASSWORD_REQUIRED/MFA entirely for any SRP login."}
  AdminRespondToAuthChallenge: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as RespondToAuthChallenge (shared backend method), including CUSTOM_CHALLENGE and PASSWORD_VERIFIER real SRP verification (this pass)"}
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
  ConfirmSignUp: {wire: ok, errors: ok, state: ok, persist: ok, note: "expiring codes, CodeMismatchException/ExpiredCodeException; PostConfirmation trigger fires fire-and-observe -- invocation errors surface but do not roll back confirmation, matching AWS; PreventUserExistenceErrors=ENABLED now masks an unknown username behind CodeMismatchException, the same error a real-but-wrong-code account produces (this pass, closes remainder of gopherstack-aib)"}
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
  ForgotPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors=ENABLED masks unknown-user UserNotFoundException as a fabricated success (prior pass, closes gopherstack-aib); CustomMessage trigger now fires (prior pass, gopherstack-8fw). THIS PASS (gopherstack-n7gh follow-up): an unknown username now also tries the UserMigration_ForgotPassword Lambda trigger (user_migration.go's tryUserMigrationForgotPassword) before falling back to PreventUserExistenceErrors masking / UserNotFoundException, matching the documented 'user migration during forgot-password flow' trigger source. Per AWS docs, no password is sent in this event (request.password is omitted entirely, not sent empty) since the user has none yet."}
  ConfirmForgotPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "PreventUserExistenceErrors=ENABLED now masks an unknown username behind CodeMismatchException, same rationale as ConfirmSignUp (this pass, closes remainder of gopherstack-aib)"}
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
  AdminGetUserAuthFactors: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-4, new SDK op. Field-diffed AdminGetUserAuthFactorsOutput against the SDK: Username/ConfiguredUserAuthFactors/PreferredMfaSetting/UserMFASettingList all present. Factors are derived from real user state, not fabricated: PASSWORD from user.PasswordHash != \"\"; SMS_OTP from UserMFASettingList containing SMS_MFA or any legacy MFAOptions[].DeliveryMedium == SMS; SOFTWARE_TOKEN from user.TOTPVerified or SOFTWARE_TOKEN_MFA in UserMFASettingList; WEB_AUTHN from a non-empty webauthnCredentials entry for the user. Shares its PASSWORD/SMS_OTP/WEB_AUTHN derivation with the existing GetUserAuthFactors via a new commonAuthFactorSetLocked helper (users.go) -- GetUserAuthFactors' own behavior/output is unchanged, only the shared plumbing was extracted. Adds SOFTWARE_TOKEN, a factor GetUserAuthFactors does not currently derive (tracked as items_still_open for that op, not fixed this pass to avoid touching a previously-graded, tested op outside this pass's scope)."}
  user_import_jobs: {status: ok, note: "op-by-op re-walk THIS PASS (gopherstack-n7gh follow-up): field-diffed userImportJobType against types.UserImportJobType and found CreateUserImportJobInput's required CloudWatchLogsRoleArn and optional PasswordHashingAlgorithm were accepted by no input field at all (silently dropped -- class a) -- fixed, now stored and echoed. Also added CreationDate/StartDate/CompletionDate (CreatedAt was already tracked internally but never echoed; StartedAt/CompletedAt added, set by StartUserImportJob/StopUserImportJob), PreSignedUrl (fabricated the same way domains.go fabricates CloudFrontDistribution/S3Bucket -- an AWS-internal value no caller can validate), and FailedUsers/ImportedUsers/SkippedUsers=0 (honest: this backend has no real CSV-processing pipeline, so zero imported/failed/skipped is literally true, not fabricated). DEFERRED, not fixed: ListUserImportJobsInput.MaxResults is a required real field this backend's listUserImportJobsInput doesn't even declare -- no pagination is implemented (matches the same gap in resource_servers, see below); ListUserImportJobs returns everything in one page regardless of MaxResults."}
  devices: {status: ok, note: "op-by-op re-walk THIS PASS: field-diffed deviceType against types.DeviceType and confirmed absence carefully -- the real DeviceType has exactly 5 fields (DeviceAttributes/DeviceCreateDate/DeviceKey/DeviceLastAuthenticatedDate/DeviceLastModifiedDate) and NO DeviceStatus field at all; device remembered status is write-only via AdminUpdateDeviceStatus/UpdateDeviceStatus's DeviceRememberedStatus and is never readable back through Get/List/AdminGet/AdminList in real Cognito. This backend's deviceType.DeviceStatus is therefore an EXTRA fabricated field not on the real wire -- flagged, NOT fixed: several existing tests (devices_test.go) assert on it, a real AWS SDK JSON client harmlessly ignores unknown response keys, and removing it would only lose test-observable state for a purely cosmetic gain. Documented as a trap below rather than silently left as-is."}
  webauthn: {status: ok, note: "op-by-op re-walk THIS PASS found and fixed two real bugs. (1) The response wire key was wrong: this backend emitted \"FriendlyName\", but the real WebAuthnCredentialDescription's JSON key (confirmed in deserializers.go) is \"FriendlyCredentialName\" -- meaning no real aws-sdk-go-v2 client could ever read this field back; a classic wrong-shape bug parity-principles.md warns about, caught only by reading the actual struct/deserializer, not the handler's own output. (2) AuthenticatorTransports, a REQUIRED field on WebAuthnCredentialDescription, was entirely absent; it is honestly derivable from the client-submitted Credential blob's response.transports (a real WebAuthn PublicKeyCredential.toJSON() field), which was already being accepted but never read (class a) -- now extracted and threaded through CompleteWebAuthnRegistration/ListWebAuthnCredentials."}
  managed_login_branding: {status: ok, note: "op-by-op re-walk THIS PASS found the largest gap in this sweep: Settings (the branding style JSON), Assets (the array of logo/background image files), and UseCognitoProvidedValues -- literally the entire payload of the 'managed login branding' feature -- were accepted by no input field at all on Create/Update and never echoed on any read (class a, not a minor omission). Fixed: stored as the raw client-supplied documents, un-transformed, the same pattern UserPool.LambdaConfig already uses for its own arbitrary-shaped config, since Settings is an AWS Document type (arbitrary JSON) this backend has no reason to model field-by-field. Also fixed CreationDate/LastModifiedDate (CreatedAt/LastModifiedAt were already tracked internally but never echoed -- class b, bounded)."}
  risk_config: {status: ok, note: "op-by-op re-walk THIS PASS: the live path (SetRiskConfigurationFull/DescribeRiskConfigurationFull, wired via securityConfigOpsB overriding securityConfigOpsA -- same domainsOpsA/B shadowing pattern as domains.go) is a real, fully typed implementation already field-diffed clean against RiskConfigurationType/AccountTakeoverRiskConfigurationType/CompromisedCredentialsRiskConfigurationType/RiskExceptionConfigurationType in a prior pass. Confirmed the securityConfigOpsA SetRiskConfiguration/DescribeRiskConfiguration handlers that hardcode nil are DEAD code (shadowed, never dispatched), not a live bug -- verified by reading handler.go's maps.Copy ordering, not assumed. DEFERRED, not fixed: RiskConfigurationType.LastModifiedDate is not tracked internally at all (no LastModifiedAt field on the risk-config storage type), so it can't be added as cheaply as the CreatedAt-echo fixes elsewhere this pass."}
  domains: {status: ok, note: "CreateUserPoolDomain/DescribeUserPoolDomain/DeleteUserPoolDomain/UpdateUserPoolDomain — field-diffed DomainDescriptionType against the SDK: DescribeUserPoolDomain was missing CustomDomainConfig entirely (prior pass) — fixed then. THIS PASS (gopherstack-n7gh follow-up): AWSAccountId/ManagedLoginVersion/S3Bucket now populated. AWSAccountId echoes the backend's own accountID (same source ARN-building already uses, e.g. arn.Build calls in user_pools.go) rather than pkgs/awsmeta, since nothing in this service's dispatch path ever calls awsmeta.Set -- reading awsmeta.Account(ctx) here would have always silently resolved to its hardcoded default, not real per-backend state. ManagedLoginVersion is a real request field on CreateUserPoolDomain/UpdateUserPoolDomainInput AND a real response field (verified in both api_op_*.go files) that was accepted by neither our create nor update input struct at all (class a) -- fixed, defaults to 1 (hosted UI classic) when unset at creation, an explicit undocumented-default assumption (AWS doesn't state the default in godoc), left unchanged on update when omitted. S3Bucket is fabricated the same way CloudFrontDistribution already was (an AWS-internal bucket name, informational-only, not independently verifiable by any client). Routing/Version (also real DomainDescriptionType fields) remain unpopulated -- multi-region domain routing and app-version reporting this backend has no model for; tracked as items_still_open, not silently dropped."}
  terms: {status: gap, note: "op-by-op re-walk THIS PASS found this family's ENTIRE wire model is fictional, not merely missing fields. Real CreateTerms/TermsType requires ClientId, Enforcement (TermsEnforcementType), TermsName, TermsSource (TermsSourceType), Links (map[string]string of language->URL), plus server-generated TermsId/CreationDate/LastModifiedDate/UserPoolId -- a real aws-sdk-go-v2 client's own request-validation middleware (validators.go) refuses to even send CreateTerms without ClientId/Enforcement/TermsName/TermsSource, so no genuine SDK client could ever successfully call this op against gopherstack today. This backend instead models one bare {UserPoolID, Text} record per pool (createTermsInput={UserPoolId}, termsType={DefaultTermsAndConditions}), a fictional simplification bearing no resemblance to the real API (which supports multiple named documents like terms-of-use/privacy-policy per app client). NOT fixed this pass: this is a full backend redesign (new storage keyed by TermsId, ClientId association, enum validation, multi-document-per-pool support), not a bounded field-level fix -- see deferred below."}
  log_delivery: {status: ok, note: "op-by-op re-walk THIS PASS found SetLogDeliveryConfiguration was a disguised stub (parity-principles.md rule 4): handleSetLogDeliveryConfiguration called Backend.SetLogDeliveryConfiguration(in.UserPoolID, nil) UNCONDITIONALLY -- the client's LogConfigurations payload (a required field) was never read at all, and the input struct didn't even declare it, so Set was a no-op regardless of what was sent, and Get always echoed back whatever Set never stored. Fixed: LogConfigurations is now accepted (stored/echoed as the raw client-supplied array, same un-transformed-map pattern as LambdaConfig/managed_login_branding's Settings, given the nested CloudWatchLogsConfigurationType/FirehoseConfigurationType/S3ConfigurationType/EventSourceName/LogLevel enum tree) and wrapped in the real LogDeliveryConfigurationType shape ({UserPoolId, LogConfigurations})."}
  identity_providers: {status: ok, note: "FULL field diff THIS PASS (not just spot-checked): identityProviderJSON/identityProviderSummaryJSON (the live 'Full'/accurate wire path, wired the same domainsOpsA/B-shadowing way as domains.go) match types.IdentityProviderType and types.ProviderDescription field-for-field -- AttributeMapping/CreationDate/IdpIdentifiers/LastModifiedDate/ProviderDetails/ProviderName/ProviderType/UserPoolId all present with correct field names and epoch-seconds timestamps. No gaps found; confirmed clean rather than assumed."}
  resource_servers: {status: ok, note: "FULL field diff THIS PASS: resourceServerAccurateType matches types.ResourceServerType exactly (Identifier/Name/Scopes/UserPoolId, no timestamp fields on the real type either). No gaps found. DEFERRED, not fixed: ListResourceServersInput.MaxResults/PaginationToken are real optional request fields and ListResourceServersOutput.NextToken is a real response field, none of which this backend implements -- ListResourceServers always returns every resource server in one page, the same unimplemented-pagination gap found in user_import_jobs above."}
  user_pool_replicas: {status: ok, note: "parity-4, new family (multi-Region replication / MRR): CreateUserPoolReplica/ListUserPoolReplicas/UpdateUserPoolReplica/DeleteUserPoolReplica. UserPoolReplicaType field-diffed against the SDK (RegionName/Role/Status/UserPoolArn); the X-Amz-Target names and CreateUserPoolReplicaOutput/DeleteUserPoolReplicaOutput/UpdateUserPoolReplicaOutput/ListUserPoolReplicasOutput field names (all singular 'UserPoolReplica' except the List op's plural 'UserPoolReplicas') were confirmed against deserializers.go, not assumed from the (looser) dev-guide prose, which shows a JSON example using a 'Replica' key that does NOT match the real wire field -- a live trap for a future auditor who trusts the docs example over the SDK. CreateUserPoolReplica validates the pool exists (ResourceNotFoundException) and rejects a replica Region equal to the primary pool's own Region (InvalidParameterException) -- both real, documented AWS behaviors. It also enforces the real documented constraint 'You can have at most one secondary replica in an additional Region per user directory' by rejecting a second CreateUserPoolReplica call for the same pool regardless of region (InvalidParameterException) -- this is NOT an invented restriction, it is quoted verbatim from the Cognito multi-Region-replication developer guide. New replicas start Status=INACTIVE per that same guide ('New secondary user pools start in the INACTIVE state'); note the guide's own JSON example elsewhere shows an initial 'PENDING_CREATE' status that is not even a member of the SDK's ReplicaStatusType enum (CREATING/ACTIVE/INACTIVE/DELETING) -- INACTIVE was chosen as the only real, both-documented-and-enum-valid option; this is a explicit, documented assumption, not a fabrication, but flagged for the next auditor to re-verify against a live pool if ever possible. DeleteUserPoolReplica returns the replica with Status transitioned to DELETING (mirroring AWS's documented async deletion) before removing it. UserPoolTags on Create are stored under the replica's own ARN via the existing resourceTags/ListTagsForResource mechanism (real state, not dropped). Persisted via a new userPoolReplicas store.Table (composite poolID:region key, byPool index), round-tripped through Snapshot/Restore, covered by TestInMemoryBackend_SnapshotRestore's full_state_round_trip case."}
  provisioned_limits: {status: ok, note: "parity-4, new family: GetProvisionedLimit/UpdateProvisionedLimit. Confirmed ACCOUNT-LEVEL (not per-user-pool) by fetching the live Cognito quotas developer guide this pass: 'Provisioned limits are account-level resources. They apply to the aggregate rate of all requests from all user pools in one AWS Region in your AWS account' -- this backend models exactly one account+Region so GetProvisionedLimit/UpdateProvisionedLimit take no UserPoolId and do no pool-existence check, which is correct, not an oversight. LimitDefinitionType/LimitType field-diffed against the SDK (LimitClass/Attributes, FreeLimitValue/ProvisionedLimitValue/LimitDefinition). The 18 API_CATEGORY default (free) RPS values in provisioned_limits.go's category table (UserAuthentication=120, UserCreation=50, UserFederation=25, UserAccountRecovery=30, UserRead=120, UserUpdate=25, UserToken=120, UserResourceRead=50, UserResourceUpdate=25, UserList=30, UserPoolRead=15, UserPoolUpdate=15, UserPoolResourceRead=20, UserPoolResourceUpdate=15, UserPoolClientRead=15, UserPoolClientUpdate=15, ClientAuthentication=150, LimitManagement=1) and their Adjustable:Yes/No flags are the real, live-fetched values from 'Amazon Cognito user pools API operation categories and request rate quotas' -- not invented. UpdateProvisionedLimit rejects non-adjustable categories (InvalidParameterException, matching 'Only adjustable quota categories support provisioning') and rejects a negative RequestedLimitValue. One explicit, documented assumption: AWS's real two-tier model has a Service-Quotas-granted 'account-level max limit' above the provisioned limit, but that ceiling is account-specific (granted by AWS Support) with no universal published number -- this backend models an adjustable category's account-level max as 10x its documented default RPS (accountMaxMultiplier in provisioned_limits.go) and enforces it with ServiceQuotaExceededException, the real exception name AWS uses for this condition. Persisted via a new flat provisionedLimits map[string]int32 (Category -> current value), round-tripped through Snapshot/Restore."}
gaps:
  - "CLOSED 2026-08-07 (gopherstack-n7gh, was gopherstack-p8i): USER_SRP_AUTH/ADMIN_USER_SRP_AUTH now implement real SRP-6a (services/cognitoidp/srp.go). The algorithm (3072-bit RFC 5054 N, g=2, k=H(PAD(N)|PAD(g)), x=H(PAD(salt)|H(poolName|username|\":\"|password)), server B=(k*v+g^b) mod N, server S=(A*v^u)^b mod N, HKDF-SHA256 with the \"Caldera Derived Key\" info string truncated to 16 bytes, HMAC-SHA256 password-claim signature over poolName|username|secretBlock|timestamp) was verified field-by-field against amazon-cognito-identity-js's AuthenticationHelper.js/CognitoUser.js/DateHelper.js -- the reference client implementation AWS itself publishes -- not reconstructed from memory. Locked in by an INDEPENDENTLY-written second implementation of the same client-side math in test code (srp_client_test.go, package cognitoidp_test, cannot see srp.go's unexported symbols) that performs a real handshake against the server and must derive the identical signature; see srp_test.go for the round-trip, FORCE_CHANGE_PASSWORD-after-SRP, tampered-signature-rejected, plaintext-InitiateAuth-rejected, and persistence-survival regression tests. Every password-setting call site (SignUp, SignUpWithValidation, ConfirmForgotPassword, ChangePassword, AdminSetUserPassword(Full), AdminCreateUser(WithPolicy/Full), RespondToNewPasswordRequired, UserMigration) now derives and stores a matching SRPSalt/SRPVerifier via the new hashAndSRP helper, and both fields survive Snapshot/Restore (persistence.go)."
  - "CLOSED 2026-08-08 (gopherstack-n7gh follow-up): UserMigration_ForgotPassword trigger source and domain AWSAccountId/ManagedLoginVersion/S3Bucket, the two items explicitly named but not reached in the SRP-6a pass -- see families.ForgotPassword and families.domains above for detail."
  - "CLOSED 2026-08-08 (gopherstack-n7gh follow-up): op-by-op re-walk of user_import_jobs/devices/webauthn/managed_login_branding/risk_config/terms/log_delivery plus a full field diff of identity_providers/resource_servers, the remaining named scope item. Found and fixed 4 real bugs beyond the headline items: webauthn's wrong wire key (FriendlyName vs FriendlyCredentialName) and missing required AuthenticatorTransports; managed_login_branding's Settings/Assets/UseCognitoProvidedValues completely discarded; SetLogDeliveryConfiguration's disguised-nil-stub; CreateUserImportJob's dropped CloudWatchLogsRoleArn/PasswordHashingAlgorithm. See families above for each. terms/ was found to be built on a fictional wire model entirely and needs a full redesign -- explicitly NOT fixed this pass, see deferred below."
deferred:
  - "terms/ (CreateTerms/DescribeTerms/UpdateTerms/DeleteTerms/ListTerms) needs a full backend redesign, not a bounded field fix. Real Cognito terms documents are keyed by TermsId, associated to an app client (ClientId), support multiple named documents per pool (TermsName like terms-of-use/privacy-policy), carry an Enforcement/TermsSource enum pair (both currently fixed to a single value: NONE/LINK) and a Links map[string]string of per-language URLs, plus CreationDate/LastModifiedDate. This backend instead stores one bare {UserPoolID, Text} record per pool with no ClientId/TermsId/TermsName/Enforcement/TermsSource/Links/timestamps at all -- a real aws-sdk-go-v2 client's own request-validation middleware refuses to even send CreateTerms without ClientId/Enforcement/TermsName/TermsSource, so this op cannot succeed against gopherstack from a genuine SDK client today. Needs new storage (keyed by TermsId, indexed by pool+client), new required-field validation, and a full handler/model rewrite -- deliberately not attempted this pass per the explicit instruction that a partial/rushed fix is worse than an honestly-documented gap."
  - "devices' deviceType.DeviceStatus is an extra field NOT present on the real DeviceType wire shape (verified by reading the complete SDK struct: only DeviceAttributes/DeviceCreateDate/DeviceKey/DeviceLastAuthenticatedDate/DeviceLastModifiedDate exist; device remembered status is write-only in real Cognito, never returned by any Get/List device op). Not removed: several existing tests assert on it and no real client breaks from an extra unknown JSON key, so removing it purely for spec purity would cost test-observable state for no functional gain. Flagged for whoever next touches devices.go so it isn't mistaken for a verified-real field."
  - "risk_config: RiskConfigurationType.LastModifiedDate is a real response field this backend doesn't track at all internally (no LastModifiedAt on the risk-config storage type, unlike domains/managed_login_branding where CreatedAt/LastModifiedAt already existed and just needed echoing) -- would need a new tracked field plus updates at every SetRiskConfiguration call site, not a one-line echo fix."
  - "Pagination is unimplemented on at least two List ops with real MaxResults/NextToken(or PaginationToken) contracts: ListUserImportJobs (MaxResults is REQUIRED on the real input, silently accepted by no field here) and ListResourceServers (MaxResults/PaginationToken optional, NextToken in output). Both always return every item in one page. ListUsers/ListWebAuthnCredentials/ListDevices already do this correctly (pkgs/page or hand-rolled token) -- the same pattern should be applied here in a future pass."
  - "domains: Routing and Version, two more real DomainDescriptionType fields (multi-region failover routing config; app version string), remain unpopulated -- this backend has no multi-region-domain-routing model and no meaningful 'app version' to report. Left absent rather than fabricated, per the same standard as terms/ above, just far smaller in scope."
leaks: {status: clean, note: "janitor.go sweeps expired refresh tokens/mfa sessions/confirm codes/attr verification codes on a bounded interval (WithJanitor); ctx cancellation observed via StartWorker. This pass added custom_auth.go (CUSTOM_AUTH state machine) and user_migration.go (UserMigration trigger), both of which reuse the existing mfaSessions map/EvictExpiredMFASessions sweep for their session state -- no new maps, goroutines, or tickers introduced. All new backend methods (tryUserMigration, applyPostMigrationFinalStatus, startCustomAuth, customAuthRound, defineAuthChallenge, createAuthChallenge, verifyCustomAuthChallenge, preAuthenticationCheck, postAuthenticationNotify) are plain functions that assume the caller already holds b.mu (documented per-function), never call b.mu.Lock/RLock themselves -- verified no double-lock/deadlock paths and confirmed via `go test -race` (full suite, 233s, clean). De-stub hygiene: the ~15-op handler.go/handler_auth.go/handler_user_pools.go/handler_user_pool_clients.go/handler_users.go dead-code shadowing flagged as deferred in the prior sweep is now fully deleted (dead handlers + their now-orphaned model types removed across 4 files + models_auth.go/models_user_pools.go/models_user_pool_clients.go/models_users.go), closing that item; golangci-lint (0 issues) confirms nothing is newly unused."}
---

## Notes

### What this pass fixed (2026-08-08, gopherstack-n7gh follow-up)

Completed the remainder of `gopherstack-n7gh`'s stated scope (SRP-6a itself was
already done earlier the same day, commit `041c16c75`). Full detail and SDK
citations are in the `ops`/`families` entries above (`ForgotPassword`, `domains`,
`user_import_jobs`, `devices`, `webauthn`, `managed_login_branding`, `risk_config`,
`terms`, `log_delivery`, `identity_providers`, `resource_servers`); summary:

1. **`UserMigration_ForgotPassword`**: `ForgotPassword` on an unknown username now
   tries the `UserMigration` Lambda trigger (correct trigger source, no plaintext
   password in the event per AWS docs) before falling back to
   `PreventUserExistenceErrors` masking / `UserNotFoundException`.
2. **Domain `AWSAccountId`/`ManagedLoginVersion`/`S3Bucket`**: all three added to
   `DescribeUserPoolDomain`; `ManagedLoginVersion` is also now a real accepted
   request field on `CreateUserPoolDomain`/`UpdateUserPoolDomain` (previously
   silently dropped).
3. **webauthn had a genuine wrong-wire-shape bug**: the response used
   `"FriendlyName"` where real Cognito uses `"FriendlyCredentialName"` (verified
   against `deserializers.go`, not assumed) -- no real SDK client could ever have
   read this field. Fixed, plus added the required `AuthenticatorTransports` field
   (extracted from the client's already-accepted-but-unread `Credential.response.
   transports`).
4. **`managed_login_branding` discarded its entire payload**: `Settings`, `Assets`,
   and `UseCognitoProvidedValues` -- not minor fields, the actual branding
   configuration -- were accepted by no input field and never echoed on
   Create/Describe/Update. Fixed as raw pass-through storage, the same pattern
   `UserPool.LambdaConfig` already uses for its own arbitrary-shaped config.
5. **`SetLogDeliveryConfiguration` was a disguised stub**: it called
   `SetLogDeliveryConfiguration(poolID, nil)` unconditionally, ignoring the
   client's `LogConfigurations` regardless of what was sent -- a real bug of
   exactly the kind `parity-principles.md` warns about ("a 'real-looking' op may
   be a disguised stub"). Fixed.
6. **`CreateUserImportJob` dropped its required `CloudWatchLogsRoleArn`** (and
   optional `PasswordHashingAlgorithm`) entirely; also added
   `CreationDate`/`StartDate`/`CompletionDate`/`PreSignedUrl`/`FailedUsers`/
   `ImportedUsers`/`SkippedUsers` to the response (the first three were already
   tracked internally and simply never echoed; the counts are honestly `0` since
   this backend has no real CSV-processing pipeline).
7. **`terms/` was found to be built on an entirely fictional wire model** -- not a
   missing-field gap but a different data model altogether (see `deferred` above
   for detail). Explicitly NOT fixed: a full redesign, not a bounded change, and
   the same principle that stopped a rushed SRP-6a attempt in an earlier pass
   applies here -- a half-correct terms/ implementation would be worse than the
   current honestly-limited one.

All new/changed wire fields were verified against
`aws-sdk-go-v2/service/cognitoidentityprovider@1.67.4` in the module cache
(types/types.go, the per-op `api_op_*.go` files, and `deserializers.go` for exact
JSON key names), not against this package's own prior output.

### What this pass fixed (2026-07-25, parity-4)

The Go SDK module was bumped (`aws-sdk-go-v2/service/cognitoidentityprovider`
1.59.1 -> 1.67.0), which shipped 7 new operations `TestSDKCompleteness` did not
yet know about: `CreateUserPoolReplica`, `ListUserPoolReplicas`,
`UpdateUserPoolReplica`, `DeleteUserPoolReplica` (multi-Region replication),
`AdminGetUserAuthFactors`, `GetProvisionedLimit`, `UpdateProvisionedLimit`. All
7 are implemented for real (routing, backend state, request parsing, response
wire shape, error codes, Snapshot/Restore) -- none were added to
`notImplemented`. See `families.user_pool_replicas`, `families.provisioned_limits`,
and `ops.AdminGetUserAuthFactors` above for the full field-diff and derivation
detail; summary:

1. **User pool replicas** (`user_pool_replicas.go`, `models_user_pool_replicas.go`,
   `handler_user_pool_replicas.go`): a new `userPoolReplicas` `store.Table`
   (composite `poolID:region` key, `byPool` index) backs multi-Region
   replication. `CreateUserPoolReplica` enforces two real, documented AWS
   constraints: the replica Region must differ from the primary pool's own
   Region, and a user pool may have at most one secondary replica ("at most
   one secondary replica in an additional Region per user directory"). New
   replicas start `INACTIVE`, matching the developer guide's prose (the same
   guide's JSON example elsewhere shows a `PENDING_CREATE` status that isn't
   even a valid `ReplicaStatusType` enum member -- a documentation
   inconsistency resolved in favor of the real SDK enum).

2. **`AdminGetUserAuthFactors`** (`users.go`, `models_users.go`,
   `handler_users.go`): derives `ConfiguredUserAuthFactors` entirely from
   existing user/MFA state -- `PasswordHash`, `UserMFASettingList`,
   `MFAOptions[].DeliveryMedium`, `TOTPVerified`, and the `webauthnCredentials`
   map -- never a fixed/fabricated list. Shares its PASSWORD/SMS_OTP/WEB_AUTHN
   logic with the pre-existing `GetUserAuthFactors` via an extracted
   `commonAuthFactorSetLocked` helper; `GetUserAuthFactors`'s own behavior is
   unchanged.

3. **Provisioned limits** (`provisioned_limits.go`, `models_provisioned_limits.go`,
   `handler_provisioned_limits.go`): confirmed account-level (not per-user-pool)
   against the live Cognito quotas guide, so these two ops take no
   `UserPoolId`. The 18 API_CATEGORY default RPS values and their
   adjustable/not-adjustable flags are the real, live-fetched AWS defaults, not
   invented. The one place this pass had to invent a number
   (`accountMaxMultiplier`, since AWS's real per-account Service-Quotas max is
   granted individually and unpublished) is called out explicitly in both the
   code comment and `families.provisioned_limits` above.

### What the 2026-07-23 pass fixed

1. **CUSTOM_AUTH did not exist as a flow at all (highest-severity finding this pass).**
   `InitiateAuth`/`AdminInitiateAuth` with `AuthFlow: "CUSTOM_AUTH"` unconditionally
   returned `InvalidUserPoolConfigurationException: unsupported auth flow "CUSTOM_AUTH"`
   — not a disguised stub, just entirely unrouted. Implemented the real Lambda-driven
   state machine (`custom_auth.go`): `DefineAuthChallenge` decides issue-tokens /
   fail / present-a-challenge each round from the accumulated session history;
   `CreateAuthChallenge` builds public (client-visible) and private (server-only)
   challenge parameters; `VerifyAuthChallengeResponse` judges the answer. A wrong
   answer does **not** auto-fail the attempt — exactly like AWS, the Lambda alone
   decides via the next `DefineAuthChallenge` call, so "fail after N wrong answers"
   policies work. `RespondToAuthChallenge`/`AdminRespondToAuthChallenge` gained a
   `CUSTOM_CHALLENGE` case, and `ChallengeParameters` is now populated end-to-end
   (was always `{}` on the wire; `AuthResult`/`authOutput` gained the field).
   Verified against `aws-lambda-go/events` (`CognitoEventUserPoolsDefineAuthChallenge/
   CreateAuthChallenge/VerifyAuthChallenge`) and the real SDK's
   `RespondToAuthChallengeOutput.ChallengeParameters` field. 7 new tests in
   `custom_auth_test.go` cover single-round issue/fail, multi-round retry, the
   "DefineAuthChallenge not configured" error, ExplicitAuthFlows restriction, and the
   Admin path.

2. **UserMigration and PreAuthentication/PostAuthentication triggers were stored but
   never invoked**, closing the remainder of `gopherstack-8fw`. `UserMigration` now
   fires (`user_migration.go`) when `USER_PASSWORD_AUTH`/`ADMIN_USER_PASSWORD_AUTH`/
   `ADMIN_NO_SRP_AUTH` names an unknown username and the pool has the trigger
   configured: a Lambda response with `userAttributes` creates and authenticates a new
   user in one round trip (matching AWS: "migrate a user from an external system on
   first sign-in"); a response with no attributes, or no trigger configured, falls
   back to the pre-existing "unknown user" handling (including
   `PreventUserExistenceErrors` masking) exactly as before. `FinalUserStatus:
   "RESET_REQUIRED"` is honored per AWS's documented semantics: *this* migrating
   sign-in still succeeds with tokens, but the account is left in
   `FORCE_CHANGE_PASSWORD` so the *next* sign-in requires a password reset (see
   "Traps" below for the residual uncertainty on this specific timing).
   `PreAuthentication`/`PostAuthentication` now fire around `authenticate()`/
   `issueTokensLocked()` respectively; a Lambda that throws fails the sign-in attempt
   (`UserLambdaValidationException`) before (PreAuthentication) or after
   (PostAuthentication) credentials are checked. `PostAuthentication` does not
   re-fire on `REFRESH_TOKEN_AUTH`, matching AWS. UserMigration is scoped to
   `InitiateAuth`/`AdminInitiateAuth` only this pass — `ForgotPassword`'s
   `UserMigration_ForgotPassword` trigger source is a documented `items_still_open`
   item, not implemented.

3. **PreventUserExistenceErrors=ENABLED did not mask `ConfirmSignUp`/
   `ConfirmForgotPassword`**, closing the remainder of `gopherstack-aib` (InitiateAuth/
   ForgotPassword/ResendConfirmationCode were already closed in prior passes). An
   unknown username on either op now returns `CodeMismatchException` — the same error
   a real account with a wrong code produces — instead of `UserNotFoundException`,
   closing the last username-enumeration vector in the auth surface. New tests in
   `prevent_user_existence_test.go`; one pre-existing test's assertion
   (`Test_ForgotPassword_PreventUserExistenceErrors/ENABLED_masks_as_a_fabricated_success`)
   was updated in place since it was asserting the now-fixed gap's old (wrong)
   behavior.

4. **`DescribeUserPoolDomain` never returned `CustomDomainConfig`.** Field-diffed
   `DomainDescriptionType` against the real SDK this pass (see `families.domains`) and
   found a custom domain's ACM `CertificateArn` was tracked internally
   (`UserPoolDomain.CertificateArn`) but never echoed back on Describe — a real fidelity
   gap for anything that reads it back to detect drift (e.g. the Terraform AWS
   provider). Fixed; `AWSAccountId`/`ManagedLoginVersion`/`S3Bucket`/`Version` remain
   unpopulated (not tracked by this backend's domain model) and are recorded as
   `items_still_open`, not silently dropped.

5. **De-stub hygiene: deleted the ~15-op handler.go dead-code shadowing** flagged as a
   deferred cleanup candidate in the prior sweep. `handler_auth.go`,
   `handler_user_pools.go`, `handler_user_pool_clients.go`, and `handler_users.go` each
   registered a non-accurate handler for an op *and* a `*Accurate`/`*WithOpts`/`*Full`
   twin under the same op name later in `dispatchTable()`'s `maps.Copy` chain, so the
   accurate version always won and the first was unreachable dead code (confirmed via
   `grep` for direct test references — none existed). Deleted the dead handler funcs,
   their now-orphaned model-only input/output types (in `models_auth.go`,
   `models_user_pools.go`, `models_user_pool_clients.go`, `models_users.go`), the
   `authOpsB()`/shadowed-entry helper functions, and the corresponding
   `dispatchTable()` wiring. `golangci-lint`'s `unused` check (0 issues) confirms
   nothing is newly orphaned.

### What prior passes fixed

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

- **`domains.go`, `security_config.go`, and `identity_providers.go` still use the
  `opsA()`/`opsB()` legacy-plus-accurate split** the 2026-07-23 pass deleted from
  `handler_auth.go`/`handler_user_pools.go`/`handler_user_pool_clients.go`/
  `handler_users.go` (see the dispatch-shadowing trap below) -- this is a DIFFERENT,
  still-live pattern in these 3 files: `opsA()` registers a legacy/incomplete handler
  under an op name, `opsB()` registers the real "Full"/"Accurate" handler under the
  same name via a matching `op<Name>` constant, and `dispatchTable()`'s `maps.Copy(table,
  h.XOpsA()); ...; maps.Copy(table, h.XOpsB())` ordering makes `opsB()` win every time.
  This is intentional (not dead code to delete) for `DeleteUserPoolDomain`/
  `DescribeUserPoolDomain` (which stay on `opsA()`, no `opsB()` override exists for
  them) but means `CreateUserPoolDomain`/`UpdateUserPoolDomain`,
  `SetRiskConfiguration`/`DescribeRiskConfiguration`, and the identity-provider
  Create/Update ops are ALL served by handlers that don't textually appear next to
  their `"OpName": service.WrapOp(...)` map entry in `opsA()` -- reading only `opsA()`
  gives a false picture of live behavior for those specific ops. Verified this
  distinction by reading `handler.go`'s `maps.Copy` call order directly for each
  family this pass, not assumed from file layout.
- **The Cognito multi-Region-replication *developer guide*'s JSON examples do not match
  the real wire shape.** Its `CreateUserPoolReplica`/`UpdateUserPoolReplica` examples show
  the response wrapped under a `"Replica"` key and an initial `"Status": "PENDING_CREATE"`.
  Neither is real: `deserializers.go` confirms the actual field is singular
  `"UserPoolReplica"` (plural `"UserPoolReplicas"` only on `ListUserPoolReplicas`), and
  `PENDING_CREATE` is not a member of `types.ReplicaStatusType` at all (the real enum is
  `CREATING`/`ACTIVE`/`INACTIVE`/`DELETING`). This implementation trusts the generated
  SDK code over the prose/examples wherever they disagree — do the same if you revisit this
  family, and don't "fix" the wire shape back to match the docs.
- **USER_SRP_AUTH/ADMIN_USER_SRP_AUTH now implement real SRP-6a** (2026-08-07,
  gopherstack-n7gh) — see `srp.go` and the closed gap above for the algorithm and
  verification method. `InitiateAuth`/`AdminInitiateAuth` route these two flow names to
  `InitiateAuthSRP`/`AdminInitiateAuthSRP` (given `AuthParameters["SRP_A"]`) instead of
  `authenticate()`, which now explicitly rejects them (`ErrInvalidUserPoolConfig`) if
  ever called directly with a plaintext password — a caller bug, not a valid path, since
  a real SRP client never sends one.
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
- **The dead-code `dispatchTable()` shadowing (dating back to an old `accuracy_handler.go`
  split) is now fully deleted (2026-07-23 pass).** Every op in `dispatchTable()` has
  exactly one live registration now; there is no more "read `handler_auth.go` and get a
  false read on behavior because a same-named `*Accurate` twin actually wins" trap. If a
  future op needs both a legacy and an accurate variant again, register only the live one
  under the bare op-name key and delete the other immediately — do not let both linger.
- **CUSTOM_AUTH's wire `ChallengeName` is always the literal `"CUSTOM_CHALLENGE"`
  string**, regardless of what name `DefineAuthChallenge`'s Lambda response used for its
  own bookkeeping (`mfaSessionEntry.CustomAuthChallengeName`, which only ever appears in
  the `session` history passed *back* to `DefineAuthChallenge`/`CreateAuthChallenge`, never
  on the wire to the client). Do not conflate the two when reading `custom_auth.go` —
  `challengeCustomChallenge` (`"CUSTOM_CHALLENGE"`) is the fixed wire constant;
  `CustomAuthChallengeName` is Lambda-internal bookkeeping only.
- **UserMigration's `FinalUserStatus: "RESET_REQUIRED"` timing is a good-faith reading of
  AWS's documented wording** ("the user must change their password *during the next
  sign-in attempt*"), not verified against a live Cognito pool: this implementation lets
  the *migrating* attempt itself succeed with tokens (the plaintext password was already
  validated by the Lambda for this one attempt) and only gates *subsequent* attempts
  behind `FORCE_CHANGE_PASSWORD`/`NEW_PASSWORD_REQUIRED`. If a real Cognito trace ever
  contradicts this ordering, `applyPostMigrationFinalStatus` in `user_migration.go` is the
  single place to fix it — it deliberately runs *after* `authenticate()` has already used
  the freshly-migrated user's `CONFIRMED` status for this attempt.
- **UserMigration only fires for `InitiateAuth`/`AdminInitiateAuth`, not `ForgotPassword`.**
  AWS also defines a `UserMigration_ForgotPassword` trigger source for migrating a user who
  tries to reset a password they never had in Cognito; this backend does not implement that
  path (`ForgotPassword` on an unknown username still just returns/masks
  `UserNotFoundException` as before). Tracked as `items_still_open`.
- **Ordering between UserMigration and PreAuthentication for a migrating sign-in is an
  implementation choice, not a verified AWS behavior.** `InitiateAuth`/`AdminInitiateAuth`
  run `tryUserMigration` first (to obtain a `*User` to authenticate at all), then call
  `authenticate()`, which fires `PreAuthentication` unconditionally -- so PreAuthentication
  does fire for a freshly-migrated user, just after migration rather than before. Real
  Cognito's exact ordering between these two triggers on a migrating request was not
  verified against a live pool.
