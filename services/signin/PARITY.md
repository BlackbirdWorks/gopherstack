---
service: signin
sdk_module: aws-sdk-go-v2/service/signin@v1.10.3
last_audit_commit: 7b35a020d
last_audit_date: 2026-10-24
overall: B            # all 11 SDK operations are real, state-backed and driven through the SDK client behind the production router; B until the open items close.
ops:
  CreateOAuth2Token: {wire: ok, errors: ok, state: ok, persist: ok, note: "bare httpPayload body both ways (camelCase); also accepts snake_case JSON and form bodies. authorization_code: single-use code, 10 min, client/redirect match and PKCE S256 verified, replay revokes the session; refresh_token: stable refresh token, credentials reissued only after their 15 min lifetime. Issued credentials resolve to the signed-in principal through the principal resolver chain"}
  CreateOAuth2TokenWithIAM: {wire: ok, errors: ok, state: ok, persist: ok, note: "HMAC-signed JWT bound to the caller identity (awsmeta principal, else account root) and the requested resource; 3600 s; grant_type must be client_credentials"}
  IntrospectOAuth2TokenWithIAM: {wire: ok, errors: ok, state: ok, persist: ok, note: "active only while unexpired (backend clock), unrevoked and owned by the caller's account; otherwise exactly {active:false}; refresh tokens report the refresh lifetime"}
  RevokeOAuth2TokenWithIAM: {wire: ok, errors: ok, state: ok, persist: ok, note: "refresh tokens only (ASOR prefix, else ValidationException); idempotent, foreign or unknown tokens succeed without effect; revoked sessions fail refresh and stop resolving credentials"}
  PutConsoleAuthorizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "targetId defaults to the caller account; account, organization (o-) and root (r-) targets; another account's ID is AccessDeniedException"}
  GetConsoleAuthorizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException until configured"}
  DeleteConsoleAuthorizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns consoleAuthorizationEnabled=false; ResourceNotFoundException when absent"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "consolidated per-account policy built from the stored statements; ResourceNotFoundException when none exist"}
  PutResourcePermissionStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "patterns from the API reference (vpc, vpce, region, principal ARN, IP/CIDR); requestedRegion required with sourceVpc or a VPC endpoint; at least one condition; clientToken replay returns the same 64-char statementId, a token reused with other parameters or the same parameters under another token within 24 h is ConflictException"}
  DeleteResourcePermissionStatement: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResourceNotFoundException for an unknown statement; malformed ID is ValidationException"}
  ListResourcePermissionStatements: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page tokens, MaxResults, oldest first; empty list when no statements exist"}
gaps: []
items_still_open:
  - "Policy evaluation: permission statements are stored and rendered by GetResourcePolicy but never evaluated; the emulator has no console sign-in path they could gate."
  - "Statement rendering: Effect, Principal, Action and the condition operators (StringNotEquals, NotIpAddress, ArnNotLike, StringEquals) of the rendered policy are inferred; the SDK documents only the shape."
  - "ConsoleAuthorizationConfiguration scope values (ACCOUNT, ORGANIZATION, ROOT) are inferred from the targetId forms; organization and root targets are not validated against the Organizations emulator."
  - "ServiceQuotaExceededException, TooManyRequestsError and InternalServerException are never returned: no statement quota or rate limit is published."
  - "DPoP key binding of refresh tokens (cnf.jkt) is not modelled; the CreateOAuth2Token SDK shape carries no proof header."
  - "Lifetimes (code 10 min, credentials 15 min, refresh 12 h, JWT 1 h) and the error HTTP statuses (401 for expired tokens, 403 for other access denials) follow the SDK docs where stated and are otherwise inferred."
  - "SDK endpoint rules ignore BaseEndpoint / AWS_ENDPOINT_URL for control-plane and OAuth operations; clients must set EndpointResolverV2 or resolve signin.<region>.api.aws to the emulator."
structural_gaps:
  - "No browser authorize flow or console session: /_gopherstack/signin/authorize is an emulator-only endpoint that mints a code for the calling identity in place of the interactive sign-in page."
deferred: []
leaks: {status: clean, note: "no goroutines; expired codes, sessions and tokens are swept on token issuance"}
---

## Notes

Protocol: REST-JSON (awsRestjson1), signing name `signin`. CreateOAuth2Token is the only
operation whose request and response bodies are the bare httpPayload document, and the SDK
sends it unsigned, so routing also accepts the `api/signin` user agent and the sign-in hosts.
Errors carry `X-Amzn-Errortype` plus the OAuth2ErrorCode in the `error` member.
