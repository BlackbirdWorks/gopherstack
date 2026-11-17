---
service: bedrockagentcore
sdk_module: aws-sdk-go-v2/service/bedrockagentcore@v1.50.3
last_audit_commit: 71065ec09
last_audit_date: 2026-10-22
overall: B            # 32 of 67 data-plane operations are served with real behavior; the rest (A/B tests, payments, evaluation runs, recommendations, harness invoke, memory extraction, browser automation) are not served.
ops:
  BatchCreateMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  BatchDeleteMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  BatchUpdateMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  CreateEvent: {wire: ok, errors: ok, state: ok, persist: ok, note: "payload union, metadata (15 max), branches checked against the root event, clientToken dedupe; no extraction runs"}
  DeleteEvent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMemoryRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteResourceTokenAuth: {wire: ok, errors: ok, state: ok, persist: ok, note: "binds userId or a verified userToken to the USER_FEDERATION session; a different user is rejected"}
  GetAgentCard: {wire: ok, errors: ok, state: ok, persist: ok, note: "A2A runtimes only; GET /.well-known/agent-card.json from the session container"}
  GetBrowserSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored state; idle sessions read as TERMINATED"}
  GetCodeInterpreterSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "idle sessions past sessionTimeoutSeconds read as TERMINATED"}
  GetEvent: {wire: ok, errors: ok, state: ok, persist: ok, note: "events past the memory eventExpiryDuration read as not found"}
  GetMemoryRecord: {wire: ok, errors: ok, state: ok, persist: ok, note: "the namespace query narrows the match"}
  GetResourceApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "reads the key from the Secrets Manager secret the control plane stored"}
  GetResourceOauth2Token: {wire: ok, errors: ok, state: ok, persist: ok, note: "M2M flow performs a real client_credentials request against the provider's token endpoint; USER_FEDERATION returns authorizationUrl, sessionUri and IN_PROGRESS until the user authorizes at the provider and CompleteResourceTokenAuth binds the user, then accessToken (refreshed with the stored refresh token); ON_BEHALF_OF_TOKEN_EXCHANGE follows the provider's onBehalfOfTokenExchangeConfig (TOKEN_EXCHANGE with M2M or no actor token, JWT_AUTHORIZATION_GRANT; the subject token is the user JWT of GetWorkloadAccessTokenForJWT, held in memory only)"}
  GetWorkloadAccessToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque token, 1 hour, workload identity must exist"}
  GetWorkloadAccessTokenForJWT: {wire: ok, errors: ok, state: ok, persist: ok, note: "verifies the RS signature, exp and sub against the JWKS of the token issuer (control plane, loopback-only fetch unless external targets are enabled, cached)"}
  GetWorkloadAccessTokenForUserId: {wire: ok, errors: ok, state: ok, persist: ok}
  InvokeAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "runs the runtime's containerConfiguration image; POST /invocations on 8080 (HTTP), /mcp on 8000 (MCP), / on 9000 (A2A); streams the response body unchanged (text/event-stream passthrough); the session id header routes to a warm container"}
  InvokeCodeInterpreter: {wire: ok, errors: ok, state: ok, persist: ok, note: "executeCode (python), executeCommand, readFiles, writeFiles, listFiles, removeFiles; result is one event-stream result event; startCommandExecution, getTask and stopTask are rejected"}
  InvokeAgentRuntimeCommand: {wire: ok, errors: ok, state: ok, persist: ok, note: "runs sh -c in the session's runtime container; contentStart, stdout and stderr contentDelta, contentStop with exitCode and COMPLETED or TIMED_OUT; timeout 1-3600 seconds, default 300; container and direct code deployment runtimes"}
  ListActors: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored events"}
  ListBrowserSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "status filter, pagination"}
  ListCodeInterpreterSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "status filter, pagination"}
  ListEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "branch filter with includeParentBranches, metadata EQUALS_TO/EXISTS/NOT_EXISTS, includePayloads, pagination"}
  ListMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "namespace or namespacePath prefix, strategy and metadata filters"}
  ListSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "derived from stored events; eventFilter start/end window"}
  RetrieveMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "lexical Okapi BM25 over record text; no embeddings; zero-overlap records are not returned; score is the BM25 value"}
  StartBrowserSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "stores session state only; no browser runs and no streams are returned"}
  StartCodeInterpreterSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "sandbox workspace is created on first use; the container starts on the first command; clientToken dedupe"}
  StopBrowserSession: {wire: ok, errors: ok, state: ok, persist: ok}
  StopCodeInterpreterSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "removes the sandbox container and workspace"}
  StopRuntimeSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "stops and removes the session container; unknown sessions are ResourceNotFoundException"}
families:
  Runtime: {status: ok, note: "Container-backed InvokeAgentRuntime over the AgentCore service contract with warm per-session containers, idle and max-lifetime reaping on an injected clock, StopRuntimeSession and A2A agent cards."}
  Memory: {status: ok, note: "Events, sessions, actors and long-term records with namespace, strategy and metadata filtering and lexical BM25 retrieval. Records are written explicitly; nothing is extracted from events."}
  CodeInterpreter: {status: ok, note: "Sessions with a host-backed workspace and a lazily started python container for code and commands."}
  Browser: {status: ok, note: "Session state only; there is no browser."}
  Identity: {status: ok, note: "Workload access tokens (user tokens verified against the issuer JWKS), API keys read from Secrets Manager, OAuth2 client-credentials tokens fetched from the provider's token endpoint and the USER_FEDERATION three-legged flow."}
  Wire: {status: ok, note: "Routes, members, headers, epoch and date-time formats and error codes follow the pinned SDK serializers and deserializers."}
items_still_open:
  - Unserved operations: AB tests, batch evaluation, payments, recommendations, InvokeHarness, IngestData, memory extraction jobs, SaveBrowserSessionProfile, InvokeBrowser and UpdateBrowserStream have no routes
  - Retrieval: RetrieveMemoryRecords ranks with lexical BM25 only, so records sharing no term with the query are not returned
  - "Runtimes: runtimes without a customJWTAuthorizer accept any caller and no IAM policy is evaluated"
  - Code interpreter: executeCode runs a fresh python process per call, so variables do not persist between calls; javascript, typescript and the async task tools are rejected
  - "Code interpreter: network mode, certificates and filesystem configurations are stored but not applied to the sandbox; a no-network mode would cut the sandbox off from the emulator endpoint (sandbox.go:119)"
  - Browser: no automation or live-view streams, no profiles, and no browser process behind a session
  - Identity: vendor providers without a discovery document have no reachable authorization or token endpoint
structural_gaps:
  - Semantic memory search: real retrieval embeds text with a model; the emulator has none, so ranking is lexical
  - Browser automation: a headless browser container with a CDP bridge is out of scope
deferred: []
leaks: {status: clean, note: "Runtime sessions and sandboxes are removed on stop, reap, Reset and Close; testleak verifies the package."}
---

## Notes

REST-JSON 1 (`awsRestjson1_`). The SDK user agent marker is `api/bedrockagentcore#` and the signing name `bedrock-agentcore`; the control plane uses `api/bedrockagentcorecontrol` and disjoint paths, so the two never claim each other's traffic.

The data plane reads the control plane through the `Control` seam (`control.go`); `bedrockagentcorecontrol.InMemoryBackend.DataPlane()` implements it and `internal/app` wires the two together.

`InvokeAgentRuntime` follows the AgentCore runtime contract: the container listens on 8080 and answers `GET /ping` and `POST /invocations` (MCP runtimes serve `/mcp` on 8000, A2A runtimes `/` on 9000). The request body and the `Content-Type`, `Accept`, bearer `Authorization`, trace and `X-Amzn-Bedrock-AgentCore-Runtime-*` headers are forwarded; the SigV4 signature is not. A non-2xx agent reply becomes `RuntimeClientError` (424).

Containers need a reachable Docker-compatible daemon (`--agentcore-engine=auto`). Without one, operations that need a container fail with `InternalServerException`; memory, identity, browser sessions and code interpreter file operations still work.

## Statuses and payload

Success statuses follow the Response Syntax of each operation in the AgentCore API Reference (docs.aws.amazon.com/bedrock-agentcore/latest/APIReference/API_<Operation>.html): CreateEvent and BatchCreateMemoryRecords answer 201, every other served operation 200. Error statuses on the InvokeAgentRuntime page: RuntimeClientError 424, RetryableConflictException 409, ThrottlingException 429, ServiceQuotaExceededException 402, UnauthorizedException 401 (StopRuntimeSession). InvokeAgentRuntime accepts payloads up to 100000000 bytes, the documented maximum.

## Primary-source checks (2026-11-13)
- InvokeAgentRuntimeCommand: serializers.go:3485 (POST /runtimes/{agentRuntimeArn}/commands, body command and timeout), types.go ResponseChunk (contentStart, contentDelta stdout/stderr, contentStop exitCode and status) and CommandExecutionStatus (COMPLETED, TIMED_OUT) in the pinned SDK.
- Runtime challenges point at GET /runtimes/<arn>/invocations/.well-known/oauth-protected-resource, now served unauthenticated as an RFC 9728 document (resource, authorization_servers, scopes_supported, bearer_methods_supported); 404 for runtimes without a customJWTAuthorizer.
- bedrockagentcore: runtime-oauth.html and runtime-mcp.html document 401 with WWW-Authenticate: Bearer resource_metadata=<runtime invocations URL>/.well-known/oauth-protected-resource?qualifier=...; the challenge now follows it (shared authorizer in bedrockagentcorecontrol).

## External effects that are recorded, not performed

- Memory extraction needs a model: events are stored and CreateEvent logs that extraction "would" run for memories with strategies; no memory records are produced, ExtractionMode and ExtractionConfig are ignored.

## Emulator choices where the vendor is silent

- ON_BEHALF_OF_TOKEN_EXCHANGE follows the provider's onBehalfOfTokenExchangeConfig as documented at devguide/on-behalf-of-token-exchange.html: TOKEN_EXCHANGE posts grant_type token-exchange with the user JWT of GetWorkloadAccessTokenForJWT as subject_token (type jwt) plus an actor_token from a client_credentials request (actorTokenContent M2M, scopes actorTokenScopes) or none (NONE); JWT_AUTHORIZATION_GRANT posts grant_type jwt-bearer with the JWT as assertion, plus requested_token_use=on_behalf_of for MicrosoftOauth2; customParameters override request parameters except grant_type. Not performed: actorTokenContent AWS_IAM_ID_TOKEN_JWT needs sts:GetWebIdentityToken for the provider's token endpoint audience and is answered with ValidationException. A provider with no onBehalfOfTokenExchangeConfig is treated as TOKEN_EXCHANGE with no actor token (the page does not say what AWS does). Client authentication is always HTTP basic. The JWT lives in memory on the workload access token (json:"-"), is dropped with it after one hour, and is never persisted: after a restart the flow answers ValidationException until a new token is issued.
- Provider token and discovery requests go through the control plane's outbound client (loopback only unless AGENTCORE_ALLOW_EXTERNAL_GATEWAY_TARGETS, no redirects), so the client secret and the user JWT are only sent to a destination that gate permits.
- GetWorkloadAccessTokenForJWT: the API reference and identity-authentication pages describe no issuer registration on a workload identity (the discovery URL page says tokens from a provider are accepted "without explicit onboarding"), so a hand-created identity accepts any issuer whose signature verifies through the loopback-gated JWKS; identities owned by a runtime or gateway additionally enforce that resource's customJWTAuthorizer (an emulator restriction).

- Persistence: sessions come back TERMINATED after a restart, and warm runtime containers and sandbox files are not persisted (container filesystems cannot be snapshotted by the emulator).
- InvokeAgentRuntimeCommand: output arrives as one contentDelta per stream after the command exits (the container runtime returns buffered output); a platform failure or timeout reports exitCode -1.
- Direct code deployment (runtime-get-started-code-deploy-python.html): the zip is unpacked read-only at /var/task (250 MB zipped, 750 MB unzipped), entryPoint ["opentelemetry-instrument", "main.py"] runs the wrapper around the interpreter and the file, and the managed runtimes are PYTHON_3_10 to PYTHON_3_14 and NODE_22 (types.AgentManagedRuntimeType); the container image is the matching python:3.x-slim or node:22-slim and arm64 wheels need an arm64 host.
- USER_FEDERATION: GetResourceOauth2TokenOutput (accessToken, authorizationUrl, sessionStatus IN_PROGRESS or FAILED, sessionUri) and CompleteResourceTokenAuth (POST /identities/CompleteResourceTokenAuth, sessionUri, userIdentifier userId or userToken) in the pinned SDK.

