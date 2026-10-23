---
service: bedrockagentcore
sdk_module: aws-sdk-go-v2/service/bedrockagentcore@v1.50.3
last_audit_commit: 71065ec09
last_audit_date: 2026-10-22
overall: B            # 30 of 67 data-plane operations are served with real behavior; the rest (A/B tests, payments, evaluation runs, recommendations, harness invoke, memory extraction, browser automation) are not served.
ops:
  BatchCreateMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  BatchDeleteMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  BatchUpdateMemoryRecords: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-record success or failure with errorCode"}
  CreateEvent: {wire: ok, errors: ok, state: ok, persist: ok, note: "payload union, metadata (15 max), branches checked against the root event, clientToken dedupe; no extraction runs"}
  DeleteEvent: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMemoryRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAgentCard: {wire: ok, errors: ok, state: ok, persist: ok, note: "A2A runtimes only; GET /.well-known/agent-card.json from the session container"}
  GetBrowserSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "stored state; idle sessions read as TERMINATED"}
  GetCodeInterpreterSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "idle sessions past sessionTimeoutSeconds read as TERMINATED"}
  GetEvent: {wire: ok, errors: ok, state: ok, persist: ok, note: "events past the memory eventExpiryDuration read as not found"}
  GetMemoryRecord: {wire: ok, errors: ok, state: ok, persist: ok, note: "the namespace query narrows the match"}
  GetResourceApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "reads the key from the Secrets Manager secret the control plane stored"}
  GetResourceOauth2Token: {wire: ok, errors: ok, state: ok, persist: ok, note: "M2M flow performs a real client_credentials request against the provider's token endpoint; USER_FEDERATION is rejected"}
  GetWorkloadAccessToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque token, 1 hour, workload identity must exist"}
  GetWorkloadAccessTokenForJWT: {wire: ok, errors: ok, state: ok, persist: ok, note: "reads the sub claim; the JWT signature is not verified"}
  GetWorkloadAccessTokenForUserId: {wire: ok, errors: ok, state: ok, persist: ok}
  InvokeAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "runs the runtime's containerConfiguration image; POST /invocations on 8080 (HTTP), /mcp on 8000 (MCP), / on 9000 (A2A); streams the response body unchanged (text/event-stream passthrough); the session id header routes to a warm container"}
  InvokeCodeInterpreter: {wire: ok, errors: ok, state: ok, persist: ok, note: "executeCode (python), executeCommand, readFiles, writeFiles, listFiles, removeFiles; result is one event-stream result event; startCommandExecution, getTask and stopTask are rejected"}
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
  Identity: {status: ok, note: "Workload access tokens, API keys read from Secrets Manager, and OAuth2 client-credentials tokens fetched from the provider's token endpoint."}
  Wire: {status: ok, note: "Routes, members, headers, epoch and date-time formats and error codes follow the pinned SDK serializers and deserializers."}
items_still_open:
  - Unserved operations: AB tests, batch evaluation, payments, recommendations, InvokeHarness, IngestData, InvokeAgentRuntimeCommand, memory extraction jobs, SaveBrowserSessionProfile, InvokeBrowser, UpdateBrowserStream and CompleteResourceTokenAuth have no routes
  - Memory extraction: events are never turned into memory records because strategies need a model; ExtractionMode and ExtractionConfig are accepted and ignored
  - Retrieval: RetrieveMemoryRecords ranks with lexical BM25 only, so records sharing no term with the query are not returned
  - Runtimes: codeConfiguration (S3 code) runtimes cannot be invoked; only container images run, and the emulated ECR registry is reached through the registry-host setting
  - Runtimes: authorizer configuration is not enforced and the invocation payload is capped at 16 MiB by the shared body reader
  - Code interpreter: executeCode runs a fresh python process per call, so variables do not persist between calls; javascript, typescript and the async task tools are rejected
  - Code interpreter: network mode, certificates and filesystem configurations are stored but not applied to the sandbox
  - Browser: no automation or live-view streams, no profiles, and no browser process behind a session
  - Identity: USER_FEDERATION OAuth2 flows are rejected, vendor providers without a discovery document have no reachable token endpoint, and user JWT signatures are not verified
  - Persistence: sessions come back TERMINATED after a restart; warm runtime containers and sandbox files are not persisted
  - Success status codes and error statuses for RuntimeClientError (424), RetryableConflictException (409) and ThrottlingException (429) come from AWS documentation, not SDK evidence
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
