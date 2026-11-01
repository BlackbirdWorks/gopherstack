---
service: lambdaweb
sdk_module: aws-sdk-go-v2/service/lambdaweb@v1.2.5
last_audit_commit: ea845bb63
last_audit_date: 2026-11-01
overall: B           # every SDK operation is real and SDK-round-trip tested; the HTTP data plane runs real containers only behind the docker engine, and several documented runtime behaviours are open (see items_still_open).
ops:
  CreateWebFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; revisionConfig and endpointConfig are optional in the SDK model, an endpoint without a revision is a ValidationException; the initial endpoint routes 100% to the initial revision (Disabled mode) or follows the newest Active revision (LatestRevision); function/revision/endpoint start Pending and become Active on the injectable clock"}
  GetWebFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "accepts a name or ARN; a Deleting function is still returned until removed"}
  ListWebFunctions: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST; filter name state only; maxResults 1-50; nextToken via pkgs/page"}
  DeleteWebFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; function and endpoints go Deleting then are removed with their revisions; running environments are stopped"}
  CreateWebFunctionRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; code is read from the in-process S3 backend: bucket must exist and be versioned and the object must exist (ValidationException), the object version is pinned; defaults timeoutSeconds 30, maxConcurrencyPerEnvironment 64, log group /aws/lambda/web/{name} and INFO levels when telemetryConfig is given"}
  GetWebFunctionRevision: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWebFunctionRevisions: {wire: ok, errors: ok, state: ok, persist: ok, note: "newest first; filter name state"}
  DeleteWebFunctionRevision: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while any endpoint routes traffic to the revision; revisions beyond 50 per function are cleaned up oldest-unused-first when a new one becomes Active"}
  CreateWebFunctionEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; HomeRegion/MultiRegion/PerRegion (regions validated, home Region added), IamAuth/ApplicationManaged, LatestRevision only for HomeRegion; Disabled mode requires 1-2 Active revisions with weights 1-100 summing to 100; scalingConfig maxEnvironments 2-10000 and throttleConfig rateLimit 0-10000 (multiples of 100 up to 1000, of 1000 above) validated; 10 endpoints per function (ServiceQuotaExceededException)"}
  GetWebFunctionEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "regionalEndpoints is returned for every type; LatestRevision endpoints report the effective routing weights"}
  ListWebFunctionEndpoints: {wire: ok, errors: ok, state: ok, persist: ok, note: "filters autoDeploymentMode, endpointType, state, updateStatus"}
  UpdateWebFunctionEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "202; state stays Active while updateStatus goes InProgress then Successful; a second update during InProgress is a ConflictException; omitted scalingConfig/throttleConfig keep the value, an empty object clears it; switching to LatestRevision needs an Active newest revision; weights in LatestRevision mode are rejected; LatestRevision to Disabled freezes the current routing"}
  DeleteWebFunctionEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; the endpoint goes Deleting (new requests get 409 Platform.EndpointNotActive) then is removed and its domain retired"}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "function ARN only; policy must be JSON with a Statement and at most 20480 bytes; RevisionId mismatch is a ConflictException"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; RevisionId query parameter is the optimistic lock"}
  GetWebAccountSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "quotas from the Lambda Web Functions docs: 10 endpoints, 50 revisions, 2000 Arm vCPUs, 100000 total rate limit; usage counts the Region's functions"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; function ARN only, matching the SDK doc comment"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "204; tagKeys query parameter"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  ControlPlane: {status: ok, note: "All 20 SDK operations round-trip through the real aws-sdk-go-v2 lambdaweb client (restJson1 via smithy schemas, lowerCamel members, date-time timestamps, Policy/RevisionId/Tags capitalised only on the policy and tag operations)."}
  DataPlane: {status: partial, note: "Requests whose Host names an endpoint domain are routed (<id>.<shard>.lambda-web.<region>.on.aws, <id>.lambda-web.global.on.aws, or the same under .localhost) to the revision picked by weight, run in a sandbox container behind --lambdaweb-engine=docker. Platform errors carry lambda-web-error-type. Unit-tested with a fake launcher and a fake container runtime; the container itself needs docker."}
gaps: []
items_still_open:
  - "Data plane is opt-in: LAMBDAWEB_ENGINE=docker (or --engines=auto/docker with a reachable runtime). With the default stub engine, requests to Active endpoint domains answer 503 and the control plane is unaffected."
  - "One execution environment per serving revision (started on the first request, stopped on revision/function delete or shutdown). maxEnvironments only caps in-flight requests at maxEnvironments x maxConcurrencyPerEnvironment (429 Platform.EndpointMaxEnvThrottle); there is no scale-out, scale-to-zero, SIGTERM drain, dynamic concurrency limit or account vCPU quota."
  - "MultiRegion and PerRegion endpoints are control-plane only: every domain is served by the home Region emulator, regionalEndpoints entries mirror the endpoint state, and no cross-Region replication or latency routing is modelled."
  - "IamAuth verifies a SigV4 signature (service lambda, X-Amz-Content-SHA256 required) against --sigv4-secret; MultiRegion endpoints only check the SigV4a header shape because the emulator cannot verify ECDSA signatures. lambda:InvokeWebFunctionEndpoint is exposed through IAMAction so --enforce-iam evaluates identity policies, but the function resource policy is stored and returned, not evaluated at invoke time."
  - "Not validated or modelled: the execution role (existence, trust, Platform.CannotAssumeRole), kmsKeyArn permissions and environment encryption (Platform.KMSDecryptionError), egressNetworkConnectorArn existence and networking, S3 bucket policy for lambda.amazonaws.com, cross-account and cross-Region buckets, the 512 MB code limit only on extraction."
  - "An S3 object version deleted after revision creation fails the revision (Failed, errors[] CodeUnavailable) on the next launch rather than within the AWS verification interval; Failed is permanent as documented."
  - "Container output is shipped line by line to the revision log group and its .local companion, not as the documented JSON events (timestamp/level/message/requestId/endpointName/region), and no platform.* system log events, applicationLogLevel/systemLogLevel filtering, or lambda-web-logstream per environment lifecycle are produced."
  - "CloudWatch metrics (AWS/Lambda/Web*) are not emitted."
  - "CloudFormation resource types AWS::Lambda::WebFunction, WebFunctionRevision and WebFunctionEndpoint are not provisioned: they are absent from the pinned CloudFormation resource spec snapshot (cmd/cfnattrgen/testdata/cfn_resource_spec.json)."
  - "Request limits not enforced: header block 128 KiB is checked on the parsed headers only; HTTP/1.0 is rejected with 505 only when the server surfaces the protocol; no response streaming guarantees beyond io.Copy."
  - "Unspecified in the docs and chosen here: ValidationException for a missing/unversioned bucket or missing object at revision creation, ConflictException for weights that name a non-Active revision, ConflictException for an update while one is InProgress, and the Pending/Deleting dwell windows (500ms-1s, override with --lifecycle-lambdaweb)."
structural_gaps:
  - "The *.on.aws endpoint domains are not resolvable and have no TLS: reach an endpoint with the emulator port and a Host header (curl -H 'Host: <domainName>' http://localhost:4566/) or the <id>.<shard>.lambda-web.<region>.localhost form. The nodejs24.x runtime image is pulled from the runtime registry (LAMBDAWEB_RUNTIME_IMAGE)."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; lifecycle transitions are lazy deadlines evaluated on access, environment goroutines end on revision stop and Close."}
---

## Notes

Added 2026-11-01 (bd gopherstack-49qct.83) against the real `aws-sdk-go-v2/service/lambdaweb@v1.2.5` module, whose
schemas/schemas.go, types and validators are the authority for every route, member, status code and enum. Semantics
(states, weight rules, quotas, domains, ARNs, headers, error types) follow the AWS Lambda Developer Guide Web Functions
chapters; LocalStack's page was used only to cross-check, and nothing exclusive to it (hot-reload buckets,
localhost.localstack.cloud domains) is implemented.
