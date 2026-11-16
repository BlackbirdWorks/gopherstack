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
  DataPlane: {status: partial, note: "Scale-out per environment concurrency with queueing behind initializing environments, scale-to-zero, SIGTERM grace spin-down, request-rate quotas, per-Region pools for MultiRegion/PerRegion endpoints, JSON/system logs with request attribution, embedded metric format, AWS/Lambda/Web metrics, role/KMS checks and resource-policy evaluation are covered (see items_still_open for the gaps). Requests whose Host names an endpoint domain are routed (<id>.<shard>.lambda-web.<region>.on.aws, <id>.lambda-web.global.on.aws, or the same under .localhost) to the revision picked by weight and run in a sandbox container: the docker engine is the default (--lambdaweb-engine=stub turns it off) and with no usable container runtime (daemon unreachable, or the sandbox image absent and not pullable) requests answer 503; a runtime that is up but fails to start the container answers 502 Application.TimeoutListenError, and an app that returns a non-HTTP response answers 503 Runtime.Unreachable. Platform errors carry lambda-web-error-type. Unit-tested with a fake launcher and a fake container runtime; the Node runtime shim is tested against a real node."}
gaps: []
items_still_open:
  - "Execution role, key and code access: when an environment starts the execution role must exist and its trust policy must allow lambda.amazonaws.com (502 Platform.CannotAssumeRole); a revision kmsKeyArn must exist, be enabled and be a symmetric encryption key (ValidationException at creation, 502 Platform.KMSDecryptionError at launch), and under --enforce-iam its key policy must allow lambda.amazonaws.com kms:Decrypt (AccessDeniedException at creation, 502 at launch), its S3 bucket policy must allow lambda.amazonaws.com s3:GetObject (AccessDeniedException; a revision that loses access or whose object version is deleted becomes Failed at the next verification), a MultiRegion/PerRegion key needs a usable replica in every target Region (else that Region fails replication) and the execution role needs logs:CreateLogStream and logs:PutLogEvents on the log group (else no log event is delivered). Not modelled: the caller's own kms:GenerateDataKey/kms:Decrypt and s3:GetObject permission checks (the emulator's IAM enforcement does not cover lambdaweb operations), encrypting environment variables and code with the key, egressNetworkConnectorArn existence and networking (no emulated service models Lambda network connectors; services/lambda has no NetworkConnector operations), cross-account and cross-Region bucket semantics (one S3 backend serves every Region) and the 512 MB code limit at creation (enforced only when the package is extracted)."
  - "Logs: not modelled are the totalCpuTimeMs field of platform.spinDownReport (CPU consumed during shutdown, web-functions-logging-system-log-schema; StopAndRemoveWithin in pkgs/container stops the container before any post-SIGTERM stats read is possible, so the shutdown delta is not read) and the CPU and memory fields whenever the container runtime cannot report stats (docker and podman can; the Kubernetes runtime cannot)."
  - "Request path is full-duplex: the proxy forwards the request body while the application may already be responding, whereas Lambda forwards the full request body before it delivers the response (web-functions-troubleshooting, HTTP request failures)."
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

Added 2026-11-05 (bd gopherstack-49qct.83), authority Lambda Developer Guide Web Functions pages: scaling
(web-functions-scaling), execution environment lifecycle (web-functions-env-lifecycle), monitoring, metrics and logging
(web-functions-monitoring, -monitoring-metrics, -logging, -logging-system-log-schema), permissions and encryption
(web-functions-permissions, -encryption) and error types (web-functions-troubleshooting). Idle period, the
same-account implicit-deny behaviour and the 502 status for Platform.CannotAssumeRole/KMSDecryptionError are not stated
in the docs and are chosen here.

## Emulator choices where the vendor is silent

- A host without a usable container runtime answers 503 (no error type); the docs' troubleshooting error types cover function-side failures only.

- Unspecified in the docs and chosen here: ValidationException for a missing/unversioned bucket or missing object at revision creation, ConflictException for weights that name a non-Active revision, ConflictException for an update while one is InProgress, and the Pending/Deleting dwell windows (500ms-1s, override with --lifecycle-lambdaweb).
- Idle environments are scaled down after 5 minutes (SetIdleTimeout); the docs name no period.
- The dynamic per-environment concurrency limit is fixed at the revision's maxConcurrencyPerEnvironment and no headroom is pre-scaled: the docs describe both as Lambda-managed and give no algorithm.
- At most 100 environments of one endpoint initialize at once (SetMaxConcurrentInits); beyond that requests get 429 Platform.EndpointConcurrentInitThrottle, which increments no throttle metric. The docs name the error but no threshold.
- The per-endpoint (10,000) and account (100,000) request-rate quotas are per Region and per second (SetRateQuotas models a quota increase); the endpoint quota answers Platform.EndpointRequestThrottle and the account quota Platform.AccountRequestThrottle.
- The MultiRegion global domain has no caller geography to measure: a request is served by the target Region named in the X-Gopherstack-Caller-Region header, else by the home Region; PerRegion domains are served by their own Region. Replication takes the endpoint dwell per Region, staggered.
- IamAuth verifies SigV4 (service lambda, X-Amz-Content-SHA256 required) against --sigv4-secret; MultiRegion endpoints verify the SigV4a ECDSA P-256 signature (key pair derived from the credential's access key id and the same secret, as the SDK v4a signer does) and the test checks it against a request signed by the real SDK signer.
- Resource-policy changes reach replica Regions immediately; web-functions-permissions says eventually, the emulator evaluates the function's current policy in every Region.
- The function resource policy is evaluated at invoke time with the IAM policy evaluator (principal, NotPrincipal, action, resource and conditions incl. aws:SourceIp, aws:PrincipalAccount, aws:PrincipalOrgID, aws:ResourceOrgID from the Organizations backend): an explicit Deny refuses, an Allow admits, no match admits a same-account caller (identity-based permission is enforced by --enforce-iam via IAMAction) and refuses a cross-account one; PutResourcePolicy rejects public policies with ValidationException per web-functions-permissions.html.
- Active revisions are re-verified (pinned object readable, bucket policy still admitting lambda.amazonaws.com) every 5 minutes (RunVerifier, VerifyRevisions); the docs say only "when Lambda next verifies access".
- A log event the log group cannot take is dropped and counted; the next delivered event is preceded by platform.logsDropped (WARN) with droppedRecords and droppedBytes. The docs' reason string covers only a slow consumer; the emulator's reason names an unavailable log group.
- Console output is tagged by a preload shim (preload.cjs, mounted in the environment) that mimics the managed runtime: console methods map to TRACE/DEBUG/INFO/WARN/ERROR and carry the request's requestId and endpointName; lines written to stdout directly keep the stdout-INFO and stderr-ERROR mapping, except embedded-metric-format objects, which are delivered unwrapped and extracted into metrics in the serving and home Regions.
- The shim sets the Node server's header-line limit to 1,000 and the container runs node with --max-http-header-size=131072, as web-functions-troubleshooting describes for the managed runtimes.
- CPU and memory figures come from container stats sampled at init completion, every 60 seconds and at shutdown; memory is the container's use without reclaimable cache, peak is the largest sample (or the runtime's reported peak).

## Sources

- CloudFormation types: docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-lambda-webfunction.html, -webfunctionrevision.html, -webfunctionendpoint.html (properties, Ref and Fn::GetAtt); the three types are in the current resource specification and were added to cmd/cfnattrgen/testdata/cfn_resource_spec.json.
- Scaling, endpoint, replication, encryption, code, HTTP, logging, metrics, EMF and troubleshooting behaviour: the Lambda Developer Guide pages web-functions-scaling, -endpoints, -endpoint-replication, -encryption, -code-s3, -http, -logging, -logging-system-log-schema, -monitoring-metrics, -emf and -troubleshooting.
