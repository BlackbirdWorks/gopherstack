---
service: ocifunctions
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: bfe657153
last_audit_date: 2026-10-25
overall: B
ops:
  CreateApplication: {wire: ok, errors: partial, state: ok, persist: ok, note: "No opc-retry-token: no functions request struct in the pinned SDK carries OpcRetryToken. Free-form and defined tags are validated against the documented limits and the OCI Identity tag definitions. compartmentId, displayName and non-empty subnetIds required (subnets and network security groups resolved against OCI Core, security groups in the subnets' VCN); shape GENERIC_X86 (default), GENERIC_ARM or GENERIC_X86_ARM; config (4096 bytes in total), networkSecurityGroupIds, syslogUrl (tcp, udp or tcp+tls), traceConfig, logging, imagePolicyConfig and tags are stored and echoed. Names are unique per compartment among live applications (409 Conflict). CREATING then ACTIVE after the transition window on the injectable clock; returns etag and opc-work-request-id."}
  GetApplication: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; DELETED applications answer 404."}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; id, displayName, lifecycleState filters; sortBy timeCreated (default, newest first), displayName or id; sortOrder; paged with opc-next-page. Summaries omit config and syslogUrl as in the SDK."}
  UpdateApplication: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body, as the SDK response declares. config, networkSecurityGroupIds, syslogUrl, traceConfig, logging, imagePolicyConfig and tags replace the stored value when supplied; If-Match gives 412 NoEtagMatch. Applies immediately (no UPDATING state). A config change makes the next invocation start fresh containers."}
  DeleteApplication: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id; DELETING then DELETED. An application that still has live functions answers 409 IncorrectState."}
  ChangeApplicationCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Moves the application and its functions; refuses a name collision in the target compartment."}
  CreateFunction: {wire: ok, errors: partial, state: ok, persist: ok, note: "sourceDetails CONTAINER_IMAGE with image (any pullable reference) and optional sha256 imageDigest. memoryInMBs 128 to 3072, timeoutInSeconds 5 to 300 (default 30), detachedModeTimeoutInSeconds 5 to 3600, provisionedConcurrencyConfig NONE (default) or CONSTANT with count, config (4096 bytes), traceConfig, failure and success destinations (NONE, STREAM, QUEUE or NOTIFICATION, checked against the emulated target services) and tags. shape comes from the application, invokeEndpoint is the base URL the client addressed. Needs an ACTIVE application; names unique per application."}
  GetFunction: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; sourceDetails and provisionedConcurrencyConfig use the SDK discriminators."}
  ListFunctions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "applicationId required; id, displayName, lifecycleState filters; sortBy timeCreated, displayName or id; paged. Summaries omit config."}
  UpdateFunction: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id and no body. memory, timeouts, config (replaced), image, concurrency, destinations, trace config and tags; the function's warm containers are removed so the next call runs the new definition."}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with opc-work-request-id; DELETING then DELETED; the warm containers are removed immediately."}
  InvokeFunction: {wire: ok, errors: partial, state: ok, persist: n/a, note: "Runs the function image under the Fn FDK contract (see families.invoke_contract). fn-invoke-type sync (default) answers with the function's body; detached answers 202 at once and runs in the background; is-dry-run validates without executing; fn-intent httprequest or cloudevent is validated and otherwise unused. Request payloads up to 6 MiB (413 beyond)."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the resource transition time, then SUCCEEDED."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; workRequestId and resourceId filters; the items envelope the SDK collection expects."}
  ListWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: no emulated operation fails asynchronously."}
  ListWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Accepted and succeeded entries."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures for API keys registered through POST /_gopherstack/oci/keys or uploaded through OCI Identity (linked key registry); accept-any otherwise, --oci-functions-enforce-auth rejects unsigned and unknown keys."}
  api_versions: {status: ok, note: "Both /20181201 (documented) and /20260325 (the version path the pinned SDK sends) are served."}
  invoke_contract: {status: partial, note: "The container is started from the function image with FN_FORMAT=http-stream, FN_LISTENER=unix:/tmp/iofs/lsnr.sock, FN_APP_NAME, FN_FN_NAME, FN_APP_ID, FN_FN_ID, FN_MEMORY, FN_TMPSIZE and the merged config (application config overridden by function config; FN_* names cannot be overridden) as environment variables. A host scratch directory is bind-mounted at /tmp/iofs; once the FDK's socket appears each call is POST /call over it with Fn-Call-Id, Fn-Deadline (RFC 3339), Fn-Http-Method, Fn-Http-Request-Url, Content-Type and the client's other headers as Fn-Http-H-*. The response body, Content-Type and Fn-* headers are returned with 200; a container 5xx is 502 FunctionInvokeExecutionFailed. Verified end to end with a real container speaking the contract (opt-in docker test)."}
  container_pool: {status: ok, note: "One call per container at a time; idle containers are kept warm per function and reused while the image and environment are unchanged, reaped after --oci-functions-idle-timeout (default 10m), capped by --oci-functions-max-containers (default 32; an idle container is evicted first, otherwise 429 TooManyRequests). Updates, deletes, Reset and Shutdown remove containers; a timed out call removes its container."}
  timeouts: {status: partial, note: "timeoutInSeconds bounds the call after the container is ready (504 FunctionInvokeTimeout, container removed); image pull and start are bounded separately by --oci-functions-startup-timeout. Detached calls run for detachedModeTimeoutInSeconds, else timeoutInSeconds."}
  engine: {status: ok, note: "--oci-functions-disable-engine or an unreachable container runtime makes every invocation (sync and detached) fail with 503 ServiceUnavailable and a clear message; a response is never fabricated."}
  lifecycle: {status: ok, note: "CREATING and DELETING last 2 seconds on the injectable backend clock; transitions are applied on every request and by a one-minute sweeper."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-functions-port / OCI_FUNCTIONS_PORT (default 8154); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards the resource maps and a separate one the container pool."}
items_still_open:
  - "Pre-built function sources (PRE_BUILT_FUNCTIONS), archive sources (ARCHIVE with runtimeConfig), the pbfListings, functionsRuntimes and triggers APIs are not implemented and CreateFunction accepts only CONTAINER_IMAGE: they need Oracle-published content the emulator has no source for (the PBF catalog and its Oracle-hosted images, the runtime catalog and Oracle's function build service); the pinned SDK types are functions/create_pre_built_function_source_details.go, create_archive_function_source_details.go, pbf_listing.go and functions_runtime.go."
  - "IAM policy checks, resource principals and instance principals are not applied; only API-key signatures are verified."
---

## Verified against Oracle documentation

- Invoke payload limit: oci-go-sdk functions/invoke_function_request_response.go states the 6MB request limit, matching maxPayload.

- Invoke errors 502 FunctionInvokeExecutionFailed, 502 FunctionInvokeResponseBodyTooLarge, 504 FunctionInvokeTimeout, 429 TooManyRequests, 503 FunctionInvokeServiceUnavailable and 413 FunctionInvokeRequestContentTooLarge: https://docs.oracle.com/en-us/iaas/Content/Functions/Tasks/functionstroubleshooting.htm.

- imagePolicyConfig.keyDetails[].kmsKeyId is validated against OCI Vault when wired (404 NotAuthorizedOrNotFound for an unknown key, 409 IncorrectState when it is not ENABLED).

- Function config keys must be ASCII letters, digits and `_` without a leading digit (functions/create_function_details.go, Config); syslogUrl supports tcp, udp and tcp+tls and is ignored when OCI Logging is enabled for the application (functions/create_application_details.go, SyslogUrl).

- Destinations apply to detached invocations: stream, queue (queueId, channelId) and notification (topicId) kinds (functions/stream_failure_destination_details.go, queue_failure_destination_details.go, notification_failure_destination_details.go).

- Service limits application-count (20), function-count (500), total-concurrency-mb (60 GB) and provisioned-concurrency-mb (40 GB): https://docs.oracle.com/en-us/iaas/Content/General/Concepts/servicelimits.htm (Universal Credits column; --oci-functions-max-applications, --oci-functions-max-functions, --oci-functions-concurrency-mb and --oci-functions-provisioned-mb change them).

- fnproject/fdk-go handler.go confirms a function reports its status as Fn-Http-Status and its headers as Fn-Http-H-*.

## Emulator choices where the vendor is silent

- A function's Fn-Http-Status and Fn-Http-H-* response headers are passed through to the caller and the status stays 200; the invoke endpoint's handling is not published (checked functionsinvokingfunctions.htm, functionsconcepts.htm, functionsusingapigateway.htm).
- Fn-Fdk-Version and Fn-Fdk-Runtime are set by the FDK and travel back as ordinary Fn-* response headers; fn-intent is validated and forwarded to nothing, because the platform's cloudevent request shaping is not published.
- FN_TMPSIZE stays 0 and no tmp size limit is applied; the tmp quota of a function is not published.
- Application and function display names are accepted up to 255 characters of any kind; the name syntax is not published (functionscreatingapps.htm gives only the uniqueness and confidentiality guidance).
- The FDK socket directory is a host bind mount, so the container runtime must run on the emulator's host (not a Docker Desktop VM or a remote daemon).
- A reference to an image of the emulated tenancy namespace on an `*.ocir.io` host must exist in OCI Artifacts (400 InvalidParameter otherwise) and an imageDigest must match it; references to other namespaces or hosts are not checked.
- With an enabled imagePolicyConfig an image must carry a signature from one of the policy's keys that OCI Vault verifies (400 InvalidParameter otherwise), at function create, update and invoke; images the emulated registry does not hold are rejected.
- The application shape selects the container platform: GENERIC_X86 linux/amd64, GENERIC_ARM linux/arm64, GENERIC_X86_ARM unconstrained.
- Provisioned concurrency CONSTANT keeps count containers started and exempt from the idle timeout, re-created by the sweeper; reserved containers do not count against total-concurrency-mb, and a call that would exceed it answers 429.
- Detached results go to the success or failure destination as the response body or, on failure, a JSON object with functionId, callId, errorCode and errorMessage; a notification gets a title naming the function.
- Function stdout and stderr lines are written to the SERVICE log (category invoke) of the application as `message` (lineFormat PLAIN_TEXT) or as a JSON object with stream, applicationId, functionId and callId (lineFormat JSON); lines no log takes go to syslogUrl as RFC 5424 messages (stdout severity info, stderr severity error).
- Limit errors answer 400 LimitExceeded.

## External effects that are recorded, not performed

- traceConfig: the settings are stored and echoed; no APM service is emulated, so each invocation of a tracing-enabled function logs through pkgs/logger that spans would be exported to the domain.
- Destinations whose target service is not enabled log that delivery was not performed.
