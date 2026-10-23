---
service: bedrockagentcorecontrol
sdk_module: aws-sdk-go-v2/service/bedrockagentcorecontrol@v1.73.0
last_audit_commit: 71065ec09
last_audit_date: 2026-10-22
overall: B            # all 171 control-plane operations hold real state and are driven through the SDK client; the data plane lives in services/bedrockagentcore and several behaviours (policy generation, evaluation runs, tool discovery) have no emulator engine.
ops:
  AddDatasetExamples: {wire: ok, errors: ok, state: ok, persist: ok, note: "draft examples only; ids generated unless supplied"}
  BatchPutGatewayRateLimits: {wire: ok, errors: ok, state: ok, persist: ok, note: "replaces the gateway rate limits atomically"}
  CreateAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "name pattern, ECR image URI shape (repository checked against the ECR backend when wired), exactly one artifact variant, VPC needs subnets and security groups, JWT discovery URL; creates a DEFAULT endpoint and a managed workload identity; CREATING then READY"}
  CreateAgentRuntimeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "DEFAULT is reserved; version must exist; liveVersion and targetVersion start equal"}
  CreateApiKeyCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "the key is stored in a Secrets Manager secret (bedrock-agentcore-identity-default/apikey/<name>/apiKey) when Secrets Manager is wired, else the managed ARN is synthesized and the value dropped; external secrets are checked against Secrets Manager when wired"}
  CreateBrowser: {wire: ok, errors: ok, state: ok, persist: ok, note: "mode PUBLIC or VPC; the service-owned aws.browser.v1 browser is listed as SYSTEM"}
  CreateBrowserProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletion blocked while a runtime uses it"}
  CreateCodeInterpreter: {wire: ok, errors: ok, state: ok, persist: ok, note: "mode PUBLIC, SANDBOX or VPC; aws.codeinterpreter.v1 is listed as SYSTEM"}
  CreateConfigurationBundle: {wire: ok, errors: ok, state: ok, persist: ok, note: "first version on the named branch (mainline by default)"}
  CreateConsentPortal: {wire: ok, errors: ok, state: ok, persist: ok, note: "idp provider and gateway sources must exist; resolvable by name or id"}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "inline examples or an S3 JSONL object (read from the S3 backend when wired, else CREATE_FAILED)"}
  CreateDatasetVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "snapshots the draft examples as the next numeric version"}
  CreateEvaluator: {wire: ok, errors: ok, state: ok, persist: ok, note: "configuration stored; nothing evaluates"}
  CreateGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "name pattern; CUSTOM_JWT needs an authorizer; policy engine ARN must exist; gatewayUrl and a managed workload identity are generated"}
  CreateGatewayRateLimit: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGatewayRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "priority 1-1000000, unique per gateway"}
  CreateGatewayTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "exactly one target type; Lambda ARN checked against the Lambda backend when wired; credential provider ARNs must exist"}
  CreateHarness: {wire: ok, errors: ok, state: ok, persist: ok, note: "version 1 snapshot; update creates the next version"}
  CreateHarnessEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "targetVersion defaults to the latest harness version"}
  CreateMemory: {wire: ok, errors: ok, state: ok, persist: ok, note: "eventExpiryDuration 7-365; strategies get generated ids and ACTIVE status"}
  CreateOauth2CredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "client secrets never come back; callbackUrl generated; vendor immutable"}
  CreateOnlineEvaluationConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "evaluator ids must exist or start with Builtin.; enableOnCreate sets executionStatus"}
  CreatePaymentConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "credential provider ARNs must be payment providers that exist"}
  CreatePaymentCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "exactly one provider configuration; secrets never come back"}
  CreatePaymentManager: {wire: ok, errors: ok, state: ok, persist: ok, note: "managed workload identity generated"}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "cedar statement required; policyGeneration definitions are rejected because generations never yield assets"}
  CreatePolicyEngine: {wire: ok, errors: ok, state: ok, persist: ok, note: "deletion blocked while policies exist or a gateway references the engine"}
  CreateRegistry: {wire: ok, errors: ok, state: ok, persist: ok, note: "autoApproval decides what SubmitRegistryRecordForApproval does"}
  CreateRegistryRecord: {wire: ok, errors: ok, state: ok, persist: ok, note: "starts DRAFT"}
  CreateWorkloadIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "name-keyed"}
  DeleteAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "version query drops a non-live version (ConflictException while an endpoint uses it); otherwise needs no custom endpoints and removes the DEFAULT endpoint and managed identity"}
  DeleteAgentRuntimeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApiKeyCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBrowser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBrowserProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCodeInterpreter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationBundle: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConsentPortal: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDatasetExamples: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEvaluator: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "ConflictException while targets exist; rules and rate limits are removed with it"}
  DeleteGatewayRateLimit: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGatewayRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGatewayTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHarness: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHarnessEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMemory: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOauth2CredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOnlineEvaluationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePaymentConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePaymentCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePaymentManager: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicyEngine: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRegistry: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRegistryRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWorkloadIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "version query returns that snapshot"}
  GetAgentRuntimeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApiKeyCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBrowser: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBrowserProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCodeInterpreter: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationBundle: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationBundleVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConsentPortal: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEvaluator: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGateway: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGatewayRateLimit: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGatewayRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGatewayTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHarness: {wire: ok, errors: ok, state: ok, persist: ok}
  GetHarnessEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMemory: {wire: ok, errors: ok, state: ok, persist: ok, note: "the view query parameter is accepted and ignored"}
  GetOauth2CredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOnlineEvaluationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPaymentConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPaymentCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPaymentManager: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicyEngine: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicyEngineSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicyGenerationSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicySummary: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistry: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRegistryRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTokenVault: {wire: ok, errors: ok, state: ok, persist: ok, note: "one vault per account and Region, ServiceManagedKey until SetTokenVaultCMK"}
  GetWorkloadIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentRuntimeEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAgentRuntimeVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "one summary per snapshot"}
  ListAgentRuntimeVersionsByCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "runtimes whose capacityProviderConfiguration names the provider"}
  ListAgentRuntimes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApiKeyCredentialProviders: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBrowserProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBrowsers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCapacityProviders: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCodeInterpreters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationBundleVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationBundles: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConsentPortals: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetExamples: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEvaluators: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGatewayRateLimits: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGatewayRules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGatewayTargets: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGateways: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHarnessEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHarnessVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHarnesses: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMemories: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOauth2CredentialProviders: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOnlineEvaluationConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPaymentConnectors: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPaymentCredentialProviders: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPaymentManagers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyEngineSummaries: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyEngines: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyGenerationAssets: {wire: ok, errors: ok, state: ok, persist: ok, note: "always empty"}
  ListPolicyGenerationSummaries: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicyGenerations: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicySummaries: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRegistries: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRegistryRecords: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListWorkloadIdentities: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "runtimes, endpoints and gateways; the document must be JSON with a Statement"}
  RotatePaymentConnectorCredentials: {wire: ok, errors: ok, state: ok, persist: ok, note: "CoinbaseCDP connectors only (the SDK models no other rotation target)"}
  SetTokenVaultCMK: {wire: ok, errors: ok, state: ok, persist: ok, note: "CustomerManagedKey needs a KMS key ARN"}
  StartPolicyGeneration: {wire: ok, errors: ok, state: ok, persist: ok, note: "always ends in GENERATE_FAILED: there is no generative model to derive policies from"}
  SubmitRegistryRecordForApproval: {wire: ok, errors: ok, state: ok, persist: ok, note: "DRAFT or REJECTED to PENDING_APPROVAL, or APPROVED when the registry auto-approves"}
  SynchronizeGatewayTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "stamps lastSynchronizedAt and passes through SYNCHRONIZING; no tool discovery happens"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "any resource with an ARN; 50 tags, aws: prefix reserved"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAgentRuntime: {wire: ok, errors: ok, state: ok, persist: ok, note: "every update snapshots a new numeric version and moves the DEFAULT endpoint to it; merges optional members"}
  UpdateAgentRuntimeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "the response keeps the previous liveVersion, stored state moves to the target"}
  UpdateApiKeyCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCapacityProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationBundle: {wire: ok, errors: ok, state: ok, persist: ok, note: "parentVersionIds must exist; creates a version on the branch of the first parent unless branchName is set"}
  UpdateConsentPortal: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatasetExamples: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEvaluator: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGateway: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGatewayRateLimit: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGatewayRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGatewayTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHarness: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHarnessEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMemory: {wire: ok, errors: ok, state: ok, persist: ok, note: "add, modify and delete strategies; addIndexedKeys appends"}
  UpdateOauth2CredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOnlineEvaluationConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePaymentConnector: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePaymentCredentialProvider: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePaymentManager: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePolicyEngine: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRegistry: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRegistryRecord: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRegistryRecordStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "PENDING_APPROVAL to APPROVED or REJECTED, APPROVED to DEPRECATED"}
  UpdateWorkloadIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Runtimes: {status: ok, note: "Runtime, versions, endpoints (DEFAULT auto-created), managed workload identity, capacity-provider linkage. Every update snapshots a version."}
  Gateways: {status: ok, note: "Gateway, targets, rules, rate limits and batch replace. Targets validate their union, Lambda ARN and credential provider ARNs."}
  Memory: {status: ok, note: "Memory with strategy lifecycle (add, modify, delete). No memory records or events exist; that is the data plane."}
  Tools: {status: ok, note: "Custom browsers, browser profiles and code interpreters; the two service-owned system resources are virtual and undeletable."}
  Evaluation: {status: ok, note: "Evaluators, online evaluation configs (evaluator reference checks, execution status), datasets with draft examples and numbered versions, S3 JSONL sources."}
  Harnesses: {status: ok, note: "Harness versions and endpoints pointing at a version."}
  Bundles: {status: ok, note: "Configuration bundles with branches, parent-version lineage and per-branch reads."}
  Policy: {status: ok, note: "Engines, policies (cedar statement required), generations (always GENERATE_FAILED), summaries."}
  Registry: {status: ok, note: "Registries and records with the DRAFT, PENDING_APPROVAL, APPROVED, REJECTED, DEPRECATED workflow and auto-approval."}
  Identity: {status: ok, note: "Workload identities, API key, OAuth2 and payment credential providers, token vault. Secret inputs never come back; managed secrets are written to Secrets Manager when it is wired (read by the data plane, rotated on update, deleted with the provider) and synthesized otherwise."}
  Payments: {status: ok, note: "Payment managers, connectors, credential rotation."}
  Tagging: {status: ok, note: "Tags and resource policies address any ARN the service issued."}
  Lifecycle: {status: ok, note: "Status is a lazy deadline (Transition until SettleAt) evaluated on read; no goroutines. Responses to create, update and delete report the transitional status; the lifecycle delay defaults to 0 so a follow-up read is already settled."}
  Wire: {status: ok, note: "Operation routes, member sets, required members, enums and per-shape timestamp formats (epoch seconds or date-time) are extracted from the pinned SDK serializers, deserializers and validators into ops.json; outputs are projected onto exactly the SDK output members."}
gaps: []
items_still_open:
  - Gateway MCP endpoint: the gateway URL is generated but nothing serves MCP tool calls on it
  - Update semantics: PUT-style updates (UpdateAgentRuntime, UpdateGateway, UpdateGatewayTarget) merge supplied members instead of clearing omitted optional ones
  - Nested validation: only union exclusivity, enums, name patterns, ARN shapes and cross-resource references are checked; deeper member constraints (JWT claim rules, schema contents, VPC endpoint overrides) are accepted as sent
  - Cross-service references: ECR repositories, Lambda functions, Secrets Manager secrets and S3 objects are verified only when internal/app wiring is present; IAM roles and KMS keys are checked for ARN shape only
  - Failure states: CREATE_FAILED and UPDATE_FAILED are only reached by dataset S3 sources; no other asynchronous failure is modeled
  - Gateway targets: SynchronizeGatewayTargets only stamps lastSynchronizedAt; Lambda, OpenAPI and MCP tool discovery never runs and OAuth targets never reach a PENDING_AUTH status
  - Registry records: synchronizationConfiguration is stored but nothing synchronizes
  - Authorizers: JWT and IAM authorizer configuration is stored, not enforced
  - GetMemory: the view query parameter is accepted and ignored
  - Success status codes: 202 for asynchronous creates, updates and deletes comes from AWS documentation; the SDK does not encode it
  - Error statuses: ConflictException 409, ResourceNotFoundException 404, ServiceQuotaExceededException 402 and ValidationException 400 come from AWS documentation, not SDK evidence
  - Quotas: no per-account resource limits are enforced
structural_gaps:
  - StartPolicyGeneration: deriving Cedar from natural language needs a generative model, so every generation ends GENERATE_FAILED with no assets
  - Evaluators and online evaluation configs: evaluation needs a model and live agent traces, so configurations are validated and stored but never run
deferred: []
leaks: {status: clean, note: "No goroutines or janitors; lifecycle is evaluated lazily; testleak verifies the package."}
---

## Notes

REST-JSON 1 (`awsRestjson1_`), JSON members are lowerCamelCase and timestamps use a per-shape format: some shapes read epoch seconds and others ISO 8601 date-time, even for the same member name, so the generated `ops.json` records the format of every output member and list element.

Routing is gated by the SigV4 signing name `bedrock-agentcore` (or the SDK user agent marker) plus a known path shape, so the generic `/tags/{arn}` and `/datasets` style routes never swallow another service's traffic.

`ops.json` is extracted from the pinned module (serializers.go for the HTTP binding and body members, validators.go for required members, deserializers.go for output members and timestamp formats, enums.go for enum sets). Regenerate it when the SDK pin moves.

Update members that the SDK models as `{"optionalValue": x}` wrappers (`UpdatedDescription` and friends) are unwrapped before they are stored, and an empty wrapper clears the member.

Identifier formats: name-based ids are `<name>-<10 hex>`, target, rule and rate-limit ids are 10 uppercase hex characters, registry and record ids are 12 hex characters. Gateway ids are lowercased.

Resources are scoped by account and Region; the two system resources (`aws.browser.v1`, `aws.codeinterpreter.v1`) are visible everywhere.
