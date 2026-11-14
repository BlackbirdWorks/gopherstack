---
service: ociidentity
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 0d22fe6b7
last_audit_date: 2026-10-25
overall: B
ops:
  GetTenancy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One fixed tenancy (TenancyID, also the root compartment id); homeRegionKey IAD. Any other id is 404 NotAuthorizedOrNotFound."}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Static table of the public commercial regions (key and name); the list is a snapshot and does not track new regions."}
  ListRegionSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "A single READY home-region subscription (IAD, us-ashburn-1). CreateRegionSubscription is not implemented."}
  ListAvailabilityDomains: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Three ADs named <4 letter tenancy prefix>:US-ASHBURN-AD-<n> with deterministic ids; compartmentId must exist."}
  ListFaultDomains: {wire: ok, errors: partial, state: ok, persist: n/a, note: "FAULT-DOMAIN-1..3 for a known AD (case-insensitive); an unknown AD is answered 400 InvalidParameter, unverified against the live service."}
  CreateCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name 1 to 100 characters unique among live siblings (case-insensitive, 409 CompartmentAlreadyExists), description 1 to 400, nesting limited to six levels below the tenancy and 1000 compartments (400 LimitExceeded). Parent must be ACTIVE."}
  GetCompartment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header. The tenancy id returns the synthesised root compartment."}
  ListCompartments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; compartmentIdInSubtree only on the tenancy (400 otherwise); accessLevel ANY or ACCESSIBLE accepted but no per-user access is modelled; name and lifecycleState filters; sortBy TIMECREATED or NAME, default ascending; paged with opc-next-page. All states are listed unless filtered."}
  UpdateCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, description, tags; If-Match gives 412 NoEtagMatch; the root compartment cannot be updated here."}
  DeleteCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id; ACTIVE to DELETING to DELETED after two seconds on the injectable clock. Refuses with 409 Conflict while the compartment holds live child compartments, policies, tag namespaces or tag defaults. Resources of other emulated services are not consulted."}
  MoveCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Re-parents the subtree at once and returns a work request (operationType MOVE_COMPARTMENT, unverified; the SDK enum lists only deletions). Rejects the current parent, the compartment itself or a descendant, a move that would exceed six levels, and sibling name collisions."}
  RecoverCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "DELETED back to ACTIVE within 90 days of deletion; 409 IncorrectState otherwise or when the name is now taken."}
  CreateUser: {wire: ok, errors: partial, state: ok, persist: ok, note: "Tenancy compartment only; name 1 to 255 unique case-insensitively (409 Conflict), description required, email checked only for an @; 2000 users. Capabilities default to all true. No identity domains."}
  GetUser: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "name and lifecycleState filters, sortBy, paged."}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "description, email, dbUserName, tags with If-Match."}
  DeleteUser: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while the user is in a group. Removes the user's credentials and unregisters its API keys from the shared signature registry."}
  CreateGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name 1 to 100 unique case-insensitively, 250 groups per tenancy."}
  GetGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "description and tags."}
  DeleteGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict unless the group is empty."}
  AddUserToGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict for a duplicate membership, 250 groups per user."}
  GetUserGroupMembership: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListUserGroupMemberships: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId must be the tenancy; userId and groupId filters."}
  RemoveUserFromGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateDynamicGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "matchingRule parsed as ANY or ALL {variable = or != 'value' or /pattern/, ...}; only the syntax is checked, variable names are not. 50 dynamic groups."}
  GetDynamicGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListDynamicGroups: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateDynamicGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "matchingRule is re-validated."}
  DeleteDynamicGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreatePolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "Every statement is parsed against the documented grammar (Allow and Deny with subject, verb inspect/read/use/manage or a permission list, resource, location and optional where conditions; Define, Endorse, Admit); 1 to 50 statements, 100 policies, name unique tenancy-wide, versionDate YYYY-MM-DD. Compartment and group names inside statements are not resolved."}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; versionDate null when unset."}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One compartment (no subtree)."}
  UpdatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "statements replace the list and are re-validated."}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  UploadApiKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "PEM RSA public key of at least 2048 bits; fingerprint is the colon MD5 of the PKIX DER computed by pkgs/ociauth; the key is registered in the registry every OCI service verifies against. 3 keys per user (400 LimitExceeded), duplicate fingerprint 409 Conflict; the limit and conflict codes are unverified."}
  ListApiKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Includes keyValue, keyId (tenancy/user/fingerprint) and fingerprint."}
  DeleteApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "Also unregisters the key, so later signatures with it are rejected."}
  CreateAuthToken: {wire: ok, errors: partial, state: ok, persist: ok, note: "The 20 character token is returned only here and is not stored. 2 per user (400 LimitExceeded, unverified code)."}
  ListAuthTokens: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the token."}
  UpdateAuthToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "description."}
  DeleteAuthToken: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateCustomerSecretKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "A 44 character base64 secret returned once; the id is a 40 hex access key. 2 per user. Not usable for S3-compatible authentication."}
  ListCustomerSecretKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the secret."}
  UpdateCustomerSecretKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName."}
  DeleteCustomerSecretKey: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateSmtpCredential: {wire: ok, errors: partial, state: ok, persist: ok, note: "username <user ocid>@<tenancy ocid>.ab.com, password returned once. 2 per user."}
  ListSmtpCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the password."}
  UpdateSmtpCredential: {wire: ok, errors: ok, state: ok, persist: ok, note: "description."}
  DeleteSmtpCredential: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateOrResetUIPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "A 16 character password returned only in this response; each call replaces the previous one. Console login is not emulated."}
  GetUserUIPasswordInformation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "404 until a password has been created."}
  CreateTagNamespace: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name unique tenancy-wide case-insensitively. lifecycleState is INACTIVE while retired."}
  GetTagNamespace: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListTagNamespaces: {wire: ok, errors: ok, state: ok, persist: n/a, note: "includeSubcompartments and lifecycleState filters."}
  UpdateTagNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "description, isRetired, tags."}
  DeleteTagNamespace: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict while tag definitions remain."}
  CreateTag: {wire: ok, errors: partial, state: ok, persist: ok, note: "Unique per namespace; validator stored verbatim, ENUM values enforced for tag defaults; at most 10 cost-tracking tags."}
  GetTag: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  UpdateTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "description, isRetired, isCostTracking, validator, tags."}
  DeleteTag: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with a DELETE_TAG_DEFINITION work request; DELETING then DELETED; the tag's defaults are removed at once."}
  CreateTagDefault: {wire: ok, errors: partial, state: ok, persist: ok, note: "One default per tag per compartment (409 Conflict); the value is checked against an ENUM validator."}
  GetTagDefault: {wire: ok, errors: ok, state: ok, persist: n/a, note: ""}
  ListTagDefaults: {wire: ok, errors: ok, state: ok, persist: n/a, note: "id, compartmentId, tagDefinitionId filters."}
  UpdateTagDefault: {wire: ok, errors: ok, state: ok, persist: ok, note: "value and isRequired."}
  DeleteTagDefault: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  ChangeTagNamespaceCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /tagNamespaces/{id}/actions/changeCompartment, 200 with no body (the SDK response carries no work request); moves the namespace and its tag definitions at once, the target must be an ACTIVE compartment; opc-retry-token replays. isLockOverride is ignored (locks are not modelled)."}
  CascadeDeleteTagNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "202 with a DELETE_NON_EMPTY_TAG_NAMESPACE work request; the namespace must be retired first (409 Conflict); its tag definitions and their defaults go DELETING then DELETED together with the namespace."}
  GetTaggingWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "/taggingWorkRequests/{id}; only DELETE_TAG_DEFINITION and DELETE_NON_EMPTY_TAG_NAMESPACE work requests are visible."}
  ListTaggingWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; resourceIdentifier filter; bare array."}
  ListTaggingWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Always empty: no tagging work request fails."}
  ListTaggingWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "An accepted line, then a succeeded line once the transition passed."}
  GetWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "IN_PROGRESS until the resource transition time, then SUCCEEDED."}
  ListWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required (the tenancy lists all); resourceIdentifier filter."}
families:
  auth: {status: ok, note: "pkgs/ociauth verifies draft-cavage signatures. API keys uploaded through UploadApiKey, and keys registered through POST /_gopherstack/oci/keys on this listener, are visible to Object Storage, Vault and Streaming through a linked registry; --oci-identity-enforce-auth rejects unsigned requests and unknown keys here."}
  shared_key_registry: {status: ok, note: "cli.go links each OCI service's own registry to the Identity registry (ociauth.Registry.Link), so one upload authenticates against every OCI service; keys registered by the admin endpoint of another OCI service stay local to that service. Deleting a key or its user stops verification everywhere."}
  error_envelope: {status: partial, note: "Errors are {code,message} with opc-request-id. InvalidParameter, MissingParameter, NotAuthenticated, NotAuthorizedOrNotFound, NoEtagMatch and IncorrectState follow the OCI reference; CompartmentAlreadyExists, TagNamespaceAlreadyExists, TagDefinitionAlreadyExists, Conflict and LimitExceeded are unverified."}
  lifecycle: {status: ok, note: "Compartment and tag deletion take two seconds on the injectable backend clock, applied on each request and by a one-minute sweeper."}
  routing_isolation: {status: ok, note: "Own http.Server bound in StartWorker to --oci-identity-port / OCI_IDENTITY_PORT (default 8153); never on the shared AWS router; cli.go reserves the port in the shared pool."}
  observability: {status: ok, note: "telemetry.WrapEchoHandler feeds ExtractOperation/ExtractResource; one lockmetrics.RWMutex guards all state."}
items_still_open:
  - "IAM policy enforcement is not implemented: policies and dynamic groups are stored and validated but never evaluated, so no request is authorised or denied by them; any verified principal may call any operation."
  - "Identity domains (domains, SCIM-style users, MFA devices, OAuth2 client credentials, DB credentials, Swift passwords), identity providers and group mappings, network sources, authentication policy and the tenancy-level region subscription, bulk tag operations (BulkDeleteTags, BulkEditTags, ListBulkEditTagsResourceTypes), standard tag templates and the IAM work request operations (/iamWorkRequests, whose operation types in oci-go-sdk identity/iam_work_request.go are all identity-domain operations) are not implemented. Tag retire and reactivate are not separate SDK operations: they are isRetired on UpdateTag and UpdateTagNamespace (identity_client.go)."
  - "Customer secret keys, auth tokens, SMTP credentials and the UI password are generated and listed but not stored, so they cannot be used for S3-compatible, registry, SMTP or console authentication."
  - "DeleteCompartment only checks the IAM resources it owns; resources other emulated OCI services keep in a compartment do not block deletion."
  - "Statement subjects and locations are parsed but group, dynamic-group and compartment names are not resolved against existing resources, and variable and resource-type names are not checked against the documented lists."
  - "Per-operation error mapping for duplicate names (409 Conflict), per-user credential limits and tenancy limits (400 LimitExceeded) is undocumented, and MOVE_COMPARTMENT is not among the SDK's work request operation types (DELETE_COMPARTMENT and DELETE_TAG_DEFINITION)."
  - "Access levels are not modelled: accessLevel is validated but every caller sees every compartment, and inactive states other than DELETING and DELETED are never produced."
  - "opc-retry-token replay is not applied to credential creation (auth tokens, secret keys, SMTP credentials, API keys), whose replies carry secrets that are not stored; resource principals and instance principals are not applied and only API-key signatures are verified."
---

## 2026-11-13 pass

- Dynamic group matching rules accept `any|all {condition, ...}` over the documented variables only: instance.compartment.id, instance.id, resource.id, resource.compartment.id, resource.type and tag.<namespace>.<key>.value (https://docs.oracle.com/iaas/Content/Identity/dynamicgroups/Writing_Matching_Rules_to_Define_Dynamic_Groups.htm). The page documents `=` and `!=`; the emulator also accepts the other comparison operators shared with policy conditions.
- CreateCompartment, CreateUser, CreateGroup, CreateDynamicGroup, CreatePolicy, CreateTagNamespace, CreateTag and CreateTagDefault replay on opc-retry-token for 24 hours (SDK request docs) and answer 409 InvalidatedRetryToken once the resource was deleted (https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm); tokens persist in the snapshot as retryTokens.
- TagDefinition resolves a namespace and key (case-insensitive, documented in https://docs.oracle.com/en-us/iaas/Content/Tagging/Concepts/taggingoverview.htm) for the tag checks of OCI Streaming and OCI Queue; retired namespaces and keys are reported as retired.
