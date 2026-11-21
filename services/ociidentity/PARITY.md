---
service: ociidentity
sdk_module: github.com/oracle/oci-go-sdk/v65@v65.126.2   # go.mod dependency (test-only): drives every operation in the service tests
last_audit_commit: 0d22fe6b7
last_audit_date: 2026-10-25
overall: B
ops:
  GetTenancy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "One fixed tenancy (TenancyID, also the root compartment id); homeRegionKey IAD. Any other id is 404 NotAuthorizedOrNotFound."}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Static table of the public commercial regions (key and name); the list is a snapshot and does not track new regions."}
  ListRegionSubscriptions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "The READY home-region subscription (IAD, us-ashburn-1) followed by the regions added with CreateRegionSubscription, each IN_PROGRESS for two seconds on the injectable clock and then READY."}
  CreateRegionSubscription: {wire: ok, errors: partial, state: ok, persist: ok, note: "regionKey (or region name) must be in the region table (400 InvalidParameter); the home region or an existing subscription is 409 Conflict (unverified code); opc-retry-token replays and answers 409 InvalidatedRetryToken once the subscription no longer exists. Subscribing only records the subscription; no region-level state is replicated."}
  CreateNetworkSource: {wire: ok, errors: partial, state: ok, persist: ok, note: "Tenancy compartment only; name 1 to 100 unique case-insensitively (409 Conflict), description required; publicSourceList entries must be IPs or CIDRs, virtualSourceList needs a VCN OCID prefix (VCNs of another service are not resolved) and IP ranges; at most 500 entries and 200 sources (400 LimitExceeded, unverified). ACTIVE at once; opc-retry-token replays."}
  GetNetworkSource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListNetworkSources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId must be the tenancy; name and lifecycleState filters, sortBy TIMECREATED or NAME, paged."}
  UpdateNetworkSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "description, source lists (an omitted list is kept, an empty one clears it), services, tags; If-Match gives 412 NoEtagMatch."}
  DeleteNetworkSource: {wire: ok, errors: partial, state: ok, persist: ok, note: "204; 409 Conflict (unverified) while the authentication policy references the source."}
  GetAuthenticationPolicy: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Only the tenancy id is accepted (404 otherwise). The default password policy (12 characters, upper, lower and numeric required) is from memory of the console defaults, not from the SDK. etag header."}
  UpdateAuthenticationPolicy: {wire: ok, errors: partial, state: ok, persist: ok, note: "passwordPolicy fields merge (length 1 to 100, an emulator bound); networkPolicy.networkSourceIds replaces the list and each id must be an existing network source (400 InvalidParameter, unverified code). Stored only: passwords and requests are not checked against it."}
  CreateIdentityProvider: {wire: ok, errors: partial, state: ok, persist: ok, note: "SAML2 only (protocol SAML2, productType IDCS or ADFS); metadata is parsed as XML (at most 256 KiB, depth 32, DTDs rejected, never fetched) and must hold a SAML2 IDPSSODescriptor with a signing X509Certificate and a SingleSignOnService, from which signingCertificate and redirectUrl are derived; metadataUrl is stored and never fetched. Name 1 to 100 unique case-insensitively, 50 providers, opc-retry-token replays."}
  GetIdentityProvider: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header; body carries protocol SAML2."}
  ListIdentityProviders: {wire: ok, errors: ok, state: ok, persist: n/a, note: "protocol (SAML2) and compartmentId required; name and lifecycleState filters, sortBy, paged."}
  UpdateIdentityProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "description, metadata (re-parsed), metadataUrl, freeformAttributes, tags; If-Match gives 412."}
  DeleteIdentityProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "Also deletes the provider's group mappings."}
  CreateIdpGroupMapping: {wire: ok, errors: partial, state: ok, persist: ok, note: "groupId must be an existing group (404); an identical idpGroupName and groupId pair is 409 Conflict (unverified); 250 mappings per provider; opc-retry-token replays."}
  GetIdpGroupMapping: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header."}
  ListIdpGroupMappings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Creation order, paged."}
  UpdateIdpGroupMapping: {wire: ok, errors: ok, state: ok, persist: ok, note: "idpGroupName and groupId; If-Match gives 412."}
  DeleteIdpGroupMapping: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  BulkEditTags: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id (a BULK_EDIT_OF_TAGS tagging work request: ACCEPTED, IN_PROGRESS after one second, then SUCCEEDED, PARTIALLY_SUCCEEDED or FAILED at two seconds on the injectable clock, when the edits land). Edits defined tags of users, groups, dynamic groups, policies, compartments and tag namespaces (ADD_WHERE_ABSENT, SET_WHERE_PRESENT, ADD_OR_SET, REMOVE; retired tags and ENUM values are refused per resource). Resources of other services, unknown types and missing ids become per-resource errors of the work request. At most 1000 resources and 50 operations (emulator bounds). opc-retry-token replays."}
  BulkDeleteTags: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with a BULK_DELETE_TAG_DEFINITION work request; unknown ids are 404, tag definitions not ACTIVE are per-id work request errors; the others go DELETING at once, their defaults are removed and the tags are stripped from the identity resources when the work request finishes. isLockOverride is ignored."}
  ListBulkEditTagsResourceTypes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "The identity resource types BulkEditTags can edit (compartment, dynamicgroup, group, policy, tagnamespace, user). These type names are the emulator's own; the SDK documents only the shape."}
  ListAvailabilityDomains: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Three ADs named <4 letter tenancy prefix>:US-ASHBURN-AD-<n> with deterministic ids; compartmentId must exist."}
  ListFaultDomains: {wire: ok, errors: partial, state: ok, persist: n/a, note: "FAULT-DOMAIN-1..3 for a known AD (case-insensitive); an unknown AD is answered 400 InvalidParameter, unverified against the live service."}
  CreateCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "Name 1 to 100 characters unique among live siblings (case-insensitive, 409 CompartmentAlreadyExists), description 1 to 400, nesting limited to six levels below the tenancy and 1000 compartments (400 LimitExceeded). Parent must be ACTIVE."}
  GetCompartment: {wire: ok, errors: ok, state: ok, persist: n/a, note: "etag header. The tenancy id returns the synthesised root compartment."}
  ListCompartments: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; compartmentIdInSubtree only on the tenancy (400 otherwise); accessLevel ANY or ACCESSIBLE accepted but no per-user access is modelled; name and lifecycleState filters; sortBy TIMECREATED or NAME, default ascending; paged with opc-next-page. All states are listed unless filtered."}
  UpdateCompartment: {wire: ok, errors: ok, state: ok, persist: ok, note: "name, description, tags; If-Match gives 412 NoEtagMatch; the root compartment cannot be updated here."}
  DeleteCompartment: {wire: ok, errors: partial, state: ok, persist: ok, note: "202 with opc-work-request-id; ACTIVE to DELETING to DELETED after two seconds on the injectable clock. Refuses with 409 Conflict while the compartment holds live child compartments, policies, tag namespaces or tag defaults, or resources of the services registered through AddCompartmentUsage (Object Storage buckets, DNS zones, views, TSIG keys and steering policies, Streaming pools, streams and harnesses), asked outside the backend lock. A resource that appears during the two second window sends the compartment back to ACTIVE and fails the work request (FAILED, error Conflict)."}
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
  DeleteGroup: {wire: ok, errors: partial, state: ok, persist: ok, note: "409 Conflict unless the group is empty and has no identity provider group mapping (the mapping case is an emulator choice; the SDK does not say)."}
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
  CreateAuthToken: {wire: ok, errors: partial, state: ok, persist: ok, note: "The 20 character token is returned only here (an opc-retry-token replay within 24 hours returns it again from memory only; after a restart the replay is InvalidatedRetryToken); only a salted PBKDF2-SHA256 digest is kept (4096 rounds, constant-time compare) so OCI Container Registry logins can be verified (VerifyAuthToken). 2 per user (400 LimitExceeded, unverified code)."}
  ListAuthTokens: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the token."}
  UpdateAuthToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "description."}
  DeleteAuthToken: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateCustomerSecretKey: {wire: ok, errors: partial, state: ok, persist: ok, note: "A 44 character base64 secret returned once and kept in the snapshot (CustomerSecretKey looks it up by access key); the id is a 40 hex access key. 2 per user. opc-retry-token within 24 hours returns the same key and secret, and 409 InvalidatedRetryToken once the key was deleted. Not wired into S3-compatible authentication."}
  ListCustomerSecretKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the secret."}
  UpdateCustomerSecretKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "displayName."}
  DeleteCustomerSecretKey: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateSmtpCredential: {wire: ok, errors: partial, state: ok, persist: ok, note: "username <user ocid>@<tenancy ocid>.ab.com, password returned once and kept in the snapshot (VerifySMTPCredential, constant-time). 2 per user. opc-retry-token replays like secret keys. Not wired into SMTP delivery."}
  ListSmtpCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Never includes the password."}
  UpdateSmtpCredential: {wire: ok, errors: ok, state: ok, persist: ok, note: "description."}
  DeleteSmtpCredential: {wire: ok, errors: ok, state: ok, persist: ok, note: ""}
  CreateOrResetUIPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "A 16 character password returned only in this response; each call replaces the previous one. Only a salted PBKDF2 digest is kept (VerifyUIPassword). opc-retry-token replays the password while it is still current, from memory only, and answers InvalidatedRetryToken after a replacement or restart. Console login is not emulated."}
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
  GetTaggingWorkRequest: {wire: ok, errors: ok, state: ok, persist: ok, note: "/taggingWorkRequests/{id}; DELETE_TAG_DEFINITION, DELETE_NON_EMPTY_TAG_NAMESPACE, BULK_DELETE_TAG_DEFINITION and BULK_EDIT_OF_TAGS work requests are visible, with per-resource results for the bulk ones."}
  ListTaggingWorkRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "compartmentId required; resourceIdentifier filter; bare array."}
  ListTaggingWorkRequestErrors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Errors of finished BULK_EDIT_OF_TAGS and BULK_DELETE_TAG_DEFINITION work requests, one per failed resource; empty for the others."}
  ListTaggingWorkRequestLogs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "An accepted line, then a succeeded line once the transition passed; bulk work requests add a started line and one line per resource."}
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
  - "Identity domains (domains, SCIM-style users, MFA devices, OAuth2 client credentials, DB credentials, Swift passwords), ListIdentityProviderGroups and ResetIdpScimClient (the emulator has no identity provider group directory or SCIM server; SAML metadata carries no groups), standard tag templates (ListStandardTagNamespaces, GetStandardTagTemplate, ImportStandardTags: the SDK has only the shapes, not the template catalogue) and the IAM work request operations (/iamWorkRequests: every operation type in oci-go-sdk identity/iam_work_request.go is an identity-domain operation, so none can exist without domains) are not implemented. Tag retire and reactivate are not separate SDK operations: they are isRetired on UpdateTag and UpdateTagNamespace (identity_client.go)."
  - "Customer secret keys, SMTP credentials and the UI password are stored but not used: they do not authenticate S3-compatible, SMTP or console requests. Auth tokens authenticate OCI Container Registry logins only."
  - "DeleteCompartment consults Object Storage, DNS and Streaming only; resources the other emulated OCI services (Core, Database, Functions, Load Balancer, Events, Logging, NoSQL, File Storage, Queue, Vault, API Gateway, Container Engine and Instances, Artifacts, Notifications, Monitoring, MySQL) keep in a compartment do not block deletion."
  - "Statement subjects and locations are parsed but group, dynamic-group and compartment names are not resolved against existing resources, and variable and resource-type names are not checked against the documented lists (the OCI documentation reachable here does not say whether unknown names are rejected, so they are accepted)."
  - "Access levels are not modelled: accessLevel is validated but every caller sees every compartment, and inactive states other than DELETING and DELETED are never produced."
  - "Resource principals and instance principals are not applied and only API-key signatures are verified."
---

## 2026-11-13 pass

- Dynamic group matching rules accept `any|all {condition, ...}` over the documented variables only: instance.compartment.id, instance.id, resource.id, resource.compartment.id, resource.type and tag.<namespace>.<key>.value (https://docs.oracle.com/iaas/Content/Identity/dynamicgroups/Writing_Matching_Rules_to_Define_Dynamic_Groups.htm). The page documents `=` and `!=`; the emulator also accepts the other comparison operators shared with policy conditions.
- CreateCompartment, CreateUser, CreateGroup, CreateDynamicGroup, CreatePolicy, CreateTagNamespace, CreateTag and CreateTagDefault replay on opc-retry-token for 24 hours (SDK request docs) and answer 409 InvalidatedRetryToken once the resource was deleted (https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm); tokens persist in the snapshot as retryTokens.
- TagDefinition resolves a namespace and key (case-insensitive, documented in https://docs.oracle.com/en-us/iaas/Content/Tagging/Concepts/taggingoverview.htm) for the tag checks of OCI Streaming and OCI Queue; retired namespaces and keys are reported as retired.

## 2026-11-20 pass

- A repeated opc-retry-token replays the first result whatever the body; the OCI error reference (https://docs.oracle.com/en-us/iaas/Content/API/References/apierrors.htm) documents only InvalidatedRetryToken and no conflict for a changed body.
- Bulk tag work requests are tagging work requests (BULK_EDIT_OF_TAGS, BULK_DELETE_TAG_DEFINITION in identity/tagging_work_request.go), fetched through /taggingWorkRequests; the IAM work request type has domain operations only.

## Emulator choices where the vendor is silent

- Per-operation error mapping for duplicate names (409 Conflict), per-user credential limits and tenancy limits (400 LimitExceeded) is undocumented, and MOVE_COMPARTMENT is not among the SDK's work request operation types (DELETE_COMPARTMENT and DELETE_TAG_DEFINITION).
- Auth tokens created before digests were stored (snapshots from older builds) have nothing to verify against, so registry logins with them fail closed (401 naming re-creation); delete and re-create the token. Verification always costs one PBKDF2 derivation (4096 rounds) whether or not the user exists, so timing does not reveal user names.
