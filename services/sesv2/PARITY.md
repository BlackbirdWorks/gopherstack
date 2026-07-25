---
service: sesv2
sdk_module: aws-sdk-go-v2/service/sesv2@v1.60.1   # version audited against
last_audit_commit: 7c297a53bedf9d9ba2f5af48da992b024774083f
last_audit_date: 2026-07-25
overall: A            # route-matcher rewrite + wire-shape DTOs; this pass derived real data for BatchGetMetricData/campaigns/statistics/recommendations and finished the typed-DTO conversion (SendBulkEmail, Tenant/MultiRegionEndpoint/ReputationEntity)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateEmailIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEmailIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEmailIdentities: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEmailIdentity: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationSets: {wire: fixed, errors: ok, state: ok, persist: ok, note: "ConfigurationSets was []{Name} objects; real shape is []string. handler.go:listConfigurationSetsOutput"}
  DeleteConfigurationSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationSetEventDestinations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "items marshalled internal EventDestination struct (lowerCamelCase tags, extra ConfigurationSetName/CreatedAt fields); added eventDestinationOutput"}
  DeleteConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationSetEventDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetSendingOptions: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "route required sub-path 'sending-options'; real path is '.../sending'. Unroutable before fix."}
  PutConfigurationSetArchivingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetDeliveryOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetReputationOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetSuppressionOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetTrackingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationSetVdmOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  SendEmail: {wire: ok, errors: ok, state: ok, persist: ok}
  SendBulkEmail: {wire: fixed, errors: ok, state: ok, persist: ok, note: "request body was parsed into map[string]any with ad-hoc type assertions; now typed (bulkEmailEntry/bulkEmailDestination/messageHeader/messageTag/replacementEmailContent/replacementTemplate in send_email.go, field-diffed against types.BulkEmailEntry et al), and the response uses bulkEmailEntryResultOutput (types.BulkEmailEntryResult) instead of a raw map. Functional behavior unchanged."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContactList: {wire: ok, errors: ok, state: ok, persist: ok}
  GetContactList: {wire: fixed, errors: ok, state: ok, persist: ok, note: "marshalled internal ContactList struct directly (lowerCamelCase tags, wrong field name 'name' vs 'ContactListName'); added contactListOutput"}
  ListContactLists: {wire: fixed, errors: ok, state: ok, persist: ok, note: "item shape now matches types.ContactList (ContactListName+LastUpdatedTimestamp only)"}
  DeleteContactList: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContactList: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateContact: {wire: ok, errors: ok, state: ok, persist: ok}
  GetContact: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added contactOutput (PascalCase, epoch timestamps, TopicPreferences item casing)"}
  ListContacts: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real route is POST .../contacts/list with NextToken/Filter in the JSON body, not GET .../contacts with a query string; gopherstack had fabricated the GET route and it was completely unroutable by a real SDK client"}
  DeleteContact: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateContact: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEmailTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "TemplateContent.HTML tag was 'html'/'text'/'subject' lowercase and top-level CreatedAt leaked into the response; real field is 'Html' and GetEmailTemplateOutput has no timestamp"}
  ListEmailTemplates: {wire: fixed, errors: ok, state: ok, persist: ok, note: "metadata items now use TemplateName+CreatedTimestamp (types.EmailTemplateMetadata), no content"}
  DeleteEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  TestRenderEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDedicatedIpPool: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDedicatedIpPool: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response was the bare internal struct (lowerCamelCase, no 'DedicatedIpPool' wrapper); real shape is {DedicatedIpPool: {PoolName, ScalingMode}}"}
  ListDedicatedIpPools: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDedicatedIpPool: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDedicatedIpPoolScalingAttributes: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "route required sub-path 'scaling-attributes'; real path is '.../scaling'. Unroutable before fix."}
  GetDedicatedIp: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDedicatedIps: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDedicatedIpInPool: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDedicatedIpWarmupAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutSuppressedDestination: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "top-level path was fabricated as '/v2/email/suppressed-destination'; real path family is '/v2/email/suppression/addresses[/{EmailAddress}]'. All 4 ops in this family were completely unroutable before fix."}
  GetSuppressedDestination: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "also needed a {SuppressedDestination: {...}} wrapper and PascalCase fields"}
  DeleteSuppressedDestination: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed}
  ListSuppressedDestinations: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed}
  CreateCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomVerificationEmailTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added customVerificationEmailTemplateOutput (PascalCase)"}
  ListCustomVerificationEmailTemplates: {wire: fixed, errors: ok, state: ok, persist: ok, note: "metadata items (no TemplateContent) now use customVerificationEmailTemplateMetadataOutput"}
  DeleteCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCustomVerificationEmailTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  SendCustomVerificationEmail: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "POST /v2/email/outbound-custom-verification-emails was not matched by any path pattern at all; added parseOutboundCustomVerificationEmailsPath"}
  GetAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBlacklistReports: {wire: ok, errors: ok, state: ok, persist: n/a}
  PutAccountDetails: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccountDedicatedIpWarmupAttributes: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "sub-path was 'dedicated-ip-warmup-attributes' (2 segs); real path is 3 segs, 'account/dedicated-ips/warmup'. Unroutable before fix."}
  PutAccountSendingAttributes: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "sub-path was 'sending-attributes'; real is 'sending'. Unroutable before fix."}
  PutAccountSuppressionAttributes: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "sub-path was 'suppression-attributes'; real is 'suppression'. Unroutable before fix."}
  PutAccountVdmAttributes: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "sub-path was 'vdm-attributes'; real is 'vdm'. A dead top-level '/v2/email/vdm-attributes' route (not a real SES path at all) was also removed."}
  BatchGetMetricData: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "now derives real per-day SEND counts from b.emails (gopherstack's actual send history) for Metric=SEND with no dimension or the EMAIL_IDENTITY dimension (matched against each Email's From address/domain, same resolution SendEmail uses) -- genuine aggregated data, not a placeholder. Every other Metric (COMPLAINT/PERMANENT_BOUNCE/TRANSIENT_BOUNCE/OPEN/CLICK/DELIVERY*) and the CONFIGURATION_SET/ISP dimensions have no backing data source (no bounce/complaint/engagement pipeline, no per-email config-set/ISP association) and honestly fall back to a single zero-valued datapoint rather than a fabricated count. Values is now []int64 (was []float64), matching types.MetricDataResult. Request StartDate/EndDate/Dimensions were previously silently dropped by the handler; now decoded (JSON-body epoch-seconds, per serializers.go)."}
  CreateExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExportJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "CreateExportJob/GetExportJob leaked lowerCamelCase jobId/jobStatus/createdAt; added exportJobOutput"}
  CancelExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExportJobs: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/list-export-jobs (filter/pagination in body) -- a distinct top-level path from /v2/email/export-jobs, not a GET on that same path. Previous GET-based route was gopherstack-invented and unroutable by a real client; removed and replaced."}
  CreateImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetImportJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same lowerCamelCase leak as ExportJob; added importJobOutput"}
  ListImportJobs: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/import-jobs/list (filter/pagination in body), not GET /v2/email/import-jobs. Previous GET-based route removed and replaced."}
  CreateEmailIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEmailIdentityPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEmailIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEmailIdentityPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailIdentityConfigurationSetAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailIdentityDkimAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailIdentityDkimSigningAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailIdentityFeedbackAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailIdentityMailFromAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeliverabilityTestReport: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeliverabilityTestReport: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "real sub-path is 'test-reports[/{ReportId}]', not 'reports[/{id}]'. Fixed alongside ListDeliverabilityTestReports."}
  ListDeliverabilityTestReports: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "same 'test-reports' vs 'reports' route fix as GetDeliverabilityTestReport."}
  GetDeliverabilityDashboardOptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "now reflects PutDeliverabilityDashboardOption state (DashboardEnabled/AccountStatus/ActiveSubscribedDomains); previously hardcoded {DashboardEnabled:false}"}
  PutDeliverabilityDashboardOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a true no-op; now persists enablement + subscribed-domain list (b.deliverabilityDashboardEnabled/b.deliverabilityDashboardDomains, wired into Reset/Snapshot/Restore) so GetDeliverabilityDashboardOptions reflects it"}
  GetDomainDeliverabilityCampaign: {wire: ok, errors: ok, state: fixed, persist: n/a, route: fixed, note: "real path is deliverability-dashboard/campaigns/{CampaignId} (campaign ID only, no domain -- confirmed against GetDomainDeliverabilityCampaignInput). Backend signature changed to drop the domain param. CampaignId/FromAddress/Subject/FirstSeenDateTime/LastSeenDateTime are now derived for real by grouping b.emails (gopherstack's actual send history) by (FromAddress, Subject) -- the same key real SES auto-detects campaigns by -- via campaignIDFor/domainCampaignsLocked (deliverability.go); a campaignID with no matching send history falls back to the prior echoed-ID placeholder rather than NotFoundException, since gopherstack has no way to distinguish a caller-guessed ID from one it legitimately handed out. InboxCount/SpamCount/ReadRate/DeleteRate/ReadDeleteRate/ProjectedVolume/Esps/SendingIps remain honest zero-valued/empty placeholders -- real inbox/spam placement tracking requires opted-in production sending history AWS tracks server-side, which gopherstack genuinely can't derive."}
  GetDomainStatisticsReport: {wire: fixed, errors: ok, state: partial, persist: n/a, route: fixed, note: "real sub-path is 'statistics-report/{Domain}', not 'statistics/{domain}'. DailyVolumes now enumerates one entry per calendar day in the requested [StartDate, EndDate] window (RFC3339 query params, parsed via parseSESv2Timestamp; capped at maxDailyVolumeDays=366) matching what GetDomainStatisticsReportOutput documents (\"data for each day\") -- previously always an empty list regardless of range, which was itself a wire-shape gap, not just a data placeholder. Every actual statistic (VolumeStatistics/DomainIspPlacements/ReadRatePercent, both per-day and in OverallVolume) is an honest zero/empty placeholder: every field in this shape measures inbox-vs-spam delivery placement, which requires real mail-delivery-outcome tracking gopherstack doesn't have -- there's no plausible partial derivation the way there is for the campaign family."}
  ListDomainDeliverabilityCampaigns: {wire: ok, errors: ok, state: fixed, persist: n/a, route: fixed, note: "real path is deliverability-dashboard/domains/{SubscribedDomain}/campaigns; gopherstack's old 'campaigns/{domain}/{id}' pattern actively misrouted a real GET to campaigns/{CampaignId} as this op with the campaign ID misread as a domain. Now derives real campaigns from b.emails via domainCampaignsLocked (see GetDomainDeliverabilityCampaign), filtered to messages with a recipient in the subscribed domain and restricted to campaigns overlapping [StartDate, EndDate] when both parse. Same InboxCount/etc. placeholder tradeoff as GetDomainDeliverabilityCampaign."}
  GetEmailAddressInsights: {wire: ok, errors: ok, state: partial, persist: n/a, route: fixed, note: "real op is POST /v2/email/email-address-insights with EmailAddress in the body; gopherstack had a fabricated GET /v2/email/email-insights/{email}. HasValidSyntax and IsRoleAddress are now real checks (regex + role-address local-part lookup); HasValidDnsRecords/IsDisposable/IsRandomInput/MailboxExists are honest MEDIUM-confidence placeholders since gopherstack has no DNS/disposable-domain/mailbox-probing data source."}
  GetMessageInsights: {wire: ok, errors: ok, state: ok, persist: n/a, route: fixed, note: "real path is /v2/email/insights/{MessageId}; gopherstack had a fabricated /v2/email/messages/{id}. Was a stub returning {}; now looks up the message in the backend's SendEmail history and returns NotFoundException for an unknown MessageId, matching real semantics -- this is the one insights op gopherstack has genuine data for."}
  ListRecommendations: {wire: ok, errors: ok, state: fixed, persist: n/a, route: fixed, note: "real op is POST /v2/email/vdm/recommendations (Filter/NextToken/PageSize in body); gopherstack had a fabricated GET /v2/email/recommendations. Filter was previously decoded by the handler and silently dropped; now threaded through and applied (TYPE/STATUS/IMPACT/RESOURCE_ARN, ANDed). Now derives real OPEN/HIGH-impact recommendations from gopherstack's actual configuration state: DKIM for identities with DkimSigningEnabled=false, SPF for identities with a MAIL FROM domain that hasn't reached SUCCESS status (gopherstack never simulates async verification, so it's honestly stuck at PENDING), COMPLAINT for reputation entities with CustomerManagedStatus=DISABLED. DMARC/BIMI and reputation-finding-driven types (BOUNCE/FEEDBACK_3P/IP_LISTING) are never returned -- gopherstack has no DNS-record model or bounce/complaint-rate pipeline to derive those from, and fabricating them would be worse than omitting them (see ListRecommendations' doc comment, deliverability.go)."}
  ListReputationEntities: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/reputation/entities (filter/pagination in body); gopherstack only accepted GET. A gopherstack-invented duplicate top-level path, /v2/email/reputation-entities/..., was also found and deleted (not in the real SDK at all; the real 'reputation/entities/...' family already covered every op in this family correctly). Now returns []reputationEntityOutput (typed) instead of []map[string]any."}
  GetReputationEntity: {wire: fixed, errors: ok, state: ok, persist: ok, note: "field-diffed against types.ReputationEntity: ReputationEntityReference/ReputationEntityType/CustomerManagedStatus (nested {Status: ...}, matching *StatusRecord)/ReputationManagementPolicy were already correct; SendingStatusAggregate (derived from CustomerManagedStatus, gopherstack has no separate AWS-SES-managed status to combine it with) unchanged. Now a typed reputationEntityOutput/statusRecordOutput DTO (wire_output.go) instead of an ad-hoc map[string]any -- same field-verified-correct shape, now compile-time checked."}
  UpdateReputationEntityCustomerManagedStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateReputationEntityPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMultiRegionEndpoint: {wire: fixed, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateMultiRegionEndpointOutput (EndpointId/Status); generates a real EndpointId and reads Details.RoutesDetails[].Region/Tags from the body. Returns AlreadyExistsException for a duplicate name. Now returns a typed *createMultiRegionEndpointOutput (wire_output.go) instead of map[string]any."}
  GetMultiRegionEndpoint: {wire: fixed, errors: ok, state: ok, persist: ok, note: "field-diffed against GetMultiRegionEndpointOutput/types.Route: CreatedTimestamp/LastUpdatedTimestamp (epoch)/Routes ([]{Region}, projected from the endpoint's region list) unchanged. Now returns a typed *multiRegionEndpointOutput (wire_output.go) instead of map[string]any."}
  DeleteMultiRegionEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against DeleteMultiRegionEndpointOutput: returns {Status: \"DELETING\"} (the status documented as returned 'right after the delete request'); NotFoundException for an unknown name."}
  ListMultiRegionEndpoints: {wire: fixed, errors: ok, state: ok, persist: ok, note: "item shape matches types.MultiRegionEndpoint (EndpointId/EndpointName/Status/Regions/CreatedTimestamp/LastUpdatedTimestamp, no Routes -- that's Get-only). Now returns []multiRegionEndpointSummaryOutput (typed, wire_output.go) instead of []map[string]any."}
  CreateTenant: {wire: fixed, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateTenantOutput/types.Tenant: TenantId/TenantArn (via pkgs/arn)/SendingStatus/CreatedTimestamp/Tags unchanged. AlreadyExistsException for a duplicate name. Now returns a typed *tenantOutput (wire_output.go) instead of map[string]any."}
  GetTenant: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/get with TenantName in the body (RPC-style, no REST path param); gopherstack had GET /v2/email/tenants/{name}. Response wrapped in {Tenant: {...}} matching GetTenantOutput, now a typed *tenantOutput instead of map[string]any."}
  DeleteTenant: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/delete with TenantName in the body; gopherstack had DELETE /v2/email/tenants/{name}. Cascades: removes the tenant's resource associations from both the tenant->resources and resource->tenants indexes so no ghost rows remain."}
  ListTenants: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/list with NextToken/PageSize in the body; gopherstack had GET /v2/email/tenants. Item shape matches types.TenantInfo (TenantName/TenantId/TenantArn/CreatedTimestamp, no SendingStatus/Tags -- those are Get-only), now []tenantInfoOutput (typed, wire_output.go) instead of []map[string]any."}
  CreateTenantResourceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/resources with TenantName+ResourceArn in the body; gopherstack had POST /v2/email/tenants/{name}/resources. NotFoundException if the tenant doesn't exist."}
  DeleteTenantResourceAssociation: {wire: ok, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/resources/delete with TenantName+ResourceArn in the body; gopherstack had DELETE /v2/email/tenants/{name}/resources/{arn}."}
  ListResourceTenants: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/resources/tenants/list -- a distinct top-level path from the rest of the tenant family (/v2/email/tenants/...) -- with ResourceArn/NextToken/PageSize in the body; gopherstack had the fabricated GET /v2/email/resource-tenants. Item shape matches types.ResourceTenantMetadata (TenantName/TenantId/ResourceArn/AssociatedTimestamp), now []resourceTenantOutput (typed, wire_output.go) instead of []map[string]any."}
  ListTenantResources: {wire: fixed, errors: ok, state: ok, persist: ok, route: fixed, note: "real op is POST /v2/email/tenants/resources/list with TenantName/Filter/NextToken/PageSize in the body; gopherstack had GET /v2/email/tenants/{name}/resources and silently dropped NextToken entirely. Item shape matches types.TenantResource (ResourceArn/ResourceType, inferred from the ARN's resource-segment prefix); the RESOURCE_TYPE filter key is honored. Now []tenantResourceOutput (typed, wire_output.go) instead of []map[string]any."}
# Families audited as a group (when per-op is impractical):
families:
  route-matcher: {status: fixed, note: "Built a full (method,path)->op regression matrix from aws-sdk-go-v2/service/sesv2 v1.60.1 serializers.go (services/sesv2/route_matrix_test.go, 110+ real routes, every real SDK route now covered -- see route_matrix_test.go). Original pass fixed 12/30 unroutable-or-misrouted routes; this pass closed the remaining 18: RPC-style tenant/resource-tenant paths (8 routes), deliverability-dashboard sub-resources (5 routes: test-reports x2, statistics-report, campaigns, domains/.../campaigns), insights/recommendations (3: email-address-insights, insights/{MessageId}, vdm/recommendations), reputation-entity listing (1, plus deletion of a gopherstack-invented duplicate 'reputation-entities' top-level path), and the POST-based list-export-jobs/import-jobs/list variants (2)."}
leaks: {status: clean, note: "no goroutines/janitors spawned; email retention capped at maxRetainedEmails (10000, FIFO-compacted) so SendEmail/SendCustomVerificationEmail can't leak memory on a long-running instance. DeleteTenant now cascades its resource-association index cleanup (both tenantResources and resourceTenants maps) so deleting a tenant with associated resources doesn't leave ghost rows."}
---

## Notes

**Root-cause bug class (fixed in the original pass, ~15 ops):** most of the
"extended" GET/List handlers (contact lists, contacts, suppressed
destinations, dedicated IP pools, event destinations, email templates,
custom verification templates, export/import jobs) marshalled their
**internal storage struct** directly as the HTTP response. Those structs
intentionally keep `lowerCamelCase` JSON tags because they double as the
on-disk snapshot format (persistence.go) — but AWS JSON-protocol responses
need `PascalCase` field names, and several also need a `{Foo: {...}}`
wrapper the internal struct doesn't have (e.g. `GetDedicatedIpPool` →
`{"DedicatedIpPool": {...}}`, `GetSuppressedDestination` →
`{"SuppressedDestination": {...}}`). Fixed by adding a parallel set of
`*Output` wire DTOs + `to*Output()` converters in `wire_output.go`,
**without** touching the internal structs' tags (that would have required
bumping `sesv2SnapshotVersion` and losing old snapshots for no wire-format
benefit). `EmailIdentity`/`ConfigurationSet` already had proper wire DTOs
before that pass (`getEmailIdentityOutput`, `getConfigurationSetOutput`,
etc.) — only `ListConfigurationSets` had a residual bug there (see below).

**`ListConfigurationSets` wire bug:** `ConfigurationSetsOutput.ConfigurationSets`
is `[]string` in the real SDK (plain names), not a list of
`{"Name": "..."}` objects. Confirmed against
`awsRestjson1_deserializeDocumentConfigurationSetNameList` in the SDK's
deserializers.go, which type-asserts each array element directly to `string`
and would fail to decode gopherstack's previous `[{"Name":"foo"}]` shape.

**Route-matcher bug class (fixed across both passes; 30/30 real-SDK
route-matcher gaps now closed):** `parseSESv2Path` (handler.go/handler_routes.go)
had several sub-path segments that were plausible-looking guesses rather
than the real SDK's REST path templates, and a handful of ops (the tenant
family, `ListExportJobs`/`ListImportJobs`) are genuinely RPC-style — a fixed
POST verb path with identifying fields in the JSON body — where gopherstack
had guessed a REST-with-path-param shape instead. Confirmed against
`aws-sdk-go-v2/service/sesv2`'s `serializers.go`
(`httpbinding.SplitURI("...")` + `request.Method = "..."` in each
`awsRestjson1_serializeOp*` type). Original-pass fixes (Account attributes,
SuppressionList, `ListContacts`, `PutConfigurationSetSendingOptions`,
`PutDedicatedIpPoolScalingAttributes`, `SendCustomVerificationEmail`) are
unchanged from the prior audit. This pass's fixes:

- **Tenants / resource-tenants** (`CreateTenant`, `GetTenant`, `DeleteTenant`,
  `ListTenants`, `CreateTenantResourceAssociation`,
  `DeleteTenantResourceAssociation`, `ListResourceTenants`,
  `ListTenantResources`): rewrote `parseTenantPath` to the real RPC-style
  verb paths (`/tenants`, `/tenants/get`, `/tenants/delete`,
  `/tenants/list`, `/tenants/resources`, `/tenants/resources/delete`,
  `/tenants/resources/list`) and added the distinct top-level
  `/resources/tenants/list` route for `ListResourceTenants`. Rewrote every
  handler in `handler_tenants.go` to decode `TenantName`/`ResourceArn` from
  the JSON body instead of a path-derived `resource` string.
- **Deliverability dashboard sub-resources**: `test-reports[/{ReportId}]`
  (was `reports[/{id}]`) and `statistics-report/{Domain}` (was
  `statistics/{domain}`).
- **`GetDomainDeliverabilityCampaign` / `ListDomainDeliverabilityCampaigns`**:
  were actively *misrouted*, not just unreachable — gopherstack's
  `campaigns/{domain}/{id}` pattern meant a real GET to
  `campaigns/{CampaignId}` (2 segments) was misinterpreted as
  `ListDomainDeliverabilityCampaigns` with the campaign ID read as a domain.
  Real paths are `campaigns/{CampaignId}` (Get, no domain param at all —
  `GetDomainDeliverabilityCampaign`'s backend signature dropped the domain
  parameter to match) and `domains/{SubscribedDomain}/campaigns` (List).
- **`GetEmailAddressInsights`**: real op is
  `POST /v2/email/email-address-insights` with `EmailAddress` in the body;
  gopherstack had `GET /v2/email/email-insights/{email}`.
- **`GetMessageInsights`**: real path is `GET /v2/email/insights/{MessageId}`;
  gopherstack had `/v2/email/messages/{id}`.
- **`ListRecommendations`**: real op is `POST /v2/email/vdm/recommendations`;
  gopherstack had `GET /v2/email/recommendations`.
- **`ListReputationEntities`**: real op is `POST /v2/email/reputation/entities`
  (filter/pagination in body); gopherstack only accepted `GET` on that path.
  A gopherstack-invented duplicate top-level path,
  `/v2/email/reputation-entities/...` (with its own copy of Get/List/Update*
  routes), was found alongside the real `/v2/email/reputation/entities/...`
  family and **deleted** — the real family already covered every op
  correctly, so the duplicate was pure invented surface, not a fallback.
- **`ListExportJobs`** / **`ListImportJobs`**: real ops are
  `POST /v2/email/list-export-jobs` and `POST /v2/email/import-jobs/list`
  (filter/pagination in body); gopherstack had fabricated `GET
  /v2/email/export-jobs` and `GET /v2/email/import-jobs` routes that don't
  exist in the real API (the real `/v2/email/export-jobs` path only has
  POST for `CreateExportJob` and GET-with-id for `GetExportJob`).

`services/sesv2/route_matrix_test.go` (`Test_RouteMatrix_AgainstRealSDK`)
now has a case for every real SDK route covered above (110+ cases) — no
routes are omitted from the matrix anymore.

**Wire/state fixes this pass:**

- `PutDeliverabilityDashboardOption` was a true no-op; now persists
  `DashboardEnabled` and the subscribed-domain list
  (`b.deliverabilityDashboardEnabled`/`b.deliverabilityDashboardDomains`,
  wired into `Reset`/`Snapshot`/`Restore`) so `GetDeliverabilityDashboardOptions`
  reflects it, including deriving `AccountStatus` (ACTIVE/DISABLED).
- `MultiRegionEndpoint` family field-diffed against
  `CreateMultiRegionEndpointOutput`/`GetMultiRegionEndpointOutput`/
  `types.MultiRegionEndpoint`/`types.Route`: added `EndpointId` generation,
  `CreatedTimestamp`/`LastUpdatedTimestamp` (epoch), `Routes`
  (`[]{Region}`, one per region the endpoint spans), and
  `AlreadyExistsException`/`NotFoundException` handling that was previously
  entirely absent (Create silently overwrote, Delete silently no-opped on an
  unknown name).
- `Tenant` family field-diffed against `CreateTenantOutput`/`GetTenantOutput`/
  `types.Tenant`/`types.TenantInfo`/`types.ResourceTenantMetadata`/
  `types.TenantResource`: added `TenantId`, `TenantArn` (via `pkgs/arn`,
  not hand-formatted), `SendingStatus`, `CreatedTimestamp`, `Tags`; List
  item shapes correctly trimmed per-type (`TenantInfo` has no
  `SendingStatus`/`Tags`; `TenantResource` has no timestamp).
  `CreateTenantResourceAssociation` now checks the tenant exists
  (`NotFoundException` otherwise) and `DeleteTenant` cascades its
  resource-association index cleanup.
- `GetReputationEntity`/`ListReputationEntities` field-diffed against
  `types.ReputationEntity`: `ReputationEntityReference`/`ReputationEntityType`/
  `CustomerManagedStatus` (nested `{Status: ...}`, matching `*StatusRecord`)/
  `ReputationManagementPolicy` were already correct from the original pass;
  added `SendingStatusAggregate`.

## This pass (2026-07-25): real derivation for the analytics-placeholder ops + typed DTOs

Closed `gopherstack-03th`. Per-op judgment calls (derive-from-real-state vs
genuinely-impossible) are in each op's `note:` above; summary:

- **`BatchGetMetricData`**: SEND metric, no dimension or EMAIL_IDENTITY
  dimension only, is now real (aggregated from `b.emails`). Every other
  metric/dimension combination remains an honest zero-valued datapoint --
  gopherstack has no bounce/complaint/open/click/delivery pipeline and no
  per-email config-set/ISP association to derive them from, and a
  plausible-looking fabricated non-zero count would be strictly worse than
  the honest zero.
- **`GetDomainDeliverabilityCampaign`/`ListDomainDeliverabilityCampaigns`**:
  CampaignId/FromAddress/Subject/FirstSeenDateTime/LastSeenDateTime are now
  derived for real by grouping `b.emails` by `(FromAddress, Subject)` --
  gopherstack's own send history is exactly the data real SES's campaign
  auto-detection is built from, just without AWS's server-side production
  tracking. InboxCount/SpamCount/ReadRate/DeleteRate/ReadDeleteRate/
  ProjectedVolume/Esps/SendingIps remain honest zero/empty: inbox-vs-spam
  placement genuinely requires infrastructure gopherstack doesn't have and
  never will (real mailbox delivery-outcome tracking), so these stay
  documented placeholders rather than invented numbers.
- **`GetDomainStatisticsReport`**: fixed a real wire-shape gap (`DailyVolumes`
  was always `[]` regardless of the requested date range; real SES documents
  one entry per day in range) without fabricating the per-day statistics
  themselves, which have the same inbox/spam-placement-only-AWS-can-know
  problem as the campaign family -- there's no partial derivation available
  here the way there is for campaigns (this shape has no "raw send count"
  field, only inbox/spam splits).
- **`ListRecommendations`**: derives real DKIM/SPF/COMPLAINT recommendations
  from stored identity/reputation-entity configuration state (see the op's
  entry above). DMARC/BIMI and reputation-finding-driven types
  (BOUNCE/FEEDBACK_3P/IP_LISTING) are never returned, not even as
  placeholders -- gopherstack has no DNS-record model or
  bounce/complaint-rate pipeline, and there's no honest zero/empty value to
  report for "does this domain have a DMARC record" the way there is for a
  count.
- **`SendBulkEmail`**: request DTO (`bulkEmailEntry` et al, `send_email.go`)
  and per-entry result DTO (`bulkEmailEntryResultOutput`, `wire_output.go`)
  are now typed, field-diffed against `types.BulkEmailEntry`/
  `types.BulkEmailEntryResult`/`types.Destination`/`types.MessageHeader`/
  `types.MessageTag`/`types.ReplacementEmailContent`/
  `types.ReplacementTemplate`. Functional behavior unchanged (still records
  sent emails with real recipients via the existing `SendEmail` path).
- **`Tenant`/`MultiRegionEndpoint`/`GetReputationEntity`/
  `ListReputationEntities`** now return typed wire DTOs
  (`tenantOutput`/`tenantInfoOutput`/`resourceTenantOutput`/
  `tenantResourceOutput`/`multiRegionEndpointOutput`/
  `multiRegionEndpointSummaryOutput`/`createMultiRegionEndpointOutput`/
  `reputationEntityOutput`/`statusRecordOutput`, all in `wire_output.go`)
  instead of ad-hoc `map[string]any`. The underlying backend storage
  (`b.tenants`/`b.multiRegionEndpoints`: `map[string]map[string]any`) is
  **unchanged** -- those maps are still both the persisted snapshot format
  and an internal staging shape; the typed DTOs are a conversion step added
  at the response boundary (`toTenantOutput`/`toMultiRegionEndpointOutput`/
  etc.), so no `sesv2SnapshotVersion` bump was needed. All fields were
  already field-verified correct from the prior pass; this is a
  compile-time-safety upgrade, not a wire-correctness fix (except where
  individually noted `wire: fixed` above for the type change itself).

**Verification**: every op above is covered by a real
`aws-sdk-go-v2/service/sesv2` client round-trip test (not just a decoded
`map[string]any` or backend-struct assertion) -- see
`newSESv2SDKClient` (`store_test.go`) and the `*SDKRoundTrip` tests in
`tenants_test.go`, `multi_region_endpoints_test.go`, `deliverability_test.go`,
`message_insights_test.go`, `send_email_test.go`.

## Remaining known limitation (not a gap — reachable, correctly routed, AWS-accurate shape)

- `BatchGetMetricData` returns real SEND counts for the SEND/no-dimension and
  SEND/EMAIL_IDENTITY-dimension cases; every other metric/dimension
  combination returns one zero-valued datapoint per query rather than real
  aggregated metrics — gopherstack has no metrics aggregation engine to
  source those specific values from. Envelope shape (`Results: [{Id,
  Timestamps, Values}]`) is correct.
- `GetDomainDeliverabilityCampaign`/`ListDomainDeliverabilityCampaigns`
  derive real campaign identity/timing from send history (see "This pass"
  above); `GetDomainStatisticsReport`'s per-day/overall statistics are
  zero-valued placeholders. All three require either opted-in-and-AWS-tracked
  production inbox/spam-placement data or a reputation findings engine
  gopherstack doesn't have for the fields that remain placeholder.
- `GetEmailAddressInsights`: `HasValidSyntax` (regex) and `IsRoleAddress`
  (local-part lookup against common role names) are real checks;
  `HasValidDnsRecords`/`IsDisposable`/`IsRandomInput`/`MailboxExists` are
  honest `MEDIUM`-confidence placeholders (no DNS/disposable-domain-list/
  mailbox-probing data source in an emulator).
- `ListRecommendations` derives real DKIM/SPF/COMPLAINT recommendations from
  stored configuration state (see "This pass" above); DMARC/BIMI and
  reputation-finding-driven types are never returned (no DNS-record model,
  no bounce/complaint-rate pipeline).
- SDK-driven integration test coverage (`test/integration/*_parity_test.go`)
  has not been run for this service — this and prior passes added a
  route/path regression test (`route_matrix_test.go`) and real-SDK-client
  round-trip unit tests (see "This pass" above); no `make build-linux` +
  Docker-based integration run was performed.

## Traps for the next auditor

- `EmailIdentity`/`ConfigurationSet`/`Tags` families already had correct
  PascalCase wire DTOs before the original pass (`getEmailIdentityOutput`,
  `dkimAttributesOutput`, `getConfigurationSetOutput`, `tagEntry`, etc.) —
  don't re-flag those as bugs; only `ListConfigurationSets`'
  `ConfigurationSets` field type was wrong.
- Don't "fix" the internal model structs' `lowerCamelCase` JSON tags
  (`EmailIdentity`, `ConfigurationSet`, `ContactList`, `Contact`,
  `SuppressedDestination`, `EmailTemplate`, `DedicatedIPPool`,
  `EventDestination`, etc. in backend.go/backend_ops.go) to fix wire output —
  those tags are the **persisted snapshot format** (persistence.go). Add a
  wire DTO in `wire_output.go` instead; changing the internal tags would
  require bumping `sesv2SnapshotVersion` and silently discarding every
  existing snapshot on upgrade for no wire benefit.
- The `tenants`/`multiRegionEndpoints` backend fields are still intentionally
  `map[string]map[string]any` with PascalCase keys (e.g. `keyTenantName =
  "TenantName"`) rather than typed structs — unlike `EmailIdentity`/
  `ConfigurationSet`/etc., these maps store the **wire-shaped** data
  directly, so adding a field there is adding it to both the snapshot and
  the eventual response in one place (build the map in `tenants.go`/
  `multi_region_endpoints.go` as before). **As of this pass, though, every
  backend method that used to *return* one of these maps directly now
  converts it through a typed `wire_output.go` DTO first**
  (`toTenantOutput`/`toMultiRegionEndpointOutput`/etc.) — don't reintroduce a
  bare `map[string]any` return type on a tenant/multi-region-endpoint/
  reputation-entity op; add fields to the existing DTO struct + its
  `to*Output` converter instead. `GetReputationEntity`/
  `ListReputationEntities` are simpler: their internal storage
  (`b.reputationEntities`, a `*store.Table[ReputationEntity]`) was already a
  typed struct — only the response conversion (`reputationEntityToMap` →
  `toReputationEntityOutput`) changed.
- `campaignIDFor`/`domainCampaignsLocked` (deliverability.go) derive
  deliverability-dashboard "campaigns" as a **pure function** of `b.emails`
  (grouped by `(FromAddress, Subject)`, hashed to a stable ID) — there is no
  separate campaign index/table and none should be added; a real campaign
  index would need its own persisted ID-generation state to stay stable
  across restarts, which the hash-of-(From,Subject) approach gets for free
  without touching `persistence.go`. Don't "fix" `GetDomainDeliverabilityCampaign`
  to return `NotFoundException` for an unrecognized `CampaignId` —
  `TestGetDomainDeliverabilityCampaignFields` (deliverability_test.go)
  documents that gopherstack has no way to distinguish a caller-guessed ID
  from a legitimately-issued one, so it echoes the placeholder shape instead
  (see that op's PARITY note above).
- `parseSESv2Timestamp` (deliverability.go) accepts both RFC3339 (the real
  wire format for the `GetDomainStatisticsReport`/
  `ListDomainDeliverabilityCampaigns` query-string `StartDate`/`EndDate`
  params, confirmed against serializers.go's `smithytime.FormatDateTime`)
  and a bare `YYYY-MM-DD` date (for backend-direct callers/tests). The
  `BatchGetMetricData` JSON-body `StartDate`/`EndDate` fields use a
  *different* wire format (epoch-seconds numbers, `smithytime.FormatEpochSeconds`)
  and are decoded separately via `epochSecondsToTime`
  (handler_message_insights.go) — don't conflate the two parsers.
- `route_matrix_test.go`'s case table now covers every real SDK route this
  service routes to (110+ cases, including the RPC-style tenant/
  resource-tenant paths and deliverability-dashboard sub-resources added
  this pass) — if you add a new op, add its route(s) here too.
- `newSESv2SDKClient` (`store_test.go`) stands up the real
  `aws-sdk-go-v2/service/sesv2` client against an in-process `httptest`
  server running the handler through the same `pkgs/service` router used in
  production — prefer it over hand-decoded `map[string]any` response
  assertions for any new DTO-conversion test; it's what actually proves wire
  compatibility (see the `*SDKRoundTrip` tests added this pass for the
  pattern).
