---
service: servicequotas
sdk_module: aws-sdk-go-v2/service/servicequotas@v1.43.4
last_audit_commit: 64eb45859
last_audit_date: 2026-10-18
overall: B            # all 26 SDK operations are backed by real state and driven through the real SDK client in-package; B until the open items below close.
ops:
  AssociateServiceQuotaTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "Organizations gate: NoAvailableOrganization, AccessDenied for non-management accounts, OrganizationNotInAllFeaturesMode, AWSServiceAccessNotEnabled"}
  CreateSupportCase: {wire: ok, errors: ok, state: ok, persist: ok, note: "opens the case of a PENDING request (CaseId is a synthetic number); InvalidResourceState when not PENDING"}
  DeleteServiceQuotaIncreaseRequestFromTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateServiceQuotaTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "ServiceQuotaTemplateNotInUse unless ASSOCIATED"}
  GetAWSDefaultServiceQuota: {wire: ok, errors: ok, state: ok, persist: ok, note: "default QuotaArn has no account segment"}
  GetAssociationForServiceQuotaTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DISASSOCIATED until first Associate"}
  GetAutoManagementConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "never-started and stopped accounts report OptInStatus DISABLED"}
  GetQuotaUtilizationReport: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated Quotas only once COMPLETED; Utilization is a percentage"}
  GetRequestedServiceQuotaChange: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceQuota: {wire: ok, errors: ok, state: ok, persist: ok, note: "applied value after approved requests; ContextId rejected (no resource-level quotas)"}
  GetServiceQuotaIncreaseRequestFromTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAWSDefaultServiceQuotas: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRequestedServiceQuotaChangeHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "oldest first; Status, ServiceCode and QuotaRequestedAtLevel filters"}
  ListRequestedServiceQuotaChangeHistoryByQuota: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServiceQuotaIncreaseRequestsInTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "AwsRegion and ServiceCode filters"}
  ListServiceQuotas: {wire: ok, errors: ok, state: ok, persist: ok, note: "QuotaCode and QuotaAppliedAtLevel filters; RESOURCE level lists nothing"}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "applied-quota ARNs of the caller account and Region"}
  PutServiceQuotaIncreaseRequestIntoTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "DesiredValue must exceed the default; quota must be adjustable"}
  RequestServiceQuotaIncrease: {wire: ok, errors: ok, state: ok, persist: ok, note: "PENDING -> CASE_OPENED -> APPROVED on the lifecycle dwell (500ms per stage, SERVICEQUOTAS_LIFECYCLE_DELAY); approval updates the applied value; ResourceAlreadyExists while a request for the quota is open; SupportCaseAllowed=false keeps the request PENDING until CreateSupportCase"}
  StartAutoManagement: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExclusionList validated against the catalog; NotificationArn must be an SNS topic ARN"}
  StartQuotaUtilizationReport: {wire: ok, errors: ok, state: ok, persist: ok, note: "PENDING -> IN_PROGRESS -> COMPLETED; one open report per account and Region"}
  StopAutoManagement: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "50 tags, aws: prefix reserved"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAutoManagement: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires an enabled configuration"}
families:
  Catalog: {status: ok, note: "Embedded catalog.json: 11 services (ec2, vpc, lambda, s3, dynamodb, iam, cloudformation, rds, elasticloadbalancing, ecs, eks) with AWS quota codes, names, defaults, units and usage metrics."}
  Requests: {status: ok, note: "Stage times are lazy deadlines settled under the backend lock on every request or quota read, no goroutines."}
  Template: {status: ok, note: "Entries are keyed by management account, Region, service and quota; the Organizations backend is consulted through internal/app/quotas_observability_wiring.go."}
  Reports: {status: ok, note: "The report snapshot is taken when the report completes."}
  AutoManagement: {status: ok, note: "One configuration per account and Region."}
items_still_open:
  - Catalog: only the 11 services above are catalogued; ListServices omits the rest of AWS and codes outside the catalog return NoSuchResourceException. Codes and defaults come from public AWS documentation and are not verified against the live API.
  - ContextId/QuotaContext: resource-level quotas are not modeled; ContextId is rejected with IllegalArgumentException
  - QuotaUtilization: only S3 buckets, EC2 Elastic IPs, VPCs, IAM roles/users/groups and DynamoDB tables report usage; the other catalogued quotas have no live counter
  - AutoManagement: NotifyAndAdjust only notifies (80% and 95% thresholds to the SNS topic); no automatic quota increase is filed
  - AppliedQuotaEnforcement: opt-in (--enforce-service-quotas) for S3 buckets, EC2 Elastic IPs and VPCs, IAM roles/users/groups and DynamoDB tables only; other quotas are never enforced and SQS/SNS/Lambda are not catalogued
  - Template: associating the template does not file increase requests for accounts that join the organization later
  - RequestStatus: DENIED is entered only for desired values above the catalogued hard max (IAM roles, groups, customer managed policies, instance profiles; from public docs, unverified); NOT_APPROVED, CASE_CLOSED and INVALID_REQUEST are never entered
  - Errors: AccessDenied for non-template operations, DependencyAccessDenied, TagPolicyViolation, TemplatesNotAvailableInRegion, TooManyRequests and ServiceException are not raised
gaps: []
structural_gaps:
  - "No AWS Support case system exists in an emulator: CaseId values are synthetic numbers and no support case can be inspected."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; no goroutines or timers."}
---

## Notes

Protocol is awsjson1.1 with target prefix `ServiceQuotasV20190624.`; timestamps are epoch-seconds numbers (`awstime.Epoch`). Shapes come from the pinned SDK schemas and types.

Emulator choices the SDK does not document:

- Applied quota values are per account and Region; an approved request stores the desired value as the applied override.
- A request needs DesiredValue above the current applied value; a quota that is not adjustable is IllegalArgumentException.
- Auto management is evaluated by a one-minute ticker (`Handler.SetTickInterval`); each quota notifies once per threshold crossing (80%, then 95%) and re-arms after utilization falls below 80%.
- Utilization usage and enforcement limits are read through `UsageSource` and `quotalimit.Func` wired in `internal/app/service_quotas_wiring.go`; enforcement of the owning services applies the default or approved value of the caller account and Region.
- Template operations need an organization with all features and service access for `servicequotas.amazonaws.com` enabled, wired from the Organizations backend; a standalone backend without a directory reports NoAvailableOrganizationException.
- Get, List and Delete on template entries do not require an association; only Disassociate does.
- Tag ARNs must name a catalogued quota of the caller account and Region.
