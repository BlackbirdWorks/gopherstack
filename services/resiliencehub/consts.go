package resiliencehub

// defaultPageLimit is the page size used by every List operation when the
// caller omits MaxResults, matching services/outposts/services/grafana's
// convention.
const defaultPageLimit = 100

// AppAssessmentScheduleType wire values (types.AppAssessmentScheduleType).
const (
	AssessmentScheduleDisabled = "Disabled"
	AssessmentScheduleDaily    = "Daily"
)

// AppComplianceStatusType wire values (types.AppComplianceStatusType).
const (
	AppComplianceStatusPolicyBreached  = "PolicyBreached"
	AppComplianceStatusPolicyMet       = "PolicyMet"
	AppComplianceStatusNotAssessed     = "NotAssessed"
	AppComplianceStatusChangesDetected = "ChangesDetected"
	AppComplianceStatusNotApplicable   = "NotApplicable"
	AppComplianceStatusMissingPolicy   = "MissingPolicy"
)

// AppDriftStatusType / DriftStatus wire values -- both enums share the
// identical three string values (types.AppDriftStatusType, types.DriftStatus).
const (
	DriftStatusNotChecked  = "NotChecked"
	DriftStatusNotDetected = "NotDetected"
	DriftStatusDetected    = "Detected"
)

// AppStatusType wire values (types.AppStatusType).
const (
	AppStatusActive   = "Active"
	AppStatusDeleting = "Deleting"
)

// AssessmentStatus wire values (types.AssessmentStatus) -- one of five
// identically-shaped Pending/InProgress/Failed/Success async status enums in
// this service (see also ResourceResolutionStatusType, ResourceImportStatusType,
// MetricsExportStatusType, ResourcesGroupingRecGenStatusType). PARITY.md's
// "Notes for the implementer" section is the source for treating all five as
// one generic timer-driven transition shape.
const (
	AssessmentStatusPending    = "Pending"
	AssessmentStatusInProgress = "InProgress"
	AssessmentStatusFailed     = "Failed"
	AssessmentStatusSuccess    = "Success"
)

// AssessmentInvoker wire values (types.AssessmentInvoker).
const (
	InvokerUser   = "User"
	InvokerSystem = "System"
)

// ComplianceStatus wire values (types.ComplianceStatus) -- used by
// AppAssessment.ComplianceStatus and DisruptionCompliance.ComplianceStatus.
// This is a DISTINCT (four-value) enum from AppComplianceStatusType's six
// values above -- confirmed from types/enums.go, not assumed.
const (
	ComplianceStatusPolicyBreached = "PolicyBreached"
	ComplianceStatusPolicyMet      = "PolicyMet"
	ComplianceStatusNotApplicable  = "NotApplicable"
	ComplianceStatusMissingPolicy  = "MissingPolicy"
)

// DisruptionType wire values (types.DisruptionType) -- the four keys a
// ResiliencyPolicy.Policy map and an AppAssessment.Compliance map are keyed
// by.
const (
	DisruptionTypeSoftware = "Software"
	DisruptionTypeHardware = "Hardware"
	DisruptionTypeAZ       = "AZ"
	DisruptionTypeRegion   = "Region"
)

// disruptionTypes lists every DisruptionType value, in the fixed order this
// backend emits map entries for (Go map iteration is otherwise unspecified).
//
//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var disruptionTypes = []string{
	DisruptionTypeSoftware, DisruptionTypeHardware, DisruptionTypeAZ, DisruptionTypeRegion,
}

// ResiliencyPolicyTier wire values (types.ResiliencyPolicyTier).
const (
	TierMissionCritical = "MissionCritical"
	TierCritical        = "Critical"
	TierImportant       = "Important"
	TierCoreServices    = "CoreServices"
	TierNonCritical     = "NonCritical"
	TierNotApplicable   = "NotApplicable"
)

// ResourceMappingType wire values (types.ResourceMappingType).
const (
	MappingTypeCfnStack       = "CfnStack"
	MappingTypeResource       = "Resource"
	MappingTypeAppRegistryApp = "AppRegistryApp"
	MappingTypeResourceGroup  = "ResourceGroup"
	MappingTypeTerraform      = "Terraform"
	MappingTypeEKS            = "EKS"
)

// PhysicalIdentifierType wire values (types.PhysicalIdentifierType).
const (
	PhysicalIDTypeArn    = "Arn"
	PhysicalIDTypeNative = "Native"
)

// ResourceSourceType wire values (types.ResourceSourceType).
const (
	ResourceSourceAppTemplate = "AppTemplate"
	ResourceSourceDiscovered  = "Discovered"
)

// TemplateFormat wire values (types.TemplateFormat).
const (
	TemplateFormatCfnYaml = "CfnYaml"
	TemplateFormatCfnJSON = "CfnJson"
)

// RecommendationTemplateStatus / ResourceResolutionStatusType /
// ResourceImportStatusType / MetricsExportStatusType /
// ResourcesGroupingRecGenStatusType wire values -- all five (plus
// AssessmentStatus above) share the identical Pending/InProgress/Failed/
// Success shape (confirmed identical string values across
// types/enums.go -- see PARITY.md).
const (
	AsyncStatusPending    = "Pending"
	AsyncStatusInProgress = "InProgress"
	AsyncStatusFailed     = "Failed"
	AsyncStatusSuccess    = "Success"
)

// RenderRecommendationType wire values (types.RenderRecommendationType).
const (
	RenderTypeAlarm = "Alarm"
	RenderTypeSop   = "Sop"
	RenderTypeTest  = "Test"
)

// GroupingRecommendationStatusType wire values
// (types.GroupingRecommendationStatusType).
const (
	GroupingStatusAccepted        = "Accepted"
	GroupingStatusRejected        = "Rejected"
	GroupingStatusPendingDecision = "PendingDecision"
)

// RecommendationStatus wire values (types.RecommendationStatus).
const (
	RecommendationStatusImplemented    = "Implemented"
	RecommendationStatusInactive       = "Inactive"
	RecommendationStatusNotImplemented = "NotImplemented"
	RecommendationStatusExcluded       = "Excluded"
)

// DataLocationConstraint wire values (types.DataLocationConstraint).
const (
	DataLocationAnyLocation   = "AnyLocation"
	DataLocationSameContinent = "SameContinent"
	DataLocationSameCountry   = "SameCountry"
)

// EstimatedCostTier wire values (types.EstimatedCostTier).
const (
	CostTierL1 = "L1"
	CostTierL2 = "L2"
	CostTierL3 = "L3"
	CostTierL4 = "L4"
)

// PermissionModelType wire values (types.PermissionModelType).
const (
	PermissionModelLegacyIAMUser = "LegacyIAMUser"
	PermissionModelRoleBased     = "RoleBased"
)

// resourceType labels used in notFoundError/conflictError messages. Message
// text only, distinct from the wire ConflictException/ResourceNotFoundException
// ResourceType string (which this service leaves unset -- see errors.go).
const (
	resourceApp                    = "app"
	resourceAppVersion             = "app version"
	resourceAppComponent           = "app component"
	resourceAppResource            = "resource"
	resourceAppInputSource         = "app input source"
	resourcePolicy                 = "resiliency policy"
	resourceAssessment             = "app assessment"
	resourceRecommendationTemplate = "recommendation template"
	resourceMetricsExport          = "metrics export"
	resourceGroupingTask           = "resource grouping recommendation task"
	resourceTaggable               = "resource"
)

// supportedArnResourceTypes / supportedNativeResourceTypes are the exact two
// closed lists documented on types.PhysicalResourceId's Type field's doc
// comment (types/types.go, "Arn"/"Native" -- read verbatim, not inferred).
// ListUnsupportedAppVersionResources uses their union to decide whether a
// PhysicalResource's ResourceType is one Resilience Hub genuinely supports --
// this is real, derivable behavior, not a fabricated classification.
//
//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var supportedArnResourceTypes = map[string]bool{
	"AWS::ECS::Service":                         true,
	"AWS::EFS::FileSystem":                      true,
	"AWS::ElasticLoadBalancingV2::LoadBalancer": true,
	"AWS::Lambda::Function":                     true,
	"AWS::SNS::Topic":                           true,
}

//nolint:gochecknoglobals // static reference table, read-only, matches services/outposts' seed_data.go pattern
var supportedNativeResourceTypes = map[string]bool{
	"AWS::ApiGateway::RestApi":                true,
	"AWS::ApiGatewayV2::Api":                  true,
	"AWS::AutoScaling::AutoScalingGroup":      true,
	"AWS::DocDB::DBCluster":                   true,
	"AWS::DocDB::DBGlobalCluster":             true,
	"AWS::DocDB::DBInstance":                  true,
	"AWS::DynamoDB::GlobalTable":              true,
	"AWS::DynamoDB::Table":                    true,
	"AWS::EC2::EC2Fleet":                      true,
	"AWS::EC2::Instance":                      true,
	"AWS::EC2::NatGateway":                    true,
	"AWS::EC2::Volume":                        true,
	"AWS::ElasticLoadBalancing::LoadBalancer": true,
	"AWS::RDS::DBCluster":                     true,
	"AWS::RDS::DBInstance":                    true,
	"AWS::RDS::GlobalCluster":                 true,
	"AWS::Route53::RecordSet":                 true,
	"AWS::S3::Bucket":                         true,
	"AWS::SQS::Queue":                         true,
}

// isSupportedResourceType reports whether resourceType (a raw CFN-style type
// string, e.g. "AWS::EC2::Instance") is one of the two closed lists above.
func isSupportedResourceType(resourceType string) bool {
	return supportedArnResourceTypes[resourceType] || supportedNativeResourceTypes[resourceType]
}

// scorePlaceholder is the fixed, documented stand-in this backend returns
// everywhere the real API would return a proprietary resiliency score
// (App.ResiliencyScore, AppAssessment.ResiliencyScore.Score,
// AppAssessmentSummary.ResiliencyScore). See PARITY.md's honest-gap section:
// AWS's real scoring model weighs resource redundancy, failover
// configuration, and backup posture across the actual resource graph, with
// no derivable formula in the SDK. Zero is the least misleading placeholder
// (it does not read as "perfectly resilient", unlike a high number would).
// NOT A REAL SCORE.
const scorePlaceholder = 0.0

// draftVersion is the sentinel AppVersion string this backend treats as the
// mutable working version every app is created with. ASSUMPTION, not
// SDK-verified: AppVersion is a bare *string with no enum/pattern trait
// anywhere in this SDK module: no file:line in the Go source confirms
// "draft" as the literal value real AWS uses. This is asserted from general
// knowledge of the documented product behavior only -- see PARITY.md's gap
// #8. If wrong, every draft-vs-published check in this package (see
// appversions.go) would need the sentinel value changed in one place.
const draftVersion = "draft"
