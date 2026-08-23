package awsconfig

// RecordingGroup holds the resource recording configuration for a recorder.
type RecordingGroup struct {
	ResourceTypes              []string `json:"resourceTypes,omitempty"`
	AllSupported               bool     `json:"allSupported,omitempty"`
	IncludeGlobalResourceTypes bool     `json:"includeGlobalResourceTypes,omitempty"`
}

// ConfigurationRecorder represents an AWS Config configuration recorder.
//
// ConnectorArn, ScopeConfiguration, and ServicePrincipal are only populated
// for a third-party service-linked recorder created via
// PutThirdPartyServiceLinkedConfigurationRecorder (verified against
// aws-sdk-go-v2/service/configservice's serializeDocumentConfigurationRecorder,
// which emits connectorArn/scopeConfiguration/servicePrincipal alongside the
// long-standing arn/name/recordingGroup/roleARN fields).
type ConfigurationRecorder struct {
	RecordingGroup     *RecordingGroup     `json:"recordingGroup,omitempty"`
	ScopeConfiguration *ScopeConfiguration `json:"scopeConfiguration,omitempty"`
	Arn                string              `json:"arn,omitempty"`
	ConnectorArn       string              `json:"connectorArn,omitempty"`
	Name               string              `json:"name"`
	RoleARN            string              `json:"roleARN"`
	ServicePrincipal   string              `json:"servicePrincipal,omitempty"`
	Status             string              `json:"status,omitempty"` // PENDING or ACTIVE
}

// ServiceLinkedRecorderLink tracks which AWS service principal owns a
// service-linked configuration recorder, so
// PutServiceLinkedConfigurationRecorder/DeleteServiceLinkedConfigurationRecorder
// can look the recorder back up by principal. Kept as its own store.Table
// (instead of a field on ConfigurationRecorder) because ConfigurationRecorder
// is serialized verbatim as the real AWS wire response -- a bookkeeping field
// there would need a json:"-" tag to stay off the wire, which would also make
// it invisible to store.Table's persistence (Snapshot/Restore marshal the
// same struct with the same tags), silently losing the service-linked
// recorder's identity across a snapshot/restore round trip.
type ServiceLinkedRecorderLink struct {
	ServicePrincipal string `json:"ServicePrincipal"`
	RecorderName     string `json:"RecorderName"`
}

// DeliverySnapshotProperties holds snapshot delivery configuration for a channel.
type DeliverySnapshotProperties struct {
	DeliveryFrequency string `json:"deliveryFrequency,omitempty"`
}

// DeliveryChannel represents an AWS Config delivery channel.
type DeliveryChannel struct {
	ConfigSnapshotDeliveryProperties *DeliverySnapshotProperties `json:"configSnapshotDeliveryProperties,omitempty"`
	Name                             string                      `json:"name"`
	S3Bucket                         string                      `json:"s3BucketName,omitempty"`
	S3KeyPrefix                      string                      `json:"s3KeyPrefix,omitempty"`
	SNSArn                           string                      `json:"snsTopicARN,omitempty"`
}

// AggregationAuthorization represents an AWS Config aggregation authorization.
type AggregationAuthorization struct {
	AggregationAuthorizationArn string `json:"AggregationAuthorizationArn,omitempty"`
	AuthorizedAccountID         string `json:"AuthorizedAccountId"`
	AuthorizedAwsRegion         string `json:"AuthorizedAwsRegion"`
	CreationTime                string `json:"CreationTime,omitempty"`
}

// ConfigRuleSource represents the source definition of an AWS Config config rule.
type ConfigRuleSource struct {
	Owner            string `json:"Owner,omitempty"`
	SourceIdentifier string `json:"SourceIdentifier,omitempty"`
}

// ConfigRuleScope restricts which resources trigger an AWS Config rule.
type ConfigRuleScope struct {
	ComplianceResourceID    string   `json:"ComplianceResourceId,omitempty"`
	TagKey                  string   `json:"TagKey,omitempty"`
	TagValue                string   `json:"TagValue,omitempty"`
	ComplianceResourceTypes []string `json:"ComplianceResourceTypes,omitempty"`
}

// ConfigRule represents an AWS Config config rule.
type ConfigRule struct {
	Source                    *ConfigRuleSource `json:"Source,omitempty"`
	Scope                     *ConfigRuleScope  `json:"Scope,omitempty"`
	ConfigRuleName            string            `json:"ConfigRuleName"`
	ConfigRuleArn             string            `json:"ConfigRuleArn,omitempty"`
	ConfigRuleID              string            `json:"ConfigRuleId,omitempty"`
	Description               string            `json:"Description,omitempty"`
	InputParameters           string            `json:"InputParameters,omitempty"`
	MaximumExecutionFrequency string            `json:"MaximumExecutionFrequency,omitempty"`
	ConfigRuleState           string            `json:"ConfigRuleState,omitempty"`
}

// AccountAggregationSource identifies AWS accounts to aggregate from.
type AccountAggregationSource struct {
	AccountIDs    []string `json:"AccountIds"`
	AwsRegions    []string `json:"AwsRegions,omitempty"`
	AllAwsRegions bool     `json:"AllAwsRegions,omitempty"`
}

// OrganizationAggregationSource identifies an organization to aggregate from.
type OrganizationAggregationSource struct {
	RoleArn       string   `json:"RoleArn"`
	AwsRegions    []string `json:"AwsRegions,omitempty"`
	AllAwsRegions bool     `json:"AllAwsRegions,omitempty"`
}

// ConfigurationAggregator represents an AWS Config configuration aggregator.
type ConfigurationAggregator struct {
	OrganizationAggregationSource *OrganizationAggregationSource `json:"OrganizationAggregationSource,omitempty"`
	ConfigurationAggregatorArn    string                         `json:"ConfigurationAggregatorArn,omitempty"`
	ConfigurationAggregatorName   string                         `json:"ConfigurationAggregatorName"`
	CreationTime                  string                         `json:"CreationTime,omitempty"`
	AccountAggregationSources     []AccountAggregationSource     `json:"AccountAggregationSources,omitempty"`
}

// ConformancePack represents an AWS Config conformance pack.
type ConformancePack struct {
	ConformancePackArn      string `json:"ConformancePackArn,omitempty"`
	ConformancePackID       string `json:"ConformancePackId,omitempty"`
	ConformancePackName     string `json:"ConformancePackName"`
	DeliveryS3Bucket        string `json:"DeliveryS3Bucket,omitempty"`
	DeliveryS3KeyPrefix     string `json:"DeliveryS3KeyPrefix,omitempty"`
	LastUpdateRequestedTime string `json:"LastUpdateRequestedTime,omitempty"`
}

// OrganizationConfigRule represents an AWS Config organization config rule.
type OrganizationConfigRule struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
}

// OrganizationConformancePack represents an AWS Config organization conformance pack.
type OrganizationConformancePack struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
}

// StoredQuery represents an AWS Config stored query.
type StoredQuery struct {
	QueryName   string `json:"QueryName"`
	QueryID     string `json:"QueryId,omitempty"`
	QueryArn    string `json:"QueryArn,omitempty"`
	Description string `json:"Description,omitempty"`
	Expression  string `json:"Expression,omitempty"`
}

// Tag represents an AWS resource tag.
type Tag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// ConfigurationRecorderStatus represents the recording status of a recorder.
type ConfigurationRecorderStatus struct {
	LastErrorCode string `json:"lastErrorCode,omitempty"`
	LastStartTime string `json:"lastStartTime,omitempty"`
	LastStatus    string `json:"lastStatus,omitempty"`
	LastStopTime  string `json:"lastStopTime,omitempty"`
	Name          string `json:"name"`
	Recording     bool   `json:"recording"`
}

// ConfigurationRecorderSummary is a lightweight summary returned by ListConfigurationRecorders.
type ConfigurationRecorderSummary struct {
	Arn            string `json:"arn"`
	Name           string `json:"name"`
	RecordingScope string `json:"recordingScope"`
}

// StoredQueryMetadata is summary metadata returned by ListStoredQueries.
type StoredQueryMetadata struct {
	QueryArn    string `json:"QueryArn"`
	QueryID     string `json:"QueryId"`
	QueryName   string `json:"QueryName"`
	Description string `json:"Description,omitempty"`
}

// BaseConfigurationItem is a lightweight configuration snapshot for a single resource.
type BaseConfigurationItem struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceID   string `json:"resourceId,omitempty"`
}

// AggregateResourceIdentifier identifies a resource in an aggregator.
type AggregateResourceIdentifier struct {
	SourceAccountID string `json:"SourceAccountId,omitempty"`
	SourceRegion    string `json:"SourceRegion,omitempty"`
	ResourceID      string `json:"ResourceId,omitempty"`
	ResourceType    string `json:"ResourceType,omitempty"`
}

// ResourceKey identifies a resource by type and ID for
// StartRemediationExecution/DescribeRemediationExecutionStatus (wire keys
// resourceType/resourceId -- verified against aws-sdk-go-v2/service/
// configservice's awsAwsjson11_serializeDocumentResourceKey).
type ResourceKey struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceID   string `json:"resourceId,omitempty"`
}

// RemediationExceptionResourceKey identifies a resource by type and ID for
// Put/DeleteRemediationExceptions. Despite the similar name, its wire keys
// are PascalCase (ResourceType/ResourceId), not lowerCamelCase like
// ResourceKey above -- verified against aws-sdk-go-v2/service/configservice's
// awsAwsjson11_serializeDocumentRemediationExceptionResourceKey, a distinct
// serializer from ResourceKey's.
type RemediationExceptionResourceKey struct {
	ResourceType string `json:"ResourceType,omitempty"`
	ResourceID   string `json:"ResourceId,omitempty"`
}

// RetentionConfiguration holds the retention period configuration.
type RetentionConfiguration struct {
	Name                  string `json:"Name"`
	RetentionPeriodInDays int32  `json:"RetentionPeriodInDays"`
}

// RemediationConfiguration holds a remediation configuration for a config rule.
type RemediationConfiguration struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	TargetType     string `json:"TargetType"`
	TargetID       string `json:"TargetId"`
}

// RemediationException holds an exception for remediation of a resource.
type RemediationException struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ResourceType   string `json:"ResourceType"`
	ResourceID     string `json:"ResourceId"`
}

// RemediationExecutionStepStatus holds the status of a single step of a
// remediation execution.
type RemediationExecutionStepStatus struct {
	Name         string  `json:"Name,omitempty"`
	State        string  `json:"State,omitempty"`
	ErrorMessage string  `json:"ErrorMessage,omitempty"`
	StartTime    float64 `json:"StartTime,omitempty"`
	StopTime     float64 `json:"StopTime,omitempty"`
}

// RemediationExecutionStatusEntry holds the status of a remediation execution
// for a single resource. RuleName is internal bookkeeping used to key/index
// executions per config rule -- it is never itself present on the wire (real
// AWS Config scopes DescribeRemediationExecutionStatus results by the
// ConfigRuleName request parameter instead of echoing it per-entry).
type RemediationExecutionStatusEntry struct {
	RuleName        string                           `json:"-"`
	State           string                           `json:"State,omitempty"`
	ResourceKey     ResourceKey                      `json:"ResourceKey"`
	StepDetails     []RemediationExecutionStepStatus `json:"StepDetails,omitempty"`
	InvocationTime  float64                          `json:"InvocationTime,omitempty"`
	LastUpdatedTime float64                          `json:"LastUpdatedTime,omitempty"`
}

// ConfigRuleEvaluationStatus holds the evaluation status for a config rule.
type ConfigRuleEvaluationStatus struct {
	ConfigRuleName               string `json:"ConfigRuleName"`
	LastSuccessfulInvocationTime string `json:"LastSuccessfulInvocationTime,omitempty"`
	LastFailedInvocationTime     string `json:"LastFailedInvocationTime,omitempty"`
	LastSuccessfulEvaluationTime string `json:"LastSuccessfulEvaluationTime,omitempty"`
	LastFailedEvaluationTime     string `json:"LastFailedEvaluationTime,omitempty"`
}

// ComplianceResult holds a compliance type value, optionally with the count of
// resources/rules responsible for that result (real AWS Config's shared
// "Compliance" shape, used by both ComplianceByConfigRule and ComplianceByResource).
type ComplianceResult struct {
	ComplianceContributorCount *ResourceCount `json:"ComplianceContributorCount,omitempty"`
	ComplianceType             string         `json:"ComplianceType"`
}

// ComplianceByConfigRule holds compliance information for a config rule.
type ComplianceByConfigRule struct {
	ConfigRuleName string           `json:"ConfigRuleName"`
	Compliance     ComplianceResult `json:"Compliance"`
}

// ComplianceByResource holds compliance information for a single AWS resource,
// as evaluated across every config rule that scoped it.
type ComplianceByResource struct {
	Compliance   ComplianceResult `json:"Compliance"`
	ResourceType string           `json:"ResourceType,omitempty"`
	ResourceID   string           `json:"ResourceId,omitempty"`
}

// ResourceCount holds a capped resource count returned by compliance summary APIs.
type ResourceCount struct {
	CappedCount int32 `json:"CappedCount"`
	CapExceeded bool  `json:"CapExceeded"`
}

// ComplianceSummaryDetail holds the per-compliance-type counts.
type ComplianceSummaryDetail struct {
	CompliantResourceCount    ResourceCount `json:"CompliantResourceCount"`
	NonCompliantResourceCount ResourceCount `json:"NonCompliantResourceCount"`
}

// ComplianceSummary holds compliant/noncompliant counts. Real shape per
// aws-sdk-go-v2/service/configservice types.ComplianceSummary
// (deserializers.go's ComplianceSummary case list: "CompliantResourceCount",
// "NonCompliantResourceCount" -- no ComplianceType member and no extra
// nesting; the previous shape here wrapped ComplianceSummaryDetail under a
// second "ComplianceSummary" key and added an invented "ComplianceType",
// neither of which exists on the wire).
type ComplianceSummary struct {
	CompliantResourceCount    ResourceCount `json:"CompliantResourceCount"`
	NonCompliantResourceCount ResourceCount `json:"NonCompliantResourceCount"`
}

// ComplianceSummaryByResourceType holds a compliance summary for one resource type.
type ComplianceSummaryByResourceType struct {
	ResourceType      string                  `json:"ResourceType"`
	ComplianceSummary ComplianceSummaryDetail `json:"ComplianceSummary"`
}

// EvaluationResult holds an evaluation result for a config rule.
type EvaluationResult struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ComplianceType string `json:"ComplianceType"`
	ResourceType   string `json:"ResourceType"`
	ResourceID     string `json:"ResourceId"`
	Annotation     string `json:"Annotation,omitempty"`
}

// EvaluationResultQualifier identifies the rule and resource an evaluation is for.
type EvaluationResultQualifier struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ResourceType   string `json:"ResourceType,omitempty"`
	ResourceID     string `json:"ResourceId,omitempty"`
}

// EvaluationResultIdentifier uniquely identifies an evaluation result.
type EvaluationResultIdentifier struct {
	EvaluationResultQualifier EvaluationResultQualifier `json:"EvaluationResultQualifier"`
	OrderingTimestamp         float64                   `json:"OrderingTimestamp"`
}

// DetailedEvaluationResult is the per-resource evaluation result returned by the
// GetComplianceDetailsBy* APIs. Timestamps are epoch seconds.
type DetailedEvaluationResult struct {
	ComplianceType             string                     `json:"ComplianceType"`
	Annotation                 string                     `json:"Annotation,omitempty"`
	EvaluationResultIdentifier EvaluationResultIdentifier `json:"EvaluationResultIdentifier"`
	ResultRecordedTime         float64                    `json:"ResultRecordedTime"`
	ConfigRuleInvokedTime      float64                    `json:"ConfigRuleInvokedTime"`
}

// DeliveryChannelStatusInfo holds status info for a delivery channel.
type DeliveryChannelStatusInfo struct {
	LastStatus      string  `json:"lastStatus"`
	LastAttemptTime float64 `json:"lastAttemptTime"`
}

// DeliveryChannelStatus holds the status of a delivery channel.
type DeliveryChannelStatus struct {
	ConfigHistoryDeliveryInfo *DeliveryChannelStatusInfo `json:"configHistoryDeliveryInfo,omitempty"`
	ConfigStreamDeliveryInfo  *DeliveryChannelStatusInfo `json:"configStreamDeliveryInfo,omitempty"`
	Name                      string                     `json:"name"`
}

// ConformancePackStatus holds status of a conformance pack.
type ConformancePackStatus struct {
	ConformancePackName  string `json:"ConformancePackName"`
	ConformancePackState string `json:"ConformancePackState"`
	ConformancePackArn   string `json:"ConformancePackArn"`
}

// ConformancePackComplianceItem holds compliance info for a conformance pack rule.
type ConformancePackComplianceItem struct {
	ConfigRuleName string   `json:"ConfigRuleName"`
	ComplianceType string   `json:"ComplianceType"`
	Controls       []string `json:"Controls,omitempty"`
}

// ConformancePackRuleLink tracks a single config rule deployed by a conformance
// pack (parsed from PutConformancePack's TemplateBody), so the compliance
// family (DescribeConformancePackCompliance/GetConformancePackComplianceDetails/
// GetConformancePackComplianceSummary/ListConformancePackComplianceScores) can
// roll up real per-rule evaluation state instead of returning an empty stub,
// and DeleteConformancePack can cascade-delete the rules it deployed. Purely
// internal bookkeeping -- never itself serialized to an AWS API response.
type ConformancePackRuleLink struct {
	ConformancePackName string `json:"ConformancePackName"`
	ConfigRuleName      string `json:"ConfigRuleName"`
}

// ConformancePackComplianceSummaryEntry holds the overall compliance status of
// a conformance pack (its deployed rules rolled up into a single status).
type ConformancePackComplianceSummaryEntry struct {
	ConformancePackComplianceStatus string `json:"ConformancePackComplianceStatus"`
	ConformancePackName             string `json:"ConformancePackName"`
}

// ConformancePackComplianceScoreEntry holds a conformance pack's compliance
// score (the percentage of compliant rule-resource combinations).
type ConformancePackComplianceScoreEntry struct {
	ConformancePackName string  `json:"ConformancePackName,omitempty"`
	Score               string  `json:"Score,omitempty"`
	LastUpdatedTime     float64 `json:"LastUpdatedTime,omitempty"`
}

// OrganizationConfigRuleStatus holds the status of an organization config rule.
type OrganizationConfigRuleStatus struct {
	OrganizationConfigRuleName string `json:"OrganizationConfigRuleName"`
	OrganizationRuleStatus     string `json:"OrganizationRuleStatus"`
}

// OrganizationConformancePackStatus holds the status of an organization conformance pack.
type OrganizationConformancePackStatus struct {
	OrganizationConformancePackName string `json:"OrganizationConformancePackName"`
	Status                          string `json:"Status"`
}

// MemberAccountStatus holds a single member account's deployment status for an
// organization config rule.
type MemberAccountStatus struct {
	AccountID               string `json:"AccountId"`
	ConfigRuleName          string `json:"ConfigRuleName"`
	MemberAccountRuleStatus string `json:"MemberAccountRuleStatus"`
}

// OrganizationConformancePackDetailedStatus holds a single member account's
// deployment status for an organization conformance pack.
type OrganizationConformancePackDetailedStatus struct {
	AccountID           string `json:"AccountId"`
	ConformancePackName string `json:"ConformancePackName"`
	Status              string `json:"Status"`
}

// ResourceConfigItem holds configuration info for a discovered resource. Real
// shape per aws-sdk-go-v2/service/configservice's
// awsAwsjson11_deserializeDocumentConfigurationItem (used by
// GetResourceConfigHistory/BatchGetResourceConfig): the four members this
// backend tracks are all lowerCamelCase on the wire ("resourceType",
// "resourceId", "configuration", "configurationItemCaptureTime"), unlike the
// PascalCase used by the service's DescribeXxx wrapper keys -- the tags here
// previously carried the PascalCase convention instead, so every consumer
// always decoded these four fields as empty/zero.
type ResourceConfigItem struct {
	ResourceType                 string  `json:"resourceType"`
	ResourceID                   string  `json:"resourceId"`
	Configuration                string  `json:"configuration"`
	ConfigurationItemCaptureTime float64 `json:"configurationItemCaptureTime"`
}

// AggregatedSourceStatus holds the sync status of one configuration
// aggregator source (an account/region pair, or an organization).
type AggregatedSourceStatus struct {
	SourceID         string  `json:"SourceId,omitempty"`
	SourceType       string  `json:"SourceType,omitempty"`
	AwsRegion        string  `json:"AwsRegion,omitempty"`
	LastUpdateStatus string  `json:"LastUpdateStatus,omitempty"`
	LastUpdateTime   float64 `json:"LastUpdateTime,omitempty"`
}

// AggregateEvaluationResult holds a single aggregate config-rule evaluation
// result for an account/region in an aggregator.
type AggregateEvaluationResult struct {
	ComplianceType             string                     `json:"ComplianceType"`
	AccountID                  string                     `json:"AccountId,omitempty"`
	AwsRegion                  string                     `json:"AwsRegion,omitempty"`
	Annotation                 string                     `json:"Annotation,omitempty"`
	EvaluationResultIdentifier EvaluationResultIdentifier `json:"EvaluationResultIdentifier"`
	ResultRecordedTime         float64                    `json:"ResultRecordedTime"`
	ConfigRuleInvokedTime      float64                    `json:"ConfigRuleInvokedTime"`
}

// AggregateComplianceCount holds the compliant/noncompliant rule counts for a
// single account/region group in an aggregator.
type AggregateComplianceCount struct {
	GroupName         string            `json:"GroupName,omitempty"`
	ComplianceSummary ComplianceSummary `json:"ComplianceSummary"`
}

// AggregateConformancePackCompliance holds one conformance pack's rule counts
// as seen through an aggregator.
type AggregateConformancePackCompliance struct {
	ComplianceType        string `json:"ComplianceType,omitempty"`
	CompliantRuleCount    int32  `json:"CompliantRuleCount"`
	NonCompliantRuleCount int32  `json:"NonCompliantRuleCount"`
	TotalRuleCount        int32  `json:"TotalRuleCount"`
}

// AggregateComplianceByConformancePack holds one conformance pack's compliance
// as seen through an aggregator for a single account/region source.
type AggregateComplianceByConformancePack struct {
	Compliance          *AggregateConformancePackCompliance `json:"Compliance,omitempty"`
	AccountID           string                              `json:"AccountId,omitempty"`
	AwsRegion           string                              `json:"AwsRegion,omitempty"`
	ConformancePackName string                              `json:"ConformancePackName,omitempty"`
}

// AggregateConformancePackComplianceCount holds compliant/noncompliant
// conformance pack counts for a single account/region group in an aggregator.
type AggregateConformancePackComplianceCount struct {
	CompliantConformancePackCount    int32 `json:"CompliantConformancePackCount"`
	NonCompliantConformancePackCount int32 `json:"NonCompliantConformancePackCount"`
}

// AggregateConformancePackComplianceSummary holds a conformance-pack
// compliance summary for one account/region group in an aggregator.
type AggregateConformancePackComplianceSummary struct {
	GroupName         string                                  `json:"GroupName,omitempty"`
	ComplianceSummary AggregateConformancePackComplianceCount `json:"ComplianceSummary"`
}

// PendingAggregationRequest identifies an account/region that requested
// aggregation permission but whose data no configuration aggregator has yet
// incorporated.
type PendingAggregationRequest struct {
	RequesterAccountID string `json:"RequesterAccountId,omitempty"`
	RequesterAwsRegion string `json:"RequesterAwsRegion,omitempty"`
}

// ScopeConfiguration specifies which resources a third-party service-linked
// configuration recorder records from the connected third-party cloud
// service provider (verified against aws-sdk-go-v2/service/configservice's
// PutThirdPartyServiceLinkedConfigurationRecorder serializer, which emits
// allRegions/includedRegions/scopeType/scopeValues; allRegions has no
// omitempty since the real serializer always writes it, even false).
type ScopeConfiguration struct {
	ScopeType       string   `json:"scopeType,omitempty"`
	IncludedRegions []string `json:"includedRegions,omitempty"`
	ScopeValues     []string `json:"scopeValues,omitempty"`
	AllRegions      bool     `json:"allRegions"`
}

// AzureConnectorConfiguration is the Azure-specific half of
// ConnectorConfiguration -- the only third-party cloud provider AWS Config
// currently documents (types.Provider's sole enum value is "AZURE").
type AzureConnectorConfiguration struct {
	ClientIdentifier string `json:"clientIdentifier"`
	TenantIdentifier string `json:"tenantIdentifier"`
}

// ConnectorConfiguration is the provider-specific configuration for a
// connector between AWS Config and a third-party cloud service provider.
// Real AWS Config requires exactly one provider to be set; Azure is the only
// one it currently supports.
type ConnectorConfiguration struct {
	Azure *AzureConnectorConfiguration `json:"azure,omitempty"`
}

// Connector represents a connection between AWS Config and a third-party
// cloud service provider, created by PutConnector (verified against
// aws-sdk-go-v2/service/configservice's GetConnector deserializer).
// CreatedTime is epoch seconds, matching this package's established
// convention for AWS Config's Date-shaped fields (see e.g.
// ResourceConfigItem.ConfigurationItemCaptureTime).
type Connector struct {
	ConnectorConfiguration *ConnectorConfiguration `json:"connectorConfiguration,omitempty"`
	Arn                    string                  `json:"arn"`
	Name                   string                  `json:"name"`
	CreatedTime            float64                 `json:"createdTime,omitempty"`
}

// ConnectorSummary is the lightweight summary returned by ListConnectors
// (verified against aws-sdk-go-v2/service/configservice's ListConnectors
// deserializer, which flattens the provider and Azure tenantIdentifier up
// from the connector's ConnectorConfiguration onto the summary itself).
type ConnectorSummary struct {
	Arn              string  `json:"arn"`
	Name             string  `json:"name"`
	Provider         string  `json:"provider"`
	TenantIdentifier string  `json:"tenantIdentifier"`
	CreatedTime      float64 `json:"createdTime,omitempty"`
}

// ConnectorFilter filters ListConnectors results (verified against
// aws-sdk-go-v2/service/configservice's ConnectorFilter type; "provider" is
// currently the only defined FilterName, with FilterValues like "AZURE").
type ConnectorFilter struct {
	FilterName   string   `json:"filterName,omitempty"`
	FilterValues []string `json:"filterValues,omitempty"`
}
