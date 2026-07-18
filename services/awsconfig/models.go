package awsconfig

// RecordingGroup holds the resource recording configuration for a recorder.
type RecordingGroup struct {
	ResourceTypes              []string `json:"resourceTypes,omitempty"`
	AllSupported               bool     `json:"allSupported,omitempty"`
	IncludeGlobalResourceTypes bool     `json:"includeGlobalResourceTypes,omitempty"`
}

// ConfigurationRecorder represents an AWS Config configuration recorder.
type ConfigurationRecorder struct {
	RecordingGroup *RecordingGroup `json:"recordingGroup,omitempty"`
	Arn            string          `json:"arn,omitempty"`
	Name           string          `json:"name"`
	RoleARN        string          `json:"roleARN"`
	Status         string          `json:"status,omitempty"` // PENDING or ACTIVE
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
	AuthorizedAccountID         string `json:"authorizedAccountId"`
	AuthorizedAwsRegion         string `json:"authorizedAwsRegion"`
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
	OrganizationConfigRuleName string `json:"organizationConfigRuleName"`
}

// OrganizationConformancePack represents an AWS Config organization conformance pack.
type OrganizationConformancePack struct {
	OrganizationConformancePackName string `json:"organizationConformancePackName"`
}

// StoredQuery represents an AWS Config stored query.
type StoredQuery struct {
	QueryName string `json:"QueryName"`
	QueryID   string `json:"QueryId,omitempty"`
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
	QueryArn  string `json:"QueryArn"`
	QueryID   string `json:"QueryId"`
	QueryName string `json:"QueryName"`
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

// ResourceKey identifies a resource by type and ID.
type ResourceKey struct {
	ResourceType string `json:"resourceType,omitempty"`
	ResourceID   string `json:"resourceId,omitempty"`
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

// ConfigRuleEvaluationStatus holds the evaluation status for a config rule.
type ConfigRuleEvaluationStatus struct {
	ConfigRuleName               string `json:"ConfigRuleName"`
	LastSuccessfulInvocationTime string `json:"LastSuccessfulInvocationTime,omitempty"`
	LastFailedInvocationTime     string `json:"LastFailedInvocationTime,omitempty"`
	LastSuccessfulEvaluationTime string `json:"LastSuccessfulEvaluationTime,omitempty"`
	LastFailedEvaluationTime     string `json:"LastFailedEvaluationTime,omitempty"`
}

// ComplianceResult holds a compliance type value.
type ComplianceResult struct {
	ComplianceType string `json:"ComplianceType"`
}

// ComplianceByConfigRule holds compliance information for a config rule.
type ComplianceByConfigRule struct {
	ConfigRuleName string           `json:"ConfigRuleName"`
	Compliance     ComplianceResult `json:"Compliance"`
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

// ComplianceSummary holds a compliance summary by type.
type ComplianceSummary struct {
	ComplianceType    string                  `json:"ComplianceType"`
	ComplianceSummary ComplianceSummaryDetail `json:"ComplianceSummary"`
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
	LastStatus      string  `json:"LastStatus"`
	LastAttemptTime float64 `json:"LastAttemptTime"`
}

// DeliveryChannelStatus holds the status of a delivery channel.
type DeliveryChannelStatus struct {
	ConfigHistoryDeliveryInfo *DeliveryChannelStatusInfo `json:"ConfigHistoryDeliveryInfo,omitempty"`
	ConfigStreamDeliveryInfo  *DeliveryChannelStatusInfo `json:"ConfigStreamDeliveryInfo,omitempty"`
	Name                      string                     `json:"Name"`
}

// ConformancePackStatus holds status of a conformance pack.
type ConformancePackStatus struct {
	ConformancePackName  string `json:"ConformancePackName"`
	ConformancePackState string `json:"ConformancePackState"`
	ConformancePackArn   string `json:"ConformancePackArn"`
}

// ConformancePackComplianceItem holds compliance info for a conformance pack rule.
type ConformancePackComplianceItem struct {
	ConfigRuleName string `json:"ConfigRuleName"`
	ComplianceType string `json:"ComplianceType"`
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

// ResourceConfigItem holds configuration info for a discovered resource.
type ResourceConfigItem struct {
	ResourceType                 string  `json:"ResourceType"`
	ResourceID                   string  `json:"ResourceId"`
	Configuration                string  `json:"Configuration"`
	ConfigurationItemCaptureTime float64 `json:"ConfigurationItemCaptureTime"`
}
