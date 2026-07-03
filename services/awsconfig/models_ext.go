package awsconfig

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
