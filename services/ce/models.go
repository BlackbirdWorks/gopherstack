package ce

import "time"

// CostCategory represents an in-memory AWS Cost Explorer cost category.
type CostCategory struct {
	CreationDate     time.Time          `json:"creationDate"`
	Tags             map[string]string  `json:"tags"`
	ARN              string             `json:"arn"`
	Name             string             `json:"name"`
	RuleVersion      string             `json:"ruleVersion"`
	DefaultValue     string             `json:"defaultValue"`
	EffectiveStart   string             `json:"effectiveStart"`
	Rules            []CostCategoryRule `json:"rules"`
	SplitChargeRules []SplitChargeRule  `json:"splitChargeRules"`
}

// CostCategoryRule represents a single cost category rule.
type CostCategoryRule struct {
	Value string `json:"value"`
}

// SplitChargeRule represents a cost category split charge rule.
type SplitChargeRule struct {
	Source  string   `json:"source"`
	Method  string   `json:"method"`
	Targets []string `json:"targets"`
}

// AnomalyMonitor represents an in-memory AWS CE anomaly monitor.
// MonitorSpecification is the Expression that scopes a CUSTOM monitor (or a
// DIMENSIONAL monitor with MonitorDimension TAG/COST_CATEGORY) -- required
// input on CreateAnomalyMonitor, echoed back on GetAnomalyMonitors per
// types.AnomalyMonitor (costexplorer@v1.67.4 types/types.go).
type AnomalyMonitor struct {
	CreationDate         time.Time         `json:"creationDate"`
	LastUpdatedDate      time.Time         `json:"lastUpdatedDate"`
	Tags                 map[string]string `json:"tags"`
	MonitorSpecification *ceExpression     `json:"monitorSpecification,omitempty"`
	MonitorARN           string            `json:"monitorARN"`
	MonitorName          string            `json:"monitorName"`
	MonitorType          string            `json:"monitorType"`
	MonitorDimension     string            `json:"monitorDimension"`
}

// AnomalySubscription represents an in-memory AWS CE anomaly subscription.
// ThresholdExpression is the non-deprecated alternative to Threshold (real
// AWS: "you can specify either Threshold or ThresholdExpression, but not
// both" -- costexplorer@v1.67.4 types/types.go's AnomalySubscription doc
// comment); both CreateAnomalySubscriptionInput and
// UpdateAnomalySubscriptionInput accept it.
type AnomalySubscription struct {
	CreationDate        time.Time         `json:"creationDate"`
	Tags                map[string]string `json:"tags"`
	ThresholdExpression *ceExpression     `json:"thresholdExpression,omitempty"`
	SubscriptionARN     string            `json:"subscriptionARN"`
	SubscriptionName    string            `json:"subscriptionName"`
	AccountID           string            `json:"accountID"`
	Frequency           string            `json:"frequency"`
	MonitorARNList      []string          `json:"monitorARNList"`
	Subscribers         []Subscriber      `json:"subscribers"`
	Threshold           float64           `json:"threshold"`
}

// AnomalyScore represents the anomaly detection score.
type AnomalyScore struct {
	MaxScore     float64 `json:"MaxScore"`
	CurrentScore float64 `json:"CurrentScore"`
}

// AnomalyRootCause identifies a root cause dimension for an anomaly.
type AnomalyRootCause struct {
	Service       string `json:"Service,omitempty"`
	Region        string `json:"Region,omitempty"`
	LinkedAccount string `json:"LinkedAccount,omitempty"`
	UsageType     string `json:"UsageType,omitempty"`
}

// Anomaly represents a detected cost anomaly in AWS CE.
type Anomaly struct {
	CreationDate     time.Time          `json:"creationDate"`
	AnomalyID        string             `json:"anomalyID"`
	AnomalyStartDate string             `json:"anomalyStartDate"`
	AnomalyEndDate   string             `json:"anomalyEndDate"`
	DimensionValue   string             `json:"dimensionValue"`
	MonitorARN       string             `json:"monitorARN"`
	SubscriptionARN  string             `json:"subscriptionARN"`
	FeedbackType     string             `json:"feedbackType"`
	RootCauses       []AnomalyRootCause `json:"rootCauses,omitempty"`
	AnomalyScore     AnomalyScore       `json:"anomalyScore"`
	TotalImpact      float64            `json:"totalImpact"`
}

// Subscriber represents a CE anomaly subscription notification target.
type Subscriber struct {
	Address string `json:"address"`
	Type    string `json:"type"`
	Status  string `json:"status"`
}

// CostEntry is a synthetic cost ledger entry for a single day+service combination.
type CostEntry struct {
	Tags          map[string]string `json:"tags"`
	Date          string            `json:"date"`
	Service       string            `json:"service"`
	Region        string            `json:"region"`
	UsageType     string            `json:"usageType"`
	Account       string            `json:"account"`
	BlendedCost   float64           `json:"blendedCost"`
	UnblendedCost float64           `json:"unblendedCost"`
	UsageQuantity float64           `json:"usageQuantity"`
}

// CostAllocationTag represents an AWS CE cost allocation tag.
type CostAllocationTag struct {
	TagKey          string `json:"tagKey"`
	Status          string `json:"status"` // Active | Inactive
	Type            string `json:"type"`   // AWSGenerated | UserDefined
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

// BackfillJob represents a cost allocation tag backfill job. BackfillID is
// internal-only -- real AWS's CostAllocationTagBackfillRequest has no unique
// identifier field at all (NextToken is fully opaque), so this is not a
// fabricated wire field, just a stable sort/pagination key this backend needs
// since RequestedAt alone (second precision) can tie between jobs created in
// the same second.
type BackfillJob struct {
	BackfillID     string `json:"backfillID"`
	BackfillFrom   string `json:"backfillFrom"`
	RequestedAt    string `json:"requestedAt"`
	CompletedAt    string `json:"completedAt,omitempty"`
	BackfillStatus string `json:"backfillStatus"` // SUCCEEDED|PROCESSING|FAILED
	LastUpdatedAt  string `json:"lastUpdatedAt"`
}

// SavingsPlansGeneration represents a Savings Plans purchase recommendation generation
// job, matching real AWS CE's GenerationSummary shape (RecommendationId, not
// GenerationId -- see api_op_StartSavingsPlansPurchaseRecommendationGeneration.go).
type SavingsPlansGeneration struct {
	RecommendationID         string `json:"recommendationId"`
	GenerationStatus         string `json:"generationStatus"` // PROCESSING|SUCCEEDED|FAILED
	GenerationStartedTime    string `json:"generationStartedTime"`
	GenerationCompletionTime string `json:"generationCompletionTime,omitempty"`
	EstimatedCompletionTime  string `json:"estimatedCompletionTime"`
}

// CommitmentAnalysis represents a commitment purchase analysis.
type CommitmentAnalysis struct {
	Configuration           any    `json:"configuration,omitempty"`
	AnalysisID              string `json:"analysisId"`
	AnalysisStatus          string `json:"analysisStatus"` // SUCCEEDED|PROCESSING|FAILED
	AnalysisStartedTime     string `json:"analysisStartedTime"`
	EstimatedCompletionTime string `json:"estimatedCompletionTime"`
	ErrorCode               string `json:"errorCode,omitempty"`
}

// GroupBySpec represents a single GroupBy dimension spec.
type GroupBySpec struct {
	Type string `json:"Type"`
	Key  string `json:"Key"`
}

// ResultByTime represents a single time period in GetCostAndUsage.
type ResultByTime struct {
	TimePeriod map[string]string      `json:"TimePeriod"`
	Total      map[string]MetricValue `json:"Total"`
	Groups     []CostGroup            `json:"Groups"`
	Estimated  bool                   `json:"Estimated"`
}

// CostGroup represents a group in a cost result.
type CostGroup struct {
	Metrics map[string]MetricValue `json:"Metrics"`
	Keys    []string               `json:"Keys"`
}

// MetricValue holds Amount+Unit for a cost metric.
type MetricValue struct {
	Amount string `json:"Amount"`
	Unit   string `json:"Unit"`
}

// SavingsPlansUtilizationResult is the total savings plans utilization.
type SavingsPlansUtilizationResult struct {
	Utilization         SavingsPlansUtilizationAgg `json:"Utilization"`
	Savings             SavingsPlansSavings        `json:"Savings"`
	AmortizedCommitment SavingsPlansAmortized      `json:"AmortizedCommitment"`
}

// SavingsPlansUtilizationAgg holds SP utilization aggregates.
type SavingsPlansUtilizationAgg struct {
	TotalCommitment       string `json:"TotalCommitment"`
	UsedCommitment        string `json:"UsedCommitment"`
	UnusedCommitment      string `json:"UnusedCommitment"`
	UtilizationPercentage string `json:"UtilizationPercentage"`
}

// SavingsPlansSavings holds SP savings.
type SavingsPlansSavings struct {
	NetSavings             string `json:"NetSavings"`
	OnDemandCostEquivalent string `json:"OnDemandCostEquivalent"`
}

// SavingsPlansAmortized holds amortized commitment.
type SavingsPlansAmortized struct {
	AmortizedRecurringCommitment string `json:"AmortizedRecurringCommitment"`
	AmortizedUpfrontCommitment   string `json:"AmortizedUpfrontCommitment"`
	TotalAmortizedCommitment     string `json:"TotalAmortizedCommitment"`
}

// ReservationUtilizationByTime holds RI utilization for a time period.
type ReservationUtilizationByTime struct {
	TimePeriod map[string]string         `json:"TimePeriod"`
	Total      ReservationUtilizationAgg `json:"Total"`
	Groups     []any                     `json:"Groups"`
}

// ReservationUtilizationAgg holds RI utilization aggregates.
type ReservationUtilizationAgg struct {
	UtilizationPercentage     string `json:"UtilizationPercentage"`
	PurchasedHours            string `json:"PurchasedHours"`
	TotalActualHours          string `json:"TotalActualHours"`
	UnusedHours               string `json:"UnusedHours"`
	OnDemandCostOfRIHoursUsed string `json:"OnDemandCostOfRIHoursUsed"`
	NetRISavings              string `json:"NetRISavings"`
	TotalPotentialRISavings   string `json:"TotalPotentialRISavings"`
	AmortizedUpfrontFee       string `json:"AmortizedUpfrontFee"`
	AmortizedRecurringFee     string `json:"AmortizedRecurringFee"`
	TotalAmortizedFee         string `json:"TotalAmortizedFee"`
	RICostForUnusedHours      string `json:"RICostForUnusedHours"`
	RealizedSavings           string `json:"RealizedSavings"`
	UnrealizedSavings         string `json:"UnrealizedSavings"`
}

// ReservationCoverageByTime holds RI coverage for a time period.
type ReservationCoverageByTime struct {
	TimePeriod map[string]string      `json:"TimePeriod"`
	Total      ReservationCoverageAgg `json:"Total"`
	Groups     []any                  `json:"Groups"`
}

// ReservationCoverageAgg holds RI coverage aggregates.
type ReservationCoverageAgg struct {
	CoverageHours           ReservationCoverageHours           `json:"CoverageHours"`
	CoverageNormalizedUnits ReservationCoverageNormalizedUnits `json:"CoverageNormalizedUnits"`
	CoverageCost            ReservationCoverageCost            `json:"CoverageCost"`
}

// ReservationCoverageHours holds hourly RI coverage data.
type ReservationCoverageHours struct {
	OnDemandHours           string `json:"OnDemandHours"`
	ReservedHours           string `json:"ReservedHours"`
	TotalRunningHours       string `json:"TotalRunningHours"`
	CoverageHoursPercentage string `json:"CoverageHoursPercentage"`
}

// ReservationCoverageNormalizedUnits holds normalized unit coverage.
type ReservationCoverageNormalizedUnits struct {
	OnDemandNormalizedUnits           string `json:"OnDemandNormalizedUnits"`
	ReservedNormalizedUnits           string `json:"ReservedNormalizedUnits"`
	TotalRunningNormalizedUnits       string `json:"TotalRunningNormalizedUnits"`
	CoverageNormalizedUnitsPercentage string `json:"CoverageNormalizedUnitsPercentage"`
}

// ReservationCoverageCost holds cost-based RI coverage.
type ReservationCoverageCost struct {
	OnDemandCost string `json:"OnDemandCost"`
}

// SavingsPlansUtilizationDetail is a per-plan utilization entry.
// Utilization/AmortizedCommitment/Savings/Attributes are pointers so
// GetSavingsPlansUtilizationDetailsInput.DataType (real
// []types.SavingsPlansDataType -- ATTRIBUTES/UTILIZATION/
// AMORTIZED_COMMITMENT/SAVINGS) can genuinely omit the sections a request
// didn't ask for, matching real AWS's per-item selective population instead
// of always emitting every section regardless of what was requested.
type SavingsPlansUtilizationDetail struct {
	Attributes          map[string]string           `json:"Attributes,omitempty"`
	Utilization         *SavingsPlansUtilizationAgg `json:"Utilization,omitempty"`
	AmortizedCommitment *SavingsPlansAmortized      `json:"AmortizedCommitment,omitempty"`
	Savings             *SavingsPlansSavings        `json:"Savings,omitempty"`
	SavingsPlanARN      string                      `json:"SavingsPlanArn"`
}

// ReservationRecommendation holds a single RI recommendation group.
type ReservationRecommendation struct {
	ServiceSpecification  map[string]any                    `json:"ServiceSpecification,omitempty"`
	RecommendationSummary map[string]string                 `json:"RecommendationSummary,omitempty"`
	AccountScope          string                            `json:"AccountScope,omitempty"`
	LookbackPeriodInDays  string                            `json:"LookbackPeriodInDays,omitempty"`
	TermInYears           string                            `json:"TermInYears,omitempty"`
	PaymentOption         string                            `json:"PaymentOption,omitempty"`
	RecommendationDetails []ReservationRecommendationDetail `json:"RecommendationDetails"`
}

// ReservationRecommendationDetail is one RI recommendation.
type ReservationRecommendationDetail struct {
	AccountID                                 string         `json:"AccountId,omitempty"`
	InstanceDetails                           map[string]any `json:"InstanceDetails,omitempty"`
	RecommendedNumberOfInstancesToPurchase    string         `json:"RecommendedNumberOfInstancesToPurchase"`
	RecommendedNormalizedUnitsToPurchase      string         `json:"RecommendedNormalizedUnitsToPurchase"`
	MinimumNumberOfInstancesUsedPerHour       string         `json:"MinimumNumberOfInstancesUsedPerHour"`
	MinimumNormalizedUnitsUsedPerHour         string         `json:"MinimumNormalizedUnitsUsedPerHour"`
	MaximumNumberOfInstancesUsedPerHour       string         `json:"MaximumNumberOfInstancesUsedPerHour"`
	MaximumNormalizedUnitsUsedPerHour         string         `json:"MaximumNormalizedUnitsUsedPerHour"`
	AverageNumberOfInstancesUsedPerHour       string         `json:"AverageNumberOfInstancesUsedPerHour"`
	AverageNormalizedUnitsUsedPerHour         string         `json:"AverageNormalizedUnitsUsedPerHour"`
	AverageUtilization                        string         `json:"AverageUtilization"`
	EstimatedBreakEvenInMonths                string         `json:"EstimatedBreakEvenInMonths"`
	CurrencyCode                              string         `json:"CurrencyCode"`
	EstimatedMonthlySavingsAmount             string         `json:"EstimatedMonthlySavingsAmount"`
	EstimatedMonthlySavingsPercentage         string         `json:"EstimatedMonthlySavingsPercentage"`
	EstimatedMonthlyOnDemandCost              string         `json:"EstimatedMonthlyOnDemandCost"`
	EstimatedReservationCostForLookbackPeriod string         `json:"EstimatedReservationCostForLookbackPeriod"`
	UpfrontCost                               string         `json:"UpfrontCost"`
	RecurringStandardMonthlyCost              string         `json:"RecurringStandardMonthlyCost"`
}

// RightsizingRecommendation is a single rightsizing recommendation.
type RightsizingRecommendation struct {
	ModifyRecommendationDetail    *RightsizingModifyDetail    `json:"ModifyRecommendationDetail,omitempty"`
	TerminateRecommendationDetail *RightsizingTerminateDetail `json:"TerminateRecommendationDetail,omitempty"`
	CurrentInstance               RightsizingCurrentInstance  `json:"CurrentInstance"`
	AccountID                     string                      `json:"AccountId"`
	RightsizingType               string                      `json:"RightsizingType"`
}

// RightsizingCurrentInstance holds details about the current instance.
type RightsizingCurrentInstance struct {
	ResourceID   string `json:"ResourceId"`
	InstanceType string `json:"InstanceType,omitempty"`
	MonthlyCost  string `json:"MonthlyCost,omitempty"`
	CurrencyCode string `json:"CurrencyCode,omitempty"`
}

// RightsizingModifyDetail holds modification target options.
type RightsizingModifyDetail struct {
	TargetInstances []RightsizingTargetInstance `json:"TargetInstances"`
}

// RightsizingTerminateDetail holds termination savings estimate.
type RightsizingTerminateDetail struct {
	EstimatedMonthlySavings string `json:"EstimatedMonthlySavings,omitempty"`
	CurrencyCode            string `json:"CurrencyCode,omitempty"`
}

// RightsizingTargetInstance is a recommended replacement instance.
type RightsizingTargetInstance struct {
	ResourceDetails            map[string]any `json:"ResourceDetails,omitempty"`
	EstimatedMonthlyCost       string         `json:"EstimatedMonthlyCost"`
	EstimatedMonthlySavings    string         `json:"EstimatedMonthlySavings"`
	EstimatedSavingsPercentage string         `json:"EstimatedSavingsPercentage"`
	CurrencyCode               string         `json:"CurrencyCode"`
	DefaultTargetInstance      bool           `json:"DefaultTargetInstance"`
}

// CostAllocationTagStatusEntry is a TagKey+Status pair for UpdateCostAllocationTagsStatus.
type CostAllocationTagStatusEntry struct {
	TagKey string `json:"TagKey"`
	Status string `json:"Status"`
}

// CostAllocationTagError holds an error for a single tag key update.
type CostAllocationTagError struct {
	TagKey  string `json:"TagKey"`
	Code    string `json:"Code"`
	Message string `json:"Message"`
}

// ForecastResult represents a single time-bucket forecast entry.
type ForecastResult struct {
	TimePeriod                   map[string]string `json:"TimePeriod"`
	MeanValue                    string            `json:"MeanValue"`
	PredictionIntervalLowerBound string            `json:"PredictionIntervalLowerBound,omitempty"`
	PredictionIntervalUpperBound string            `json:"PredictionIntervalUpperBound,omitempty"`
}
