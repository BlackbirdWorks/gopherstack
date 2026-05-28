// Package ce provides an in-memory implementation of the AWS Cost Explorer (Ce) service.
package ce

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// DefaultAnomalyTTL is the default time-to-live for detected anomalies.
const DefaultAnomalyTTL = 30 * 24 * time.Hour

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)
	// ErrValidation is returned when input parameters fail validation.
	ErrValidation = errors.New("InvalidParameterException")
	// ErrDataUnavailable is returned when queried data is not available for the time range.
	ErrDataUnavailable = awserr.New("DataUnavailableException", awserr.ErrNotFound)
)

// isValidMonitorType reports whether t is a valid AnomalyMonitor MonitorType.
func isValidMonitorType(t string) bool {
	switch t {
	case "DIMENSIONAL", "CUSTOM":
		return true
	default:
		return false
	}
}

// isValidFrequency reports whether f is a valid AnomalySubscription Frequency.
func isValidFrequency(f string) bool {
	switch f {
	case "DAILY", "IMMEDIATE", "WEEKLY":
		return true
	default:
		return false
	}
}

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
type AnomalyMonitor struct {
	CreationDate     time.Time         `json:"creationDate"`
	Tags             map[string]string `json:"tags"`
	MonitorARN       string            `json:"monitorARN"`
	MonitorName      string            `json:"monitorName"`
	MonitorType      string            `json:"monitorType"`
	MonitorDimension string            `json:"monitorDimension"`
}

// AnomalySubscription represents an in-memory AWS CE anomaly subscription.
type AnomalySubscription struct {
	CreationDate     time.Time         `json:"creationDate"`
	Tags             map[string]string `json:"tags"`
	SubscriptionARN  string            `json:"subscriptionARN"`
	SubscriptionName string            `json:"subscriptionName"`
	Frequency        string            `json:"frequency"`
	MonitorARNList   []string          `json:"monitorARNList"`
	Subscribers      []Subscriber      `json:"subscribers"`
	Threshold        float64           `json:"threshold"`
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
	AnomalyScore     AnomalyScore       `json:"anomalyScore"`
	TotalImpact      float64            `json:"totalImpact"`
	RootCauses       []AnomalyRootCause `json:"rootCauses,omitempty"`
}

// Subscriber represents a CE anomaly subscription notification target.
type Subscriber struct {
	Address string `json:"address"`
	Type    string `json:"type"`
	Status  string `json:"status"`
}

// CostEntry is a synthetic cost ledger entry for a single day+service combination.
type CostEntry struct {
	Date          string            `json:"date"`
	Service       string            `json:"service"`
	Region        string            `json:"region"`
	UsageType     string            `json:"usageType"`
	Account       string            `json:"account"`
	Tags          map[string]string `json:"tags"`
	BlendedCost   float64           `json:"blendedCost"`
	UnblendedCost float64           `json:"unblendedCost"`
	UsageQuantity float64           `json:"usageQuantity"`
}

// CostAllocationTag represents an AWS CE cost allocation tag.
type CostAllocationTag struct {
	TagKey          string `json:"tagKey"`
	Status          string `json:"status"`          // Active | Inactive
	Type            string `json:"type"`             // AWSGenerated | UserDefined
	LastUpdatedDate string `json:"lastUpdatedDate"`
}

// BackfillJob represents a cost allocation tag backfill job.
type BackfillJob struct {
	BackfillFrom   string `json:"backfillFrom"`
	RequestedAt    string `json:"requestedAt"`
	CompletedAt    string `json:"completedAt,omitempty"`
	BackfillStatus string `json:"backfillStatus"` // SUCCEEDED|PROCESSING|FAILED
	LastUpdatedAt  string `json:"lastUpdatedAt"`
}

// CommitmentAnalysis represents a commitment purchase analysis.
type CommitmentAnalysis struct {
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
	Keys    []string               `json:"Keys"`
	Metrics map[string]MetricValue `json:"Metrics"`
}

// MetricValue holds Amount+Unit for a cost metric.
type MetricValue struct {
	Amount string `json:"Amount"`
	Unit   string `json:"Unit"`
}

// SavingsPlansUtilizationResult is the total savings plans utilization.
type SavingsPlansUtilizationResult struct {
	Utilization         SavingsPlansUtilizationAgg `json:"Utilization"`
	Savings             SavingsPlansSavings        `json:"Savings,omitempty"`
	AmortizedCommitment SavingsPlansAmortized      `json:"AmortizedCommitment,omitempty"`
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
	Groups     []any                     `json:"Groups"`
	Total      ReservationUtilizationAgg `json:"Total"`
}

// ReservationUtilizationAgg holds RI utilization aggregates.
type ReservationUtilizationAgg struct {
	UtilizationPercentage       string `json:"UtilizationPercentage"`
	PurchasedHours              string `json:"PurchasedHours"`
	TotalActualHours            string `json:"TotalActualHours"`
	UnusedHours                 string `json:"UnusedHours"`
	OnDemandCostOfRIHoursUsed   string `json:"OnDemandCostOfRIHoursUsed"`
	NetRISavings                string `json:"NetRISavings"`
	TotalPotentialRISavings     string `json:"TotalPotentialRISavings"`
	AmortizedUpfrontFee         string `json:"AmortizedUpfrontFee"`
	AmortizedRecurringFee       string `json:"AmortizedRecurringFee"`
	TotalAmortizedFee           string `json:"TotalAmortizedFee"`
	RICostForUnusedHours        string `json:"RICostForUnusedHours"`
	RealizedSavings             string `json:"RealizedSavings"`
	UnrealizedSavings           string `json:"UnrealizedSavings"`
}

// ReservationCoverageByTime holds RI coverage for a time period.
type ReservationCoverageByTime struct {
	TimePeriod map[string]string     `json:"TimePeriod"`
	Groups     []any                 `json:"Groups"`
	Total      ReservationCoverageAgg `json:"Total"`
}

// ReservationCoverageAgg holds RI coverage aggregates.
type ReservationCoverageAgg struct {
	CoverageHours           ReservationCoverageHours           `json:"CoverageHours"`
	CoverageNormalizedUnits ReservationCoverageNormalizedUnits `json:"CoverageNormalizedUnits,omitempty"`
	CoverageCost            ReservationCoverageCost            `json:"CoverageCost,omitempty"`
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
type SavingsPlansUtilizationDetail struct {
	SavingsPlanARN      string                     `json:"SavingsPlanArn"`
	Utilization         SavingsPlansUtilizationAgg `json:"Utilization"`
	Savings             SavingsPlansSavings        `json:"Savings"`
	AmortizedCommitment SavingsPlansAmortized      `json:"AmortizedCommitment"`
	Attributes          map[string]string          `json:"Attributes,omitempty"`
}

// ReservationRecommendation holds a single RI recommendation group.
type ReservationRecommendation struct {
	AccountScope          string                            `json:"AccountScope,omitempty"`
	LookbackPeriodInDays  string                            `json:"LookbackPeriodInDays,omitempty"`
	TermInYears           string                            `json:"TermInYears,omitempty"`
	PaymentOption         string                            `json:"PaymentOption,omitempty"`
	ServiceSpecification  map[string]any                    `json:"ServiceSpecification,omitempty"`
	RecommendationDetails []ReservationRecommendationDetail `json:"RecommendationDetails"`
	RecommendationSummary map[string]string                 `json:"RecommendationSummary,omitempty"`
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
	AccountID                     string                      `json:"AccountId"`
	CurrentInstance               RightsizingCurrentInstance  `json:"CurrentInstance"`
	RightsizingType               string                      `json:"RightsizingType"`
	ModifyRecommendationDetail    *RightsizingModifyDetail    `json:"ModifyRecommendationDetail,omitempty"`
	TerminateRecommendationDetail *RightsizingTerminateDetail `json:"TerminateRecommendationDetail,omitempty"`
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
	EstimatedMonthlyCost       string         `json:"EstimatedMonthlyCost"`
	EstimatedMonthlySavings    string         `json:"EstimatedMonthlySavings"`
	EstimatedSavingsPercentage string         `json:"EstimatedSavingsPercentage"`
	CurrencyCode               string         `json:"CurrencyCode"`
	DefaultTargetInstance      bool           `json:"DefaultTargetInstance"`
	ResourceDetails            map[string]any `json:"ResourceDetails,omitempty"`
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

// InMemoryBackend is a thread-safe in-memory store for Cost Explorer resources.
type InMemoryBackend struct {
	costCategories       map[string]*CostCategory
	anomalyMonitors      map[string]*AnomalyMonitor
	anomalySubscriptions map[string]*AnomalySubscription
	anomalies            map[string]*Anomaly
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
	anomalyTTL           time.Duration
	costLedger           []CostEntry
	costAllocationTags   map[string]*CostAllocationTag
	backfillJobs         []*BackfillJob
	commitmentAnalyses   map[string]*CommitmentAnalysis
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		costCategories:       make(map[string]*CostCategory),
		anomalyMonitors:      make(map[string]*AnomalyMonitor),
		anomalySubscriptions: make(map[string]*AnomalySubscription),
		anomalies:            make(map[string]*Anomaly),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("ce"),
		anomalyTTL:           DefaultAnomalyTTL,
		costAllocationTags:   make(map[string]*CostAllocationTag),
		commitmentAnalyses:   make(map[string]*CommitmentAnalysis),
	}
	b.seedCostLedger()

	return b
}

// StartJanitor launches a background goroutine that evicts anomalies older than
// the backend's TTL. It stops when ctx is cancelled.
func (b *InMemoryBackend) StartJanitor(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.evictExpiredAnomalies()
			}
		}
	}()
}

// evictExpiredAnomalies removes anomalies whose CreationDate is older than anomalyTTL.
func (b *InMemoryBackend) evictExpiredAnomalies() {
	b.mu.Lock("evictExpiredAnomalies")
	defer b.mu.Unlock()

	cutoff := time.Now().UTC().Add(-b.anomalyTTL)

	for id, a := range b.anomalies {
		if a.CreationDate.Before(cutoff) {
			delete(b.anomalies, id)
		}
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.costCategories = make(map[string]*CostCategory)
	b.anomalyMonitors = make(map[string]*AnomalyMonitor)
	b.anomalySubscriptions = make(map[string]*AnomalySubscription)
	b.anomalies = make(map[string]*Anomaly)
	b.costLedger = nil
	b.costAllocationTags = make(map[string]*CostAllocationTag)
	b.backfillJobs = nil
	b.commitmentAnalyses = make(map[string]*CommitmentAnalysis)
	b.seedCostLedger()
}

func (b *InMemoryBackend) buildCostCategoryARN(name string) string {
	return fmt.Sprintf("arn:aws:ce::%s:costcategory/%s", b.accountID, name)
}

func (b *InMemoryBackend) buildAnomalyMonitorARN() string {
	return fmt.Sprintf("arn:aws:ce::%s:anomalymonitor/%s", b.accountID, uuid.NewString())
}

func (b *InMemoryBackend) buildAnomalySubscriptionARN() string {
	return fmt.Sprintf("arn:aws:ce::%s:anomalysubscription/%s", b.accountID, uuid.NewString())
}

func effectiveStart() string {
	now := time.Now().UTC()

	return fmt.Sprintf("%d-%02d-01T00:00:00Z", now.Year(), now.Month())
}

// CreateCostCategoryDefinition creates a new cost category and returns it.
func (b *InMemoryBackend) CreateCostCategoryDefinition(
	name, ruleVersion, defaultValue string,
	rules []CostCategoryRule,
	resourceTags map[string]string,
) (*CostCategory, error) {
	b.mu.Lock("CreateCostCategoryDefinition")
	defer b.mu.Unlock()

	catARN := b.buildCostCategoryARN(name)
	if _, exists := b.costCategories[catARN]; exists {
		return nil, ErrAlreadyExists
	}

	tagsCopy := make(map[string]string, len(resourceTags))
	maps.Copy(tagsCopy, resourceTags)

	rulesCopy := make([]CostCategoryRule, len(rules))
	copy(rulesCopy, rules)

	cat := &CostCategory{
		ARN:            catARN,
		Name:           name,
		RuleVersion:    ruleVersion,
		DefaultValue:   defaultValue,
		Rules:          rulesCopy,
		EffectiveStart: effectiveStart(),
		CreationDate:   time.Now().UTC(),
		Tags:           tagsCopy,
	}
	b.costCategories[catARN] = cat

	out := *cat
	out.Rules = make([]CostCategoryRule, len(cat.Rules))
	copy(out.Rules, cat.Rules)

	return &out, nil
}

// DeleteCostCategoryDefinition removes a cost category by ARN.
func (b *InMemoryBackend) DeleteCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.Lock("DeleteCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories[catARN]
	if !exists {
		return nil, ErrNotFound
	}

	delete(b.costCategories, catARN)

	out := *cat

	return &out, nil
}

// DescribeCostCategoryDefinition returns a cost category by ARN.
func (b *InMemoryBackend) DescribeCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.RLock("DescribeCostCategoryDefinition")
	defer b.mu.RUnlock()

	cat, exists := b.costCategories[catARN]
	if !exists {
		return nil, ErrNotFound
	}

	out := *cat

	return &out, nil
}

// ListCostCategoryDefinitions returns all cost categories sorted by name.
func (b *InMemoryBackend) ListCostCategoryDefinitions() []*CostCategory {
	b.mu.RLock("ListCostCategoryDefinitions")
	defer b.mu.RUnlock()

	result := make([]*CostCategory, 0, len(b.costCategories))
	for _, cat := range b.costCategories {
		out := *cat
		result = append(result, &out)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})

	return result
}

// UpdateCostCategoryDefinition updates an existing cost category.
func (b *InMemoryBackend) UpdateCostCategoryDefinition(
	catARN, ruleVersion, defaultValue string,
	rules []CostCategoryRule,
	splitChargeRules []SplitChargeRule,
) (*CostCategory, error) {
	b.mu.Lock("UpdateCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories[catARN]
	if !exists {
		return nil, ErrNotFound
	}

	cat.RuleVersion = ruleVersion
	cat.DefaultValue = defaultValue
	// Deep-copy both slices so the caller cannot alias backend-owned state.
	rulesCopy := make([]CostCategoryRule, len(rules))
	copy(rulesCopy, rules)
	cat.Rules = rulesCopy

	splitCopy := make([]SplitChargeRule, len(splitChargeRules))
	for i, s := range splitChargeRules {
		sc := s
		if s.Targets != nil {
			sc.Targets = make([]string, len(s.Targets))
			copy(sc.Targets, s.Targets)
		}

		splitCopy[i] = sc
	}

	cat.SplitChargeRules = splitCopy
	cat.EffectiveStart = effectiveStart()

	out := *cat
	out.Rules = make([]CostCategoryRule, len(cat.Rules))
	copy(out.Rules, cat.Rules)
	out.SplitChargeRules = make([]SplitChargeRule, len(cat.SplitChargeRules))
	copy(out.SplitChargeRules, cat.SplitChargeRules)

	return &out, nil
}

// ListTagsForResource returns the tags for a CE resource by ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if cat, ok := b.costCategories[resourceARN]; ok {
		out := make(map[string]string, len(cat.Tags))
		maps.Copy(out, cat.Tags)

		return out, nil
	}

	if mon, ok := b.anomalyMonitors[resourceARN]; ok {
		out := make(map[string]string, len(mon.Tags))
		maps.Copy(out, mon.Tags)

		return out, nil
	}

	if sub, ok := b.anomalySubscriptions[resourceARN]; ok {
		out := make(map[string]string, len(sub.Tags))
		maps.Copy(out, sub.Tags)

		return out, nil
	}

	return nil, ErrNotFound
}

// TagResource adds or updates tags on a CE resource.
func (b *InMemoryBackend) TagResource(resourceARN string, resourceTags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if cat, ok := b.costCategories[resourceARN]; ok {
		maps.Copy(cat.Tags, resourceTags)

		return nil
	}

	if mon, ok := b.anomalyMonitors[resourceARN]; ok {
		maps.Copy(mon.Tags, resourceTags)

		return nil
	}

	if sub, ok := b.anomalySubscriptions[resourceARN]; ok {
		maps.Copy(sub.Tags, resourceTags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CE resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if cat, ok := b.costCategories[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(cat.Tags, k)
		}

		return nil
	}

	if mon, ok := b.anomalyMonitors[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(mon.Tags, k)
		}

		return nil
	}

	if sub, ok := b.anomalySubscriptions[resourceARN]; ok {
		for _, k := range tagKeys {
			delete(sub.Tags, k)
		}

		return nil
	}

	return ErrNotFound
}

// CreateAnomalyMonitor creates a new anomaly monitor.
func (b *InMemoryBackend) CreateAnomalyMonitor(
	monitorName, monitorType, monitorDimension string,
	resourceTags map[string]string,
) (*AnomalyMonitor, error) {
	b.mu.Lock("CreateAnomalyMonitor")
	defer b.mu.Unlock()

	if monitorType != "" {
		if !isValidMonitorType(monitorType) {
			return nil, fmt.Errorf("%w: MonitorType must be one of DIMENSIONAL, CUSTOM", ErrValidation)
		}
	}

	tagsCopy := make(map[string]string, len(resourceTags))
	maps.Copy(tagsCopy, resourceTags)

	monARN := b.buildAnomalyMonitorARN()
	mon := &AnomalyMonitor{
		MonitorARN:       monARN,
		MonitorName:      monitorName,
		MonitorType:      monitorType,
		MonitorDimension: monitorDimension,
		CreationDate:     time.Now().UTC(),
		Tags:             tagsCopy,
	}
	b.anomalyMonitors[monARN] = mon

	out := *mon

	return &out, nil
}

// DeleteAnomalyMonitor removes an anomaly monitor by ARN.
func (b *InMemoryBackend) DeleteAnomalyMonitor(monARN string) error {
	b.mu.Lock("DeleteAnomalyMonitor")
	defer b.mu.Unlock()

	if _, exists := b.anomalyMonitors[monARN]; !exists {
		return ErrNotFound
	}

	delete(b.anomalyMonitors, monARN)

	return nil
}

// GetAnomalyMonitors returns anomaly monitors, optionally filtered by ARNs, sorted by MonitorARN.
func (b *InMemoryBackend) GetAnomalyMonitors(monitorARNList []string) []*AnomalyMonitor {
	b.mu.RLock("GetAnomalyMonitors")
	defer b.mu.RUnlock()

	var result []*AnomalyMonitor

	if len(monitorARNList) == 0 {
		result = make([]*AnomalyMonitor, 0, len(b.anomalyMonitors))
		for _, mon := range b.anomalyMonitors {
			out := *mon
			result = append(result, &out)
		}
	} else {
		set := make(map[string]struct{}, len(monitorARNList))
		for _, a := range monitorARNList {
			set[a] = struct{}{}
		}

		result = make([]*AnomalyMonitor, 0, len(monitorARNList))

		for _, mon := range b.anomalyMonitors {
			if _, ok := set[mon.MonitorARN]; ok {
				out := *mon
				result = append(result, &out)
			}
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].MonitorARN < result[j].MonitorARN
	})

	return result
}

// UpdateAnomalyMonitor updates the name of an anomaly monitor.
func (b *InMemoryBackend) UpdateAnomalyMonitor(monARN, monitorName string) (*AnomalyMonitor, error) {
	b.mu.Lock("UpdateAnomalyMonitor")
	defer b.mu.Unlock()

	mon, exists := b.anomalyMonitors[monARN]
	if !exists {
		return nil, ErrNotFound
	}

	mon.MonitorName = monitorName

	out := *mon

	return &out, nil
}

// CreateAnomalySubscription creates a new anomaly subscription.
func (b *InMemoryBackend) CreateAnomalySubscription(
	subscriptionName, frequency string,
	monitorARNList []string,
	subscribers []Subscriber,
	threshold float64,
	resourceTags map[string]string,
) (*AnomalySubscription, error) {
	b.mu.Lock("CreateAnomalySubscription")
	defer b.mu.Unlock()

	if frequency != "" {
		if !isValidFrequency(frequency) {
			return nil, fmt.Errorf("%w: Frequency must be one of DAILY, IMMEDIATE, WEEKLY", ErrValidation)
		}
	}

	tagsCopy := make(map[string]string, len(resourceTags))
	maps.Copy(tagsCopy, resourceTags)

	monCopy := make([]string, len(monitorARNList))
	copy(monCopy, monitorARNList)

	subsCopy := make([]Subscriber, len(subscribers))
	copy(subsCopy, subscribers)

	subARN := b.buildAnomalySubscriptionARN()
	sub := &AnomalySubscription{
		SubscriptionARN:  subARN,
		SubscriptionName: subscriptionName,
		Frequency:        frequency,
		MonitorARNList:   monCopy,
		Subscribers:      subsCopy,
		Threshold:        threshold,
		CreationDate:     time.Now().UTC(),
		Tags:             tagsCopy,
	}
	b.anomalySubscriptions[subARN] = sub

	out := *sub

	return &out, nil
}

// DeleteAnomalySubscription removes an anomaly subscription by ARN.
func (b *InMemoryBackend) DeleteAnomalySubscription(subARN string) error {
	b.mu.Lock("DeleteAnomalySubscription")
	defer b.mu.Unlock()

	if _, exists := b.anomalySubscriptions[subARN]; !exists {
		return ErrNotFound
	}

	delete(b.anomalySubscriptions, subARN)

	return nil
}

// GetAnomalySubscriptions returns anomaly subscriptions, optionally filtered by ARNs or monitor ARN,
// sorted by SubscriptionARN.
func (b *InMemoryBackend) GetAnomalySubscriptions(
	subscriptionARNList []string,
	monitorARN string,
) []*AnomalySubscription {
	b.mu.RLock("GetAnomalySubscriptions")
	defer b.mu.RUnlock()

	var result []*AnomalySubscription

	if len(subscriptionARNList) == 0 {
		result = make([]*AnomalySubscription, 0, len(b.anomalySubscriptions))

		for _, sub := range b.anomalySubscriptions {
			if monitorARN != "" && !containsString(sub.MonitorARNList, monitorARN) {
				continue
			}

			out := *sub
			result = append(result, &out)
		}
	} else {
		set := make(map[string]struct{}, len(subscriptionARNList))
		for _, a := range subscriptionARNList {
			set[a] = struct{}{}
		}

		result = make([]*AnomalySubscription, 0, len(subscriptionARNList))

		for _, sub := range b.anomalySubscriptions {
			if _, ok := set[sub.SubscriptionARN]; !ok {
				continue
			}

			if monitorARN != "" && !containsString(sub.MonitorARNList, monitorARN) {
				continue
			}

			out := *sub
			result = append(result, &out)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].SubscriptionARN < result[j].SubscriptionARN
	})

	return result
}

// containsString reports whether s appears in slice.
func containsString(slice []string, s string) bool {
	return slices.Contains(slice, s)
}

// UpdateAnomalySubscription updates a CE anomaly subscription.
func (b *InMemoryBackend) UpdateAnomalySubscription(
	subARN, frequency, subscriptionName string,
	monitorARNList []string,
	subscribers []Subscriber,
	threshold float64,
) (*AnomalySubscription, error) {
	b.mu.Lock("UpdateAnomalySubscription")
	defer b.mu.Unlock()

	sub, exists := b.anomalySubscriptions[subARN]
	if !exists {
		return nil, ErrNotFound
	}

	if frequency != "" {
		sub.Frequency = frequency
	}

	if subscriptionName != "" {
		sub.SubscriptionName = subscriptionName
	}

	if len(monitorARNList) > 0 {
		monCopy := make([]string, len(monitorARNList))
		copy(monCopy, monitorARNList)
		sub.MonitorARNList = monCopy
	}

	if len(subscribers) > 0 {
		subsCopy := make([]Subscriber, len(subscribers))
		copy(subsCopy, subscribers)
		sub.Subscribers = subsCopy
	}

	if threshold > 0 {
		sub.Threshold = threshold
	}

	out := *sub

	return &out, nil
}

// GetAnomalies returns detected anomalies, optionally filtered by monitor ARN and feedback type,
// sorted by AnomalyID.
func (b *InMemoryBackend) GetAnomalies(monitorARN, feedback string) []*Anomaly {
	b.mu.RLock("GetAnomalies")
	defer b.mu.RUnlock()

	result := make([]*Anomaly, 0, len(b.anomalies))

	for _, a := range b.anomalies {
		if monitorARN != "" && a.MonitorARN != monitorARN {
			continue
		}

		if feedback != "" && a.FeedbackType != feedback {
			continue
		}

		out := *a
		result = append(result, &out)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AnomalyID < result[j].AnomalyID
	})

	return result
}

// AddAnomaly inserts an anomaly into the backend. It is intended for testing.
func (b *InMemoryBackend) AddAnomaly(a Anomaly) {
	b.mu.Lock("AddAnomaly")
	defer b.mu.Unlock()

	if a.AnomalyID == "" {
		a.AnomalyID = uuid.NewString()
	}

	if a.CreationDate.IsZero() {
		a.CreationDate = time.Now().UTC()
	}

	cp := a
	b.anomalies[a.AnomalyID] = &cp
}

// GetCostCategories returns the distinct cost category values stored in the
// backend, optionally filtered by cost category name. Values are sorted alphabetically.
func (b *InMemoryBackend) GetCostCategories(costCategoryName string) []string {
	b.mu.RLock("GetCostCategories")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var values []string

	for _, cat := range b.costCategories {
		if costCategoryName != "" && cat.Name != costCategoryName {
			continue
		}

		for _, rule := range cat.Rules {
			if _, exists := seen[rule.Value]; !exists && rule.Value != "" {
				seen[rule.Value] = struct{}{}
				values = append(values, rule.Value)
			}
		}
	}

	sort.Strings(values)

	return values
}

// seedCostLedger populates the cost ledger with 90 days of synthetic data.
// Seeded distributions: EC2~40%, S3~15%, RDS~10%, Lambda~8%, CloudFront~7%, others rest.
func (b *InMemoryBackend) seedCostLedger() {
	type serviceDef struct {
		name      string
		usageType string
		weight    float64
	}

	services := []serviceDef{
		{"Amazon Elastic Compute Cloud - Compute", "BoxUsage:t3.medium", 0.40},
		{"Amazon Simple Storage Service", "TimedStorage-ByteHrs", 0.15},
		{"Amazon Relational Database Service", "InstanceUsage:db.t3.medium", 0.10},
		{"AWS Lambda", "Lambda-GB-Second", 0.08},
		{"Amazon CloudFront", "US-DataTransfer-Out-Bytes", 0.07},
		{"Amazon DynamoDB", "TimedStorage-ByteHrs", 0.05},
		{"Amazon Elastic Load Balancing", "LoadBalancerUsage", 0.04},
		{"AWS Key Management Service", "KMS-Requests", 0.03},
		{"Amazon Route 53", "DNS-Queries", 0.02},
		{"AWS CloudTrail", "EUS-DataScanned", 0.01},
		{"Amazon SNS", "DeliveryAttempts-HTTP", 0.005},
		{"Amazon SQS", "SQS-Requests", 0.005},
	}

	now := time.Now().UTC()
	totalDailyBase := 150.0

	for day := 89; day >= 0; day-- {
		d := now.AddDate(0, 0, -day)
		dateStr := d.Format("2006-01-02")

		dayMultiplier := 1.0
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			dayMultiplier = 0.8
		}

		jitter := 1.0 + float64(d.YearDay()%10-5)*0.01

		for _, svc := range services {
			amount := totalDailyBase * svc.weight * dayMultiplier * jitter
			b.costLedger = append(b.costLedger, CostEntry{
				Date:          dateStr,
				Service:       svc.name,
				Region:        b.region,
				UsageType:     svc.usageType,
				Account:       b.accountID,
				BlendedCost:   amount,
				UnblendedCost: amount * 0.98,
				UsageQuantity: amount * 10,
			})
		}
	}
}

// costLedgerInBucket returns entries where start <= date < end (no lock — caller holds).
func (b *InMemoryBackend) costLedgerInBucket(start, end string) []CostEntry {
	var out []CostEntry

	for _, e := range b.costLedger {
		if e.Date >= start && e.Date < end {
			out = append(out, e)
		}
	}

	return out
}

type timeBucket struct{ start, end string }

// buildTimeBuckets generates date-bucket boundaries for DAILY/MONTHLY/HOURLY granularity.
func buildTimeBuckets(start, end, granularity string) []timeBucket {
	var buckets []timeBucket

	startT, err1 := time.Parse("2006-01-02", start)
	endT, err2 := time.Parse("2006-01-02", end)

	if err1 != nil || err2 != nil {
		return buckets
	}

	switch strings.ToUpper(granularity) {
	case "MONTHLY":
		cur := time.Date(startT.Year(), startT.Month(), 1, 0, 0, 0, 0, time.UTC)
		for cur.Before(endT) {
			next := cur.AddDate(0, 1, 0)
			buckets = append(buckets, timeBucket{
				start: cur.Format("2006-01-02"),
				end:   next.Format("2006-01-02"),
			})
			cur = next
		}
	default: // DAILY (HOURLY treated as DAILY for simplicity)
		cur := startT
		for cur.Before(endT) {
			next := cur.AddDate(0, 0, 1)
			buckets = append(buckets, timeBucket{
				start: cur.Format("2006-01-02"),
				end:   next.Format("2006-01-02"),
			})
			cur = next
		}
	}

	return buckets
}

func extractGroupKeys(e CostEntry, groupBy []GroupBySpec) []string {
	keys := make([]string, 0, len(groupBy))

	for _, g := range groupBy {
		switch strings.ToUpper(g.Key) {
		case "SERVICE":
			keys = append(keys, e.Service)
		case "REGION":
			keys = append(keys, e.Region)
		case "USAGE_TYPE":
			keys = append(keys, e.UsageType)
		case "LINKED_ACCOUNT":
			keys = append(keys, e.Account)
		default:
			keys = append(keys, e.Tags[g.Key])
		}
	}

	return keys
}

func getMetricValue(e CostEntry, metric string) float64 {
	switch strings.ToUpper(metric) {
	case "BLENDEDCOST":
		return e.BlendedCost
	case "UNBLENDEDCOST":
		return e.UnblendedCost
	case "AMORTIZEDCOST", "NETAMORTIZEDCOST":
		return e.UnblendedCost * 0.99
	case "NETUNBLENDEDCOST":
		return e.UnblendedCost * 0.97
	case "USAGEQUANTITY":
		return e.UsageQuantity
	case "NORMALIZEDUSAGEAMOUNT":
		return e.UsageQuantity * 4
	default:
		return e.BlendedCost
	}
}

func metricUnit(metric string) string {
	switch strings.ToUpper(metric) {
	case "USAGEQUANTITY", "NORMALIZEDUSAGEAMOUNT":
		return "N/A"
	default:
		return "USD"
	}
}

func calcMeanStddev(values []float64) (mean, stddev float64) {
	if len(values) == 0 {
		return 0, 1
	}

	for _, v := range values {
		mean += v
	}

	mean /= float64(len(values))

	for _, v := range values {
		diff := v - mean
		stddev += diff * diff
	}

	if len(values) > 1 {
		stddev = stddev / float64(len(values)-1)
	}

	stddev = math.Sqrt(stddev)

	if stddev < 0.01 {
		stddev = mean * 0.05
	}

	return mean, stddev
}

// GetCostAndUsage aggregates cost ledger entries by granularity, applying optional GroupBy.
func (b *InMemoryBackend) GetCostAndUsage(
	start, end, granularity string,
	metrics []string,
	groupBy []GroupBySpec,
) []ResultByTime {
	b.mu.RLock("GetCostAndUsage")
	defer b.mu.RUnlock()

	if len(metrics) == 0 {
		metrics = []string{"BlendedCost"}
	}

	buckets := buildTimeBuckets(start, end, granularity)
	results := make([]ResultByTime, 0, len(buckets))

	now := time.Now().UTC().Format("2006-01-02")

	for _, bucket := range buckets {
		entries := b.costLedgerInBucket(bucket.start, bucket.end)
		isEstimated := bucket.start >= now || bucket.end > now

		r := ResultByTime{
			TimePeriod: map[string]string{"Start": bucket.start, "End": bucket.end},
			Estimated:  isEstimated,
			Groups:     []CostGroup{},
		}

		if len(groupBy) > 0 {
			groupMap := make(map[string]map[string]float64)
			keyMap := make(map[string][]string)

			for _, e := range entries {
				keys := extractGroupKeys(e, groupBy)
				gkey := strings.Join(keys, "|")

				if _, ok := groupMap[gkey]; !ok {
					groupMap[gkey] = make(map[string]float64)
					keyMap[gkey] = keys
				}

				for _, m := range metrics {
					groupMap[gkey][m] += getMetricValue(e, m)
				}
			}

			gkeys := make([]string, 0, len(groupMap))
			for gk := range groupMap {
				gkeys = append(gkeys, gk)
			}

			sort.Strings(gkeys)

			for _, gk := range gkeys {
				mv := make(map[string]MetricValue, len(metrics))

				for _, m := range metrics {
					mv[m] = MetricValue{
						Amount: fmt.Sprintf("%.4f", groupMap[gk][m]),
						Unit:   metricUnit(m),
					}
				}

				r.Groups = append(r.Groups, CostGroup{
					Keys:    keyMap[gk],
					Metrics: mv,
				})
			}

			r.Total = map[string]MetricValue{}
		} else {
			totals := make(map[string]float64)

			for _, e := range entries {
				for _, m := range metrics {
					totals[m] += getMetricValue(e, m)
				}
			}

			r.Total = make(map[string]MetricValue, len(metrics))
			for _, m := range metrics {
				r.Total[m] = MetricValue{
					Amount: fmt.Sprintf("%.4f", totals[m]),
					Unit:   metricUnit(m),
				}
			}
		}

		results = append(results, r)
	}

	return results
}

// GetDimensionValues returns unique values for the given dimension from the cost ledger.
func (b *InMemoryBackend) GetDimensionValues(dimension string) []string {
	b.mu.RLock("GetDimensionValues")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		var val string

		switch strings.ToUpper(dimension) {
		case "SERVICE":
			val = e.Service
		case "REGION", "AZ":
			val = e.Region
		case "USAGE_TYPE":
			val = e.UsageType
		case "LINKED_ACCOUNT":
			val = e.Account
		case "INSTANCE_TYPE":
			val = "t3.medium"
		case "OPERATING_SYSTEM":
			val = "Linux"
		case "TENANCY":
			val = "Shared"
		case "PURCHASE_TYPE":
			val = "On Demand"
		case "RECORD_TYPE":
			val = "Usage"
		default:
			continue
		}

		if val != "" {
			seen[val] = struct{}{}
		}
	}

	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}

	sort.Strings(vals)

	return vals
}

// GetTagKeys returns all distinct tag keys used across the cost ledger.
func (b *InMemoryBackend) GetTagKeys() []string {
	b.mu.RLock("GetTagKeys")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		for k := range e.Tags {
			seen[k] = struct{}{}
		}
	}

	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

// GetTagValues returns distinct values for a tag key.
func (b *InMemoryBackend) GetTagValues(tagKey string) []string {
	b.mu.RLock("GetTagValues")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, e := range b.costLedger {
		if v, ok := e.Tags[tagKey]; ok && v != "" {
			seen[v] = struct{}{}
		}
	}

	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}

	sort.Strings(vals)

	return vals
}

// GetSavingsPlansUtilization returns a synthetic savings-plans utilization aggregate.
func (b *InMemoryBackend) GetSavingsPlansUtilization(start, end string) *SavingsPlansUtilizationResult {
	b.mu.RLock("GetSavingsPlansUtilization")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	commitment := total * 0.60
	used := commitment * 0.85
	unused := commitment - used

	return &SavingsPlansUtilizationResult{
		Utilization: SavingsPlansUtilizationAgg{
			TotalCommitment:       fmt.Sprintf("%.4f", commitment),
			UsedCommitment:        fmt.Sprintf("%.4f", used),
			UnusedCommitment:      fmt.Sprintf("%.4f", unused),
			UtilizationPercentage: "85.0000",
		},
		Savings: SavingsPlansSavings{
			NetSavings:             fmt.Sprintf("%.4f", total*0.25),
			OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
		},
		AmortizedCommitment: SavingsPlansAmortized{
			AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
			AmortizedUpfrontCommitment:   "0.0000",
			TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
		},
	}
}

// GetSavingsPlansUtilizationDetails returns per-plan utilization details.
func (b *InMemoryBackend) GetSavingsPlansUtilizationDetails(start, end string) []SavingsPlansUtilizationDetail {
	b.mu.RLock("GetSavingsPlansUtilizationDetails")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	if total == 0 {
		return nil
	}

	commitment := total * 0.60
	used := commitment * 0.85

	return []SavingsPlansUtilizationDetail{
		{
			SavingsPlanARN: fmt.Sprintf("arn:aws:savingsplans::%s:savingsplan/synthetic-sp-1", b.accountID),
			Utilization: SavingsPlansUtilizationAgg{
				TotalCommitment:       fmt.Sprintf("%.4f", commitment),
				UsedCommitment:        fmt.Sprintf("%.4f", used),
				UnusedCommitment:      fmt.Sprintf("%.4f", commitment-used),
				UtilizationPercentage: "85.0000",
			},
			Savings: SavingsPlansSavings{
				NetSavings:             fmt.Sprintf("%.4f", total*0.25),
				OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
			},
			AmortizedCommitment: SavingsPlansAmortized{
				AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
				AmortizedUpfrontCommitment:   "0.0000",
				TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
			},
			Attributes: map[string]string{
				"SavingsPlansType": "COMPUTE_SP",
				"Region":           b.region,
				"InstanceFamily":   "m5",
				"PaymentOption":    "No Upfront",
			},
		},
	}
}

// GetReservationUtilization returns synthetic RI utilization by time.
func (b *InMemoryBackend) GetReservationUtilization(start, end, granularity string) []ReservationUtilizationByTime {
	b.mu.RLock("GetReservationUtilization")
	defer b.mu.RUnlock()

	buckets := buildTimeBuckets(start, end, granularity)
	result := make([]ReservationUtilizationByTime, 0, len(buckets))

	for _, bucket := range buckets {
		var total float64
		for _, e := range b.costLedgerInBucket(bucket.start, bucket.end) {
			if strings.Contains(e.Service, "Elastic Compute Cloud") {
				total += e.BlendedCost
			}
		}

		purchased := total * 0.40
		actual := purchased * 0.88
		unused := purchased - actual

		result = append(result, ReservationUtilizationByTime{
			TimePeriod: map[string]string{"Start": bucket.start, "End": bucket.end},
			Groups:     []any{},
			Total: ReservationUtilizationAgg{
				UtilizationPercentage:       "88.0000",
				PurchasedHours:              fmt.Sprintf("%.4f", purchased*10),
				TotalActualHours:            fmt.Sprintf("%.4f", actual*10),
				UnusedHours:                 fmt.Sprintf("%.4f", unused*10),
				OnDemandCostOfRIHoursUsed:   fmt.Sprintf("%.4f", actual),
				NetRISavings:                fmt.Sprintf("%.4f", total*0.30),
				TotalPotentialRISavings:     fmt.Sprintf("%.4f", total*0.35),
				AmortizedUpfrontFee:         "0.0000",
				AmortizedRecurringFee:       fmt.Sprintf("%.4f", purchased*0.70),
				TotalAmortizedFee:           fmt.Sprintf("%.4f", purchased*0.70),
				RICostForUnusedHours:        fmt.Sprintf("%.4f", unused*0.70),
				RealizedSavings:             fmt.Sprintf("%.4f", total*0.28),
				UnrealizedSavings:           fmt.Sprintf("%.4f", total*0.07),
			},
		})
	}

	return result
}

// GetReservationCoverage returns synthetic RI coverage by time.
func (b *InMemoryBackend) GetReservationCoverage(start, end, granularity string) []ReservationCoverageByTime {
	b.mu.RLock("GetReservationCoverage")
	defer b.mu.RUnlock()

	buckets := buildTimeBuckets(start, end, granularity)
	result := make([]ReservationCoverageByTime, 0, len(buckets))

	for _, bucket := range buckets {
		var total float64
		for _, e := range b.costLedgerInBucket(bucket.start, bucket.end) {
			total += e.BlendedCost
		}

		hours := total * 10
		riHours := hours * 0.65
		odHours := hours - riHours

		result = append(result, ReservationCoverageByTime{
			TimePeriod: map[string]string{"Start": bucket.start, "End": bucket.end},
			Groups:     []any{},
			Total: ReservationCoverageAgg{
				CoverageHours: ReservationCoverageHours{
					OnDemandHours:           fmt.Sprintf("%.4f", odHours),
					ReservedHours:           fmt.Sprintf("%.4f", riHours),
					TotalRunningHours:       fmt.Sprintf("%.4f", hours),
					CoverageHoursPercentage: "65.0000",
				},
				CoverageNormalizedUnits: ReservationCoverageNormalizedUnits{
					OnDemandNormalizedUnits:           fmt.Sprintf("%.4f", odHours*4),
					ReservedNormalizedUnits:           fmt.Sprintf("%.4f", riHours*4),
					TotalRunningNormalizedUnits:       fmt.Sprintf("%.4f", hours*4),
					CoverageNormalizedUnitsPercentage: "65.0000",
				},
				CoverageCost: ReservationCoverageCost{
					OnDemandCost: fmt.Sprintf("%.4f", odHours*0.05),
				},
			},
		})
	}

	return result
}

// GetReservationPurchaseRecommendations returns synthetic RI purchase recommendations.
func (b *InMemoryBackend) GetReservationPurchaseRecommendations(service, lookback, term, payment string) []ReservationRecommendation {
	b.mu.RLock("GetReservationPurchaseRecommendations")
	defer b.mu.RUnlock()

	days := 30
	switch lookback {
	case "SIXTY_DAYS":
		days = 60
	case "SEVEN_DAYS":
		days = 7
	}

	end := time.Now().UTC().Format("2006-01-02")
	start := time.Now().UTC().AddDate(0, 0, -days).Format("2006-01-02")

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		if service == "" || strings.EqualFold(e.Service, service) ||
			strings.Contains(strings.ToLower(e.Service), strings.ToLower(service)) {
			total += e.BlendedCost
		}
	}

	if total == 0 {
		return nil
	}

	upfrontMultiplier := 1.0
	if payment == "ALL_UPFRONT" {
		upfrontMultiplier = 0.90
	} else if payment == "PARTIAL_UPFRONT" {
		upfrontMultiplier = 0.95
	}

	termMonths := 12
	if term == "THREE_YEARS" {
		termMonths = 36
		upfrontMultiplier *= 0.85
	}

	monthlyCost := total / float64(days) * 30
	riMonthlyCost := monthlyCost * upfrontMultiplier * 0.60
	savings := monthlyCost - riMonthlyCost

	return []ReservationRecommendation{
		{
			AccountScope:         "LINKED",
			LookbackPeriodInDays: lookback,
			TermInYears:          term,
			PaymentOption:        payment,
			ServiceSpecification: map[string]any{
				"EC2Specification": map[string]string{
					"OfferingClass": "STANDARD",
				},
			},
			RecommendationDetails: []ReservationRecommendationDetail{
				{
					AccountID:                                 b.accountID,
					InstanceDetails:                           map[string]any{"EC2InstanceDetails": map[string]string{"InstanceType": "t3.medium", "Region": b.region, "Platform": "Linux/UNIX"}},
					RecommendedNumberOfInstancesToPurchase:    "2",
					RecommendedNormalizedUnitsToPurchase:      "16",
					MinimumNumberOfInstancesUsedPerHour:       "1",
					MinimumNormalizedUnitsUsedPerHour:         "8",
					MaximumNumberOfInstancesUsedPerHour:       "3",
					MaximumNormalizedUnitsUsedPerHour:         "24",
					AverageNumberOfInstancesUsedPerHour:       "2",
					AverageNormalizedUnitsUsedPerHour:         "16",
					AverageUtilization:                        "80.0000",
					EstimatedBreakEvenInMonths:                fmt.Sprintf("%d", termMonths/2),
					CurrencyCode:                              "USD",
					EstimatedMonthlySavingsAmount:             fmt.Sprintf("%.4f", savings),
					EstimatedMonthlySavingsPercentage:         "40.0000",
					EstimatedMonthlyOnDemandCost:              fmt.Sprintf("%.4f", monthlyCost),
					EstimatedReservationCostForLookbackPeriod: fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)),
					UpfrontCost:                               fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)*0.50),
					RecurringStandardMonthlyCost:              fmt.Sprintf("%.4f", riMonthlyCost*0.50),
				},
			},
			RecommendationSummary: map[string]string{
				"TotalEstimatedMonthlySavingsAmount":     fmt.Sprintf("%.4f", savings),
				"TotalEstimatedMonthlySavingsPercentage": "40.0000",
				"CurrencyCode":                           "USD",
			},
		},
	}
}

// GetRightsizingRecommendations returns synthetic rightsizing recommendations.
func (b *InMemoryBackend) GetRightsizingRecommendations(service string) []RightsizingRecommendation {
	b.mu.RLock("GetRightsizingRecommendations")
	defer b.mu.RUnlock()

	end := time.Now().UTC().Format("2006-01-02")
	start := time.Now().UTC().AddDate(0, 0, -14).Format("2006-01-02")

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		if strings.Contains(e.Service, "Elastic Compute Cloud") {
			total += e.BlendedCost
		}
	}

	if total == 0 {
		return nil
	}

	instanceID := fmt.Sprintf("i-synthetic%s", b.accountID[:8])
	resourceARN := fmt.Sprintf("arn:aws:ec2:%s:%s:instance/%s", b.region, b.accountID, instanceID)

	return []RightsizingRecommendation{
		{
			AccountID:           b.accountID,
			CurrentInstance:     RightsizingCurrentInstance{ResourceID: resourceARN, InstanceType: "t3.large", MonthlyCost: fmt.Sprintf("%.4f", total/14*30), CurrencyCode: "USD"},
			RightsizingType:     "MODIFY",
			ModifyRecommendationDetail: &RightsizingModifyDetail{
				TargetInstances: []RightsizingTargetInstance{
					{
						EstimatedMonthlyCost:       fmt.Sprintf("%.4f", total/14*30*0.5),
						EstimatedMonthlySavings:    fmt.Sprintf("%.4f", total/14*30*0.5),
						EstimatedSavingsPercentage: "50.0000",
						CurrencyCode:               "USD",
						DefaultTargetInstance:      true,
						ResourceDetails:            map[string]any{"EC2ResourceDetails": map[string]string{"InstanceType": "t3.medium", "Region": b.region, "Platform": "Linux/UNIX", "Tenancy": "Shared", "OperatingSystem": "Linux"}},
					},
				},
			},
		},
	}
}

// ListCostAllocationTags returns cost allocation tags, optionally filtered.
func (b *InMemoryBackend) ListCostAllocationTags(status, tagType string, tagKeys []string) []*CostAllocationTag {
	b.mu.RLock("ListCostAllocationTags")
	defer b.mu.RUnlock()

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	var result []*CostAllocationTag

	for _, tag := range b.costAllocationTags {
		if status != "" && tag.Status != status {
			continue
		}

		if tagType != "" && tag.Type != tagType {
			continue
		}

		if len(keySet) > 0 {
			if _, ok := keySet[tag.TagKey]; !ok {
				continue
			}
		}

		cp := *tag
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].TagKey < result[j].TagKey
	})

	return result
}

// UpdateCostAllocationTagsStatus updates the Active/Inactive status of cost allocation tags.
// Returns a list of errors for tags that could not be updated.
func (b *InMemoryBackend) UpdateCostAllocationTagsStatus(updates []CostAllocationTagStatusEntry) []CostAllocationTagError {
	b.mu.Lock("UpdateCostAllocationTagsStatus")
	defer b.mu.Unlock()

	var errs []CostAllocationTagError

	for _, u := range updates {
		if u.Status != "Active" && u.Status != "Inactive" {
			errs = append(errs, CostAllocationTagError{
				TagKey:  u.TagKey,
				Code:    "InvalidParameterException",
				Message: fmt.Sprintf("Status must be Active or Inactive, got %q", u.Status),
			})
			continue
		}

		if tag, ok := b.costAllocationTags[u.TagKey]; ok {
			tag.Status = u.Status
			tag.LastUpdatedDate = time.Now().UTC().Format(time.RFC3339)
		} else {
			b.costAllocationTags[u.TagKey] = &CostAllocationTag{
				TagKey:          u.TagKey,
				Status:          u.Status,
				Type:            "UserDefined",
				LastUpdatedDate: time.Now().UTC().Format(time.RFC3339),
			}
		}
	}

	if errs == nil {
		errs = []CostAllocationTagError{}
	}

	return errs
}

// CreateBackfillJob creates a new cost allocation tag backfill job.
func (b *InMemoryBackend) CreateBackfillJob(backfillFrom string) *BackfillJob {
	b.mu.Lock("CreateBackfillJob")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	job := &BackfillJob{
		BackfillFrom:   backfillFrom,
		RequestedAt:    now.Format(time.RFC3339),
		BackfillStatus: "PROCESSING",
		LastUpdatedAt:  now.Format(time.RFC3339),
	}

	b.backfillJobs = append(b.backfillJobs, job)

	return job
}

// ListBackfillHistory returns backfill jobs sorted by RequestedAt descending.
func (b *InMemoryBackend) ListBackfillHistory() []*BackfillJob {
	b.mu.RLock("ListBackfillHistory")
	defer b.mu.RUnlock()

	result := make([]*BackfillJob, len(b.backfillJobs))
	for i, j := range b.backfillJobs {
		cp := *j
		result[i] = &cp
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].RequestedAt > result[j].RequestedAt
	})

	return result
}

// CreateCommitmentAnalysis starts a new commitment purchase analysis.
func (b *InMemoryBackend) CreateCommitmentAnalysis() *CommitmentAnalysis {
	b.mu.Lock("CreateCommitmentAnalysis")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	estimated := now.Add(5 * time.Minute)
	a := &CommitmentAnalysis{
		AnalysisID:              uuid.NewString(),
		AnalysisStatus:          "PROCESSING",
		AnalysisStartedTime:     now.Format(time.RFC3339),
		EstimatedCompletionTime: estimated.Format(time.RFC3339),
	}

	b.commitmentAnalyses[a.AnalysisID] = a

	return a
}

// GetCommitmentAnalysis retrieves a commitment analysis by ID.
func (b *InMemoryBackend) GetCommitmentAnalysis(analysisID string) (*CommitmentAnalysis, error) {
	b.mu.RLock("GetCommitmentAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.commitmentAnalyses[analysisID]
	if !ok {
		return nil, ErrNotFound
	}

	cp := *a

	return &cp, nil
}

// ListCommitmentAnalyses returns all commitment analyses sorted by AnalysisStartedTime.
func (b *InMemoryBackend) ListCommitmentAnalyses() []*CommitmentAnalysis {
	b.mu.RLock("ListCommitmentAnalyses")
	defer b.mu.RUnlock()

	result := make([]*CommitmentAnalysis, 0, len(b.commitmentAnalyses))
	for _, a := range b.commitmentAnalyses {
		cp := *a
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].AnalysisStartedTime > result[j].AnalysisStartedTime
	})

	return result
}

// ProvideAnomalyFeedback persists feedback for an anomaly.
func (b *InMemoryBackend) ProvideAnomalyFeedback(anomalyID, feedback string) error {
	b.mu.Lock("ProvideAnomalyFeedback")
	defer b.mu.Unlock()

	switch feedback {
	case "YES", "NO", "PLANNED_ACTIVITY":
	default:
		return fmt.Errorf("%w: Feedback must be YES, NO, or PLANNED_ACTIVITY", ErrValidation)
	}

	a, ok := b.anomalies[anomalyID]
	if !ok {
		return ErrNotFound
	}

	a.FeedbackType = feedback

	return nil
}

// GetForecastByTime returns per-bucket cost forecasts for a time range.
func (b *InMemoryBackend) GetForecastByTime(start, end, granularity string, predictionIntervalLevel int) ([]ForecastResult, float64, float64, float64) {
	b.mu.RLock("GetForecastByTime")
	defer b.mu.RUnlock()

	histEnd := time.Now().UTC().Format("2006-01-02")
	histStart := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")

	var histValues []float64
	histBuckets := buildTimeBuckets(histStart, histEnd, granularity)

	for _, hb := range histBuckets {
		var bucketTotal float64
		for _, e := range b.costLedgerInBucket(hb.start, hb.end) {
			bucketTotal += e.BlendedCost
		}
		histValues = append(histValues, bucketTotal)
	}

	mean, stddev := calcMeanStddev(histValues)

	if predictionIntervalLevel < 51 {
		predictionIntervalLevel = 80
	}

	if predictionIntervalLevel > 99 {
		predictionIntervalLevel = 99
	}

	z := 1.28 + float64(predictionIntervalLevel-80)*0.02

	buckets := buildTimeBuckets(start, end, granularity)
	forecasts := make([]ForecastResult, 0, len(buckets))

	totalMean := mean * float64(len(buckets))

	for _, bucket := range buckets {
		lo := mean - z*stddev
		if lo < 0 {
			lo = 0
		}

		forecasts = append(forecasts, ForecastResult{
			TimePeriod:                   map[string]string{"Start": bucket.start, "End": bucket.end},
			MeanValue:                    fmt.Sprintf("%.4f", mean),
			PredictionIntervalLowerBound: fmt.Sprintf("%.4f", lo),
			PredictionIntervalUpperBound: fmt.Sprintf("%.4f", mean+z*stddev),
		})
	}

	return forecasts, totalMean, totalMean - z*stddev*float64(len(buckets)), totalMean + z*stddev*float64(len(buckets))
}
