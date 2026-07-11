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
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// DefaultAnomalyTTL is the default time-to-live for detected anomalies.
const DefaultAnomalyTTL = 30 * 24 * time.Hour

const (
	granularityMonthly      = "MONTHLY"
	metricUnitUSD           = "USD"
	metricUnitNA            = "N/A"
	timePeriodKeyEnd        = "End"
	timePeriodKeyStart      = "Start"
	syntheticInstanceType   = "t3.medium"
	spUtilizationPct        = "85.0000"
	riCoveragePct           = "65.0000"
	riUtilizationPct        = "88.0000"
	zeroAmountStr           = "0.0000"
	defaultSavingsPlansType = "COMPUTE_SP"
	mapKeyRegion            = "Region"
	mapKeyCurrencyCode      = "CurrencyCode"
	dimKeyService           = "SERVICE"
	dimKeyRegion            = "REGION"
	dimKeyUsageType         = "USAGE_TYPE"
	dimKeyLinkedAccount     = "LINKED_ACCOUNT"
)

// Synthetic data ratio constants used in cost simulation.
const (
	syntheticJitterRange     = 5    // half-range for YearDay jitter (±5 units)
	syntheticJitterScale     = 0.01 // 1% variation per YearDay unit
	unblendedCostFactor      = 0.98 // UnblendedCost = BlendedCost * 98%
	usageQuantityFactor      = 10   // UsageQuantity units per cost dollar
	amortizedCostFactor      = 0.99 // AmortizedCost = UnblendedCost * 99%
	netUnblendedCostFactor   = 0.97 // NetUnblendedCost = UnblendedCost * 97%
	normalizedUsageFactor    = 4    // normalized usage units per quantity unit
	stddevMinThreshold       = 0.01 // minimum stddev; values below use fallback
	stddevFallbackRatio      = 0.05 // fallback stddev = mean * 5%
	spCommitmentRatio        = 0.60 // SP commitment = total cost * 60%
	spUsedCommitmentRatio    = 0.85 // SP used commitment = total commitment * 85%
	spNetSavingsRatio        = 0.25 // SP net savings = total cost * 25%
	riPurchasedCostRatio     = 0.40 // RI purchased hours ratio relative to total cost
	riActualUsageRatio       = 0.88 // RI actual hours = purchased * 88%
	costToHoursMultiplier    = 10   // synthetic hours per cost dollar
	riNetSavingsRatio        = 0.30 // RI net savings = total * 30%
	riPotentialSavingsRatio  = 0.35 // RI potential savings = total * 35%
	riAmortizedFeeRatio      = 0.70 // RI amortized fee = purchased * 70%
	riRealizedSavingsRatio   = 0.28 // RI realized savings = total * 28%
	riUnrealizedSavingsRatio = 0.07 // RI unrealized savings = total * 7%
	riCoverageRatio          = 0.65 // RI covered hours fraction
	normalizedUnitsPerHour   = 4    // normalized units per running hour
	onDemandCostRate         = 0.05 // on-demand cost per synthetic hour
	daysPerMonth             = 30   // synthetic month length in days
	riMonthlyCostRatio       = 0.60 // RI monthly cost = on-demand cost * 60%
	riBreakEvenDivisor       = 2    // break-even months = term months / 2
	riUpfrontSplitRatio      = 0.50 // upfront and recurring each at 50% of RI cost
	rightsizingSavingsRatio  = 0.5  // rightsizing target saves 50% of current cost
	analysisETAMinutes       = 5    // estimated minutes until commitment analysis completes
	forecastMinLevel         = 51   // minimum valid prediction interval level
	forecastMaxLevel         = 99   // maximum valid prediction interval level
	forecastDefaultLevel     = 80   // default prediction interval level
	forecastBaseZ            = 1.28 // z-score at the default 80% prediction interval level
	forecastZScalePerPct     = 0.02 // z-score increment per percentage point above default
)

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
	LastUpdatedDate  time.Time         `json:"lastUpdatedDate"`
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
	AccountID        string            `json:"accountID"`
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
type SavingsPlansUtilizationDetail struct {
	Attributes          map[string]string          `json:"Attributes,omitempty"`
	Utilization         SavingsPlansUtilizationAgg `json:"Utilization"`
	AmortizedCommitment SavingsPlansAmortized      `json:"AmortizedCommitment"`
	Savings             SavingsPlansSavings        `json:"Savings"`
	SavingsPlanARN      string                     `json:"SavingsPlanArn"`
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

// InMemoryBackend is a thread-safe in-memory store for Cost Explorer resources.
type InMemoryBackend struct {
	// registry holds every store.Table-backed resource field so their
	// Reset/Snapshot/Restore collapse to one call each -- every table below
	// is "clean" (registered directly under its own real, non-json:"-"
	// identity field, no DTO-registry needed); see store_setup.go.
	registry             *store.Registry
	costCategories       *store.Table[CostCategory]
	anomalyMonitors      *store.Table[AnomalyMonitor]
	anomalySubscriptions *store.Table[AnomalySubscription]
	anomalies            *store.Table[Anomaly]
	mu                   *lockmetrics.RWMutex
	costAllocationTags   *store.Table[CostAllocationTag]
	commitmentAnalyses   *store.Table[CommitmentAnalysis]
	accountID            string
	region               string
	costLedger           []CostEntry
	backfillJobs         []*BackfillJob
	anomalyTTL           time.Duration
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		registry:   store.NewRegistry(),
		accountID:  accountID,
		region:     region,
		mu:         lockmetrics.New("ce"),
		anomalyTTL: DefaultAnomalyTTL,
	}
	registerAllTables(b)
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

	for _, a := range b.anomalies.All() {
		if a.CreationDate.Before(cutoff) {
			b.anomalies.Delete(a.AnomalyID)
		}
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all in-memory state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.costLedger = nil
	b.backfillJobs = nil
	b.seedCostLedger()
}

func (b *InMemoryBackend) buildCostCategoryARN(name string) string {
	return arn.Build("ce", "", b.accountID, fmt.Sprintf("costcategory/%s", name))
}

func (b *InMemoryBackend) buildAnomalyMonitorARN() string {
	return arn.Build("ce", "", b.accountID, fmt.Sprintf("anomalymonitor/%s", uuid.NewString()))
}

func (b *InMemoryBackend) buildAnomalySubscriptionARN() string {
	return arn.Build(
		"ce",
		"",
		b.accountID,
		fmt.Sprintf("anomalysubscription/%s", uuid.NewString()),
	)
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
	if b.costCategories.Has(catARN) {
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
	b.costCategories.Put(cat)

	out := *cat
	out.Rules = make([]CostCategoryRule, len(cat.Rules))
	copy(out.Rules, cat.Rules)

	return &out, nil
}

// DeleteCostCategoryDefinition removes a cost category by ARN.
func (b *InMemoryBackend) DeleteCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.Lock("DeleteCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories.Get(catARN)
	if !exists {
		return nil, ErrNotFound
	}

	b.costCategories.Delete(catARN)

	out := *cat

	return &out, nil
}

// DescribeCostCategoryDefinition returns a cost category by ARN.
func (b *InMemoryBackend) DescribeCostCategoryDefinition(catARN string) (*CostCategory, error) {
	b.mu.RLock("DescribeCostCategoryDefinition")
	defer b.mu.RUnlock()

	cat, exists := b.costCategories.Get(catARN)
	if !exists {
		return nil, ErrNotFound
	}

	out := *cat

	return &out, nil
}

// ListCostCategoryDefinitions returns cost categories sorted by name with opaque pagination.
func (b *InMemoryBackend) ListCostCategoryDefinitions(maxResults int, nextPageToken string) ([]*CostCategory, string) {
	b.mu.RLock("ListCostCategoryDefinitions")
	defer b.mu.RUnlock()

	all := b.costCategories.All()
	result := make([]*CostCategory, 0, len(all))
	for _, cat := range all {
		out := *cat
		result = append(result, &out)
	}

	return paginateList(result, maxResults, nextPageToken, func(c *CostCategory) string {
		return c.Name
	})
}

// UpdateCostCategoryDefinition updates an existing cost category.
func (b *InMemoryBackend) UpdateCostCategoryDefinition(
	catARN, ruleVersion, defaultValue string,
	rules []CostCategoryRule,
	splitChargeRules []SplitChargeRule,
) (*CostCategory, error) {
	b.mu.Lock("UpdateCostCategoryDefinition")
	defer b.mu.Unlock()

	cat, exists := b.costCategories.Get(catARN)
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

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		out := make(map[string]string, len(cat.Tags))
		maps.Copy(out, cat.Tags)

		return out, nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		out := make(map[string]string, len(mon.Tags))
		maps.Copy(out, mon.Tags)

		return out, nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
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

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		maps.Copy(cat.Tags, resourceTags)

		return nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		maps.Copy(mon.Tags, resourceTags)

		return nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
		maps.Copy(sub.Tags, resourceTags)

		return nil
	}

	return ErrNotFound
}

// UntagResource removes tags from a CE resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if cat, ok := b.costCategories.Get(resourceARN); ok {
		for _, k := range tagKeys {
			delete(cat.Tags, k)
		}

		return nil
	}

	if mon, ok := b.anomalyMonitors.Get(resourceARN); ok {
		for _, k := range tagKeys {
			delete(mon.Tags, k)
		}

		return nil
	}

	if sub, ok := b.anomalySubscriptions.Get(resourceARN); ok {
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
			return nil, fmt.Errorf(
				"%w: MonitorType must be one of DIMENSIONAL, CUSTOM",
				ErrValidation,
			)
		}
	}

	tagsCopy := make(map[string]string, len(resourceTags))
	maps.Copy(tagsCopy, resourceTags)

	now := time.Now().UTC()
	monARN := b.buildAnomalyMonitorARN()
	mon := &AnomalyMonitor{
		MonitorARN:       monARN,
		MonitorName:      monitorName,
		MonitorType:      monitorType,
		MonitorDimension: monitorDimension,
		CreationDate:     now,
		LastUpdatedDate:  now,
		Tags:             tagsCopy,
	}
	b.anomalyMonitors.Put(mon)

	out := *mon

	return &out, nil
}

// DeleteAnomalyMonitor removes an anomaly monitor by ARN.
func (b *InMemoryBackend) DeleteAnomalyMonitor(monARN string) error {
	b.mu.Lock("DeleteAnomalyMonitor")
	defer b.mu.Unlock()

	if !b.anomalyMonitors.Has(monARN) {
		return ErrNotFound
	}

	b.anomalyMonitors.Delete(monARN)

	return nil
}

// GetAnomalyMonitors returns anomaly monitors, optionally filtered by ARNs, sorted by MonitorARN.
// maxResults and nextPageToken implement opaque-cursor pagination (real AWS behaviour).
func (b *InMemoryBackend) GetAnomalyMonitors(
	monitorARNList []string, maxResults int, nextPageToken string,
) ([]*AnomalyMonitor, string) {
	b.mu.RLock("GetAnomalyMonitors")
	defer b.mu.RUnlock()

	var result []*AnomalyMonitor

	if len(monitorARNList) == 0 {
		all := b.anomalyMonitors.All()
		result = make([]*AnomalyMonitor, 0, len(all))
		for _, mon := range all {
			out := *mon
			result = append(result, &out)
		}
	} else {
		set := make(map[string]struct{}, len(monitorARNList))
		for _, a := range monitorARNList {
			set[a] = struct{}{}
		}

		result = make([]*AnomalyMonitor, 0, len(monitorARNList))

		for _, mon := range b.anomalyMonitors.All() {
			if _, ok := set[mon.MonitorARN]; ok {
				out := *mon
				result = append(result, &out)
			}
		}
	}

	return paginateList(result, maxResults, nextPageToken, func(m *AnomalyMonitor) string {
		return m.MonitorARN
	})
}

// UpdateAnomalyMonitor updates the name of an anomaly monitor.
func (b *InMemoryBackend) UpdateAnomalyMonitor(
	monARN, monitorName string,
) (*AnomalyMonitor, error) {
	b.mu.Lock("UpdateAnomalyMonitor")
	defer b.mu.Unlock()

	mon, exists := b.anomalyMonitors.Get(monARN)
	if !exists {
		return nil, ErrNotFound
	}

	mon.MonitorName = monitorName
	mon.LastUpdatedDate = time.Now().UTC()

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
			return nil, fmt.Errorf(
				"%w: Frequency must be one of DAILY, IMMEDIATE, WEEKLY",
				ErrValidation,
			)
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
		AccountID:        b.accountID,
		Frequency:        frequency,
		MonitorARNList:   monCopy,
		Subscribers:      subsCopy,
		Threshold:        threshold,
		CreationDate:     time.Now().UTC(),
		Tags:             tagsCopy,
	}
	b.anomalySubscriptions.Put(sub)

	out := *sub

	return &out, nil
}

// DeleteAnomalySubscription removes an anomaly subscription by ARN.
func (b *InMemoryBackend) DeleteAnomalySubscription(subARN string) error {
	b.mu.Lock("DeleteAnomalySubscription")
	defer b.mu.Unlock()

	if !b.anomalySubscriptions.Has(subARN) {
		return ErrNotFound
	}

	b.anomalySubscriptions.Delete(subARN)

	return nil
}

// GetAnomalySubscriptions returns anomaly subscriptions, optionally filtered by ARNs or monitor ARN,
// sorted by SubscriptionARN. maxResults and nextPageToken implement opaque-cursor pagination.
func (b *InMemoryBackend) GetAnomalySubscriptions(
	subscriptionARNList []string,
	monitorARN string,
	maxResults int,
	nextPageToken string,
) ([]*AnomalySubscription, string) {
	b.mu.RLock("GetAnomalySubscriptions")
	defer b.mu.RUnlock()

	var result []*AnomalySubscription

	if len(subscriptionARNList) == 0 {
		all := b.anomalySubscriptions.All()
		result = make([]*AnomalySubscription, 0, len(all))

		for _, sub := range all {
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

		for _, sub := range b.anomalySubscriptions.All() {
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

	return paginateList(result, maxResults, nextPageToken, func(s *AnomalySubscription) string {
		return s.SubscriptionARN
	})
}

// containsString reports whether s appears in slice.
func containsString(slice []string, s string) bool {
	return slices.Contains(slice, s)
}

// paginateList sorts list by keyFn, applies the opaque nextPageToken cursor, and returns
// at most maxResults items plus the token for the following page (empty on the last page).
func paginateList[T any](list []T, maxResults int, nextPageToken string, keyFn func(T) string) ([]T, string) {
	sort.Slice(list, func(i, j int) bool {
		return keyFn(list[i]) < keyFn(list[j])
	})

	start := 0
	if nextPageToken != "" {
		for i := range list {
			if keyFn(list[i]) >= nextPageToken {
				start = i

				break
			}

			start = i + 1
		}
	}

	const defaultPageSize = 100
	limit := maxResults
	if limit <= 0 || limit > defaultPageSize {
		limit = defaultPageSize
	}

	end := min(start+limit, len(list))
	page := list[start:end]

	next := ""
	if end < len(list) {
		next = keyFn(list[end])
	}

	return page, next
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

	sub, exists := b.anomalySubscriptions.Get(subARN)
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

// GetAnomalies returns detected anomalies, optionally filtered by monitor ARN, feedback type,
// and date interval. maxResults and nextPageToken implement opaque-cursor pagination.
func (b *InMemoryBackend) GetAnomalies(
	monitorARN, feedback, startDate, endDate string, maxResults int, nextPageToken string,
) ([]*Anomaly, string) {
	b.mu.RLock("GetAnomalies")
	defer b.mu.RUnlock()

	all := b.anomalies.All()
	result := make([]*Anomaly, 0, len(all))

	for _, a := range all {
		if monitorARN != "" && a.MonitorARN != monitorARN {
			continue
		}

		if feedback != "" && a.FeedbackType != feedback {
			continue
		}

		// Filter by date interval: anomaly must overlap [startDate, endDate].
		if startDate != "" && a.AnomalyEndDate != "" && a.AnomalyEndDate < startDate {
			continue
		}

		if endDate != "" && a.AnomalyStartDate != "" && a.AnomalyStartDate > endDate {
			continue
		}

		out := *a
		result = append(result, &out)
	}

	return paginateList(result, maxResults, nextPageToken, func(a *Anomaly) string {
		return a.AnomalyID
	})
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
	b.anomalies.Put(&cp)
}

// GetCostCategories returns the distinct cost category values stored in the
// backend, optionally filtered by cost category name. Values are sorted alphabetically.
func (b *InMemoryBackend) GetCostCategories(costCategoryName string) []string {
	b.mu.RLock("GetCostCategories")
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})
	var values []string

	for _, cat := range b.costCategories.All() {
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

// syntheticServiceDef describes a synthetic AWS service used in the cost ledger seed
// and as a fallback when real ledger entries are absent for a queried time period.
type syntheticServiceDef struct {
	name      string
	usageType string
	weight    float64
}

// syntheticServiceCatalog is the canonical list of services used in both the cost
// ledger seed and the synthetic-group fallback for GroupBy queries.
//
//nolint:gochecknoglobals // package-level catalog shared by seed and fallback
var syntheticServiceCatalog = []syntheticServiceDef{
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

// syntheticBaseMonthlyTotal is the base monthly cost used when no ledger entries
// exist for the queried period (e.g. historical ranges before the seed window).
const syntheticBaseMonthlyTotal = 3000.0

// syntheticGroupsFallback generates GroupBy result groups from the service catalog
// when the cost ledger contains no entries for the queried period.
// The groups mirror what would appear for real data, giving callers a valid
// non-empty response shape for any time period.
func syntheticGroupsFallback(accountID, region string, groupBy []GroupBySpec, metrics []string) []CostGroup {
	groups := make([]CostGroup, 0, len(syntheticServiceCatalog))

	for _, svc := range syntheticServiceCatalog {
		keys := make([]string, 0, len(groupBy))
		for _, g := range groupBy {
			switch strings.ToUpper(g.Key) {
			case dimKeyService:
				keys = append(keys, svc.name)
			case dimKeyRegion:
				keys = append(keys, region)
			case dimKeyUsageType:
				keys = append(keys, svc.usageType)
			case dimKeyLinkedAccount:
				keys = append(keys, accountID)
			default:
				keys = append(keys, "Other")
			}
		}

		amount := syntheticBaseMonthlyTotal * svc.weight
		amounts := make(map[string]float64, len(metrics))
		for _, m := range metrics {
			amounts[m] = amount
		}

		groups = append(groups, CostGroup{
			Keys:    keys,
			Metrics: buildMetricValues(amounts, metrics),
		})
	}

	return groups
}

// seedCostLedger populates the cost ledger with 90 days of synthetic data.
// Seeded distributions: EC2~40%, S3~15%, RDS~10%, Lambda~8%, CloudFront~7%, others rest.
func (b *InMemoryBackend) seedCostLedger() {
	services := syntheticServiceCatalog

	now := time.Now().UTC()
	totalDailyBase := 150.0

	for day := 89; day >= 0; day-- {
		d := now.AddDate(0, 0, -day)
		dateStr := d.Format("2006-01-02")

		dayMultiplier := 1.0
		if d.Weekday() == time.Saturday || d.Weekday() == time.Sunday {
			dayMultiplier = 0.8
		}

		jitter := 1.0 + float64(d.YearDay()%10-syntheticJitterRange)*syntheticJitterScale

		for _, svc := range services {
			amount := totalDailyBase * svc.weight * dayMultiplier * jitter
			b.costLedger = append(b.costLedger, CostEntry{
				Date:          dateStr,
				Service:       svc.name,
				Region:        b.region,
				UsageType:     svc.usageType,
				Account:       b.accountID,
				BlendedCost:   amount,
				UnblendedCost: amount * unblendedCostFactor,
				UsageQuantity: amount * usageQuantityFactor,
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
	case granularityMonthly:
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
		case dimKeyService:
			keys = append(keys, e.Service)
		case dimKeyRegion:
			keys = append(keys, e.Region)
		case dimKeyUsageType:
			keys = append(keys, e.UsageType)
		case dimKeyLinkedAccount:
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
		return e.UnblendedCost * amortizedCostFactor
	case "NETUNBLENDEDCOST":
		return e.UnblendedCost * netUnblendedCostFactor
	case "USAGEQUANTITY":
		return e.UsageQuantity
	case "NORMALIZEDUSAGEAMOUNT":
		return e.UsageQuantity * normalizedUsageFactor
	default:
		return e.BlendedCost
	}
}

func metricUnit(metric string) string {
	switch strings.ToUpper(metric) {
	case "USAGEQUANTITY", "NORMALIZEDUSAGEAMOUNT":
		return metricUnitNA
	default:
		return metricUnitUSD
	}
}

func calcMeanStddev(values []float64) (float64, float64) {
	if len(values) == 0 {
		return 0, 1
	}

	var avg float64

	for _, v := range values {
		avg += v
	}

	avg /= float64(len(values))

	var sd float64

	for _, v := range values {
		diff := v - avg
		sd += diff * diff
	}

	if len(values) > 1 {
		sd /= float64(len(values) - 1)
	}

	sd = math.Sqrt(sd)

	if sd < stddevMinThreshold {
		sd = avg * stddevFallbackRatio
	}

	return avg, sd
}

// buildMetricValues converts a map of metric amounts into MetricValue structs.
func buildMetricValues(amounts map[string]float64, metrics []string) map[string]MetricValue {
	mv := make(map[string]MetricValue, len(metrics))
	for _, m := range metrics {
		mv[m] = MetricValue{
			Amount: fmt.Sprintf("%.4f", amounts[m]),
			Unit:   metricUnit(m),
		}
	}

	return mv
}

// aggregateByGroup groups entries by GroupBy keys and returns sorted CostGroups.
func aggregateByGroup(entries []CostEntry, groupBy []GroupBySpec, metrics []string) []CostGroup {
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

	gkeys := collections.SortedKeys(groupMap)

	groups := make([]CostGroup, 0, len(gkeys))
	for _, gk := range gkeys {
		groups = append(groups, CostGroup{
			Keys:    keyMap[gk],
			Metrics: buildMetricValues(groupMap[gk], metrics),
		})
	}

	return groups
}

// aggregateTotals sums entries across all metrics and returns MetricValue structs.
func aggregateTotals(entries []CostEntry, metrics []string) map[string]MetricValue {
	totals := make(map[string]float64)
	for _, e := range entries {
		for _, m := range metrics {
			totals[m] += getMetricValue(e, m)
		}
	}

	return buildMetricValues(totals, metrics)
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
		r := ResultByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Estimated:  bucket.start >= now || bucket.end > now,
			Groups:     []CostGroup{},
		}

		if len(groupBy) > 0 {
			r.Groups = aggregateByGroup(entries, groupBy, metrics)
			if len(r.Groups) == 0 {
				r.Groups = syntheticGroupsFallback(b.accountID, b.region, groupBy, metrics)
			}

			r.Total = map[string]MetricValue{}
		} else {
			r.Total = aggregateTotals(entries, metrics)
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
		case dimKeyService:
			val = e.Service
		case dimKeyRegion, "AZ":
			val = e.Region
		case dimKeyUsageType:
			val = e.UsageType
		case dimKeyLinkedAccount:
			val = e.Account
		case "INSTANCE_TYPE":
			val = syntheticInstanceType
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

	vals := collections.SortedKeys(seen)

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

	keys := collections.SortedKeys(seen)

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

	vals := collections.SortedKeys(seen)

	return vals
}

// GetSavingsPlansUtilization returns a synthetic savings-plans utilization aggregate.
func (b *InMemoryBackend) GetSavingsPlansUtilization(
	start, end string,
) *SavingsPlansUtilizationResult {
	b.mu.RLock("GetSavingsPlansUtilization")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	commitment := total * spCommitmentRatio
	used := commitment * spUsedCommitmentRatio
	unused := commitment - used

	return &SavingsPlansUtilizationResult{
		Utilization: SavingsPlansUtilizationAgg{
			TotalCommitment:       fmt.Sprintf("%.4f", commitment),
			UsedCommitment:        fmt.Sprintf("%.4f", used),
			UnusedCommitment:      fmt.Sprintf("%.4f", unused),
			UtilizationPercentage: spUtilizationPct,
		},
		Savings: SavingsPlansSavings{
			NetSavings:             fmt.Sprintf("%.4f", total*spNetSavingsRatio),
			OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
		},
		AmortizedCommitment: SavingsPlansAmortized{
			AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
			AmortizedUpfrontCommitment:   zeroAmountStr,
			TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
		},
	}
}

// GetSavingsPlansUtilizationDetails returns per-plan utilization details.
func (b *InMemoryBackend) GetSavingsPlansUtilizationDetails(
	start, end string,
) []SavingsPlansUtilizationDetail {
	b.mu.RLock("GetSavingsPlansUtilizationDetails")
	defer b.mu.RUnlock()

	var total float64
	for _, e := range b.costLedgerInBucket(start, end) {
		total += e.BlendedCost
	}

	if total == 0 {
		total = syntheticBaseMonthlyTotal
	}

	commitment := total * spCommitmentRatio
	used := commitment * spUsedCommitmentRatio

	return []SavingsPlansUtilizationDetail{
		{
			SavingsPlanARN: arn.Build(
				"savingsplans",
				"",
				b.accountID,
				"savingsplan/synthetic-sp-1",
			),
			Utilization: SavingsPlansUtilizationAgg{
				TotalCommitment:       fmt.Sprintf("%.4f", commitment),
				UsedCommitment:        fmt.Sprintf("%.4f", used),
				UnusedCommitment:      fmt.Sprintf("%.4f", commitment-used),
				UtilizationPercentage: spUtilizationPct,
			},
			Savings: SavingsPlansSavings{
				NetSavings:             fmt.Sprintf("%.4f", total*spNetSavingsRatio),
				OnDemandCostEquivalent: fmt.Sprintf("%.4f", total),
			},
			AmortizedCommitment: SavingsPlansAmortized{
				AmortizedRecurringCommitment: fmt.Sprintf("%.4f", commitment),
				AmortizedUpfrontCommitment:   zeroAmountStr,
				TotalAmortizedCommitment:     fmt.Sprintf("%.4f", commitment),
			},
			Attributes: map[string]string{
				"SavingsPlansType": defaultSavingsPlansType,
				mapKeyRegion:       b.region,
				"InstanceFamily":   "m5",
				"PaymentOption":    "No Upfront",
			},
		},
	}
}

// GetReservationUtilization returns synthetic RI utilization by time.
func (b *InMemoryBackend) GetReservationUtilization(
	start, end, granularity string,
) []ReservationUtilizationByTime {
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

		purchased := total * riPurchasedCostRatio
		actual := purchased * riActualUsageRatio
		unused := purchased - actual

		result = append(result, ReservationUtilizationByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Groups:     []any{},
			Total: ReservationUtilizationAgg{
				UtilizationPercentage:     riUtilizationPct,
				PurchasedHours:            fmt.Sprintf("%.4f", purchased*costToHoursMultiplier),
				TotalActualHours:          fmt.Sprintf("%.4f", actual*costToHoursMultiplier),
				UnusedHours:               fmt.Sprintf("%.4f", unused*costToHoursMultiplier),
				OnDemandCostOfRIHoursUsed: fmt.Sprintf("%.4f", actual),
				NetRISavings:              fmt.Sprintf("%.4f", total*riNetSavingsRatio),
				TotalPotentialRISavings:   fmt.Sprintf("%.4f", total*riPotentialSavingsRatio),
				AmortizedUpfrontFee:       zeroAmountStr,
				AmortizedRecurringFee:     fmt.Sprintf("%.4f", purchased*riAmortizedFeeRatio),
				TotalAmortizedFee:         fmt.Sprintf("%.4f", purchased*riAmortizedFeeRatio),
				RICostForUnusedHours:      fmt.Sprintf("%.4f", unused*riAmortizedFeeRatio),
				RealizedSavings:           fmt.Sprintf("%.4f", total*riRealizedSavingsRatio),
				UnrealizedSavings:         fmt.Sprintf("%.4f", total*riUnrealizedSavingsRatio),
			},
		})
	}

	return result
}

// GetReservationCoverage returns synthetic RI coverage by time.
func (b *InMemoryBackend) GetReservationCoverage(
	start, end, granularity string,
) []ReservationCoverageByTime {
	b.mu.RLock("GetReservationCoverage")
	defer b.mu.RUnlock()

	buckets := buildTimeBuckets(start, end, granularity)
	result := make([]ReservationCoverageByTime, 0, len(buckets))

	for _, bucket := range buckets {
		var total float64
		for _, e := range b.costLedgerInBucket(bucket.start, bucket.end) {
			total += e.BlendedCost
		}

		hours := total * costToHoursMultiplier
		riHours := hours * riCoverageRatio
		odHours := hours - riHours

		result = append(result, ReservationCoverageByTime{
			TimePeriod: map[string]string{timePeriodKeyStart: bucket.start, timePeriodKeyEnd: bucket.end},
			Groups:     []any{},
			Total: ReservationCoverageAgg{
				CoverageHours: ReservationCoverageHours{
					OnDemandHours:           fmt.Sprintf("%.4f", odHours),
					ReservedHours:           fmt.Sprintf("%.4f", riHours),
					TotalRunningHours:       fmt.Sprintf("%.4f", hours),
					CoverageHoursPercentage: riCoveragePct,
				},
				CoverageNormalizedUnits: ReservationCoverageNormalizedUnits{
					OnDemandNormalizedUnits:           fmt.Sprintf("%.4f", odHours*normalizedUnitsPerHour),
					ReservedNormalizedUnits:           fmt.Sprintf("%.4f", riHours*normalizedUnitsPerHour),
					TotalRunningNormalizedUnits:       fmt.Sprintf("%.4f", hours*normalizedUnitsPerHour),
					CoverageNormalizedUnitsPercentage: riCoveragePct,
				},
				CoverageCost: ReservationCoverageCost{
					OnDemandCost: fmt.Sprintf("%.4f", odHours*onDemandCostRate),
				},
			},
		})
	}

	return result
}

func (b *InMemoryBackend) riDetail(
	monthlyCost, riMonthlyCost, savings float64, termMonths int,
) ReservationRecommendationDetail {
	return ReservationRecommendationDetail{
		AccountID: b.accountID,
		InstanceDetails: map[string]any{
			"EC2InstanceDetails": map[string]string{
				"InstanceType": syntheticInstanceType,
				mapKeyRegion:   b.region,
				"Platform":     "Linux/UNIX",
			},
		},
		RecommendedNumberOfInstancesToPurchase:    "2",
		RecommendedNormalizedUnitsToPurchase:      "16",
		MinimumNumberOfInstancesUsedPerHour:       "1",
		MinimumNormalizedUnitsUsedPerHour:         "8",
		MaximumNumberOfInstancesUsedPerHour:       "3",
		MaximumNormalizedUnitsUsedPerHour:         "24",
		AverageNumberOfInstancesUsedPerHour:       "2",
		AverageNormalizedUnitsUsedPerHour:         "16",
		AverageUtilization:                        "80.0000",
		EstimatedBreakEvenInMonths:                strconv.Itoa(termMonths / riBreakEvenDivisor),
		CurrencyCode:                              metricUnitUSD,
		EstimatedMonthlySavingsAmount:             fmt.Sprintf("%.4f", savings),
		EstimatedMonthlySavingsPercentage:         "40.0000",
		EstimatedMonthlyOnDemandCost:              fmt.Sprintf("%.4f", monthlyCost),
		EstimatedReservationCostForLookbackPeriod: fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)),
		UpfrontCost:                  fmt.Sprintf("%.4f", riMonthlyCost*float64(termMonths)*riUpfrontSplitRatio),
		RecurringStandardMonthlyCost: fmt.Sprintf("%.4f", riMonthlyCost*riUpfrontSplitRatio),
	}
}

// GetReservationPurchaseRecommendations returns synthetic RI purchase recommendations.
func (b *InMemoryBackend) GetReservationPurchaseRecommendations(
	service, lookback, term, payment string,
) []ReservationRecommendation {
	b.mu.RLock("GetReservationPurchaseRecommendations")
	defer b.mu.RUnlock()

	days := daysPerMonth
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
	switch payment {
	case "ALL_UPFRONT":
		upfrontMultiplier = 0.90
	case "PARTIAL_UPFRONT":
		upfrontMultiplier = 0.95
	}

	termMonths := 12
	if term == "THREE_YEARS" {
		termMonths = 36
		upfrontMultiplier *= spUsedCommitmentRatio
	}

	monthlyCost := total / float64(days) * daysPerMonth
	riMonthlyCost := monthlyCost * upfrontMultiplier * riMonthlyCostRatio
	savings := monthlyCost - riMonthlyCost

	return []ReservationRecommendation{
		{
			AccountScope:         "LINKED",
			LookbackPeriodInDays: lookback,
			TermInYears:          term,
			PaymentOption:        payment,
			ServiceSpecification: map[string]any{
				"EC2Specification": map[string]string{"OfferingClass": "STANDARD"},
			},
			RecommendationDetails: []ReservationRecommendationDetail{
				b.riDetail(monthlyCost, riMonthlyCost, savings, termMonths),
			},
			RecommendationSummary: map[string]string{
				"TotalEstimatedMonthlySavingsAmount":     fmt.Sprintf("%.4f", savings),
				"TotalEstimatedMonthlySavingsPercentage": "40.0000",
				"CurrencyCode":                           metricUnitUSD,
			},
		},
	}
}

// GetRightsizingRecommendations returns synthetic rightsizing recommendations.
func (b *InMemoryBackend) GetRightsizingRecommendations(
	_ string,
) []RightsizingRecommendation {
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
	resourceARN := arn.Build("ec2", b.region, b.accountID, fmt.Sprintf("instance/%s", instanceID))

	return []RightsizingRecommendation{
		{
			AccountID: b.accountID,
			CurrentInstance: RightsizingCurrentInstance{
				ResourceID:   resourceARN,
				InstanceType: "t3.large",
				MonthlyCost:  fmt.Sprintf("%.4f", total/14*daysPerMonth),
				CurrencyCode: metricUnitUSD,
			},
			RightsizingType: "MODIFY",
			ModifyRecommendationDetail: &RightsizingModifyDetail{
				TargetInstances: []RightsizingTargetInstance{
					{
						EstimatedMonthlyCost:       fmt.Sprintf("%.4f", total/14*daysPerMonth*rightsizingSavingsRatio),
						EstimatedMonthlySavings:    fmt.Sprintf("%.4f", total/14*daysPerMonth*rightsizingSavingsRatio),
						EstimatedSavingsPercentage: "50.0000",
						CurrencyCode:               "USD",
						DefaultTargetInstance:      true,
						ResourceDetails: map[string]any{
							"EC2ResourceDetails": map[string]string{
								"InstanceType":    "t3.medium",
								mapKeyRegion:      b.region,
								"Platform":        "Linux/UNIX",
								"Tenancy":         "Shared",
								"OperatingSystem": "Linux",
							},
						},
					},
				},
			},
		},
	}
}

// ListCostAllocationTags returns cost allocation tags, optionally filtered.
func (b *InMemoryBackend) ListCostAllocationTags(
	status, tagType string,
	tagKeys []string,
) []*CostAllocationTag {
	b.mu.RLock("ListCostAllocationTags")
	defer b.mu.RUnlock()

	keySet := make(map[string]struct{}, len(tagKeys))
	for _, k := range tagKeys {
		keySet[k] = struct{}{}
	}

	var result []*CostAllocationTag

	for _, tag := range b.costAllocationTags.All() {
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
func (b *InMemoryBackend) UpdateCostAllocationTagsStatus(
	updates []CostAllocationTagStatusEntry,
) []CostAllocationTagError {
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

		if tag, ok := b.costAllocationTags.Get(u.TagKey); ok {
			tag.Status = u.Status
			tag.LastUpdatedDate = time.Now().UTC().Format(time.RFC3339)
		} else {
			b.costAllocationTags.Put(&CostAllocationTag{
				TagKey:          u.TagKey,
				Status:          u.Status,
				Type:            "UserDefined",
				LastUpdatedDate: time.Now().UTC().Format(time.RFC3339),
			})
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
	estimated := now.Add(analysisETAMinutes * time.Minute)
	a := &CommitmentAnalysis{
		AnalysisID:              uuid.NewString(),
		AnalysisStatus:          "PROCESSING",
		AnalysisStartedTime:     now.Format(time.RFC3339),
		EstimatedCompletionTime: estimated.Format(time.RFC3339),
	}

	b.commitmentAnalyses.Put(a)

	return a
}

// GetCommitmentAnalysis retrieves a commitment analysis by ID.
func (b *InMemoryBackend) GetCommitmentAnalysis(analysisID string) (*CommitmentAnalysis, error) {
	b.mu.RLock("GetCommitmentAnalysis")
	defer b.mu.RUnlock()

	a, ok := b.commitmentAnalyses.Get(analysisID)
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

	all := b.commitmentAnalyses.All()
	result := make([]*CommitmentAnalysis, 0, len(all))
	for _, a := range all {
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

	a, ok := b.anomalies.Get(anomalyID)
	if !ok {
		return ErrNotFound
	}

	a.FeedbackType = feedback

	return nil
}

// GetForecastByTime returns per-bucket cost forecasts for a time range.
func (b *InMemoryBackend) GetForecastByTime(
	start, end, granularity string,
	predictionIntervalLevel int,
) ([]ForecastResult, float64, float64, float64) {
	b.mu.RLock("GetForecastByTime")
	defer b.mu.RUnlock()

	histEnd := time.Now().UTC().Format("2006-01-02")
	histStart := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")

	histBuckets := buildTimeBuckets(histStart, histEnd, granularity)
	histValues := make([]float64, 0, len(histBuckets))

	for _, hb := range histBuckets {
		var bucketTotal float64
		for _, e := range b.costLedgerInBucket(hb.start, hb.end) {
			bucketTotal += e.BlendedCost
		}
		histValues = append(histValues, bucketTotal)
	}

	mean, stddev := calcMeanStddev(histValues)

	if predictionIntervalLevel < forecastMinLevel {
		predictionIntervalLevel = forecastDefaultLevel
	}

	if predictionIntervalLevel > forecastMaxLevel {
		predictionIntervalLevel = forecastMaxLevel
	}

	z := forecastBaseZ + float64(predictionIntervalLevel-forecastDefaultLevel)*forecastZScalePerPct

	buckets := buildTimeBuckets(start, end, granularity)
	forecasts := make([]ForecastResult, 0, len(buckets))

	totalMean := mean * float64(len(buckets))

	for _, bucket := range buckets {
		lo := mean - z*stddev
		if lo < 0 {
			lo = 0
		}

		forecasts = append(forecasts, ForecastResult{
			TimePeriod: map[string]string{
				timePeriodKeyStart: bucket.start,
				timePeriodKeyEnd:   bucket.end,
			},
			MeanValue:                    fmt.Sprintf("%.4f", mean),
			PredictionIntervalLowerBound: fmt.Sprintf("%.4f", lo),
			PredictionIntervalUpperBound: fmt.Sprintf("%.4f", mean+z*stddev),
		})
	}

	return forecasts, totalMean, totalMean - z*stddev*float64(
			len(buckets),
		), totalMean + z*stddev*float64(
			len(buckets),
		)
}
