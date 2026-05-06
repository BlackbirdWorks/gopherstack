// Package ce provides an in-memory implementation of the AWS Cost Explorer (Ce) service.
package ce

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
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

// Anomaly represents a detected cost anomaly in AWS CE.
type Anomaly struct {
	CreationDate     time.Time `json:"creationDate"`
	AnomalyID        string    `json:"anomalyID"`
	AnomalyStartDate string    `json:"anomalyStartDate"`
	AnomalyEndDate   string    `json:"anomalyEndDate"`
	DimensionValue   string    `json:"dimensionValue"`
	MonitorARN       string    `json:"monitorARN"`
	SubscriptionARN  string    `json:"subscriptionARN"`
	FeedbackType     string    `json:"feedbackType"`
	AnomalyScore     float64   `json:"anomalyScore"`
	TotalImpact      float64   `json:"totalImpact"`
}

// Subscriber represents a CE anomaly subscription notification target.
type Subscriber struct {
	Address string `json:"address"`
	Type    string `json:"type"`
	Status  string `json:"status"`
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
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		costCategories:       make(map[string]*CostCategory),
		anomalyMonitors:      make(map[string]*AnomalyMonitor),
		anomalySubscriptions: make(map[string]*AnomalySubscription),
		anomalies:            make(map[string]*Anomaly),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("ce"),
		anomalyTTL:           DefaultAnomalyTTL,
	}
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
