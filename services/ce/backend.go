// Package ce provides an in-memory implementation of the AWS Cost Explorer (Ce) service.
package ce

import (
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource with the same name already exists.
	ErrAlreadyExists = awserr.New("ServiceQuotaExceededException", awserr.ErrConflict)
)

// CostCategory represents an in-memory AWS Cost Explorer cost category.
type CostCategory struct {
	CreationDate     time.Time
	Tags             map[string]string
	ARN              string
	Name             string
	RuleVersion      string
	DefaultValue     string
	EffectiveStart   string
	Rules            []CostCategoryRule
	SplitChargeRules []SplitChargeRule
}

// CostCategoryRule represents a single cost category rule.
type CostCategoryRule struct {
	Value string
}

// SplitChargeRule represents a cost category split charge rule.
type SplitChargeRule struct {
	Source  string
	Method  string
	Targets []string
}

// AnomalyMonitor represents an in-memory AWS CE anomaly monitor.
type AnomalyMonitor struct {
	CreationDate     time.Time
	Tags             map[string]string
	MonitorARN       string
	MonitorName      string
	MonitorType      string
	MonitorDimension string
}

// AnomalySubscription represents an in-memory AWS CE anomaly subscription.
type AnomalySubscription struct {
	CreationDate     time.Time
	Tags             map[string]string
	SubscriptionARN  string
	SubscriptionName string
	Frequency        string
	MonitorARNList   []string
	Subscribers      []Subscriber
	Threshold        float64
}

// Subscriber represents a CE anomaly subscription notification target.
type Subscriber struct {
	Address string
	Type    string
	Status  string
}

// InMemoryBackend is a thread-safe in-memory store for Cost Explorer resources.
type InMemoryBackend struct {
	costCategories       map[string]*CostCategory
	anomalyMonitors      map[string]*AnomalyMonitor
	anomalySubscriptions map[string]*AnomalySubscription
	mu                   *lockmetrics.RWMutex
	accountID            string
	region               string
}

// NewInMemoryBackend creates a new backend for the given account and region.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		costCategories:       make(map[string]*CostCategory),
		anomalyMonitors:      make(map[string]*AnomalyMonitor),
		anomalySubscriptions: make(map[string]*AnomalySubscription),
		accountID:            accountID,
		region:               region,
		mu:                   lockmetrics.New("ce"),
	}
}

// Region returns the region for this backend instance.
func (b *InMemoryBackend) Region() string { return b.region }

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

	cat := &CostCategory{
		ARN:            catARN,
		Name:           name,
		RuleVersion:    ruleVersion,
		DefaultValue:   defaultValue,
		Rules:          rules,
		EffectiveStart: effectiveStart(),
		CreationDate:   time.Now().UTC(),
		Tags:           tagsCopy,
	}
	b.costCategories[catARN] = cat

	out := *cat

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

// ListCostCategoryDefinitions returns all cost categories.
func (b *InMemoryBackend) ListCostCategoryDefinitions() []*CostCategory {
	b.mu.RLock("ListCostCategoryDefinitions")
	defer b.mu.RUnlock()

	result := make([]*CostCategory, 0, len(b.costCategories))
	for _, cat := range b.costCategories {
		out := *cat
		result = append(result, &out)
	}

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
	cat.Rules = rules
	cat.SplitChargeRules = splitChargeRules
	cat.EffectiveStart = effectiveStart()

	out := *cat

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

// GetAnomalyMonitors returns anomaly monitors, optionally filtered by ARNs.
func (b *InMemoryBackend) GetAnomalyMonitors(monitorARNList []string) []*AnomalyMonitor {
	b.mu.RLock("GetAnomalyMonitors")
	defer b.mu.RUnlock()

	if len(monitorARNList) == 0 {
		result := make([]*AnomalyMonitor, 0, len(b.anomalyMonitors))
		for _, mon := range b.anomalyMonitors {
			out := *mon
			result = append(result, &out)
		}

		return result
	}

	set := make(map[string]struct{}, len(monitorARNList))
	for _, a := range monitorARNList {
		set[a] = struct{}{}
	}

	result := make([]*AnomalyMonitor, 0, len(monitorARNList))
	for _, mon := range b.anomalyMonitors {
		if _, ok := set[mon.MonitorARN]; ok {
			out := *mon
			result = append(result, &out)
		}
	}

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

// GetAnomalySubscriptions returns anomaly subscriptions, optionally filtered by ARNs.
func (b *InMemoryBackend) GetAnomalySubscriptions(subscriptionARNList []string) []*AnomalySubscription {
	b.mu.RLock("GetAnomalySubscriptions")
	defer b.mu.RUnlock()

	if len(subscriptionARNList) == 0 {
		result := make([]*AnomalySubscription, 0, len(b.anomalySubscriptions))
		for _, sub := range b.anomalySubscriptions {
			out := *sub
			result = append(result, &out)
		}

		return result
	}

	set := make(map[string]struct{}, len(subscriptionARNList))
	for _, a := range subscriptionARNList {
		set[a] = struct{}{}
	}

	result := make([]*AnomalySubscription, 0, len(subscriptionARNList))
	for _, sub := range b.anomalySubscriptions {
		if _, ok := set[sub.SubscriptionARN]; ok {
			out := *sub
			result = append(result, &out)
		}
	}

	return result
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
