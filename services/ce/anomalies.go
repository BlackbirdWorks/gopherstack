package ce

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// CreateAnomalyMonitor creates a new anomaly monitor.
func (b *InMemoryBackend) CreateAnomalyMonitor(
	monitorName, monitorType, monitorDimension string,
	monitorSpecification *ceExpression,
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
		MonitorARN:           monARN,
		MonitorName:          monitorName,
		MonitorType:          monitorType,
		MonitorDimension:     monitorDimension,
		MonitorSpecification: monitorSpecification,
		CreationDate:         now,
		LastUpdatedDate:      now,
		Tags:                 tagsCopy,
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
		return ErrUnknownMonitor
	}

	b.anomalyMonitors.Delete(monARN)

	return nil
}

// GetAnomalyMonitors returns anomaly monitors, optionally filtered by ARNs, sorted by MonitorARN.
// maxResults and nextPageToken implement opaque-cursor pagination (real AWS behaviour).
// If monitorARNList references an ARN that doesn't exist, it returns ErrUnknownMonitor,
// matching real AWS.
func (b *InMemoryBackend) GetAnomalyMonitors(
	monitorARNList []string, maxResults int, nextPageToken string,
) ([]*AnomalyMonitor, string, error) {
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
			if !b.anomalyMonitors.Has(a) {
				return nil, "", ErrUnknownMonitor
			}

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

	page, next := paginateList(result, maxResults, nextPageToken, func(m *AnomalyMonitor) string {
		return m.MonitorARN
	})

	return page, next, nil
}

// UpdateAnomalyMonitor updates the name of an anomaly monitor.
func (b *InMemoryBackend) UpdateAnomalyMonitor(
	monARN, monitorName string,
) (*AnomalyMonitor, error) {
	b.mu.Lock("UpdateAnomalyMonitor")
	defer b.mu.Unlock()

	mon, exists := b.anomalyMonitors.Get(monARN)
	if !exists {
		return nil, ErrUnknownMonitor
	}

	// Real AWS: "Specify the fields that you want to update. Omitted fields are
	// unchanged." MonitorName is the only mutable field, so an empty/omitted value
	// leaves the existing name untouched instead of blanking it out.
	if monitorName != "" {
		mon.MonitorName = monitorName
	}

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
	thresholdExpression *ceExpression,
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

	for _, monARN := range monitorARNList {
		if !b.anomalyMonitors.Has(monARN) {
			return nil, ErrUnknownMonitor
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
		SubscriptionARN:     subARN,
		SubscriptionName:    subscriptionName,
		AccountID:           b.accountID,
		Frequency:           frequency,
		MonitorARNList:      monCopy,
		Subscribers:         subsCopy,
		Threshold:           threshold,
		ThresholdExpression: thresholdExpression,
		CreationDate:        time.Now().UTC(),
		Tags:                tagsCopy,
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
		return ErrUnknownSubscription
	}

	b.anomalySubscriptions.Delete(subARN)

	return nil
}

// GetAnomalySubscriptions returns anomaly subscriptions, optionally filtered by ARNs or monitor ARN,
// sorted by SubscriptionARN. maxResults and nextPageToken implement opaque-cursor pagination.
// If subscriptionARNList references an ARN that doesn't exist, it returns
// ErrUnknownSubscription, matching real AWS. monitorARN is a simple filter (real AWS does
// not require it to reference an existing monitor).
func (b *InMemoryBackend) GetAnomalySubscriptions(
	subscriptionARNList []string,
	monitorARN string,
	maxResults int,
	nextPageToken string,
) ([]*AnomalySubscription, string, error) {
	b.mu.RLock("GetAnomalySubscriptions")
	defer b.mu.RUnlock()

	var arnFilter map[string]struct{}

	if len(subscriptionARNList) > 0 {
		var err error

		arnFilter, err = b.anomalySubscriptionARNSet(subscriptionARNList)
		if err != nil {
			return nil, "", err
		}
	}

	all := b.anomalySubscriptions.All()
	result := make([]*AnomalySubscription, 0, len(all))

	for _, sub := range all {
		if _, ok := arnFilter[sub.SubscriptionARN]; arnFilter != nil && !ok {
			continue
		}

		if monitorARN != "" && !containsString(sub.MonitorARNList, monitorARN) {
			continue
		}

		out := *sub
		result = append(result, &out)
	}

	page, next := paginateList(result, maxResults, nextPageToken, func(s *AnomalySubscription) string {
		return s.SubscriptionARN
	})

	return page, next, nil
}

// anomalySubscriptionARNSet validates that every ARN in arns names an existing anomaly
// subscription and returns the set for membership filtering. Returns ErrUnknownSubscription
// on the first unknown ARN, matching real AWS. Caller must hold at least a read lock.
func (b *InMemoryBackend) anomalySubscriptionARNSet(arns []string) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(arns))

	for _, a := range arns {
		if !b.anomalySubscriptions.Has(a) {
			return nil, ErrUnknownSubscription
		}

		set[a] = struct{}{}
	}

	return set, nil
}

// UpdateAnomalySubscription updates a CE anomaly subscription.
func (b *InMemoryBackend) UpdateAnomalySubscription(
	subARN, frequency, subscriptionName string,
	monitorARNList []string,
	subscribers []Subscriber,
	threshold float64,
	thresholdExpression *ceExpression,
) (*AnomalySubscription, error) {
	b.mu.Lock("UpdateAnomalySubscription")
	defer b.mu.Unlock()

	sub, exists := b.anomalySubscriptions.Get(subARN)
	if !exists {
		return nil, ErrUnknownSubscription
	}

	if len(monitorARNList) > 0 {
		for _, monARN := range monitorARNList {
			if !b.anomalyMonitors.Has(monARN) {
				return nil, ErrUnknownMonitor
			}
		}
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

	if thresholdExpression != nil {
		sub.ThresholdExpression = thresholdExpression
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
