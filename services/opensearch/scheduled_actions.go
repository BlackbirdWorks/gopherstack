package opensearch

import (
	"fmt"
	"time"
)

// ScheduleAt values, matching the AWS ScheduleAt enum.
const (
	scheduleAtNow           = "NOW"
	scheduleAtTimestamp     = "TIMESTAMP"
	scheduleAtOffPeakWindow = "OFF_PEAK_WINDOW"
)

// scheduledByCustomer matches the AWS ScheduledBy enum's CUSTOMER value: a
// manual UpdateScheduledAction call always implies customer-initiated
// (the SYSTEM value is only ever set by AWS's own automatic scheduling,
// which gopherstack does not emulate).
const scheduledByCustomer = "CUSTOMER"

// offPeakWindowDefaultDelay is the placeholder delay used to compute
// ScheduledTime when ScheduleAt is OFF_PEAK_WINDOW. Real AWS picks the next
// upcoming off-peak window per the domain's OffPeakWindowOptions; gopherstack
// has no scheduler, so this is a reasonable non-stub default.
const offPeakWindowDefaultDelay = 24 * time.Hour

// ListScheduledActions returns scheduled actions for a domain.
func (b *InMemoryBackend) ListScheduledActions(domainName string) []*ScheduledAction {
	b.mu.RLock("ListScheduledActions")
	defer b.mu.RUnlock()

	src := b.scheduledActions[domainName]
	out := make([]*ScheduledAction, len(src))

	for i, sa := range src {
		cp := *sa
		out[i] = &cp
	}

	return out
}

// UpdateScheduledAction reschedules an existing scheduled action (identified
// by ActionID + ActionType) for a domain. Real AWS Service creates scheduled
// actions automatically ahead of service-software updates or JVM tuning
// changes; this operation only reschedules one that already exists -- it
// cannot create a new one. See AddScheduledActionInternal (export_test.go)
// for test seeding of the initial action.
func (b *InMemoryBackend) UpdateScheduledAction(
	domainName, actionID, actionType, scheduleAt string,
	desiredStartTime int64,
) (*ScheduledAction, error) {
	if domainName == "" {
		return nil, fmt.Errorf("%w: DomainName is required", ErrInvalidParameter)
	}

	if actionID == "" {
		return nil, fmt.Errorf("%w: ActionID is required", ErrInvalidParameter)
	}

	if actionType == "" {
		return nil, fmt.Errorf("%w: ActionType is required", ErrInvalidParameter)
	}

	if scheduleAt == "" {
		return nil, fmt.Errorf("%w: ScheduleAt is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateScheduledAction")
	defer b.mu.Unlock()

	for _, sa := range b.scheduledActions[domainName] {
		if sa.ID != actionID || sa.Type != actionType {
			continue
		}

		sa.ScheduledBy = scheduledByCustomer

		switch scheduleAt {
		case scheduleAtTimestamp:
			sa.ScheduledTime = float64(desiredStartTime)
		case scheduleAtOffPeakWindow:
			sa.ScheduledTime = float64(b.clock().Add(offPeakWindowDefaultDelay).Unix())
		default: // NOW, or any unrecognized value
			sa.ScheduledTime = float64(b.clock().Unix())
		}

		cp := *sa

		return &cp, nil
	}

	return nil, fmt.Errorf(
		"%w: scheduled action %s (%s) not found for domain %s",
		ErrScheduledActionNotFound, actionID, actionType, domainName,
	)
}
