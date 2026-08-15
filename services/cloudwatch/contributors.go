package cloudwatch

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// DescribeAlarmContributors returns a page of contributors for the specified alarm.
//
// For a composite alarm, the contributors are the referenced child alarms whose
// state currently satisfies their state-function condition (e.g. a child named in
// ALARM(...) that is itself in ALARM) — i.e. the alarms actually driving the
// composite alarm's state. Each contributor's ContributorId is the child alarm
// name, and StateReason/StateTransitionedTimestamp are copied from that child
// alarm's own real state, never fabricated. For a metric alarm, AWS returns no
// contributors, so the result is an empty (but successful) page.
func (b *InMemoryBackend) DescribeAlarmContributors(
	alarmName, nextToken string,
) (page.Page[AlarmContributor], error) {
	b.mu.RLock("DescribeAlarmContributors")
	defer b.mu.RUnlock()

	if b.alarms.Has(alarmName) {
		// Metric alarms have no contributors in AWS.
		return page.New(
			[]AlarmContributor{},
			nextToken,
			0,
			cwDefaultDescribeAlarmContributorsLimit,
		), nil
	}

	ca, ok := b.compositeAlarms.Get(alarmName)
	if !ok {
		return page.Page[AlarmContributor]{}, fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
	}

	contributors := b.compositeContributorsLocked(ca)

	return page.New(
		contributors,
		nextToken,
		0,
		cwDefaultDescribeAlarmContributorsLimit,
	), nil
}

// childAlarmState is the subset of a child alarm's real state this backend
// has available to report as a contributor.
type childAlarmState struct {
	stateTransitioned time.Time
	stateValue        string
	stateReason       string
}

// resolveChildAlarmLocked reads a referenced child alarm's own state,
// StateReason, and StateTransitionedTimestamp. Caller must hold b.mu.
func (b *InMemoryBackend) resolveChildAlarmLocked(name string) childAlarmState {
	if a, ok := b.alarms.Get(name); ok {
		return childAlarmState{
			stateValue:        a.StateValue,
			stateReason:       a.StateReason,
			stateTransitioned: a.StateTransitionedTimestamp,
		}
	}
	if child, ok := b.compositeAlarms.Get(name); ok {
		return childAlarmState{
			stateValue:        child.StateValue,
			stateReason:       child.StateReason,
			stateTransitioned: child.StateTransitionedTimestamp,
		}
	}

	return childAlarmState{stateValue: alarmStateInsufficientData}
}

// compositeContributorsLocked computes the child alarms currently satisfying their
// state-function condition in a composite alarm's rule. Caller must hold b.mu.
func (b *InMemoryBackend) compositeContributorsLocked(ca *CompositeAlarm) []AlarmContributor {
	refs := extractAlarmRuleRefs(ca.AlarmRule)

	seen := make(map[string]bool, len(refs))
	contributors := make([]AlarmContributor, 0, len(refs))

	for _, ref := range refs {
		if seen[ref.Name] {
			continue
		}

		child := b.resolveChildAlarmLocked(ref.Name)
		// A child alarm contributes only when its actual state matches the state
		// its state-function tests for (that is the condition currently firing).
		if child.stateValue != ref.Func {
			continue
		}

		seen[ref.Name] = true
		contributors = append(contributors, AlarmContributor{
			ContributorID: ref.Name,
			ContributorAttributes: map[string]string{
				"AlarmName": ref.Name,
				"State":     child.stateValue,
			},
			StateReason:                child.stateReason,
			StateTransitionedTimestamp: child.stateTransitioned,
		})
	}

	return contributors
}
