package cloudwatch

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// DescribeAlarmContributors returns a page of contributors for the specified alarm.
//
// For a composite alarm, the contributors are the referenced child alarms whose
// state currently satisfies their state-function condition (e.g. a child named in
// ALARM(...) that is itself in ALARM) — i.e. the alarms actually driving the
// composite alarm's state. Each contributor is keyed by [childAlarmName, state]
// with Sum=1. For a metric alarm, AWS returns no contributors, so the result is an
// empty (but successful) page.
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

// compositeContributorsLocked computes the child alarms currently satisfying their
// state-function condition in a composite alarm's rule. Caller must hold b.mu.
func (b *InMemoryBackend) compositeContributorsLocked(ca *CompositeAlarm) []AlarmContributor {
	refs := extractAlarmRuleRefs(ca.AlarmRule)

	resolve := func(name string) string {
		if a, ok := b.alarms.Get(name); ok {
			return a.StateValue
		}
		if child, ok := b.compositeAlarms.Get(name); ok {
			return child.StateValue
		}

		return alarmStateInsufficientData
	}

	seen := make(map[string]bool, len(refs))
	contributors := make([]AlarmContributor, 0, len(refs))

	for _, ref := range refs {
		if seen[ref.Name] {
			continue
		}

		state := resolve(ref.Name)
		// A child alarm contributes only when its actual state matches the state
		// its state-function tests for (that is the condition currently firing).
		if state != ref.Func {
			continue
		}

		seen[ref.Name] = true
		contributors = append(contributors, AlarmContributor{
			Keys: []string{ref.Name, state},
			Sum:  1,
		})
	}

	return contributors
}
