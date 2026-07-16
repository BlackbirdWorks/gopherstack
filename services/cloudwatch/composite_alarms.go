package cloudwatch

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// PutCompositeAlarm creates or updates a composite alarm and evaluates its state.
func (b *InMemoryBackend) PutCompositeAlarm(alarm *CompositeAlarm) error {
	if alarm.AlarmName == "" {
		return ErrAlarmNameRequired
	}
	if alarm.AlarmRule == "" {
		return ErrAlarmRuleRequired
	}

	b.mu.Lock("PutCompositeAlarm")
	defer b.mu.Unlock()

	isNew := !b.compositeAlarms.Has(alarm.AlarmName)

	if alarm.AlarmArn == "" {
		alarm.AlarmArn = arn.Build("cloudwatch", b.region, b.accountID, "alarm:"+alarm.AlarmName)
	}
	if alarm.CreatedAt.IsZero() {
		alarm.CreatedAt = time.Now()
	}

	// Evaluate state based on AlarmRule and current child alarm states.
	newState := b.evalCompositeRule(alarm.AlarmRule)
	if existing, ok := b.compositeAlarms.Get(alarm.AlarmName); ok {
		if existing.StateTransitionedTimestamp.IsZero() || newState != existing.StateValue {
			alarm.StateTransitionedTimestamp = time.Now().UTC()
		} else {
			alarm.StateTransitionedTimestamp = existing.StateTransitionedTimestamp
		}
	} else {
		alarm.StateTransitionedTimestamp = time.Now().UTC()
	}
	alarm.StateValue = newState
	if alarm.StateReason == "" {
		alarm.StateReason = "Rule evaluated to " + newState
	}

	cp := *alarm
	b.compositeAlarms.Put(&cp)

	histType := historyTypeConfigurationUpdate
	historySummary := fmt.Sprintf("Composite alarm %q updated", alarm.AlarmName)
	if isNew {
		historySummary = fmt.Sprintf("Composite alarm %q created", alarm.AlarmName)
	}
	b.appendHistory(alarm.AlarmName, "CompositeAlarm", histType, historySummary, "")

	return nil
}

// evalCompositeRule evaluates the composite alarm rule using current alarm states.
// It guards against circular composite alarm references by tracking visited names.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) evalCompositeRule(rule string) string {
	return b.evalCompositeRuleDepth(rule, make(map[string]bool), 0)
}

// evalCompositeRuleDepth is the recursive implementation of evalCompositeRule.
// visited tracks composite alarm names currently on the call stack to detect cycles.
// depth enforces an absolute recursion cap as a secondary safety measure.
// This function is always called while b.mu is held, so visited is accessed
// single-threadedly and does not require additional synchronisation.
// Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) evalCompositeRuleDepth(
	rule string,
	visited map[string]bool,
	depth int,
) string {
	if depth > cwMaxCompositeEvalDepth {
		return alarmStateInsufficientData
	}

	resolve := func(name string) string {
		if a, ok := b.alarms.Get(name); ok {
			return a.StateValue
		}
		if ca, ok := b.compositeAlarms.Get(name); ok {
			if visited[name] {
				// Circular dependency detected: treat as INSUFFICIENT_DATA.
				return alarmStateInsufficientData
			}
			visited[name] = true
			state := b.evalCompositeRuleDepth(ca.AlarmRule, visited, depth+1)
			delete(visited, name)

			return state
		}

		return alarmStateInsufficientData
	}

	return evaluateAlarmRule(rule, resolve)
}

// fireCompositeTransitions fires the actions for composite alarms that changed
// state. Composite alarms have no metric dimensions, so EC2 instance IDs are not
// carried over — only the SNS/Lambda collaborators are reused.
func (b *InMemoryBackend) fireCompositeTransitions(
	ctx context.Context,
	transitions []compositeAlarmTransition,
	deps alarmActionDeps,
) {
	compositeDeps := alarmActionDeps{snsPub: deps.snsPub, lambdaInv: deps.lambdaInv}
	for _, tr := range transitions {
		payload := b.buildAlarmActionPayload(
			tr.alarmName, tr.alarmDesc, tr.alarmArn,
			tr.oldState, tr.newState, tr.reason,
		)
		b.executeActions(ctx, tr.actions, tr.alarmName, "CompositeAlarm", payload, compositeDeps)
	}
}

// compositeAlarmTransition records a composite alarm state change and the actions to fire.
type compositeAlarmTransition struct {
	alarmName string
	alarmArn  string
	alarmDesc string
	oldState  string
	newState  string
	reason    string
	actions   []string
}

// reevaluateCompositeAlarms re-checks all composite alarms and updates their state.
// Returns the list of state transitions so the caller can fire actions after releasing the lock.
// Caller must hold b.mu (write lock).
func (b *InMemoryBackend) reevaluateCompositeAlarms() []compositeAlarmTransition {
	var transitions []compositeAlarmTransition

	for _, ca := range b.compositeAlarms.All() {
		newState := b.evalCompositeRule(ca.AlarmRule)
		if newState == ca.StateValue {
			continue
		}

		oldState := ca.StateValue
		reason := "Rule evaluated to " + newState
		ca.StateValue = newState
		ca.StateReason = reason
		ca.StateTransitionedTimestamp = time.Now().UTC()
		summary := fmt.Sprintf(
			"Composite alarm %q changed from %s to %s",
			ca.AlarmName,
			oldState,
			newState,
		)
		histData := b.stateChangeHistoryData(ca.AlarmName, oldState, newState, reason)
		b.appendHistory(ca.AlarmName, "CompositeAlarm", historyTypeStateUpdate, summary, histData)

		if ca.ActionsEnabled {
			var actions []string
			switch newState {
			case alarmStateAlarm:
				actions = ca.AlarmActions
			case alarmStateOK:
				actions = ca.OKActions
			case alarmStateInsufficientData:
				actions = ca.InsufficientDataActions
			}
			if len(actions) > 0 {
				transitions = append(transitions, compositeAlarmTransition{
					alarmName: ca.AlarmName,
					alarmArn:  ca.AlarmArn,
					alarmDesc: ca.AlarmDescription,
					oldState:  oldState,
					newState:  newState,
					reason:    reason,
					actions:   actions,
				})
			}
		}
	}

	return transitions
}
