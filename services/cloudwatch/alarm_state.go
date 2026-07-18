package cloudwatch

import (
	"context"
	"fmt"
	"time"
)

// alarmStateUpdate carries the state produced under lock by setAlarmStateLocked
// that SetAlarmState needs once the lock has been released to fire alarm
// actions and composite-alarm transitions.
type alarmStateUpdate struct {
	oldState             string
	alarmArn             string
	alarmDesc            string
	histAlarmType        string
	deps                 alarmActionDeps
	alarmActions         []string
	okActions            []string
	insuffActions        []string
	compositeTransitions []compositeAlarmTransition
	actionsEnabled       bool
}

// setAlarmStateLocked applies the new state to the named metric or composite
// alarm, records history, and re-evaluates dependent composite alarms. Must be
// called with b.mu unlocked; it takes the write lock itself. Extracted from
// SetAlarmState to keep the locked region out of the parent's funlen count.
func (b *InMemoryBackend) setAlarmStateLocked(
	alarmName, stateValue, stateReason, stateReasonData string,
) (*alarmStateUpdate, error) {
	b.mu.Lock("SetAlarmState")
	defer b.mu.Unlock()

	metricAlarm, hasMetric := b.alarms.Get(alarmName)
	compositeAlarm, hasComposite := b.compositeAlarms.Get(alarmName)

	if !hasMetric && !hasComposite {
		return nil, fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
	}

	update := &alarmStateUpdate{}

	var instanceIDs []string

	if hasMetric {
		update.oldState = metricAlarm.StateValue
		update.alarmArn = metricAlarm.AlarmArn
		update.alarmDesc = metricAlarm.AlarmDescription
		update.alarmActions = metricAlarm.AlarmActions
		update.okActions = metricAlarm.OKActions
		update.insuffActions = metricAlarm.InsufficientDataActions
		update.actionsEnabled = metricAlarm.ActionsEnabled
		instanceIDs = instanceIDsFromDimensions(metricAlarm.Dimensions)

		metricAlarm.StateValue = stateValue
		metricAlarm.StateReason = stateReason
		metricAlarm.StateReasonData = stateReasonData
		if update.oldState != stateValue {
			metricAlarm.StateTransitionedTimestamp = time.Now().UTC()
		}
	} else {
		update.oldState = compositeAlarm.StateValue
		update.alarmArn = compositeAlarm.AlarmArn
		update.alarmDesc = compositeAlarm.AlarmDescription
		update.alarmActions = compositeAlarm.AlarmActions
		update.okActions = compositeAlarm.OKActions
		update.insuffActions = compositeAlarm.InsufficientDataActions
		update.actionsEnabled = compositeAlarm.ActionsEnabled

		compositeAlarm.StateValue = stateValue
		compositeAlarm.StateReason = stateReason
		if update.oldState != stateValue {
			compositeAlarm.StateTransitionedTimestamp = time.Now().UTC()
		}
	}

	summary := fmt.Sprintf("Alarm %q changed from %s to %s", alarmName, update.oldState, stateValue)
	histData := b.stateChangeHistoryData(alarmName, update.oldState, stateValue, stateReason)
	update.histAlarmType = "MetricAlarm"
	if !hasMetric {
		update.histAlarmType = "CompositeAlarm"
	}
	b.appendHistory(alarmName, update.histAlarmType, historyTypeStateUpdate, summary, histData)

	// re-evaluate composite alarms that may reference this alarm, collecting any transitions
	update.compositeTransitions = b.reevaluateCompositeAlarms()

	update.deps = alarmActionDeps{
		snsPub:      b.snsPublisher,
		lambdaInv:   b.lambdaInvoker,
		ec2:         b.ec2Actioner,
		asg:         b.asgExecutor,
		instanceIDs: instanceIDs,
	}

	return update, nil
}

// SetAlarmState manually sets the state of an alarm and fires the corresponding actions.
func (b *InMemoryBackend) SetAlarmState(
	ctx context.Context,
	alarmName, stateValue, stateReason, stateReasonData string,
) error {
	update, err := b.setAlarmStateLocked(alarmName, stateValue, stateReason, stateReasonData)
	if err != nil {
		return err
	}

	if update.actionsEnabled && stateValue != update.oldState {
		var actions []string
		switch stateValue {
		case alarmStateAlarm:
			actions = update.alarmActions
		case alarmStateOK:
			actions = update.okActions
		case alarmStateInsufficientData:
			actions = update.insuffActions
		}

		payload := b.buildAlarmActionPayload(
			alarmName,
			update.alarmDesc,
			update.alarmArn,
			update.oldState,
			stateValue,
			stateReason,
		)
		b.executeActions(ctx, actions, alarmName, update.histAlarmType, payload, update.deps)
	}

	b.fireCompositeTransitions(ctx, update.compositeTransitions, update.deps)

	return nil
}
