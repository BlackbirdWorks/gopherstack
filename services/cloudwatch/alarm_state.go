package cloudwatch

import (
	"context"
	"fmt"
	"time"
)

// SetAlarmState manually sets the state of an alarm and fires the corresponding actions.
func (b *InMemoryBackend) SetAlarmState(
	ctx context.Context,
	alarmName, stateValue, stateReason, stateReasonData string,
) error {
	var oldState string
	var alarmArn string
	var alarmDesc string
	var alarmActions, okActions, insuffActions []string
	var actionsEnabled bool
	var histAlarmType string
	var compositeTransitions []compositeAlarmTransition
	var deps alarmActionDeps

	err := func() error {
		b.mu.Lock("SetAlarmState")
		defer b.mu.Unlock()

		metricAlarm, hasMetric := b.alarms.Get(alarmName)
		compositeAlarm, hasComposite := b.compositeAlarms.Get(alarmName)

		if !hasMetric && !hasComposite {
			return fmt.Errorf("%w: %s", ErrAlarmNotFound, alarmName)
		}

		var instanceIDs []string

		if hasMetric {
			oldState = metricAlarm.StateValue
			alarmArn = metricAlarm.AlarmArn
			alarmDesc = metricAlarm.AlarmDescription
			alarmActions = metricAlarm.AlarmActions
			okActions = metricAlarm.OKActions
			insuffActions = metricAlarm.InsufficientDataActions
			actionsEnabled = metricAlarm.ActionsEnabled
			instanceIDs = instanceIDsFromDimensions(metricAlarm.Dimensions)

			metricAlarm.StateValue = stateValue
			metricAlarm.StateReason = stateReason
			metricAlarm.StateReasonData = stateReasonData
			if oldState != stateValue {
				metricAlarm.StateTransitionedTimestamp = time.Now().UTC()
			}
		} else {
			oldState = compositeAlarm.StateValue
			alarmArn = compositeAlarm.AlarmArn
			alarmDesc = compositeAlarm.AlarmDescription
			alarmActions = compositeAlarm.AlarmActions
			okActions = compositeAlarm.OKActions
			insuffActions = compositeAlarm.InsufficientDataActions
			actionsEnabled = compositeAlarm.ActionsEnabled

			compositeAlarm.StateValue = stateValue
			compositeAlarm.StateReason = stateReason
			if oldState != stateValue {
				compositeAlarm.StateTransitionedTimestamp = time.Now().UTC()
			}
		}

		summary := fmt.Sprintf("Alarm %q changed from %s to %s", alarmName, oldState, stateValue)
		histData := b.stateChangeHistoryData(alarmName, oldState, stateValue, stateReason)
		histAlarmType = "MetricAlarm"
		if !hasMetric {
			histAlarmType = "CompositeAlarm"
		}
		b.appendHistory(alarmName, histAlarmType, historyTypeStateUpdate, summary, histData)

		// re-evaluate composite alarms that may reference this alarm, collecting any transitions
		compositeTransitions = b.reevaluateCompositeAlarms()

		deps = alarmActionDeps{
			snsPub:      b.snsPublisher,
			lambdaInv:   b.lambdaInvoker,
			ec2:         b.ec2Actioner,
			asg:         b.asgExecutor,
			instanceIDs: instanceIDs,
		}

		return nil
	}()
	if err != nil {
		return err
	}

	if actionsEnabled && stateValue != oldState {
		var actions []string
		switch stateValue {
		case alarmStateAlarm:
			actions = alarmActions
		case alarmStateOK:
			actions = okActions
		case alarmStateInsufficientData:
			actions = insuffActions
		}

		payload := b.buildAlarmActionPayload(
			alarmName,
			alarmDesc,
			alarmArn,
			oldState,
			stateValue,
			stateReason,
		)
		b.executeActions(ctx, actions, alarmName, histAlarmType, payload, deps)
	}

	b.fireCompositeTransitions(ctx, compositeTransitions, deps)

	return nil
}
