package cloudformation

import (
	"fmt"

	cloudwatchbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---- CloudWatch CompositeAlarm ----

func (rc *ResourceCreator) createCloudWatchCompositeAlarm(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	alarmName := strProp(props, "AlarmName", params, physicalIDs)
	if alarmName == "" {
		alarmName = logicalID
	}

	alarmRule := strProp(props, "AlarmRule", params, physicalIDs)
	alarmDescription := strProp(props, "AlarmDescription", params, physicalIDs)

	alarm := &cloudwatchbackend.CompositeAlarm{
		AlarmName:        alarmName,
		AlarmRule:        alarmRule,
		AlarmDescription: alarmDescription,
		StateValue:       "INSUFFICIENT_DATA",
		ActionsEnabled:   true,
	}

	if err := rc.backends.CloudWatch.Backend.PutCompositeAlarm(alarm); err != nil {
		return "", fmt.Errorf("create CloudWatch composite alarm %s: %w", alarmName, err)
	}

	return alarmName, nil
}

// ---- CloudWatch Dashboard ----

func (rc *ResourceCreator) createCloudWatchDashboard(
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.CloudWatch == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DashboardName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	body := strProp(props, "DashboardBody", params, physicalIDs)
	if body == "" {
		body = `{"widgets":[]}`
	}

	if _, err := rc.backends.CloudWatch.Backend.PutDashboard(name, body); err != nil {
		return "", fmt.Errorf("create CloudWatch dashboard %s: %w", name, err)
	}

	return name, nil
}

func (rc *ResourceCreator) deleteCloudWatchDashboard(name string) error {
	if rc.backends.CloudWatch == nil {
		return nil
	}

	return rc.backends.CloudWatch.Backend.DeleteDashboards([]string{name})
}
