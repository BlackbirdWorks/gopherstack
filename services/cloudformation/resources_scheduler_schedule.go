package cloudformation

import (
	"context"
	"fmt"
	"strings"
	"time"

	schedulerbackend "github.com/blackbirdworks/gopherstack/services/scheduler"
)

type cfnScheduleProps struct {
	Target                     schedulerbackend.Target
	Name                       string
	GroupName                  string
	Description                string
	ScheduleExpression         string
	ScheduleExpressionTimezone string
	State                      string
	KmsKeyArn                  string
	StartDate                  string
	EndDate                    string
	FlexibleTimeWindow         schedulerbackend.FlexibleTimeWindow
}

func (rc *ResourceCreator) createSchedulerSchedule(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Scheduler == nil {
		return logicalID + "-stub", nil
	}

	var in cfnScheduleProps
	if err := decodeProps(props, params, physicalIDs, &in); err != nil {
		return "", err
	}
	if in.Name == "" {
		in.Name = logicalID
	}
	if in.State == "" {
		in.State = "ENABLED"
	}
	if in.FlexibleTimeWindow.Mode == "" {
		in.FlexibleTimeWindow.Mode = "OFF"
	}
	opts, err := scheduleDateOptions(in)
	if err != nil {
		return "", err
	}
	if in.KmsKeyArn != "" {
		opts = append(opts, schedulerbackend.WithKmsKeyArn(in.KmsKeyArn))
	}

	sched, err := rc.backends.Scheduler.Backend.CreateSchedule(ctx, in.Name, in.GroupName, in.ScheduleExpression,
		in.Description, in.ScheduleExpressionTimezone, in.Target, in.State, in.FlexibleTimeWindow, opts...)
	if err != nil {
		return "", fmt.Errorf("create Scheduler schedule %s: %w", in.Name, err)
	}

	return sched.ARN, nil
}

func scheduleDateOptions(in cfnScheduleProps) ([]schedulerbackend.ScheduleOption, error) {
	var opts []schedulerbackend.ScheduleOption
	if in.StartDate != "" {
		t, err := time.Parse(time.RFC3339, in.StartDate)
		if err != nil {
			return nil, fmt.Errorf("invalid StartDate %q: %w", in.StartDate, err)
		}
		opts = append(opts, schedulerbackend.WithStartDate(t))
	}
	if in.EndDate != "" {
		t, err := time.Parse(time.RFC3339, in.EndDate)
		if err != nil {
			return nil, fmt.Errorf("invalid EndDate %q: %w", in.EndDate, err)
		}
		opts = append(opts, schedulerbackend.WithEndDate(t))
	}

	return opts, nil
}

// deleteSchedulerSchedule reads group and name from a ".../schedule/<group>/<name>" ARN.
func (rc *ResourceCreator) deleteSchedulerSchedule(ctx context.Context, arn string) error {
	if rc.backends.Scheduler == nil {
		return nil
	}

	group, name := "", resourceNameFromARN(arn)
	if _, rest, ok := strings.Cut(arn, ":schedule/"); ok {
		if g, n, found := strings.Cut(rest, "/"); found {
			group, name = g, n
		}
	}

	return rc.backends.Scheduler.Backend.DeleteSchedule(ctx, name, group)
}
