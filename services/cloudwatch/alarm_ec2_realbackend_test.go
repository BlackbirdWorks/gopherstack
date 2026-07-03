package cloudwatch_test

import (
	"testing"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

// cwEC2RealAdapter wires the real EC2 backend into the CloudWatch alarm-action
// interface so that arn:aws:automate EC2 actions mutate real instance state.
type cwEC2RealAdapter struct {
	backend *ec2backend.InMemoryBackend
}

func (a *cwEC2RealAdapter) StopInstances(ids []string) error {
	_, err := a.backend.StopInstances(ids)

	return err
}

func (a *cwEC2RealAdapter) TerminateInstances(ids []string) error {
	_, err := a.backend.TerminateInstances(ids)

	return err
}

func (a *cwEC2RealAdapter) RebootInstances(ids []string) error {
	return a.backend.RebootInstances(ids)
}

// TestCloudWatchEC2AlarmStopsRealInstance verifies end-to-end that an EC2
// automate:stop alarm action drives the actual EC2 backend: the instance named by
// the alarm's InstanceId dimension transitions out of "running"/"pending" into
// "stopping". This exercises the real cross-service wiring (not a mock), asserting
// the target backend actually mutated.
func TestCloudWatchEC2AlarmStopsRealInstance(t *testing.T) {
	t.Parallel()

	ec2Bk := ec2backend.NewInMemoryBackend("123456789012", "us-east-1")

	instances, err := ec2Bk.RunInstances("ami-12345678", "t3.micro", "", 1)
	if err != nil {
		t.Fatalf("RunInstances: %v", err)
	}
	if len(instances) != 1 {
		t.Fatalf("expected 1 instance, got %d", len(instances))
	}
	instanceID := instances[0].ID
	if instanceID == "" {
		t.Fatal("instance ID is empty")
	}

	cwBk := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	cwBk.SetEC2Actioner(&cwEC2RealAdapter{backend: ec2Bk})

	if err = cwBk.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "instance-cpu",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{"arn:aws:automate:us-east-1:ec2:stop"},
		Dimensions:     []cloudwatch.Dimension{{Name: "InstanceId", Value: instanceID}},
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	// Fire the alarm.
	if err = cwBk.SetAlarmState(t.Context(), "instance-cpu", "ALARM", "cpu high", ""); err != nil {
		t.Fatalf("SetAlarmState: %v", err)
	}

	// The real EC2 backend must now report the instance as stopping.
	status := ec2Bk.DescribeInstanceStatus([]string{instanceID})
	if len(status) != 1 {
		t.Fatalf("expected 1 instance status, got %d", len(status))
	}
	if got := status[0].State.Name; got != "stopping" {
		t.Fatalf("instance state = %q, want %q (alarm action should have stopped it)", got, "stopping")
	}
}
