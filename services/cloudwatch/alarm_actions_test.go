package cloudwatch_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// recordingEC2Actioner records the instance IDs passed to each EC2 action.
type recordingEC2Actioner struct {
	err        error
	stopped    []string
	terminated []string
	rebooted   []string
}

func (m *recordingEC2Actioner) StopInstances(ids []string) error {
	m.stopped = append(m.stopped, ids...)

	return m.err
}

func (m *recordingEC2Actioner) TerminateInstances(ids []string) error {
	m.terminated = append(m.terminated, ids...)

	return m.err
}

func (m *recordingEC2Actioner) RebootInstances(ids []string) error {
	m.rebooted = append(m.rebooted, ids...)

	return m.err
}

// recordingASGExecutor records scaling-policy executions.
type recordingASGExecutor struct {
	err    error
	asg    string
	policy string
	calls  int
}

func (m *recordingASGExecutor) ExecuteScalingPolicy(asg, policy string) error {
	m.asg = asg
	m.policy = policy
	m.calls++

	return m.err
}

func TestParseEC2AutomateVerb(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		wantVerb string
		wantOK   bool
	}{
		{"stop", "arn:aws:automate:us-east-1:ec2:stop", "stop", true},
		{"terminate", "arn:aws:automate:us-west-2:ec2:terminate", "terminate", true},
		{"reboot", "arn:aws:automate:eu-west-1:ec2:reboot", "reboot", true},
		{"recover", "arn:aws:automate:us-east-1:ec2:recover", "recover", true},
		{"wrong service", "arn:aws:automate:us-east-1:rds:reboot", "", false},
		{"unknown verb", "arn:aws:automate:us-east-1:ec2:explode", "", false},
		{"too few parts", "arn:aws:automate:ec2:stop", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			verb, ok := cloudwatch.ParseEC2AutomateVerbForTest(tt.action)
			if ok != tt.wantOK || verb != tt.wantVerb {
				t.Fatalf("got (%q, %v), want (%q, %v)", verb, ok, tt.wantVerb, tt.wantOK)
			}
		})
	}
}

func TestParseScalingPolicyARN(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:" +
		"abcd-1234:autoScalingGroupName/my-asg:policyName/scale-out"

	tests := []struct {
		name       string
		action     string
		wantASG    string
		wantPolicy string
		wantOK     bool
	}{
		{"full arn", arn, "my-asg", "scale-out", true},
		{"missing policy", "arn:aws:autoscaling:us-east-1:1:scalingPolicy:x:autoScalingGroupName/g", "", "", false},
		{"not a policy arn", "arn:aws:autoscaling:us-east-1:1:foo", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			asg, policy, ok := cloudwatch.ParseScalingPolicyARNForTest(tt.action)
			if ok != tt.wantOK || asg != tt.wantASG || policy != tt.wantPolicy {
				t.Fatalf("got (%q, %q, %v), want (%q, %q, %v)",
					asg, policy, ok, tt.wantASG, tt.wantPolicy, tt.wantOK)
			}
		})
	}
}

func TestEC2AlarmActionsExecute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		action      string
		wantStopped []string
		wantTerm    []string
		wantReboot  []string
	}{
		{
			name:        "stop",
			action:      "arn:aws:automate:us-east-1:ec2:stop",
			wantStopped: []string{"i-abc123"},
		},
		{
			name:     "terminate",
			action:   "arn:aws:automate:us-east-1:ec2:terminate",
			wantTerm: []string{"i-abc123"},
		},
		{
			name:       "reboot",
			action:     "arn:aws:automate:us-east-1:ec2:reboot",
			wantReboot: []string{"i-abc123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			ec2 := &recordingEC2Actioner{}
			b.SetEC2Actioner(ec2)

			if err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
				AlarmName:      "cpu-alarm",
				StateValue:     "OK",
				ActionsEnabled: true,
				AlarmActions:   []string{tt.action},
				Dimensions:     []cloudwatch.Dimension{{Name: "InstanceId", Value: "i-abc123"}},
			}); err != nil {
				t.Fatalf("PutMetricAlarm: %v", err)
			}

			if err := b.SetAlarmState(t.Context(), "cpu-alarm", "ALARM", "test", ""); err != nil {
				t.Fatalf("SetAlarmState: %v", err)
			}

			assertIDs(t, "stopped", ec2.stopped, tt.wantStopped)
			assertIDs(t, "terminated", ec2.terminated, tt.wantTerm)
			assertIDs(t, "rebooted", ec2.rebooted, tt.wantReboot)
		})
	}
}

func TestEC2AlarmActionSkippedWithoutInstanceDimension(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	ec2 := &recordingEC2Actioner{}
	b.SetEC2Actioner(ec2)

	if err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "no-dim",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{"arn:aws:automate:us-east-1:ec2:stop"},
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	if err := b.SetAlarmState(t.Context(), "no-dim", "ALARM", "test", ""); err != nil {
		t.Fatalf("SetAlarmState: %v", err)
	}

	if len(ec2.stopped) != 0 {
		t.Fatalf("expected no stop without InstanceId dimension, got %v", ec2.stopped)
	}
}

func TestAutoScalingAlarmActionExecutes(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	asg := &recordingASGExecutor{}
	b.SetAutoScalingExecutor(asg)

	const arn = "arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:" +
		"uuid-1:autoScalingGroupName/web-asg:policyName/scale-up"

	if err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "asg-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{arn},
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	if err := b.SetAlarmState(t.Context(), "asg-alarm", "ALARM", "test", ""); err != nil {
		t.Fatalf("SetAlarmState: %v", err)
	}

	if asg.calls != 1 || asg.asg != "web-asg" || asg.policy != "scale-up" {
		t.Fatalf("ExecuteScalingPolicy got (calls=%d asg=%q policy=%q), want (1, web-asg, scale-up)",
			asg.calls, asg.asg, asg.policy)
	}
}

func assertIDs(t *testing.T, label string, got, want []string) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s[%d] = %q, want %q", label, i, got[i], want[i])
		}
	}
}

// mockSNSPublisher captures published messages for assertions.
type mockSNSPublisher struct {
	messages []string
}

func (m *mockSNSPublisher) PublishToTopic(_ string, message string) error {
	m.messages = append(m.messages, message)

	return nil
}

// mockLambdaInvoker captures lambda invocations for assertions.
type mockLambdaInvoker struct {
	invocations []string
}

func (m *mockLambdaInvoker) InvokeFunction(
	_ context.Context,
	name string,
	_ string,
	_ []byte,
) ([]byte, int, error) {
	m.invocations = append(m.invocations, name)

	return nil, 200, nil
}

func TestCloudWatchBackend_LambdaActionFires(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	inv := &mockLambdaInvoker{}
	b.SetLambdaInvoker(inv)

	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:my-fn"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "lambda-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{lambdaARN},
	}))

	require.NoError(t, b.SetAlarmState(t.Context(), "lambda-alarm", "ALARM", "test", ""))

	assert.Len(t, inv.invocations, 1)
	assert.Equal(t, lambdaARN, inv.invocations[0])
}

func TestCloudWatchBackend_ExecuteActions_NoInvoker(t *testing.T) {
	t.Parallel()

	// Ensures executeActions does not panic when publisher/invoker is nil.
	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "no-invoker-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions: []string{
			"arn:aws:sns:us-east-1:123:topic",
			"arn:aws:lambda:us-east-1:123:function:fn",
			"arn:aws:ec2:us-east-1:123:action/stop",
		},
	}))

	// Should not panic even with nil publisher/invoker.
	require.NoError(t, b.SetAlarmState(t.Context(), "no-invoker-alarm", "ALARM", "test", ""))
}

// mockCancelledLambdaInvoker blocks until the context is cancelled, then returns an error.
type mockCancelledLambdaInvoker struct {
	called chan struct{}
}

func (m *mockCancelledLambdaInvoker) InvokeFunction(
	ctx context.Context,
	_ string,
	_ string,
	_ []byte,
) ([]byte, int, error) {
	close(m.called)
	<-ctx.Done()

	return nil, 0, ctx.Err()
}

func TestCloudWatchBackend_ExecuteActions_ContextPropagated(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	inv := &mockCancelledLambdaInvoker{called: make(chan struct{})}
	b.SetLambdaInvoker(inv)

	lambdaARN := "arn:aws:lambda:us-east-1:123456789012:function:ctx-fn"

	require.NoError(t, b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName:      "ctx-alarm",
		StateValue:     "OK",
		ActionsEnabled: true,
		AlarmActions:   []string{lambdaARN},
	}))

	ctx, cancel := context.WithCancel(t.Context())

	done := make(chan error, 1)
	go func() {
		done <- b.SetAlarmState(ctx, "ctx-alarm", "ALARM", "test", "")
	}()

	// Wait for the invoker to be called, then cancel the context.
	<-inv.called
	cancel()

	err := <-done
	require.NoError(t, err, "SetAlarmState itself should not propagate Lambda delivery errors")
}
