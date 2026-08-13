package cloudwatch_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cloudwatch "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestDescribeAlarmContributors_SDKRoundTrip drives DescribeAlarmContributors
// through the real aws-sdk-go-v2 cloudwatch client (rpc-v2 CBOR, the only
// wire protocol this SDK version speaks -- see sdk_roundtrip_helper_test.go).
// The SDK decodes nothing from a response key it doesn't recognize, so this
// is the only proof that AlarmContributors (not the previous Contributors)
// and its real ContributorId/StateReason/StateTransitionedTimestamp fields
// (not the previous Keys/Sum) actually reach a real client.
func TestDescribeAlarmContributors_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client, backend := newTestHandlerAndClientWithBackend(t)

	require.NoError(t, backend.PutMetricAlarm(&cloudwatch.MetricAlarm{AlarmName: "child-a"}))
	require.NoError(t, backend.SetAlarmState(t.Context(), "child-a", "ALARM", "breach detected", ""))
	require.NoError(t, backend.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "composite",
		AlarmRule: `ALARM("child-a")`,
	}))

	out, err := client.DescribeAlarmContributors(t.Context(), &cwsdk.DescribeAlarmContributorsInput{
		AlarmName: aws.String("composite"),
	})
	require.NoError(t, err)
	require.Len(t, out.AlarmContributors, 1,
		"AlarmContributors is the real wire key; an empty list means the wrong key is still in play")

	got := out.AlarmContributors[0]
	assert.Equal(t, "child-a", aws.ToString(got.ContributorId))
	assert.Equal(t, "breach detected", aws.ToString(got.StateReason))
	assert.Equal(t, "ALARM", got.ContributorAttributes["State"])
}

func TestExtractAlarmRuleRefs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rule      string
		wantFuncs []string
		wantNames []string
	}{
		{
			name:      "single alarm",
			rule:      `ALARM("cpu-high")`,
			wantFuncs: []string{"ALARM"},
			wantNames: []string{"cpu-high"},
		},
		{
			name:      "and of two",
			rule:      `ALARM("a") AND OK("b")`,
			wantFuncs: []string{"ALARM", "OK"},
			wantNames: []string{"a", "b"},
		},
		{
			name:      "nested with not and insufficient",
			rule:      `(ALARM("a") OR INSUFFICIENT_DATA("c")) AND NOT OK("b")`,
			wantFuncs: []string{"ALARM", "INSUFFICIENT_DATA", "OK"},
			wantNames: []string{"a", "c", "b"},
		},
		{
			name:      "no references",
			rule:      `TRUE`,
			wantFuncs: nil,
			wantNames: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			funcs, names := cloudwatch.ExtractAlarmRuleRefsForTest(tt.rule)
			assertStrSlice(t, "funcs", funcs, tt.wantFuncs)
			assertStrSlice(t, "names", names, tt.wantNames)
		})
	}
}

func TestDescribeAlarmContributors(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	mustAlarm := func(name, state string) {
		t.Helper()
		if err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
			AlarmName: name, StateValue: state,
		}); err != nil {
			t.Fatalf("PutMetricAlarm %s: %v", name, err)
		}
	}

	mustAlarm("child-a", "ALARM")
	mustAlarm("child-b", "OK")
	mustAlarm("child-c", "ALARM")

	if err := b.PutCompositeAlarm(&cloudwatch.CompositeAlarm{
		AlarmName: "composite",
		AlarmRule: `ALARM("child-a") OR ALARM("child-b") OR OK("child-c")`,
	}); err != nil {
		t.Fatalf("PutCompositeAlarm: %v", err)
	}

	// child-a is in ALARM (matches ALARM(...)) -> contributor.
	// child-b is OK but referenced by ALARM(...) -> NOT a contributor.
	// child-c is ALARM but referenced by OK(...) -> NOT a contributor.
	page, err := b.DescribeAlarmContributors("composite", "")
	if err != nil {
		t.Fatalf("DescribeAlarmContributors: %v", err)
	}
	if len(page.Data) != 1 {
		t.Fatalf("contributors = %d (%+v), want 1", len(page.Data), page.Data)
	}
	got := page.Data[0]
	if got.ContributorID != "child-a" {
		t.Fatalf("contributor ContributorID = %q, want child-a", got.ContributorID)
	}
	if got.ContributorAttributes["State"] != "ALARM" {
		t.Fatalf("contributor ContributorAttributes[State] = %q, want ALARM", got.ContributorAttributes["State"])
	}
}

func TestDescribeAlarmContributorsMetricAlarmEmpty(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	if err := b.PutMetricAlarm(&cloudwatch.MetricAlarm{
		AlarmName: "plain", StateValue: "ALARM",
	}); err != nil {
		t.Fatalf("PutMetricAlarm: %v", err)
	}

	page, err := b.DescribeAlarmContributors("plain", "")
	if err != nil {
		t.Fatalf("DescribeAlarmContributors: %v", err)
	}
	if len(page.Data) != 0 {
		t.Fatalf("metric alarm contributors = %d, want 0", len(page.Data))
	}
}

func TestDescribeAlarmContributorsNotFound(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	_, err := b.DescribeAlarmContributors("ghost", "")
	if err == nil {
		t.Fatal("expected error for unknown alarm")
	}
	if !errors.Is(err, cloudwatch.ErrAlarmNotFound) {
		t.Fatalf("error = %v, want ErrAlarmNotFound", err)
	}
}

func assertStrSlice(t *testing.T, label string, got, want []string) {
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
