package cloudwatch_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

type transformedInsightLogs struct{ insightLogs }

func (l transformedInsightLogs) TransformedInsightEvents(
	region string, patterns []string, start, end time.Time,
) []cloudwatch.InsightLogEvent {
	evs := l.InsightEvents(region, patterns, start, end)
	for i := range evs {
		evs[i].Message = strings.ReplaceAll(evs[i].Message, "abc", "ABC")
	}

	return evs
}

func TestInsightRuleApplyOnTransformedLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantKeys    [][]string
		transformed bool
	}{
		{name: "original_events", wantKeys: [][]string{{"abc"}}},
		{name: "transformed_events", transformed: true, wantKeys: [][]string{{"ABC"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, bk := newTestHandlerAndClientWithBackend(t)
			logs := cloudwatchlogs.NewInMemoryBackend()
			bk.SetLogEventSource(transformedInsightLogs{insightLogs{b: logs}})
			env := &insightEnv{
				client: client,
				logs:   logs,
				start:  time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute),
			}

			env.putEvents(t, "app-1", []insightEvent{{msg: `{"ip":"abc"}`}})

			_, err := client.PutInsightRule(t.Context(), &cwsdk.PutInsightRuleInput{
				RuleName:               aws.String("r"),
				RuleDefinition:         aws.String(jsonRule(`"$.ip"`, "") + `,"AggregateOn":"Count"}`),
				ApplyOnTransformedLogs: aws.Bool(tt.transformed),
			})
			require.NoError(t, err)

			desc, err := client.DescribeInsightRules(t.Context(), &cwsdk.DescribeInsightRulesInput{})
			require.NoError(t, err)
			require.Len(t, desc.InsightRules, 1)
			assert.Equal(t, tt.transformed, aws.ToBool(desc.InsightRules[0].ApplyOnTransformedLogs))

			assert.Equal(t, tt.wantKeys, contributorKeys(env.report(t, "r", nil)))
		})
	}
}
