package cloudwatch_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// InsightRule: lifecycle
// ---------------------------------------------------------------------------

func TestBackend_InsightRule_CRUD(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()

	require.NoError(t, b.PutInsightRule(&cloudwatch.InsightRule{
		Name:       "rule1",
		Definition: `{"Schema":{"Name":"CloudWatchLogRule","Version":1}}`,
	}))

	rule, err := b.GetInsightRule("rule1")
	require.NoError(t, err)
	assert.Equal(t, "rule1", rule.Name)
	assert.Equal(t, "ENABLED", rule.State)
	assert.NotEmpty(t, rule.Arn)

	// Disable
	failures, err := b.DisableInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	rule, _ = b.GetInsightRule("rule1")
	assert.Equal(t, "DISABLED", rule.State)

	// Re-enable
	failures, err = b.EnableInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	rule, _ = b.GetInsightRule("rule1")
	assert.Equal(t, "ENABLED", rule.State)

	// Delete
	failures, err = b.DeleteInsightRules([]string{"rule1"})
	require.NoError(t, err)
	assert.Empty(t, failures)

	_, err = b.GetInsightRule("rule1")
	assert.Error(t, err)
}

func TestBackend_InsightRule_DeleteNonExistent(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	failures, err := b.DeleteInsightRules([]string{"missing"})
	require.NoError(t, err)
	require.Len(t, failures, 1)
	assert.Equal(t, "missing", failures[0].RuleName)
}

func TestCloudWatchBackend_GetInsightRuleContributors(t *testing.T) {
	t.Parallel()

	b := cloudwatch.NewInMemoryBackend()
	require.NoError(t, b.PutInsightRule(&cloudwatch.InsightRule{
		Name:       "rule-1",
		Definition: `{}`,
		Schema:     "CloudWatchLogRule",
	}))

	ts := time.Now().UTC().Add(-30 * time.Second)
	err := b.PutMetricData("App", []cloudwatch.MetricDatum{
		{
			MetricName: "Hits", Value: 10, Count: 10, Sum: 100, Min: 8, Max: 12, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "h1"}},
		},
		{
			MetricName: "Hits", Value: 5, Count: 5, Sum: 50, Min: 9, Max: 11, Timestamp: ts,
			Dimensions: []cloudwatch.Dimension{{Name: "Host", Value: "h2"}},
		},
	})
	require.NoError(t, err)

	contributors, err := b.GetInsightRuleContributorsForTest(
		"rule-1",
		time.Now().UTC().Add(-2*time.Minute),
		time.Now().UTC(),
		10,
		"Sum",
	)
	require.NoError(t, err)
	require.Len(t, contributors, 2, "should return contributors for each dimension set")
	// h1 has higher sum so should be first.
	assert.Equal(t, []string{"h1"}, contributors[0].Keys)
}
