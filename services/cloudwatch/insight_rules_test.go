package cloudwatch_test

import (
	"testing"

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
