package eventbridge_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

const (
	testBusName  = "default"
	testRuleName = "rule-arn-index-test"
	testTargetID = "target-1"
	testTargetID2 = "target-2"
	testARN      = "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	testARN2     = "arn:aws:lambda:us-east-1:000000000000:function:other-fn"
	testRegion   = "us-east-1"
)

func newEB(t *testing.T) *eventbridge.InMemoryBackend {
	t.Helper()
	b := eventbridge.NewInMemoryBackendWithConfig("000000000000", testRegion)
	t.Cleanup(func() { _ = b.Shutdown(context.Background()) })
	return b
}

// TestEBTargetsByARNIndexConsistency verifies that the targetsByARN index stays
// consistent with the canonical targets map across PutTargets, RemoveTargets,
// DeleteRule, and Reset.
func TestEBTargetsByARNIndexConsistency(t *testing.T) {
	t.Parallel()

	t.Run("put_and_remove", func(t *testing.T) {
		t.Parallel()
		b := newEB(t)

		_, err := b.CreateRule(context.Background(), testRuleName, testBusName, "", "rate(1 minute)", "")
		require.NoError(t, err)

		failed, err := b.PutTargets(context.Background(), testRuleName, testBusName, []eventbridge.Target{
			{ID: testTargetID, Arn: testARN},
		})
		require.NoError(t, err)
		require.Empty(t, failed)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after PutTargets: %s", msg)
		assert.Equal(t, 1, b.TargetsByARNCount(testRegion, testARN))

		failed, err = b.RemoveTargets(context.Background(), testRuleName, testBusName, []string{testTargetID})
		require.NoError(t, err)
		require.Empty(t, failed)

		ok, msg = b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after RemoveTargets: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(testRegion, testARN))
	})

	t.Run("delete_rule_removes_entries", func(t *testing.T) {
		t.Parallel()
		b := newEB(t)

		_, err := b.CreateRule(context.Background(), testRuleName, testBusName, "", "rate(1 minute)", "")
		require.NoError(t, err)

		failed, err := b.PutTargets(context.Background(), testRuleName, testBusName, []eventbridge.Target{
			{ID: testTargetID, Arn: testARN},
			{ID: testTargetID2, Arn: testARN2},
		})
		require.NoError(t, err)
		require.Empty(t, failed)

		require.Equal(t, 1, b.TargetsByARNCount(testRegion, testARN))
		require.Equal(t, 1, b.TargetsByARNCount(testRegion, testARN2))

		err = b.DeleteRule(context.Background(), testRuleName, testBusName)
		require.NoError(t, err)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after DeleteRule: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(testRegion, testARN))
		assert.Equal(t, 0, b.TargetsByARNCount(testRegion, testARN2))
	})

	t.Run("reset_clears_index", func(t *testing.T) {
		t.Parallel()
		b := newEB(t)

		_, err := b.CreateRule(context.Background(), testRuleName, testBusName, "", "rate(1 minute)", "")
		require.NoError(t, err)

		_, err = b.PutTargets(context.Background(), testRuleName, testBusName, []eventbridge.Target{
			{ID: testTargetID, Arn: testARN},
		})
		require.NoError(t, err)

		b.Reset()

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after Reset: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(testRegion, testARN))
	})

	t.Run("update_target_arn", func(t *testing.T) {
		t.Parallel()
		b := newEB(t)

		_, err := b.CreateRule(context.Background(), testRuleName, testBusName, "", "rate(1 minute)", "")
		require.NoError(t, err)

		_, err = b.PutTargets(context.Background(), testRuleName, testBusName, []eventbridge.Target{
			{ID: testTargetID, Arn: testARN},
		})
		require.NoError(t, err)

		// Update the same target ID with a different ARN.
		_, err = b.PutTargets(context.Background(), testRuleName, testBusName, []eventbridge.Target{
			{ID: testTargetID, Arn: testARN2},
		})
		require.NoError(t, err)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after ARN update: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(testRegion, testARN), "old ARN must be removed")
		assert.Equal(t, 1, b.TargetsByARNCount(testRegion, testARN2), "new ARN must be present")
	})
}

// TestEBListRuleNamesByTargetUsesIndex verifies that ListRuleNamesByTarget returns
// correct results using the ARN index.
func TestEBListRuleNamesByTargetUsesIndex(t *testing.T) {
	t.Parallel()

	b := newEB(t)

	const numRules = 10
	for i := range numRules {
		ruleName := fmt.Sprintf("rule-%d", i)
		_, err := b.CreateRule(context.Background(), ruleName, testBusName, "", "rate(1 minute)", "")
		require.NoError(t, err)

		// Put the same ARN on rules 0,2,4,... (even-indexed rules).
		if i%2 == 0 {
			_, err = b.PutTargets(context.Background(), ruleName, testBusName, []eventbridge.Target{
				{ID: testTargetID, Arn: testARN},
			})
			require.NoError(t, err)
		}
	}

	names, _, err := b.ListRuleNamesByTarget(context.Background(), testARN, testBusName, "")
	require.NoError(t, err)
	assert.Len(t, names, numRules/2, "expected exactly half of rules to match")
}

// BenchmarkEBListRuleNamesByTarget benchmarks ListRuleNamesByTarget with a large
// number of rules to demonstrate the O(matched-rules) index behaviour.
func BenchmarkEBListRuleNamesByTarget(b *testing.B) {
	backend := eventbridge.NewInMemoryBackendWithConfig("000000000000", testRegion)
	defer func() { _ = backend.Shutdown(context.Background()) }()

	const numRules = 200
	for i := range numRules {
		ruleName := fmt.Sprintf("bench-rule-%d", i)
		_, err := backend.CreateRule(context.Background(), ruleName, testBusName, "", "rate(1 minute)", "")
		if err != nil {
			b.Fatal(err)
		}
		// Only one rule has the target ARN we'll search for.
		if i == 0 {
			_, err = backend.PutTargets(context.Background(), ruleName, testBusName, []eventbridge.Target{
				{ID: testTargetID, Arn: testARN},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		names, _, err := backend.ListRuleNamesByTarget(context.Background(), testARN, testBusName, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(names) != 1 {
			b.Fatalf("expected 1 rule, got %d", len(names))
		}
	}
}
