package eventbridge_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

const (
	arnTestBusName   = "default"
	arnTestRuleName  = "rule-arn-index-test"
	arnTestTargetID  = "target-1"
	arnTestTargetID2 = "target-2"
	arnTestARN       = "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	arnTestARN2      = "arn:aws:lambda:us-east-1:000000000000:function:other-fn"
	arnTestRegion    = "us-east-1"
)

func newEBBackend(t *testing.T) *eventbridge.InMemoryBackend {
	t.Helper()
	b := eventbridge.NewInMemoryBackendWithConfig("000000000000", arnTestRegion)
	t.Cleanup(func() { b.Close() })

	return b
}

func putTestRule(t *testing.T, b *eventbridge.InMemoryBackend, name string) {
	t.Helper()
	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:               name,
		ScheduleExpression: "rate(1 minute)",
	})
	require.NoError(t, err)
}

// TestEBTargetsByARNIndexConsistency verifies that the targetsByARN index stays
// consistent with the canonical targets map across PutTargets, RemoveTargets,
// DeleteRule, and Reset.
func TestEBTargetsByARNIndexConsistency(t *testing.T) {
	t.Parallel()

	t.Run("put_and_remove", func(t *testing.T) {
		t.Parallel()
		b := newEBBackend(t)
		putTestRule(t, b, arnTestRuleName)

		failed, err := b.PutTargets(context.Background(), arnTestRuleName, arnTestBusName, []eventbridge.Target{
			{ID: arnTestTargetID, Arn: arnTestARN},
		})
		require.NoError(t, err)
		require.Empty(t, failed)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after PutTargets: %s", msg)
		assert.Equal(t, 1, b.TargetsByARNCount(arnTestRegion, arnTestARN))

		failed, err = b.RemoveTargets(context.Background(), arnTestRuleName, arnTestBusName, []string{arnTestTargetID})
		require.NoError(t, err)
		require.Empty(t, failed)

		ok, msg = b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after RemoveTargets: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(arnTestRegion, arnTestARN))
	})

	t.Run("delete_rule_removes_entries", func(t *testing.T) {
		t.Parallel()
		b := newEBBackend(t)
		putTestRule(t, b, arnTestRuleName)

		failed, err := b.PutTargets(context.Background(), arnTestRuleName, arnTestBusName, []eventbridge.Target{
			{ID: arnTestTargetID, Arn: arnTestARN},
			{ID: arnTestTargetID2, Arn: arnTestARN2},
		})
		require.NoError(t, err)
		require.Empty(t, failed)

		require.Equal(t, 1, b.TargetsByARNCount(arnTestRegion, arnTestARN))
		require.Equal(t, 1, b.TargetsByARNCount(arnTestRegion, arnTestARN2))

		err = b.DeleteRule(context.Background(), arnTestRuleName, arnTestBusName)
		require.NoError(t, err)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after DeleteRule: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(arnTestRegion, arnTestARN))
		assert.Equal(t, 0, b.TargetsByARNCount(arnTestRegion, arnTestARN2))
	})

	t.Run("reset_clears_index", func(t *testing.T) {
		t.Parallel()
		b := newEBBackend(t)
		putTestRule(t, b, arnTestRuleName)

		_, err := b.PutTargets(context.Background(), arnTestRuleName, arnTestBusName, []eventbridge.Target{
			{ID: arnTestTargetID, Arn: arnTestARN},
		})
		require.NoError(t, err)

		b.Reset()

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after Reset: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(arnTestRegion, arnTestARN))
	})

	t.Run("update_target_arn", func(t *testing.T) {
		t.Parallel()
		b := newEBBackend(t)
		putTestRule(t, b, arnTestRuleName)

		_, err := b.PutTargets(context.Background(), arnTestRuleName, arnTestBusName, []eventbridge.Target{
			{ID: arnTestTargetID, Arn: arnTestARN},
		})
		require.NoError(t, err)

		// Update the same target ID with a different ARN.
		_, err = b.PutTargets(context.Background(), arnTestRuleName, arnTestBusName, []eventbridge.Target{
			{ID: arnTestTargetID, Arn: arnTestARN2},
		})
		require.NoError(t, err)

		ok, msg := b.ARNIndexConsistent()
		require.True(t, ok, "index inconsistent after ARN update: %s", msg)
		assert.Equal(t, 0, b.TargetsByARNCount(arnTestRegion, arnTestARN), "old ARN must be removed")
		assert.Equal(t, 1, b.TargetsByARNCount(arnTestRegion, arnTestARN2), "new ARN must be present")
	})
}

// TestEBListRuleNamesByTargetUsesIndex verifies that ListRuleNamesByTarget returns
// correct results using the ARN index.
func TestEBListRuleNamesByTargetUsesIndex(t *testing.T) {
	t.Parallel()

	b := newEBBackend(t)

	const numRules = 10
	for i := range numRules {
		ruleName := fmt.Sprintf("rule-%d", i)
		putTestRule(t, b, ruleName)

		// Put the same ARN on rules 0,2,4,... (even-indexed rules).
		if i%2 == 0 {
			_, err := b.PutTargets(context.Background(), ruleName, arnTestBusName, []eventbridge.Target{
				{ID: arnTestTargetID, Arn: arnTestARN},
			})
			require.NoError(t, err)
		}
	}

	names, _, err := b.ListRuleNamesByTarget(context.Background(), arnTestARN, arnTestBusName, "")
	require.NoError(t, err)
	assert.Len(t, names, numRules/2, "expected exactly half of rules to match")
}

// BenchmarkEBListRuleNamesByTarget benchmarks ListRuleNamesByTarget with many rules
// to demonstrate the O(matched-rules) index lookup vs O(all-rules×all-targets).
func BenchmarkEBListRuleNamesByTarget(b *testing.B) {
	backend := eventbridge.NewInMemoryBackendWithConfig("000000000000", arnTestRegion)
	defer backend.Close()

	const numRules = 200
	for i := range numRules {
		ruleName := fmt.Sprintf("bench-rule-%d", i)
		_, err := backend.PutRule(context.Background(), eventbridge.PutRuleInput{
			Name:               ruleName,
			ScheduleExpression: "rate(1 minute)",
		})
		if err != nil {
			b.Fatal(err)
		}
		// Only rule-0 has the target ARN being searched.
		if i == 0 {
			_, err = backend.PutTargets(context.Background(), ruleName, arnTestBusName, []eventbridge.Target{
				{ID: arnTestTargetID, Arn: arnTestARN},
			})
			if err != nil {
				b.Fatal(err)
			}
		}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		names, _, err := backend.ListRuleNamesByTarget(context.Background(), arnTestARN, arnTestBusName, "")
		if err != nil {
			b.Fatal(err)
		}
		if len(names) != 1 {
			b.Fatalf("expected 1 rule, got %d", len(names))
		}
	}
}
