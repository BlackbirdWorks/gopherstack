package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

const conformancePackComplianceTestTemplate = `{
	"Resources": {
		"RuleA": {
			"Type": "AWS::Config::ConfigRule",
			"Properties": {"ConfigRuleName": "rule-a",
				"Source": {"Owner": "AWS", "SourceIdentifier": "ENCRYPTED_VOLUMES"}}
		},
		"RuleB": {
			"Type": "AWS::Config::ConfigRule",
			"Properties": {"ConfigRuleName": "rule-b",
				"Source": {"Owner": "AWS", "SourceIdentifier": "ENCRYPTED_VOLUMES"}}
		}
	}
}`

// newCompliancePackBackend returns a fresh backend with a conformance pack
// ("pack1") deploying rule-a/rule-b, evaluated so rule-a is NON_COMPLIANT (one
// non-compliant volume) and rule-b is COMPLIANT (no volumes matched, so it
// rolls up... note both rules share the same evaluator/resource-type scope
// (ENCRYPTED_VOLUMES), so both see the same single non-compliant volume and
// both roll up NON_COMPLIANT; tests that need one COMPLIANT and one
// NON_COMPLIANT rule build their own backend instead.
func newCompliancePackBackend(t *testing.T) *awsconfig.InMemoryBackend {
	t.Helper()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConformancePack("pack1", "", "", conformancePackComplianceTestTemplate))
	require.NoError(t, b.PutResourceConfig("AWS::EC2::Volume", "vol-bad", `{"Encrypted":false}`))
	require.NoError(t, b.StartConfigRulesEvaluation())

	return b
}

func TestAWSConfigBackend_DeleteConformancePack_CascadesRules(t *testing.T) {
	t.Parallel()

	b := newCompliancePackBackend(t)

	rules, err := b.DescribeConfigRules(nil)
	require.NoError(t, err)
	require.Len(t, rules, 2)

	require.NoError(t, b.DeleteConformancePack("pack1"))

	rules, err = b.DescribeConfigRules(nil)
	require.NoError(t, err)
	assert.Empty(t, rules, "deleting the conformance pack must cascade-delete its deployed rules")
}

func TestAWSConfigBackend_DescribeConformancePackCompliance_Full(t *testing.T) {
	t.Parallel()

	t.Run("returns_every_deployed_rule", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		items, err := b.DescribeConformancePackCompliance("pack1", nil, "")
		require.NoError(t, err)
		require.Len(t, items, 2)
	})

	t.Run("unknown_rule_filter_errors", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		_, err := b.DescribeConformancePackCompliance("pack1", []string{"not-in-pack"}, "")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchConfigRuleInConformancePack)
	})

	t.Run("compliance_type_filter", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		items, err := b.DescribeConformancePackCompliance("pack1", nil, "NON_COMPLIANT")
		require.NoError(t, err)
		require.Len(t, items, 2)
	})

	t.Run("unknown_pack_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()

		_, err := b.DescribeConformancePackCompliance("does-not-exist", nil, "")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchConformancePack)
	})
}

func TestAWSConfigBackend_GetConformancePackComplianceDetails_Full(t *testing.T) {
	t.Parallel()

	b := newCompliancePackBackend(t)

	details, err := b.GetConformancePackComplianceDetails("pack1", nil, "", nil, "")
	require.NoError(t, err)
	require.Len(t, details, 2)
	assert.Equal(t, "vol-bad", details[0].EvaluationResultIdentifier.EvaluationResultQualifier.ResourceID)
	assert.Equal(t, "NON_COMPLIANT", details[0].ComplianceType)
}

func TestAWSConfigBackend_GetConformancePackComplianceSummary_Full(t *testing.T) {
	t.Parallel()

	t.Run("unknown_pack_errors", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()

		_, err := b.GetConformancePackComplianceSummary([]string{"does-not-exist"})
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchConformancePack)
	})

	t.Run("non_compliant_rule_makes_pack_non_compliant", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		summaries, err := b.GetConformancePackComplianceSummary([]string{"pack1"})
		require.NoError(t, err)
		require.Len(t, summaries, 1)
		assert.Equal(t, "NON_COMPLIANT", summaries[0].ConformancePackComplianceStatus)
	})
}

func TestAWSConfigBackend_ListConformancePackComplianceScores_Full(t *testing.T) {
	t.Parallel()

	t.Run("no_filter_returns_every_pack", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)
		require.NoError(t, b.PutConformancePack("empty-pack", "", "", ""))

		scores := b.ListConformancePackComplianceScores(nil)
		require.Len(t, scores, 2)
	})

	t.Run("pack_with_evaluations_has_numeric_score", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		scores := b.ListConformancePackComplianceScores([]string{"pack1"})
		require.Len(t, scores, 1)
		assert.NotEqual(t, "INSUFFICIENT_DATA", scores[0].Score)
	})

	t.Run("pack_with_no_evaluations_is_insufficient_data", func(t *testing.T) {
		t.Parallel()

		b := awsconfig.NewInMemoryBackend()
		require.NoError(t, b.PutConformancePack("empty-pack", "", "", ""))

		scores := b.ListConformancePackComplianceScores([]string{"empty-pack"})
		require.Len(t, scores, 1)
		assert.Equal(t, "INSUFFICIENT_DATA", scores[0].Score)
	})
}

func TestAWSConfigBackend_DescribeAggregateComplianceByConformancePacks_Full(t *testing.T) {
	t.Parallel()

	t.Run("unknown_aggregator_errors", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		_, err := b.DescribeAggregateComplianceByConformancePacks("does-not-exist", "", "")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchAggregator)
	})

	t.Run("reports_pack_rule_counts", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)
		require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil))

		out, err := b.DescribeAggregateComplianceByConformancePacks("agg1", "123456789012", "us-east-1")
		require.NoError(t, err)
		require.Len(t, out, 1)
		require.NotNil(t, out[0].Compliance)
		assert.Equal(t, int32(2), out[0].Compliance.TotalRuleCount)
		assert.Equal(t, int32(2), out[0].Compliance.NonCompliantRuleCount)
		assert.Equal(t, "123456789012", out[0].AccountID)
		assert.Equal(t, "us-east-1", out[0].AwsRegion)
	})
}

func TestAWSConfigBackend_GetAggregateConformancePackComplianceSummary_Full(t *testing.T) {
	t.Parallel()

	t.Run("unknown_aggregator_errors", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)

		_, err := b.GetAggregateConformancePackComplianceSummary("does-not-exist", "")
		require.Error(t, err)
		assert.ErrorIs(t, err, awsconfig.ErrNoSuchAggregator)
	})

	t.Run("counts_non_compliant_pack", func(t *testing.T) {
		t.Parallel()

		b := newCompliancePackBackend(t)
		require.NoError(t, b.PutConfigurationAggregator("agg1", nil, nil))

		out, err := b.GetAggregateConformancePackComplianceSummary("agg1", "")
		require.NoError(t, err)
		require.Len(t, out, 1)
		assert.Equal(t, int32(1), out[0].ComplianceSummary.NonCompliantConformancePackCount)
	})
}
