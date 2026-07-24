package awsconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestPutConformancePack_TemplateBody_DeploysConfigRules exercises
// parseConformancePackConfigRules indirectly through PutConformancePack (the
// parser itself is unexported), verifying a JSON conformance-pack template's
// AWS::Config::ConfigRule resources become real, evaluable config rules.
func TestPutConformancePack_TemplateBody_DeploysConfigRules(t *testing.T) {
	t.Parallel()

	const template = `{
		"Resources": {
			"S3VersioningRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {
					"ConfigRuleName": "s3-versioning-enabled",
					"Source": {"Owner": "AWS", "SourceIdentifier": "S3_BUCKET_VERSIONING_ENABLED"}
				}
			},
			"UnrelatedResource": {
				"Type": "AWS::S3::Bucket",
				"Properties": {}
			}
		}
	}`

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConformancePack("pack1", "", "", template))

	rules, err := b.DescribeConfigRules([]string{"s3-versioning-enabled"})
	require.NoError(t, err)
	require.Len(t, rules, 1)
	require.NotNil(t, rules[0].Source)
	assert.Equal(t, "S3_BUCKET_VERSIONING_ENABLED", rules[0].Source.SourceIdentifier)

	// The evaluation engine treats it as a real managed rule.
	require.NoError(t, b.PutResourceConfig(
		"AWS::S3::Bucket", "b1", `{"VersioningConfiguration":{"Status":"Enabled"}}`,
	))
	require.NoError(t, b.StartConfigRulesEvaluation())
	assert.Equal(t, "COMPLIANT", b.GetConfigRuleComplianceType("s3-versioning-enabled"))
}

// TestPutConformancePack_TemplateBody_DerivesNameFromLogicalID verifies a
// template resource without an explicit ConfigRuleName gets one derived from
// the pack name and logical ID.
func TestPutConformancePack_TemplateBody_DerivesNameFromLogicalID(t *testing.T) {
	t.Parallel()

	const template = `{
		"Resources": {
			"MyRule": {
				"Type": "AWS::Config::ConfigRule",
				"Properties": {"Source": {"Owner": "AWS", "SourceIdentifier": "ENCRYPTED_VOLUMES"}}
			}
		}
	}`

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConformancePack("mypack", "", "", template))

	rules, err := b.DescribeConfigRules([]string{"mypack-MyRule"})
	require.NoError(t, err)
	require.Len(t, rules, 1)
}

// TestPutConformancePack_TemplateBody_Unparsable deploys zero rules rather
// than erroring, since PutConformancePack does not require a valid template
// to succeed in this emulator (see parseConformancePackConfigRules's doc
// comment).
func TestPutConformancePack_TemplateBody_Unparsable(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConformancePack("pack1", "", "", "not: valid: json: at: all"))

	rules, err := b.DescribeConfigRules(nil)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

// TestPutConformancePack_TemplateBody_UpdateReplacesRuleSet verifies updating
// a pack's template drops rules no longer present (cascade-deleting their
// evaluations) and adds newly-present ones.
func TestPutConformancePack_TemplateBody_UpdateReplacesRuleSet(t *testing.T) {
	t.Parallel()

	const v1 = `{"Resources":{"RuleA":{"Type":"AWS::Config::ConfigRule",
		"Properties":{"ConfigRuleName":"rule-a","Source":{"Owner":"AWS","SourceIdentifier":"ENCRYPTED_VOLUMES"}}}}}`
	const v2 = `{"Resources":{"RuleB":{"Type":"AWS::Config::ConfigRule",
		"Properties":{"ConfigRuleName":"rule-b","Source":{"Owner":"AWS","SourceIdentifier":"ENCRYPTED_VOLUMES"}}}}}`

	b := awsconfig.NewInMemoryBackend()
	require.NoError(t, b.PutConformancePack("pack1", "", "", v1))

	rules, err := b.DescribeConfigRules(nil)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-a", rules[0].ConfigRuleName)

	require.NoError(t, b.PutConformancePack("pack1", "", "", v2))

	rules, err = b.DescribeConfigRules(nil)
	require.NoError(t, err)
	require.Len(t, rules, 1)
	assert.Equal(t, "rule-b", rules[0].ConfigRuleName)
}
