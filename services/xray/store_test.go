package xray_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

// TestGroupARNUsesBackendRegion verifies that group ARNs contain the region
// and account ID passed to NewInMemoryBackend, not config.DefaultRegion.
func TestGroupARNUsesBackendRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		groupName string
	}{
		{
			name:      "custom region and account",
			accountID: "123456789012",
			region:    "eu-west-1",
			groupName: "my-group",
		},
		{
			name:      "us-west-2 region",
			accountID: "999999999999",
			region:    "us-west-2",
			groupName: "another-group",
		},
		{
			name:      "default test values",
			accountID: "000000000000",
			region:    "us-east-1",
			groupName: "default",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend(tc.accountID, tc.region)
			g, err := b.CreateGroup(tc.groupName, "")
			require.NoError(t, err)

			assert.Contains(t, g.GroupARN, tc.region, "ARN should contain the configured region")
			assert.Contains(t, g.GroupARN, tc.accountID, "ARN should contain the configured account ID")
			assert.True(t, strings.HasPrefix(g.GroupARN, "arn:aws:xray:"), "ARN should start with arn:aws:xray:")
		})
	}
}

// TestSamplingRuleARNUsesBackendRegion verifies that sampling rule ARNs contain the
// region and account ID passed to NewInMemoryBackend.
func TestSamplingRuleARNUsesBackendRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		region    string
		ruleName  string
	}{
		{
			name:      "custom region",
			accountID: "123456789012",
			region:    "ap-southeast-1",
			ruleName:  "my-rule",
		},
		{
			name:      "another region",
			accountID: "000000000000",
			region:    "ca-central-1",
			ruleName:  "test-rule",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend(tc.accountID, tc.region)
			rule := xray.SamplingRule{
				RuleName:      tc.ruleName,
				ResourceARN:   "*",
				ServiceName:   "*",
				ServiceType:   "*",
				Host:          "*",
				HTTPMethod:    "*",
				URLPath:       "*",
				FixedRate:     0.05,
				Priority:      100,
				ReservoirSize: 1,
			}
			created, err := b.CreateSamplingRule(rule)
			require.NoError(t, err)

			assert.Contains(t, created.RuleARN, tc.region, "rule ARN should contain the configured region")
			assert.Contains(t, created.RuleARN, tc.accountID, "rule ARN should contain the configured account ID")
			assert.True(
				t,
				strings.HasPrefix(created.RuleARN, "arn:aws:xray:"),
				"rule ARN should start with arn:aws:xray:",
			)
		})
	}
}

// TestCountHelpers verifies all export_test count helpers.
func TestCountHelpers(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, b.GroupCount())
	// A fresh backend always has the Default sampling rule pre-seeded.
	assert.Equal(t, 1, b.SamplingRuleCount())
	assert.Equal(t, 0, b.TraceCount())
	assert.Equal(t, 0, b.InsightCount())
	assert.Equal(t, 0, b.ResourcePolicyCount())

	_, err := b.CreateGroup("g1", "")
	require.NoError(t, err)

	assert.Equal(t, 1, b.GroupCount())

	b.AddInsightInternal(xray.Insight{InsightID: "ins-1", State: "ACTIVE", StartTime: time.Now()})
	assert.Equal(t, 1, b.InsightCount())

	b.AddResourcePolicyInternal(xray.ResourcePolicy{PolicyName: "pol-1", PolicyDocument: `{}`})
	assert.Equal(t, 1, b.ResourcePolicyCount())
}

// TestResetClearsAllState verifies Reset clears every field.
func TestResetClearsAllState(t *testing.T) {
	t.Parallel()

	b := xray.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateGroup("g1", "")
	require.NoError(t, err)

	b.AddInsightInternal(xray.Insight{InsightID: "i1", State: "ACTIVE", StartTime: time.Now()})
	b.AddResourcePolicyInternal(xray.ResourcePolicy{PolicyName: "p1", PolicyDocument: `{}`})

	b.Reset()

	assert.Equal(t, 0, b.GroupCount())
	assert.Equal(t, 0, b.InsightCount())
	assert.Equal(t, 0, b.ResourcePolicyCount())
	assert.Equal(t, 0, b.TraceCount())
}
