package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestRealClient_EnableSecurityHubControlFindingGenerator proves EnableSecurityHub's
// ControlFindingGenerator (gopherstack-xhu2t; dropped:
// undeclared, hardcoded to SECURITY_CONTROL) is honored and round-trips
// through DescribeHub.
func TestRealClient_EnableSecurityHubControlFindingGenerator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "honors_standard_control",
			run: func(t *testing.T) {
				t.Helper()

				backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
				client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
				ctx := t.Context()

				_, err := client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
					ControlFindingGenerator: types.ControlFindingGeneratorStandardControl,
				})
				require.NoError(t, err)

				desc, err := client.DescribeHub(ctx, &securityhubsdk.DescribeHubInput{})
				require.NoError(t, err)
				assert.Equal(t, types.ControlFindingGeneratorStandardControl, desc.ControlFindingGenerator)
			},
		},
		{
			name: "defaults_to_security_control",
			run: func(t *testing.T) {
				t.Helper()

				backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
				client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
				ctx := t.Context()

				_, err := client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{})
				require.NoError(t, err)

				desc, err := client.DescribeHub(ctx, &securityhubsdk.DescribeHubInput{})
				require.NoError(t, err)
				assert.Equal(t, types.ControlFindingGeneratorSecurityControl, desc.ControlFindingGenerator)
			},
		},
		{
			name: "rejects_invalid_value",
			run: func(t *testing.T) {
				t.Helper()

				backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
				client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
				ctx := t.Context()

				_, err := client.EnableSecurityHub(ctx, &securityhubsdk.EnableSecurityHubInput{
					ControlFindingGenerator: "BOGUS",
				})
				require.Error(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestRealClient_OrganizationAdminAccountFeature proves Enable/DisableOrganizationAdminAccount's
// Feature (gopherstack-xhu2t; dropped: undeclared, this
// backend previously tracked one flat admin-account set with no feature
// scoping) is honored: an account enabled for one feature does not appear
// when listing admin accounts under the other feature, and disabling under
// the wrong feature is a no-op.
func TestRealClient_OrganizationAdminAccountFeature(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
	ctx := t.Context()

	_, err := client.EnableOrganizationAdminAccount(ctx, &securityhubsdk.EnableOrganizationAdminAccountInput{
		AdminAccountId: aws.String("111111111111"),
		Feature:        types.SecurityHubFeatureSecurityHub,
	})
	require.NoError(t, err)

	_, err = client.EnableOrganizationAdminAccount(ctx, &securityhubsdk.EnableOrganizationAdminAccountInput{
		AdminAccountId: aws.String("222222222222"),
		Feature:        types.SecurityHubFeatureSecurityHubV2,
	})
	require.NoError(t, err)

	v1List, err := client.ListOrganizationAdminAccounts(ctx, &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: types.SecurityHubFeatureSecurityHub,
	})
	require.NoError(t, err)
	require.Len(t, v1List.AdminAccounts, 1)
	assert.Equal(t, "111111111111", aws.ToString(v1List.AdminAccounts[0].AccountId))

	v2List, err := client.ListOrganizationAdminAccounts(ctx, &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: types.SecurityHubFeatureSecurityHubV2,
	})
	require.NoError(t, err)
	require.Len(t, v2List.AdminAccounts, 1)
	assert.Equal(t, "222222222222", aws.ToString(v2List.AdminAccounts[0].AccountId))

	// Disabling under the wrong feature is a no-op: the account stays a
	// SecurityHub admin.
	_, err = client.DisableOrganizationAdminAccount(ctx, &securityhubsdk.DisableOrganizationAdminAccountInput{
		AdminAccountId: aws.String("111111111111"),
		Feature:        types.SecurityHubFeatureSecurityHubV2,
	})
	require.NoError(t, err)

	stillThere, err := client.ListOrganizationAdminAccounts(ctx, &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: types.SecurityHubFeatureSecurityHub,
	})
	require.NoError(t, err)
	require.Len(t, stillThere.AdminAccounts, 1)

	_, err = client.DisableOrganizationAdminAccount(ctx, &securityhubsdk.DisableOrganizationAdminAccountInput{
		AdminAccountId: aws.String("111111111111"),
		Feature:        types.SecurityHubFeatureSecurityHub,
	})
	require.NoError(t, err)

	gone, err := client.ListOrganizationAdminAccounts(ctx, &securityhubsdk.ListOrganizationAdminAccountsInput{
		Feature: types.SecurityHubFeatureSecurityHub,
	})
	require.NoError(t, err)
	assert.Empty(t, gone.AdminAccounts)
}

// TestRealClient_GetFindingStatisticsV2SortOrder proves GetFindingStatisticsV2's
// SortOrder (gopherstack-xhu2t; dropped: undeclared,
// GroupByValues were returned in first-seen order regardless of count) is
// honored -- descending (the default) and ascending both order by count.
func TestRealClient_GetFindingStatisticsV2SortOrder(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
	ctx := t.Context()

	// Three findings with SeverityLabel HIGH, two with LOW -- HIGH has the
	// larger count.
	_, _, _ = backend.ImportFindings([]map[string]any{
		securityhub.ValidFinding(map[string]any{
			"Id": "f1", "Severity": map[string]any{"Label": "LOW"},
		}),
		securityhub.ValidFinding(map[string]any{
			"Id": "f2", "Severity": map[string]any{"Label": "LOW"},
		}),
		securityhub.ValidFinding(map[string]any{
			"Id": "f3", "Severity": map[string]any{"Label": "HIGH"},
		}),
		securityhub.ValidFinding(map[string]any{
			"Id": "f4", "Severity": map[string]any{"Label": "HIGH"},
		}),
		securityhub.ValidFinding(map[string]any{
			"Id": "f5", "Severity": map[string]any{"Label": "HIGH"},
		}),
	})

	desc, err := client.GetFindingStatisticsV2(ctx, &securityhubsdk.GetFindingStatisticsV2Input{
		GroupByRules: []types.GroupByRule{{GroupByField: "severity"}},
		SortOrder:    types.SortOrderDescending,
	})
	require.NoError(t, err)
	require.Len(t, desc.GroupByResults, 1)
	require.NotEmpty(t, desc.GroupByResults[0].GroupByValues)
	assert.Equal(t, int32(3), aws.ToInt32(desc.GroupByResults[0].GroupByValues[0].Count))

	asc, err := client.GetFindingStatisticsV2(ctx, &securityhubsdk.GetFindingStatisticsV2Input{
		GroupByRules: []types.GroupByRule{{GroupByField: "severity"}},
		SortOrder:    types.SortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, asc.GroupByResults, 1)
	require.NotEmpty(t, asc.GroupByResults[0].GroupByValues)
	assert.Equal(t, int32(2), aws.ToInt32(asc.GroupByResults[0].GroupByValues[0].Count))
}

// TestRealClient_BatchUpdateFindingsVerificationState confirms
// BatchUpdateFindings.VerificationState (reqfielddiff tier-1, flagged
// undeclared) is a FALSE POSITIVE: handleBatchUpdateFindings
// (handler_findings.go) collects every body field except
// FindingIdentifiers into a generic "updates" map applied wholesale via
// maps.Copy onto the stored ASFF finding (findings.go:510) -- the tool's
// declaration search can't see a field read via a generic loop over the
// whole body, but the field is genuinely honored.
func TestRealClient_BatchUpdateFindingsVerificationState(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))
	ctx := t.Context()

	finding := securityhub.ValidFinding(map[string]any{"Id": "vs-finding"})
	_, _, _ = backend.ImportFindings([]map[string]any{finding})

	_, err := client.BatchUpdateFindings(ctx, &securityhubsdk.BatchUpdateFindingsInput{
		FindingIdentifiers: []types.AwsSecurityFindingIdentifier{
			{Id: aws.String("vs-finding"), ProductArn: aws.String(finding["ProductArn"].(string))},
		},
		VerificationState: types.VerificationStateTruePositive,
	})
	require.NoError(t, err)

	out, err := client.GetFindings(ctx, &securityhubsdk.GetFindingsInput{
		Filters: &types.AwsSecurityFindingFilters{
			Id: []types.StringFilter{{Value: aws.String("vs-finding"), Comparison: types.StringFilterComparisonEquals}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Findings, 1)
	assert.Equal(t, types.VerificationStateTruePositive, out.Findings[0].VerificationState)
}
