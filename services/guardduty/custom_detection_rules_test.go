package guardduty_test

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

const (
	ruleAlpha = "iam-root-login"
	ruleBeta  = "s3-public-policy"
)

func seedCustomRules(t *testing.T, h *guardduty.Handler) {
	t.Helper()

	for _, r := range []guardduty.CustomDetectionRule{
		{
			RuleID: ruleAlpha, Name: "Root login", Description: "Root user signs in",
			DataSource: "CloudTrailManagementEvent",
			Service:    "iam", Severity: "HIGH", Tactic: "InitialAccess", Technique: "T1078",
			Language: "SQL", Schema: "CloudTrail", Expression: "eventName = 'ConsoleLogin'",
		},
		{
			RuleID: ruleBeta, Name: "Public bucket policy", Description: "Bucket policy opened",
			DataSource: "CloudTrailManagementEvent",
			Service:    "s3", Severity: "MEDIUM", Tactic: "Exfiltration", Technique: "T1530",
			Language: "SQL", Schema: "CloudTrail", Expression: "eventName = 'PutBucketPolicy'",
		},
	} {
		require.NoError(t, h.Backend.RegisterCustomDetectionRule(r))
	}
}

func apiErrorCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func TestCustomDetectionRules_Catalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   guarddutysdk.ListCustomDetectionRulesInput
		wantIDs []string
	}{
		{name: "all", input: guarddutysdk.ListCustomDetectionRulesInput{}, wantIDs: []string{ruleAlpha, ruleBeta}},
		{
			name: "equals",
			input: guarddutysdk.ListCustomDetectionRulesInput{Filters: []types.DetectionRuleFilter{
				{
					Name:      types.FilterFieldNameSeverity,
					Values:    []string{"HIGH"},
					Condition: types.DetectionRuleFilterConditionEquals,
				},
			}},
			wantIDs: []string{ruleAlpha},
		},
		{
			name: "contains",
			input: guarddutysdk.ListCustomDetectionRulesInput{Filters: []types.DetectionRuleFilter{
				{
					Name:      types.FilterFieldNameName,
					Values:    []string{"bucket"},
					Condition: types.DetectionRuleFilterConditionContains,
				},
				{
					Name:      types.FilterFieldNameName,
					Values:    []string{"Public"},
					Condition: types.DetectionRuleFilterConditionContains,
				},
			}},
			wantIDs: []string{ruleBeta},
		},
		{
			name: "values or",
			input: guarddutysdk.ListCustomDetectionRulesInput{Filters: []types.DetectionRuleFilter{
				{Name: types.FilterFieldNameService, Values: []string{"iam", "s3"}},
			}},
			wantIDs: []string{ruleAlpha, ruleBeta},
		},
		{
			name: "no match",
			input: guarddutysdk.ListCustomDetectionRulesInput{Filters: []types.DetectionRuleFilter{
				{Name: types.FilterFieldNameTactic, Values: []string{"Nope"}},
			}},
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedCustomRules(t, h)
			client := newTestGuardDutyClient(t, h)

			out, err := client.ListCustomDetectionRules(t.Context(), &tt.input)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Rules))
			for _, r := range out.Rules {
				got = append(got, aws.ToString(r.RuleId))
			}

			assert.Equal(t, tt.wantIDs, got)
		})
	}
}

func TestCustomDetectionRules_PaginationAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedCustomRules(t, h)
	client := newTestGuardDutyClient(t, h)

	first, err := client.ListCustomDetectionRules(
		t.Context(),
		&guarddutysdk.ListCustomDetectionRulesInput{MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.Len(t, first.Rules, 1)
	require.NotNil(t, first.NextToken)
	assert.Equal(t, ruleAlpha, aws.ToString(first.Rules[0].RuleId))

	second, err := client.ListCustomDetectionRules(t.Context(), &guarddutysdk.ListCustomDetectionRulesInput{
		MaxResults: aws.Int32(1), NextToken: first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.Rules, 1)
	assert.Nil(t, second.NextToken)
	assert.Equal(t, ruleBeta, aws.ToString(second.Rules[0].RuleId))

	got, err := client.GetCustomDetectionRule(
		t.Context(),
		&guarddutysdk.GetCustomDetectionRuleInput{RuleId: aws.String(ruleAlpha)},
	)
	require.NoError(t, err)
	assert.Equal(t, "eventName = 'ConsoleLogin'", aws.ToString(got.Rule.Definition.Expression))
	assert.Equal(t, types.RuleLanguageSql, got.Rule.Language)
	assert.NotNil(t, got.Rule.CreatedAt)
	assert.Contains(t, aws.ToString(got.Rule.Arn), "custom-detection-rule/"+ruleAlpha)

	_, err = client.GetCustomDetectionRule(
		t.Context(),
		&guarddutysdk.GetCustomDetectionRuleInput{RuleId: aws.String("missing")},
	)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))

	_, err = client.ListCustomDetectionRules(t.Context(), &guarddutysdk.ListCustomDetectionRulesInput{
		Filters: []types.DetectionRuleFilter{{Name: "bogus", Values: []string{"x"}}},
	})
	require.Error(t, err)
	assert.Equal(t, "BadRequestException", apiErrorCode(t, err))
}

func TestCustomDetectionRuleAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedCustomRules(t, h)
	client := newTestGuardDutyClient(t, h)
	ctx := t.Context()

	created, err := client.CreateCustomDetectionRuleAssociation(
		ctx,
		&guarddutysdk.CreateCustomDetectionRuleAssociationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeDryRun, Tags: map[string]string{"team": "sec"},
		},
	)
	require.NoError(t, err)

	assoc := created.RuleAssociation
	assocID := aws.ToString(assoc.AssociationId)
	assert.Equal(t, "123456789012", aws.ToString(assoc.AccountId))
	require.NotNil(t, assoc.ExpiresAt)
	assert.WithinDuration(t, assoc.CreatedAt.Add(14*24*time.Hour), *assoc.ExpiresAt, time.Second)

	got, err := client.GetCustomDetectionRuleAssociation(ctx, &guarddutysdk.GetCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleAlpha), AssociationId: aws.String(assocID),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "sec"}, got.Tags)

	tags, err := client.ListTagsForResource(ctx, &guarddutysdk.ListTagsForResourceInput{ResourceArn: assoc.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "sec"}, tags.Tags)

	_, err = client.CreateCustomDetectionRuleAssociation(ctx, &guarddutysdk.CreateCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeLive,
	})
	require.Error(t, err)
	assert.Equal(t, "ConflictException", apiErrorCode(t, err))

	_, err = client.CreateCustomDetectionRuleAssociation(ctx, &guarddutysdk.CreateCustomDetectionRuleAssociationInput{
		RuleId: aws.String("unknown-rule"), Mode: types.AssociationModeLive,
	})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))

	_, err = client.UpdateCustomDetectionRuleAssociation(ctx, &guarddutysdk.UpdateCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleAlpha), AssociationId: aws.String(assocID), Mode: types.AssociationModeLive,
	})
	require.NoError(t, err)

	listed, err := client.ListCustomDetectionRuleAssociations(
		ctx,
		&guarddutysdk.ListCustomDetectionRuleAssociationsInput{
			Mode: types.AssociationModeLive,
		},
	)
	require.NoError(t, err)
	require.Len(t, listed.RuleAssociations, 1)
	assert.Equal(t, types.AssociationModeLive, listed.RuleAssociations[0].Mode)
	assert.Nil(t, listed.RuleAssociations[0].ExpiresAt)

	dry, err := client.ListCustomDetectionRuleAssociations(ctx, &guarddutysdk.ListCustomDetectionRuleAssociationsInput{
		Mode: types.AssociationModeDryRun,
	})
	require.NoError(t, err)
	assert.Empty(t, dry.RuleAssociations)

	del := &guarddutysdk.DeleteCustomDetectionRuleAssociationInput{
		RuleId:        aws.String(ruleAlpha),
		AssociationId: aws.String(assocID),
	}
	_, err = client.DeleteCustomDetectionRuleAssociation(ctx, del)
	require.NoError(t, err)

	_, err = client.DeleteCustomDetectionRuleAssociation(ctx, del)
	require.NoError(t, err, "delete is idempotent")

	_, err = client.GetCustomDetectionRuleAssociation(ctx, &guarddutysdk.GetCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleAlpha), AssociationId: aws.String(assocID),
	})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))

	_, err = client.DeleteCustomDetectionRuleAssociation(ctx, &guarddutysdk.DeleteCustomDetectionRuleAssociationInput{
		RuleId: aws.String("unknown-rule"), AssociationId: aws.String(assocID),
	})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))
}

func TestCustomDetectionRuleAssociation_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedCustomRules(t, h)
	client := newTestGuardDutyClient(t, h)

	in := &guarddutysdk.CreateCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleBeta), Mode: types.AssociationModeLive, ClientToken: aws.String("tok-1"),
	}

	a, err := client.CreateCustomDetectionRuleAssociation(t.Context(), in)
	require.NoError(t, err)

	b, err := client.CreateCustomDetectionRuleAssociation(t.Context(), in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(a.RuleAssociation.AssociationId), aws.ToString(b.RuleAssociation.AssociationId))
}

func TestCustomDetectionRuleOrgConfiguration_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedCustomRules(t, h)
	client := newTestGuardDutyClient(t, h)
	ctx := t.Context()

	_, err := client.CreateCustomDetectionRuleOrgConfiguration(
		ctx,
		&guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeDryRun,
			IncludeAccountIds: []string{"111111111111", "222222222222"},
		},
	)
	require.NoError(t, err)

	got, err := client.GetCustomDetectionRuleOrgConfiguration(
		ctx,
		&guarddutysdk.GetCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeDryRun,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"111111111111", "222222222222"}, got.Configuration.IncludeAccountIds)
	assert.Empty(t, got.Configuration.ExcludeAccountIds)
	assert.Equal(t, types.DetectionRuleConfigurationStatusActive, got.Configuration.Status)
	require.NotNil(t, got.Configuration.ExpiresAt)

	_, err = client.GetCustomDetectionRuleOrgConfiguration(
		ctx,
		&guarddutysdk.GetCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeLive,
		},
	)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))

	_, err = client.UpdateCustomDetectionRuleOrgConfiguration(
		ctx,
		&guarddutysdk.UpdateCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(
				ruleAlpha,
			),
			Mode:              types.AssociationModeDryRun,
			ExcludeAccountIds: []string{"333333333333"},
		},
	)
	require.NoError(t, err)

	got, err = client.GetCustomDetectionRuleOrgConfiguration(
		ctx,
		&guarddutysdk.GetCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeDryRun,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, got.Configuration.IncludeAccountIds)
	assert.Equal(t, []string{"333333333333"}, got.Configuration.ExcludeAccountIds)

	active, err := client.ListCustomDetectionRuleOrgConfigurations(
		ctx,
		&guarddutysdk.ListCustomDetectionRuleOrgConfigurationsInput{
			Status: types.DetectionRuleConfigurationStatusActive,
		},
	)
	require.NoError(t, err)
	require.Len(t, active.Configurations, 1)
	assert.Equal(t, ruleAlpha, aws.ToString(active.Configurations[0].RuleId))

	failed, err := client.ListCustomDetectionRuleOrgConfigurations(
		ctx,
		&guarddutysdk.ListCustomDetectionRuleOrgConfigurationsInput{
			Status: types.DetectionRuleConfigurationStatusFailed,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, failed.Configurations)

	del := &guarddutysdk.DeleteCustomDetectionRuleOrgConfigurationInput{
		RuleId: aws.String(ruleAlpha),
		Mode:   types.AssociationModeDryRun,
	}
	_, err = client.DeleteCustomDetectionRuleOrgConfiguration(ctx, del)
	require.NoError(t, err)

	_, err = client.DeleteCustomDetectionRuleOrgConfiguration(ctx, del)
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiErrorCode(t, err))
}

func TestCustomDetectionRuleOrgConfiguration_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		existing *guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput
		name     string
		wantCode string
		input    guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput
	}{
		{
			name: "include and exclude",
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				IncludeAccountIds: []string{"111111111111"},
				ExcludeAccountIds: []string{"222222222222"},
			},
			wantCode: "BadRequestException",
		},
		{
			name:     "short account id",
			input:    guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{IncludeAccountIds: []string{"123"}},
			wantCode: "BadRequestException",
		},
		{
			name:     "unknown rule",
			input:    guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{RuleId: aws.String("nope")},
			wantCode: "ResourceNotFoundException",
		},
		{
			name: "same mode twice",
			existing: &guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"111111111111"},
			},
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"222222222222"},
			},
			wantCode: "ConflictException",
		},
		{
			name:     "whole org blocks other mode",
			existing: &guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{Mode: types.AssociationModeDryRun},
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"222222222222"},
			},
			wantCode: "ConflictException",
		},
		{
			name: "exclude claims whole org",
			existing: &guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"111111111111"},
			},
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeDryRun,
				ExcludeAccountIds: []string{"999999999999"},
			},
			wantCode: "ConflictException",
		},
		{
			name: "shared account across modes",
			existing: &guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"111111111111", "222222222222"},
			},
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeDryRun,
				IncludeAccountIds: []string{"222222222222"},
			},
			wantCode: "ConflictException",
		},
		{
			name: "disjoint includes coexist",
			existing: &guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeLive,
				IncludeAccountIds: []string{"111111111111"},
			},
			input: guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
				Mode:              types.AssociationModeDryRun,
				IncludeAccountIds: []string{"222222222222"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			seedCustomRules(t, h)
			client := newTestGuardDutyClient(t, h)

			if tt.existing != nil {
				tt.existing.RuleId = aws.String(ruleAlpha)
				_, err := client.CreateCustomDetectionRuleOrgConfiguration(t.Context(), tt.existing)
				require.NoError(t, err)
			}

			in := tt.input
			if in.RuleId == nil {
				in.RuleId = aws.String(ruleAlpha)
			}

			if in.Mode == "" {
				in.Mode = types.AssociationModeLive
			}

			_, err := client.CreateCustomDetectionRuleOrgConfiguration(t.Context(), &in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.Equal(t, tt.wantCode, apiErrorCode(t, err))
		})
	}
}

func TestCustomDetectionRules_SnapshotRestore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	seedCustomRules(t, h)
	client := newTestGuardDutyClient(t, h)

	created, err := client.CreateCustomDetectionRuleAssociation(
		t.Context(),
		&guarddutysdk.CreateCustomDetectionRuleAssociationInput{
			RuleId: aws.String(ruleAlpha), Mode: types.AssociationModeLive,
		},
	)
	require.NoError(t, err)

	_, err = client.CreateCustomDetectionRuleOrgConfiguration(
		t.Context(),
		&guarddutysdk.CreateCustomDetectionRuleOrgConfigurationInput{
			RuleId: aws.String(ruleBeta), Mode: types.AssociationModeLive,
		},
	)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	restored := newTestHandler(t)
	require.NoError(t, restored.Restore(t.Context(), snap))

	rc := newTestGuardDutyClient(t, restored)

	got, err := rc.GetCustomDetectionRuleAssociation(t.Context(), &guarddutysdk.GetCustomDetectionRuleAssociationInput{
		RuleId: aws.String(ruleAlpha), AssociationId: created.RuleAssociation.AssociationId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.AssociationModeLive, got.RuleAssociation.Mode)

	cfgs, err := rc.ListCustomDetectionRuleOrgConfigurations(
		t.Context(),
		&guarddutysdk.ListCustomDetectionRuleOrgConfigurationsInput{},
	)
	require.NoError(t, err)
	assert.Len(t, cfgs.Configurations, 1)

	rules, err := rc.ListCustomDetectionRules(t.Context(), &guarddutysdk.ListCustomDetectionRulesInput{})
	require.NoError(t, err)
	assert.Len(t, rules.Rules, 2)
}

func TestCustomDetectionRules_DryRunExpiry(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newTestBackend()
		require.NoError(t, b.RegisterCustomDetectionRule(guardduty.CustomDetectionRule{RuleID: ruleAlpha, Name: "r"}))

		assoc, err := b.CreateCustomDetectionRuleAssociation(ruleAlpha, "DRY_RUN", nil)
		require.NoError(t, err)
		require.NoError(t, b.CreateCustomDetectionRuleOrgConfiguration(ruleAlpha, "DRY_RUN", nil, nil))

		time.Sleep(13 * 24 * time.Hour)

		_, _, err = b.GetCustomDetectionRuleAssociation(ruleAlpha, assoc.AssociationID)
		require.NoError(t, err)

		time.Sleep(25 * time.Hour)

		_, _, err = b.GetCustomDetectionRuleAssociation(ruleAlpha, assoc.AssociationID)
		require.ErrorIs(t, err, guardduty.ErrCustomDetectionRuleNotFound)

		_, err = b.GetCustomDetectionRuleOrgConfiguration(ruleAlpha, "DRY_RUN")
		require.ErrorIs(t, err, guardduty.ErrCustomDetectionRuleNotFound)

		_, err = b.CreateCustomDetectionRuleAssociation(ruleAlpha, "LIVE", nil)
		require.NoError(t, err, "an expired association no longer conflicts")

		live, err := b.CreateCustomDetectionRuleAssociation(ruleAlpha, "LIVE", nil)
		require.ErrorIs(t, err, guardduty.ErrCustomDetectionRuleConflict)
		assert.Nil(t, live)

		time.Sleep(365 * 24 * time.Hour)

		list, _, err := b.ListCustomDetectionRuleAssociations("", "LIVE", 0, "")
		require.NoError(t, err)
		assert.Len(t, list, 1, "live associations never expire")
	})
}
