package securityhub_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func enabledHubClient(t *testing.T) *securityhubsdk.Client {
	t.Helper()

	_, c := newRealClientBackendAndClient(t)

	_, err := c.EnableSecurityHub(t.Context(), &securityhubsdk.EnableSecurityHubInput{
		EnableDefaultStandards: aws.Bool(false),
	})
	require.NoError(t, err)

	return c
}

func apiCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

func TestSDK_StandardsSubscriptionARNsAndLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		standardsArn string
		wantSub      string
		wantCode     string
	}{
		{
			name:         "standards_form",
			standardsArn: "arn:aws:securityhub:us-east-1::standards/aws-foundational-security-best-practices/v/1.0.0",
			wantSub: "arn:aws:securityhub:us-east-1:000000000000:subscription/" +
				"aws-foundational-security-best-practices/v/1.0.0",
		},
		{
			name:         "ruleset_form",
			standardsArn: "arn:aws:securityhub:::ruleset/cis-aws-foundations-benchmark/v/1.2.0",
			wantSub:      "arn:aws:securityhub:us-east-1:000000000000:subscription/cis-aws-foundations-benchmark/v/1.2.0",
		},
		{
			name:         "unknown_standard",
			standardsArn: "arn:aws:securityhub:us-east-1::standards/bogus/v/1.0.0",
			wantCode:     "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := enabledHubClient(t)

			out, err := c.BatchEnableStandards(t.Context(), &securityhubsdk.BatchEnableStandardsInput{
				StandardsSubscriptionRequests: []types.StandardsSubscriptionRequest{
					{StandardsArn: aws.String(tt.standardsArn)},
				},
			})
			if tt.wantCode != "" {
				require.Error(t, err)
				assert.Equal(t, tt.wantCode, apiCode(t, err))

				return
			}

			require.NoError(t, err)
			require.Len(t, out.StandardsSubscriptions, 1)

			sub := out.StandardsSubscriptions[0]
			assert.Equal(t, tt.wantSub, aws.ToString(sub.StandardsSubscriptionArn))
			assert.Equal(t, types.StandardsStatusPending, sub.StandardsStatus)

			ready, err := c.GetEnabledStandards(t.Context(), &securityhubsdk.GetEnabledStandardsInput{})
			require.NoError(t, err)
			require.Len(t, ready.StandardsSubscriptions, 1)
			assert.Equal(t, types.StandardsStatusReady, ready.StandardsSubscriptions[0].StandardsStatus)

			again, err := c.BatchEnableStandards(t.Context(), &securityhubsdk.BatchEnableStandardsInput{
				StandardsSubscriptionRequests: []types.StandardsSubscriptionRequest{
					{StandardsArn: aws.String(tt.standardsArn)},
				},
			})
			require.NoError(t, err)
			require.Len(t, again.StandardsSubscriptions, 1)
			assert.Equal(t, types.StandardsStatusReady, again.StandardsSubscriptions[0].StandardsStatus,
				"re-enabling must not reset a READY subscription to PENDING")
		})
	}
}

func TestSDK_DefaultStandardsUseRealSubscriptionARNs(t *testing.T) {
	t.Parallel()

	_, c := newRealClientBackendAndClient(t)

	_, err := c.EnableSecurityHub(t.Context(), &securityhubsdk.EnableSecurityHubInput{
		EnableDefaultStandards: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := c.GetEnabledStandards(t.Context(), &securityhubsdk.GetEnabledStandardsInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.StandardsSubscriptions)

	for _, s := range out.StandardsSubscriptions {
		arn := aws.ToString(s.StandardsSubscriptionArn)
		assert.NotContains(t, arn, "default-")
		assert.True(t, strings.HasPrefix(arn, "arn:aws:securityhub:us-east-1:000000000000:subscription/"), arn)
	}

	controls, err := c.DescribeStandardsControls(t.Context(), &securityhubsdk.DescribeStandardsControlsInput{
		StandardsSubscriptionArn: out.StandardsSubscriptions[0].StandardsSubscriptionArn,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, controls.Controls)

	_, err = c.DescribeStandardsControls(t.Context(), &securityhubsdk.DescribeStandardsControlsInput{
		StandardsSubscriptionArn: aws.String("arn:aws:securityhub:us-east-1:000000000000:subscription/nope/v/1.0.0"),
	})
	require.Error(t, err)
	assert.Equal(t, "ResourceNotFoundException", apiCode(t, err))
}

func TestSDK_GetFindingsPagingBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		token     *string
		name      string
		max       int32
		wantError bool
	}{
		{name: "defaults"},
		{name: "max_100", max: 100},
		{name: "max_101", max: 101, wantError: true},
		{name: "bad_token", token: aws.String("not-a-token"), wantError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := enabledHubClient(t)

			in := &securityhubsdk.GetFindingsInput{NextToken: tt.token}
			if tt.max != 0 {
				in.MaxResults = aws.Int32(tt.max)
			}

			_, err := c.GetFindings(t.Context(), in)
			if tt.wantError {
				require.Error(t, err)
				assert.Equal(t, "InvalidInputException", apiCode(t, err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_CreateActionTargetLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		id      string
		wantErr bool
	}{
		{name: "ok", target: "Send to chat", id: "SendToChat1"},
		{name: "id_with_dash", target: "x", id: "send-to-chat", wantErr: true},
		{name: "id_too_long", target: "x", id: strings.Repeat("a", 21), wantErr: true},
		{name: "name_too_long", target: strings.Repeat("n", 21), id: "ok", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := enabledHubClient(t)

			_, err := c.CreateActionTarget(t.Context(), &securityhubsdk.CreateActionTargetInput{
				Name: aws.String(tt.target), Description: aws.String("d"), Id: aws.String(tt.id),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Equal(t, "InvalidInputException", apiCode(t, err))

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSDK_BatchImportFindingsRejectsBadEnums(t *testing.T) {
	t.Parallel()

	finding := func(label types.SeverityLabel, state types.RecordState) types.AwsSecurityFinding {
		return types.AwsSecurityFinding{
			SchemaVersion: aws.String("2018-10-08"),
			Id:            aws.String(fmt.Sprintf("f-%s-%s", label, state)),
			ProductArn:    aws.String("arn:aws:securityhub:us-east-1:000000000000:product/000000000000/default"),
			GeneratorId:   aws.String("g"),
			AwsAccountId:  aws.String("000000000000"),
			Types:         []string{"Software and Configuration Checks"},
			CreatedAt:     aws.String("2026-01-01T00:00:00.000Z"),
			UpdatedAt:     aws.String("2026-01-01T00:00:00.000Z"),
			Severity:      &types.Severity{Label: label},
			RecordState:   state,
			Title:         aws.String("t"),
			Description:   aws.String("d"),
			Resources:     []types.Resource{{Type: aws.String("Other"), Id: aws.String("r")}},
		}
	}

	tests := []struct {
		name       string
		label      types.SeverityLabel
		state      types.RecordState
		wantFailed int32
	}{
		{name: "valid", label: types.SeverityLabelHigh, state: types.RecordStateActive},
		{name: "bad_label", label: types.SeverityLabel("SEVERE"), wantFailed: 1},
		{name: "bad_state", label: types.SeverityLabelLow, state: types.RecordState("GONE"), wantFailed: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := enabledHubClient(t)

			out, err := c.BatchImportFindings(t.Context(), &securityhubsdk.BatchImportFindingsInput{
				Findings: []types.AwsSecurityFinding{finding(tt.label, tt.state)},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantFailed, aws.ToInt32(out.FailedCount))
		})
	}
}
