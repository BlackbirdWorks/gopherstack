package servicequotas_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	sqtypes "github.com/aws/aws-sdk-go-v2/service/servicequotas/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/servicequotas"
)

func requestIncrease(
	t *testing.T,
	f *fixture,
	desired float64,
	caseAllowed *bool,
) *sqtypes.RequestedServiceQuotaChange {
	t.Helper()

	out, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
		ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"),
		DesiredValue: aws.Float64(desired), SupportCaseAllowed: caseAllowed,
	})
	require.NoError(t, err)

	return out.RequestedQuota
}

func getRequest(t *testing.T, f *fixture, id *string) *sqtypes.RequestedServiceQuotaChange {
	t.Helper()

	out, err := f.client.GetRequestedServiceQuotaChange(
		t.Context(),
		&sqsdk.GetRequestedServiceQuotaChangeInput{RequestId: id},
	)
	require.NoError(t, err)

	return out.RequestedQuota
}

func appliedValue(t *testing.T, f *fixture) float64 {
	t.Helper()

	out, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
		ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"),
	})
	require.NoError(t, err)

	return aws.ToFloat64(out.Quota.Value)
}

func TestRequests_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "pending_to_case_opened_to_approved",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				req := requestIncrease(t, f, 20, nil)
				assert.Equal(t, sqtypes.RequestStatusPending, req.Status)
				assert.Nil(t, req.CaseId)
				assert.Len(t, aws.ToString(req.Id), 32)
				assert.InDelta(t, 20, aws.ToFloat64(req.DesiredValue), 0)
				assert.Equal(
					t,
					"arn:aws:servicequotas:us-east-1:000000000000:ec2/L-1216C47A",
					aws.ToString(req.QuotaArn),
				)
				assert.InDelta(t, 5, appliedValue(t, f), 0)

				f.clock.advance(600 * time.Millisecond)

				got := getRequest(t, f, req.Id)
				assert.Equal(t, sqtypes.RequestStatusCaseOpened, got.Status)
				assert.NotEmpty(t, aws.ToString(got.CaseId))
				assert.InDelta(t, 5, appliedValue(t, f), 0)

				f.clock.advance(600 * time.Millisecond)

				got = getRequest(t, f, req.Id)
				assert.Equal(t, sqtypes.RequestStatusApproved, got.Status)
				assert.True(t, got.LastUpdated.After(*got.Created))
				assert.InDelta(t, 20, appliedValue(t, f), 0)
			},
		},
		{
			name: "single_jump_settles_all_stages",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				req := requestIncrease(t, f, 30, nil)
				f.clock.advance(time.Hour)

				assert.Equal(t, sqtypes.RequestStatusApproved, getRequest(t, f, req.Id).Status)
				assert.InDelta(t, 30, appliedValue(t, f), 0)
			},
		},
		{
			name: "support_case_disallowed_waits_for_create_support_case",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				req := requestIncrease(t, f, 20, aws.Bool(false))
				f.clock.advance(time.Hour)
				assert.Equal(t, sqtypes.RequestStatusPending, getRequest(t, f, req.Id).Status)

				_, err := f.client.CreateSupportCase(t.Context(), &sqsdk.CreateSupportCaseInput{RequestId: req.Id})
				require.NoError(t, err)
				assert.Equal(t, sqtypes.RequestStatusCaseOpened, getRequest(t, f, req.Id).Status)

				f.clock.advance(time.Hour)
				assert.Equal(t, sqtypes.RequestStatusApproved, getRequest(t, f, req.Id).Status)
			},
		},
		{
			name: "create_support_case_requires_pending",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				req := requestIncrease(t, f, 20, nil)
				f.clock.advance(time.Hour)

				_, err := f.client.CreateSupportCase(t.Context(), &sqsdk.CreateSupportCaseInput{RequestId: req.Id})
				assert.Equal(t, "InvalidResourceStateException", apiCode(err))

				_, err = f.client.CreateSupportCase(
					t.Context(),
					&sqsdk.CreateSupportCaseInput{RequestId: aws.String("nope")},
				)
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "duplicate_open_request",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				requestIncrease(t, f, 20, nil)

				_, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"), DesiredValue: aws.Float64(25),
				})
				assert.Equal(t, "ResourceAlreadyExistsException", apiCode(err))

				f.clock.advance(time.Hour)

				again := requestIncrease(t, f, 40, nil)
				assert.Equal(t, sqtypes.RequestStatusPending, again.Status)
			},
		},
		{
			name: "desired_not_above_applied",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"), DesiredValue: aws.Float64(5),
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "unknown_quota",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-00000000"), DesiredValue: aws.Float64(9),
				})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "history_filters",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				first := requestIncrease(t, f, 20, nil)
				f.clock.advance(time.Hour)

				_, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
					ServiceCode: aws.String(
						"lambda",
					),
					QuotaCode:    aws.String("L-B99A9384"),
					DesiredValue: aws.Float64(2000),
				})
				require.NoError(t, err)

				all, err := f.client.ListRequestedServiceQuotaChangeHistory(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryInput{})
				require.NoError(t, err)
				require.Len(t, all.RequestedQuotas, 2)
				assert.Equal(t, aws.ToString(first.Id), aws.ToString(all.RequestedQuotas[0].Id))

				pending, err := f.client.ListRequestedServiceQuotaChangeHistory(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryInput{Status: sqtypes.RequestStatusPending})
				require.NoError(t, err)
				assert.Len(t, pending.RequestedQuotas, 1)

				byService, err := f.client.ListRequestedServiceQuotaChangeHistory(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryInput{ServiceCode: aws.String("ec2")})
				require.NoError(t, err)
				assert.Len(t, byService.RequestedQuotas, 1)

				byQuota, err := f.client.ListRequestedServiceQuotaChangeHistoryByQuota(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryByQuotaInput{
						ServiceCode: aws.String("lambda"), QuotaCode: aws.String("L-B99A9384"),
					})
				require.NoError(t, err)
				require.Len(t, byQuota.RequestedQuotas, 1)
				assert.Equal(t, "lambda", aws.ToString(byQuota.RequestedQuotas[0].ServiceCode))

				page1, err := f.client.ListRequestedServiceQuotaChangeHistory(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryInput{MaxResults: aws.Int32(1)})
				require.NoError(t, err)
				assert.Len(t, page1.RequestedQuotas, 1)
				assert.NotNil(t, page1.NextToken)

				_, err = f.client.ListRequestedServiceQuotaChangeHistory(t.Context(),
					&sqsdk.ListRequestedServiceQuotaChangeHistoryInput{Status: "BOGUS"})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "get_unknown_request",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.GetRequestedServiceQuotaChange(t.Context(),
					&sqsdk.GetRequestedServiceQuotaChangeInput{RequestId: aws.String("nope")})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newFixture(t))
		})
	}
}

func TestRequests_NonAdjustableQuota(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.backend.SetCatalog(servicequotas.NewCatalog([]servicequotas.ServiceDef{{
		Code: "ec2", Name: "EC2",
		Quotas: []servicequotas.QuotaDef{{Code: "L-FIXED", Name: "Fixed", Unit: "None", Default: 1, Adjustable: false}},
	}}))

	_, err := f.client.RequestServiceQuotaIncrease(t.Context(), &sqsdk.RequestServiceQuotaIncreaseInput{
		ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-FIXED"), DesiredValue: aws.Float64(2),
	})
	assert.Equal(t, "IllegalArgumentException", apiCode(err))
}
