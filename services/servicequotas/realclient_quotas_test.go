package servicequotas_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sqsdk "github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuotas_ListAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, f *fixture)
		name string
	}{
		{
			name: "list_services_paginates",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				var seen []string

				var token *string

				for {
					out, err := f.client.ListServices(t.Context(), &sqsdk.ListServicesInput{
						MaxResults: aws.Int32(4), NextToken: token,
					})
					require.NoError(t, err)

					for _, s := range out.Services {
						seen = append(seen, aws.ToString(s.ServiceCode))
					}

					if token = out.NextToken; token == nil {
						break
					}
				}

				assert.Contains(t, seen, "ec2")
				assert.Contains(t, seen, "lambda")
				assert.Contains(t, seen, "iam")
				assert.IsIncreasing(t, seen)
			},
		},
		{
			name: "list_service_quotas",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.ListServiceQuotas(t.Context(), &sqsdk.ListServiceQuotasInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"),
				})
				require.NoError(t, err)
				require.Len(t, out.Quotas, 1)

				q := out.Quotas[0]
				assert.InDelta(t, 5, aws.ToFloat64(q.Value), 0)
				assert.True(t, q.Adjustable)
				assert.Equal(t, "arn:aws:servicequotas:us-east-1:000000000000:ec2/L-1216C47A", aws.ToString(q.QuotaArn))
				assert.Equal(t, "ACCOUNT", string(q.QuotaAppliedAtLevel))
				require.NotNil(t, q.UsageMetric)
				assert.Equal(t, "AWS/Usage", aws.ToString(q.UsageMetric.MetricNamespace))
				assert.Equal(t, "vCPU", q.UsageMetric.MetricDimensions["Resource"])
			},
		},
		{
			name: "default_quotas",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.ListAWSDefaultServiceQuotas(t.Context(), &sqsdk.ListAWSDefaultServiceQuotasInput{
					ServiceCode: aws.String("lambda"),
				})
				require.NoError(t, err)
				assert.GreaterOrEqual(t, len(out.Quotas), 2)

				got, err := f.client.GetAWSDefaultServiceQuota(t.Context(), &sqsdk.GetAWSDefaultServiceQuotaInput{
					ServiceCode: aws.String("lambda"), QuotaCode: aws.String("L-B99A9384"),
				})
				require.NoError(t, err)
				assert.InDelta(t, 1000, aws.ToFloat64(got.Quota.Value), 0)
				assert.Equal(t, "arn:aws:servicequotas:us-east-1::lambda/L-B99A9384", aws.ToString(got.Quota.QuotaArn))
			},
		},
		{
			name: "get_service_quota",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
					ServiceCode: aws.String("s3"), QuotaCode: aws.String("L-DC2B2D3D"),
				})
				require.NoError(t, err)
				assert.Equal(t, "Buckets", aws.ToString(out.Quota.QuotaName))
				assert.InDelta(t, 100, aws.ToFloat64(out.Quota.Value), 0)
			},
		},
		{
			name: "iam_is_global",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
					ServiceCode: aws.String("iam"), QuotaCode: aws.String("L-F4A5425F"),
				})
				require.NoError(t, err)
				assert.True(t, out.Quota.GlobalQuota)
			},
		},
		{
			name: "unknown_service",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.ListServiceQuotas(
					t.Context(),
					&sqsdk.ListServiceQuotasInput{ServiceCode: aws.String("nope")},
				)
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "unknown_quota",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-00000000"),
				})
				assert.Equal(t, "NoSuchResourceException", apiCode(err))
			},
		},
		{
			name: "bad_token",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.ListServices(t.Context(), &sqsdk.ListServicesInput{NextToken: aws.String("%%%")})
				assert.Equal(t, "InvalidPaginationTokenException", apiCode(err))
			},
		},
		{
			name: "max_results_range",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.ListServices(t.Context(), &sqsdk.ListServicesInput{MaxResults: aws.Int32(101)})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "context_id_unsupported",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				_, err := f.client.GetServiceQuota(t.Context(), &sqsdk.GetServiceQuotaInput{
					ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-1216C47A"), ContextId: aws.String("ctx"),
				})
				assert.Equal(t, "IllegalArgumentException", apiCode(err))
			},
		},
		{
			name: "resource_level_filter_is_empty",
			run: func(t *testing.T, f *fixture) {
				t.Helper()

				out, err := f.client.ListServiceQuotas(t.Context(), &sqsdk.ListServiceQuotasInput{
					ServiceCode: aws.String("ec2"), QuotaAppliedAtLevel: "RESOURCE",
				})
				require.NoError(t, err)
				assert.Empty(t, out.Quotas)
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
