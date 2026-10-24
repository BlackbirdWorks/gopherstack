package cloudwatch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const rmcInstanceArn = "arn:aws:ec2:us-east-1:000000000000:instance/i-0123456789abcdef0"

func TestRealClient_ResourceMetricsConfiguration(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, client *cwsdk.Client)
		name string
	}{
		{name: "lifecycle", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			ctx := t.Context()
			sel := []types.ResourceMetricSelection{{IncludeMetrics: []string{"CPUUtilization", "NetworkIn"}}}

			created, err := client.CreateResourceMetricsConfiguration(
				ctx,
				&cwsdk.CreateResourceMetricsConfigurationInput{
					ResourceArn: aws.String(rmcInstanceArn), MetricSelections: sel,
				},
			)
			require.NoError(t, err)
			cfg := created.ResourceMetricsConfiguration
			assert.Equal(t, rmcInstanceArn, aws.ToString(cfg.ResourceArn))
			assert.Equal(t, sel, cfg.MetricSelections)
			require.NotNil(t, cfg.CreatedAt)
			assert.Equal(t, cfg.CreatedAt, cfg.UpdatedAt)

			got, err := client.GetResourceMetricsConfiguration(ctx, &cwsdk.GetResourceMetricsConfigurationInput{
				ResourceArn: aws.String(rmcInstanceArn),
			})
			require.NoError(t, err)
			assert.Equal(t, sel, got.ResourceMetricsConfiguration.MetricSelections)

			newSel := []types.ResourceMetricSelection{{IncludeMetrics: []string{"DiskReadOps"}}}
			upd, err := client.UpdateResourceMetricsConfiguration(ctx, &cwsdk.UpdateResourceMetricsConfigurationInput{
				ResourceArn: aws.String(rmcInstanceArn), MetricSelections: newSel,
			})
			require.NoError(t, err)
			assert.Equal(t, newSel, upd.ResourceMetricsConfiguration.MetricSelections, "selections replace, not merge")
			assert.Equal(t, cfg.CreatedAt, upd.ResourceMetricsConfiguration.CreatedAt)

			cleared, err := client.UpdateResourceMetricsConfiguration(
				ctx,
				&cwsdk.UpdateResourceMetricsConfigurationInput{
					ResourceArn: aws.String(rmcInstanceArn),
				},
			)
			require.NoError(t, err)
			assert.Empty(t, cleared.ResourceMetricsConfiguration.MetricSelections)

			_, err = client.DeleteResourceMetricsConfiguration(ctx, &cwsdk.DeleteResourceMetricsConfigurationInput{
				ResourceArn: aws.String(rmcInstanceArn),
			})
			require.NoError(t, err)

			_, err = client.GetResourceMetricsConfiguration(ctx, &cwsdk.GetResourceMetricsConfigurationInput{
				ResourceArn: aws.String(rmcInstanceArn),
			})
			var rnf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf)
		}},
		{name: "create twice conflicts", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			ctx := t.Context()
			in := &cwsdk.CreateResourceMetricsConfigurationInput{ResourceArn: aws.String(rmcInstanceArn)}
			_, err := client.CreateResourceMetricsConfiguration(ctx, in)
			require.NoError(t, err)

			_, err = client.CreateResourceMetricsConfiguration(ctx, in)
			var conflict *types.ConflictException
			require.ErrorAs(t, err, &conflict)
		}},
		{name: "missing configuration", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			ctx := t.Context()
			arn := aws.String("arn:aws:ec2:us-east-1:000000000000:instance/i-0fedcba9876543210")
			var rnf *types.ResourceNotFoundException

			_, err := client.UpdateResourceMetricsConfiguration(
				ctx,
				&cwsdk.UpdateResourceMetricsConfigurationInput{ResourceArn: arn},
			)
			require.ErrorAs(t, err, &rnf)

			_, err = client.DeleteResourceMetricsConfiguration(
				ctx,
				&cwsdk.DeleteResourceMetricsConfigurationInput{ResourceArn: arn},
			)
			require.ErrorAs(t, err, &rnf)
		}},
		{name: "invalid arn", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			_, err := client.CreateResourceMetricsConfiguration(
				t.Context(),
				&cwsdk.CreateResourceMetricsConfigurationInput{
					ResourceArn: aws.String("not-an-arn"),
				},
			)
			var ipv *types.InvalidParameterValueException
			require.ErrorAs(t, err, &ipv)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.run(t, newTestHandlerAndClient(t))
		})
	}
}

func TestRealClient_UpdateOTelEnrichment(t *testing.T) {
	t.Parallel()

	inc := []types.OTelEnrichmentMetricSelector{
		{Namespace: aws.String("AWS/EC2"), MetricNames: []string{"CPUUtilization"}},
		{Namespace: aws.String("AWS/S3")},
	}
	exc := []types.OTelEnrichmentMetricSelector{{Namespace: aws.String("AWS/EC2"), MetricNames: []string{"NetworkIn"}}}

	cases := []struct {
		run  func(t *testing.T, client *cwsdk.Client)
		name string
	}{
		{name: "not running", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			_, err := client.UpdateOTelEnrichment(t.Context(), &cwsdk.UpdateOTelEnrichmentInput{IncludeFilters: inc})
			var rnf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf)
		}},
		{name: "replaces filters as a pair", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			ctx := t.Context()
			started, err := client.StartOTelEnrichment(ctx, &cwsdk.StartOTelEnrichmentInput{})
			require.NoError(t, err)
			require.NotNil(t, started.CreatedAt)

			upd, err := client.UpdateOTelEnrichment(ctx, &cwsdk.UpdateOTelEnrichmentInput{
				IncludeFilters: inc, ExcludeFilters: exc,
			})
			require.NoError(t, err)
			assert.Equal(t, inc, upd.IncludeFilters)
			assert.Equal(t, exc, upd.ExcludeFilters)
			assert.Equal(t, started.CreatedAt, upd.CreatedAt)
			require.NotNil(t, upd.UpdatedAt)

			got, err := client.GetOTelEnrichment(ctx, &cwsdk.GetOTelEnrichmentInput{})
			require.NoError(t, err)
			assert.Equal(t, types.OTelEnrichmentStatusRunning, got.Status)
			assert.Equal(t, inc, got.IncludeFilters)
			assert.Equal(t, exc, got.ExcludeFilters)

			only, err := client.UpdateOTelEnrichment(ctx, &cwsdk.UpdateOTelEnrichmentInput{IncludeFilters: inc[:1]})
			require.NoError(t, err)
			assert.Equal(t, inc[:1], only.IncludeFilters)
			assert.Empty(t, only.ExcludeFilters, "omitted ExcludeFilters clears the stored pair")

			_, err = client.StopOTelEnrichment(ctx, &cwsdk.StopOTelEnrichmentInput{})
			require.NoError(t, err)
			stopped, err := client.GetOTelEnrichment(ctx, &cwsdk.GetOTelEnrichmentInput{})
			require.NoError(t, err)
			assert.Empty(t, stopped.IncludeFilters)
		}},
		{name: "too many filters", run: func(t *testing.T, client *cwsdk.Client) {
			t.Helper()

			ctx := t.Context()
			_, err := client.StartOTelEnrichment(ctx, &cwsdk.StartOTelEnrichmentInput{})
			require.NoError(t, err)

			many := make([]types.OTelEnrichmentMetricSelector, 101)
			for i := range many {
				many[i] = types.OTelEnrichmentMetricSelector{Namespace: aws.String("AWS/EC2")}
			}
			_, err = client.UpdateOTelEnrichment(ctx, &cwsdk.UpdateOTelEnrichmentInput{IncludeFilters: many})
			var ipv *types.InvalidParameterValueException
			require.ErrorAs(t, err, &ipv)
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.run(t, newTestHandlerAndClient(t))
		})
	}
}
