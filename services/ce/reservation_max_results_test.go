package ce_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestReservationOps_MaxResultsAndBadToken checks MaxResults paging and InvalidNextTokenException via the SDK.
func TestReservationOps_MaxResultsAndBadToken(t *testing.T) {
	t.Parallel()

	period := &cetypes.DateInterval{Start: aws.String("2024-01-01"), End: aws.String("2024-01-06")}

	type listFn func(ctx context.Context, c *costexplorersdk.Client, size *int32, token *string) (int, *string, error)

	tests := []struct {
		list listFn
		name string
	}{
		{
			name: "coverage",
			list: func(ctx context.Context, c *costexplorersdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.GetReservationCoverage(ctx, &costexplorersdk.GetReservationCoverageInput{
					TimePeriod: period, Granularity: cetypes.GranularityDaily, MaxResults: s, NextPageToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.CoveragesByTime), o.NextPageToken, nil
			},
		},
		{
			name: "utilization",
			list: func(ctx context.Context, c *costexplorersdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.GetReservationUtilization(ctx, &costexplorersdk.GetReservationUtilizationInput{
					TimePeriod: period, Granularity: cetypes.GranularityDaily, MaxResults: s, NextPageToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(o.UtilizationsByTime), o.NextPageToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCEClient(t, ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1")))

			var (
				token *string
				got   []int
			)

			for range 5 {
				n, next, err := tt.list(t.Context(), client, aws.Int32(2), token)
				require.NoError(t, err)

				got = append(got, n)
				if token = next; token == nil {
					break
				}
			}

			assert.Equal(t, []int{2, 2, 1}, got)

			_, _, err := tt.list(t.Context(), client, nil, aws.String("not-a-bucket"))
			require.ErrorContains(t, err, "InvalidNextTokenException")
		})
	}
}
