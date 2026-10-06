package securityhub_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// TestRealClient_TrendOpsHonourPaging covers the MaxResults/NextToken members of the V2 trend ops.
func TestRealClient_TrendOpsHonourPaging(t *testing.T) {
	t.Parallel()

	start := aws.Time(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	end := aws.Time(time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC))

	tests := []struct {
		call    func(c *securityhubsdk.Client, m *int32, tok *string) (int, error)
		max     *int32
		token   *string
		name    string
		want    int
		wantErr bool
	}{
		{
			name: "findings_first_page",
			max:  aws.Int32(1),
			want: 1,
			call: func(c *securityhubsdk.Client, m *int32, tok *string) (int, error) {
				out, err := c.GetFindingsTrendsV2(t.Context(), &securityhubsdk.GetFindingsTrendsV2Input{
					StartTime: start, EndTime: end, MaxResults: m, NextToken: tok,
				})
				if err != nil {
					return 0, err
				}

				return len(out.TrendsMetrics), nil
			},
		},
		{
			name:  "findings_past_end",
			token: aws.String("5"),
			want:  0,
			call: func(c *securityhubsdk.Client, m *int32, tok *string) (int, error) {
				out, err := c.GetFindingsTrendsV2(t.Context(), &securityhubsdk.GetFindingsTrendsV2Input{
					StartTime: start, EndTime: end, MaxResults: m, NextToken: tok,
				})
				if err != nil {
					return 0, err
				}

				return len(out.TrendsMetrics), nil
			},
		},
		{
			name:    "findings_bad_token",
			token:   aws.String("zz"),
			wantErr: true,
			call: func(c *securityhubsdk.Client, m *int32, tok *string) (int, error) {
				_, err := c.GetFindingsTrendsV2(t.Context(), &securityhubsdk.GetFindingsTrendsV2Input{
					StartTime: start, EndTime: end, MaxResults: m, NextToken: tok,
				})

				return 0, err
			},
		},
		{
			name:    "resources_bad_token",
			token:   aws.String("-3"),
			wantErr: true,
			call: func(c *securityhubsdk.Client, m *int32, tok *string) (int, error) {
				_, err := c.GetResourcesTrendsV2(t.Context(), &securityhubsdk.GetResourcesTrendsV2Input{
					StartTime: start, EndTime: end, MaxResults: m, NextToken: tok,
				})

				return 0, err
			},
		},
		{
			name:    "resources_negative_max",
			max:     aws.Int32(-1),
			wantErr: true,
			call: func(c *securityhubsdk.Client, m *int32, tok *string) (int, error) {
				_, err := c.GetResourcesTrendsV2(t.Context(), &securityhubsdk.GetResourcesTrendsV2Input{
					StartTime: start, EndTime: end, MaxResults: m, NextToken: tok,
				})

				return 0, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := securityhub.NewHandler(securityhub.NewInMemoryBackend("000000000000", "us-east-1"))
			n, err := tt.call(newTestSecurityHubClient(t, h), tt.max, tt.token)

			if tt.wantErr {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				require.Equal(t, "InvalidInputException", apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			require.Equal(t, tt.want, n)
		})
	}
}
