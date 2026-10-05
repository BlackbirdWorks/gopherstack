package route53_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	route53sdk "github.com/aws/aws-sdk-go-v2/service/route53"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListQueryLoggingConfigs_Pagination pins MaxResults/NextToken
// (route53@v1.65.6 api_op_ListQueryLoggingConfigs.go: default 100).
func TestListQueryLoggingConfigs_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResult *int32
		name      string
		wantPages int
	}{
		{name: "page_size_2", maxResult: aws.Int32(2), wantPages: 2},
		{name: "default", maxResult: nil, wantPages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestRoute53Client(t, route53.NewHandler(route53.NewInMemoryBackend()))

			for i := range 3 {
				z, err := client.CreateHostedZone(t.Context(), &route53sdk.CreateHostedZoneInput{
					Name:            aws.String(fmt.Sprintf("qlc-%d.example.com.", i)),
					CallerReference: aws.String(fmt.Sprintf("qlc-ref-%d", i)),
				})
				require.NoError(t, err)

				_, err = client.CreateQueryLoggingConfig(t.Context(), &route53sdk.CreateQueryLoggingConfigInput{
					HostedZoneId: z.HostedZone.Id,
					CloudWatchLogsLogGroupArn: aws.String(
						fmt.Sprintf("arn:aws:logs:us-east-1:123456789012:log-group:/r53/%d", i),
					),
				})
				require.NoError(t, err)
			}

			total, pages := 0, 0

			var token *string

			for {
				out, err := client.ListQueryLoggingConfigs(t.Context(), &route53sdk.ListQueryLoggingConfigsInput{
					MaxResults: tt.maxResult, NextToken: token,
				})
				require.NoError(t, err)

				total += len(out.QueryLoggingConfigs)
				pages++

				if out.NextToken == nil || *out.NextToken == "" {
					break
				}

				token = out.NextToken
			}

			require.Equal(t, 3, total)
			require.Equal(t, tt.wantPages, pages)
		})
	}
}
