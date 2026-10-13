package appconfig_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// terraform-provider-aws with skip_requesting_account_id builds resource ARNs
// with an empty account segment before calling ListTagsForResource.
func TestListTagsForResource_EmptyAccountARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arnFmt   string
		useStrat bool
		wantErr  bool
	}{
		{name: "app with account", arnFmt: "arn:aws:appconfig:us-east-1:123456789012:application/%s"},
		{name: "app empty account", arnFmt: "arn:aws:appconfig:us-east-1::application/%s"},
		{name: "strategy empty account", arnFmt: "arn:aws:appconfig:us-east-1::deploymentstrategy/%s", useStrat: true},
		{name: "app other account", arnFmt: "arn:aws:appconfig:us-east-1:999999999999:application/%s", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAppConfigClient(t, newRealClientHandler())

			var id string

			if tt.useStrat {
				out, err := client.CreateDeploymentStrategy(t.Context(), &appconfigsdk.CreateDeploymentStrategyInput{
					Name: aws.String("s"), DeploymentDurationInMinutes: aws.Int32(1), GrowthFactor: aws.Float32(10),
					ReplicateTo: "NONE",
				})
				require.NoError(t, err)

				id = aws.ToString(out.Id)
			} else {
				out, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{
					Name: aws.String("a"),
				})
				require.NoError(t, err)

				id = aws.ToString(out.Id)
			}

			_, err := client.ListTagsForResource(t.Context(), &appconfigsdk.ListTagsForResourceInput{
				ResourceArn: aws.String(fmt.Sprintf(tt.arnFmt, id)),
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.NotContains(t, err.Error(), "ResourceNotFoundException: ResourceNotFoundException")

				return
			}

			require.NoError(t, err)
		})
	}
}
