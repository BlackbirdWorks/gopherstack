package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func TestDescribeSessions_UserIDRequiresAuthType_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userID   *string
		authType types.AuthenticationType
		wantCode string
		wantLen  int
	}{
		{name: "user without auth type", userID: aws.String("u1"), wantCode: "InvalidParameterCombinationException"},
		{name: "user with auth type", userID: aws.String("u1"), authType: types.AuthenticationTypeApi, wantLen: 1},
		{
			name:     "unknown user with auth type",
			userID:   aws.String("nobody"),
			authType: types.AuthenticationTypeApi,
			wantLen:  0,
		},
		{name: "no user no auth type", wantLen: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAppStreamClient(
				t,
				appstream.NewHandler(appstream.NewInMemoryBackend("123456789012", "us-east-1")),
			)
			ctx := t.Context()

			_, err := client.CreateStack(ctx, &appstreamsdk.CreateStackInput{Name: aws.String("dsu-stack")})
			require.NoError(t, err)

			_, err = client.CreateFleet(ctx, &appstreamsdk.CreateFleetInput{
				Name:            aws.String("dsu-fleet"),
				InstanceType:    aws.String("stream.standard.medium"),
				ImageName:       aws.String("some-image"),
				ComputeCapacity: &types.ComputeCapacity{DesiredInstances: aws.Int32(1)},
			})
			require.NoError(t, err)

			_, err = client.CreateStreamingURL(ctx, &appstreamsdk.CreateStreamingURLInput{
				StackName: aws.String("dsu-stack"),
				FleetName: aws.String("dsu-fleet"),
				UserId:    aws.String("u1"),
			})
			require.NoError(t, err)

			out, err := client.DescribeSessions(ctx, &appstreamsdk.DescribeSessionsInput{
				StackName:          aws.String("dsu-stack"),
				FleetName:          aws.String("dsu-fleet"),
				UserId:             tt.userID,
				AuthenticationType: tt.authType,
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Sessions, tt.wantLen)
		})
	}
}
