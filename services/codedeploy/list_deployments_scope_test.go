package codedeploy_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

func TestListDeployments_ScopePairing_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		app      *string
		group    *string
		wantCode string
		wantLen  int
	}{
		{name: "neither", wantLen: 1},
		{name: "both", app: aws.String("lds-app"), group: aws.String("lds-dg"), wantLen: 1},
		{name: "app only", app: aws.String("lds-app"), wantCode: "DeploymentGroupNameRequiredException"},
		{name: "group only", group: aws.String("lds-dg"), wantCode: "ApplicationNameRequiredException"},
		{
			name:     "unknown app",
			app:      aws.String("nope"),
			group:    aws.String("lds-dg"),
			wantCode: "ApplicationDoesNotExistException",
		},
		{
			name:     "unknown group",
			app:      aws.String("lds-app"),
			group:    aws.String("nope"),
			wantCode: "DeploymentGroupDoesNotExistException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeDeployClient(
				t,
				codedeploy.NewHandler(codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)),
			)

			_, err := client.CreateApplication(
				t.Context(),
				&codedeploysdk.CreateApplicationInput{ApplicationName: aws.String("lds-app")},
			)
			require.NoError(t, err)

			_, err = client.CreateDeploymentGroup(t.Context(), &codedeploysdk.CreateDeploymentGroupInput{
				ApplicationName:     aws.String("lds-app"),
				DeploymentGroupName: aws.String("lds-dg"),
				ServiceRoleArn:      aws.String("arn:aws:iam::000000000000:role/role"),
			})
			require.NoError(t, err)

			_, err = client.CreateDeployment(t.Context(), &codedeploysdk.CreateDeploymentInput{
				ApplicationName:     aws.String("lds-app"),
				DeploymentGroupName: aws.String("lds-dg"),
			})
			require.NoError(t, err)

			out, err := client.ListDeployments(t.Context(), &codedeploysdk.ListDeploymentsInput{
				ApplicationName:     tt.app,
				DeploymentGroupName: tt.group,
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Deployments, tt.wantLen)
		})
	}
}
