package codedeploy_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codedeploysdk "github.com/aws/aws-sdk-go-v2/service/codedeploy"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codedeploy"
)

// TestListOps_RejectUnissuedNextToken covers nextToken on the list ops (types/errors.go:3694).
func TestListOps_RejectUnissuedNextToken(t *testing.T) {
	t.Parallel()

	tok := aws.String("bogus")
	app := aws.String("tok-app")

	tests := []struct {
		call func(ctx context.Context, c *codedeploysdk.Client, token *string) error
		name string
	}{
		{name: "applications", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListApplications(ctx, &codedeploysdk.ListApplicationsInput{NextToken: tk})

			return err
		}},
		{name: "deployment_configs", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListDeploymentConfigs(ctx, &codedeploysdk.ListDeploymentConfigsInput{NextToken: tk})

			return err
		}},
		{name: "github_tokens", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListGitHubAccountTokenNames(ctx, &codedeploysdk.ListGitHubAccountTokenNamesInput{NextToken: tk})

			return err
		}},
		{name: "deployment_groups", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListDeploymentGroups(ctx, &codedeploysdk.ListDeploymentGroupsInput{
				ApplicationName: app, NextToken: tk,
			})

			return err
		}},
		{name: "application_revisions", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListApplicationRevisions(ctx, &codedeploysdk.ListApplicationRevisionsInput{
				ApplicationName: app, NextToken: tk,
			})

			return err
		}},
		{name: "on_premises_instances", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListOnPremisesInstances(ctx, &codedeploysdk.ListOnPremisesInstancesInput{NextToken: tk})

			return err
		}},
		{name: "deployments", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListDeployments(ctx, &codedeploysdk.ListDeploymentsInput{NextToken: tk})

			return err
		}},
		{name: "deployment_targets", call: func(ctx context.Context, c *codedeploysdk.Client, tk *string) error {
			_, err := c.ListDeploymentTargets(ctx, &codedeploysdk.ListDeploymentTargetsInput{
				DeploymentId: aws.String("d-X"), NextToken: tk,
			})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCodeDeployClient(
				t,
				codedeploy.NewHandler(codedeploy.NewInMemoryBackend("000000000000", rtTestRegion)),
			)

			_, err := client.CreateApplication(t.Context(), &codedeploysdk.CreateApplicationInput{ApplicationName: app})
			require.NoError(t, err)

			err = tt.call(t.Context(), client, tok)
			require.Error(t, err)

			var apiErr smithy.APIError

			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "InvalidNextTokenException", apiErr.ErrorCode())
		})
	}
}
