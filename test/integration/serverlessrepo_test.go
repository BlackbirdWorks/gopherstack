package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	sartypes "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ServerlessRepo_ApplicationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createServerlessRepoClient(t)
	ctx := t.Context()

	appName := "test-integration-app"

	// CreateApplication
	createOut, err := client.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{
		Name:        aws.String(appName),
		Description: aws.String("Integration test application"),
		Author:      aws.String("integration-test"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.ApplicationId)
	assert.Contains(t, *createOut.ApplicationId, appName)

	// GetApplication
	getOut, err := client.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{
		ApplicationId: createOut.ApplicationId,
	})
	require.NoError(t, err)
	assert.Equal(t, appName, *getOut.Name)
	assert.Equal(t, "Integration test application", *getOut.Description)

	// ListApplications
	listOut, err := client.ListApplications(ctx, &serverlessapplicationrepository.ListApplicationsInput{})
	require.NoError(t, err)

	found := false
	for _, a := range listOut.Applications {
		if a.ApplicationId != nil && *a.ApplicationId == *createOut.ApplicationId {
			found = true

			break
		}
	}
	assert.True(t, found, "created application should appear in ListApplications")

	// UpdateApplication
	_, err = client.UpdateApplication(ctx, &serverlessapplicationrepository.UpdateApplicationInput{
		ApplicationId: createOut.ApplicationId,
		Description:   aws.String("Updated description"),
	})
	require.NoError(t, err)

	// Get to verify update
	getOut2, err := client.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{
		ApplicationId: createOut.ApplicationId,
	})
	require.NoError(t, err)
	assert.Equal(t, "Updated description", *getOut2.Description)

	// DeleteApplication
	_, err = client.DeleteApplication(ctx, &serverlessapplicationrepository.DeleteApplicationInput{
		ApplicationId: createOut.ApplicationId,
	})
	require.NoError(t, err)

	// Verify gone
	_, err = client.GetApplication(ctx, &serverlessapplicationrepository.GetApplicationInput{
		ApplicationId: createOut.ApplicationId,
	})
	assert.Error(t, err, "deleted application should return error")
}

func TestIntegration_ServerlessRepo_CloudFormationTemplate(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createServerlessRepoClient(t)
	ctx := t.Context()

	appName := "test-cftemplate-app"

	// Create application
	createOut, err := client.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{
		Name:            aws.String(appName),
		Description:     aws.String("CF template test application"),
		Author:          aws.String("test-author"),
		SemanticVersion: aws.String("1.0.0"),
	})
	require.NoError(t, err)

	// CreateCloudFormationTemplate
	tmplOut, err := client.CreateCloudFormationTemplate(
		ctx,
		&serverlessapplicationrepository.CreateCloudFormationTemplateInput{
			ApplicationId:   createOut.ApplicationId,
			SemanticVersion: aws.String("1.0.0"),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, tmplOut.TemplateId)
	assert.Equal(t, "ACTIVE", string(tmplOut.Status))

	// GetCloudFormationTemplate
	getOut, err := client.GetCloudFormationTemplate(
		ctx,
		&serverlessapplicationrepository.GetCloudFormationTemplateInput{
			ApplicationId: createOut.ApplicationId,
			TemplateId:    tmplOut.TemplateId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, *tmplOut.TemplateId, *getOut.TemplateId)
}

func TestIntegration_ServerlessRepo_ApplicationPolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createServerlessRepoClient(t)
	ctx := t.Context()

	appName := "test-policy-app"

	// Create application
	createOut, err := client.CreateApplication(ctx, &serverlessapplicationrepository.CreateApplicationInput{
		Name:        aws.String(appName),
		Description: aws.String("Policy test application"),
		Author:      aws.String("test-author"),
	})
	require.NoError(t, err)

	// PutApplicationPolicy
	_, err = client.PutApplicationPolicy(ctx, &serverlessapplicationrepository.PutApplicationPolicyInput{
		ApplicationId: createOut.ApplicationId,
		Statements: []sartypes.ApplicationPolicyStatement{
			{
				Actions:    []string{"Deploy"},
				Principals: []string{"*"},
			},
		},
	})
	require.NoError(t, err)

	// GetApplicationPolicy
	policyOut, err := client.GetApplicationPolicy(ctx, &serverlessapplicationrepository.GetApplicationPolicyInput{
		ApplicationId: createOut.ApplicationId,
	})
	require.NoError(t, err)
	assert.Len(t, policyOut.Statements, 1)
}
