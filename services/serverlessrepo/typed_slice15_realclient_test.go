package serverlessrepo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sarsdk "github.com/aws/aws-sdk-go-v2/service/serverlessapplicationrepository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

// TestTypedSlice15RealClient drives serverlessrepo's typed-coverage-blind
// ops (gopherstack-n3zi slice 15) through the real aws-sdk-go-v2 client.
func TestTypedSlice15RealClient(t *testing.T) {
	t.Parallel()

	t.Run("application version lifecycle", func(t *testing.T) {
		t.Parallel()

		backend := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
		h := serverlessrepo.NewHandler(backend)
		client, _ := newTestSARSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateApplication(ctx, &sarsdk.CreateApplicationInput{
			Name:            aws.String("s15-versions-app"),
			Author:          aws.String("s15-author"),
			Description:     aws.String("s15 versions app"),
			SemanticVersion: aws.String("1.0.0"),
			TemplateBody:    aws.String(`{"Resources":{}}`),
		})
		require.NoError(t, err)
		appID := aws.ToString(createOut.ApplicationId)
		require.NotEmpty(t, appID)

		verOut, err := client.CreateApplicationVersion(ctx, &sarsdk.CreateApplicationVersionInput{
			ApplicationId:   aws.String(appID),
			SemanticVersion: aws.String("2.0.0"),
			TemplateBody:    aws.String(`{"Resources":{}}`),
			SourceCodeUrl:   aws.String("https://example.com/v2"),
		})
		require.NoError(t, err)
		assert.Equal(t, "2.0.0", aws.ToString(verOut.SemanticVersion))
		assert.Equal(t, "https://example.com/v2", aws.ToString(verOut.SourceCodeUrl))

		listOut, err := client.ListApplicationVersions(ctx, &sarsdk.ListApplicationVersionsInput{
			ApplicationId: aws.String(appID),
		})
		require.NoError(t, err)
		versions := map[string]bool{}
		for _, v := range listOut.Versions {
			versions[aws.ToString(v.SemanticVersion)] = true
			assert.Equal(t, appID, aws.ToString(v.ApplicationId))
		}
		assert.True(t, versions["1.0.0"])
		assert.True(t, versions["2.0.0"])
	})

	t.Run("cloudformation change set", func(t *testing.T) {
		t.Parallel()

		backend := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
		h := serverlessrepo.NewHandler(backend)
		client, _ := newTestSARSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateApplication(ctx, &sarsdk.CreateApplicationInput{
			Name:            aws.String("s15-changeset-app"),
			Author:          aws.String("s15-author"),
			Description:     aws.String("s15 changeset app"),
			SemanticVersion: aws.String("1.0.0"),
			TemplateBody:    aws.String(`{"Resources":{}}`),
		})
		require.NoError(t, err)
		appID := aws.ToString(createOut.ApplicationId)

		csOut, err := client.CreateCloudFormationChangeSet(ctx, &sarsdk.CreateCloudFormationChangeSetInput{
			ApplicationId:   aws.String(appID),
			StackName:       aws.String("s15-stack"),
			SemanticVersion: aws.String("1.0.0"),
			ChangeSetName:   aws.String("s15-changeset"),
		})
		require.NoError(t, err)
		assert.Equal(t, appID, aws.ToString(csOut.ApplicationId))
		assert.NotEmpty(t, aws.ToString(csOut.ChangeSetId))
		assert.NotEmpty(t, aws.ToString(csOut.StackId))
		assert.Equal(t, "1.0.0", aws.ToString(csOut.SemanticVersion))
	})

	t.Run("application dependencies", func(t *testing.T) {
		t.Parallel()

		backend := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
		h := serverlessrepo.NewHandler(backend)
		client, _ := newTestSARSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateApplication(ctx, &sarsdk.CreateApplicationInput{
			Name:            aws.String("s15-deps-app"),
			Author:          aws.String("s15-author"),
			Description:     aws.String("s15 deps app"),
			SemanticVersion: aws.String("1.0.0"),
			TemplateBody:    aws.String(`{"Resources":{}}`),
		})
		require.NoError(t, err)
		appID := aws.ToString(createOut.ApplicationId)

		require.NoError(t, backend.AddApplicationDependencyInternal("s15-deps-app", "1.0.0",
			serverlessrepo.ApplicationDependency{
				ApplicationID:   "nested-app-arn",
				SemanticVersion: "3.0.0",
			}))

		depsOut, err := client.ListApplicationDependencies(ctx, &sarsdk.ListApplicationDependenciesInput{
			ApplicationId:   aws.String(appID),
			SemanticVersion: aws.String("1.0.0"),
		})
		require.NoError(t, err)
		require.Len(t, depsOut.Dependencies, 1)
		assert.Equal(t, "nested-app-arn", aws.ToString(depsOut.Dependencies[0].ApplicationId))
		assert.Equal(t, "3.0.0", aws.ToString(depsOut.Dependencies[0].SemanticVersion))
	})

	t.Run("unshare application", func(t *testing.T) {
		t.Parallel()

		backend := serverlessrepo.NewInMemoryBackend("123456789012", "us-east-1")
		h := serverlessrepo.NewHandler(backend)
		client, _ := newTestSARSDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateApplication(ctx, &sarsdk.CreateApplicationInput{
			Name:            aws.String("s15-unshare-app"),
			Author:          aws.String("s15-author"),
			Description:     aws.String("s15 unshare app"),
			SemanticVersion: aws.String("1.0.0"),
			TemplateBody:    aws.String(`{"Resources":{}}`),
		})
		require.NoError(t, err)
		appID := aws.ToString(createOut.ApplicationId)

		_, err = client.UnshareApplication(ctx, &sarsdk.UnshareApplicationInput{
			ApplicationId:  aws.String(appID),
			OrganizationId: aws.String("o-example123"),
		})
		require.NoError(t, err)

		// Unknown application must still fail -- proves the op reached real
		// backend state, not a stub that always succeeds.
		_, err = client.UnshareApplication(ctx, &sarsdk.UnshareApplicationInput{
			ApplicationId:  aws.String(appID + "-does-not-exist"),
			OrganizationId: aws.String("o-example123"),
		})
		require.Error(t, err)
	})
}
