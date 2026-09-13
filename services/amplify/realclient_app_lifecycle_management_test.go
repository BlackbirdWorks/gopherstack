package amplify_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	amplifysdk "github.com/aws/aws-sdk-go-v2/service/amplify"
	amplifytypes "github.com/aws/aws-sdk-go-v2/service/amplify/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amplify"
)

// TestRealClient_AppLifecycleManagement drives amplify's remaining
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client:
// CreateDeployment, DeleteBackendEnvironment, DeleteDomainAssociation,
// DeleteJob, DeleteWebhook, GenerateAccessLogs, GetBackendEnvironment,
// GetJob, GetWebhook, StartDeployment, StopJob, UpdateApp, UpdateBranch,
// UpdateDomainAssociation, UpdateWebhook.
func TestRealClient_AppLifecycleManagement(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "deployment and job lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			appOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("s23-deploy-app")})
			require.NoError(t, err)
			appID := appOut.App.AppId

			_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
				AppId: appID, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			createDeployOut, err := client.CreateDeployment(ctx, &amplifysdk.CreateDeploymentInput{
				AppId: appID, BranchName: aws.String("main"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(createDeployOut.ZipUploadUrl))
			require.NotEmpty(t, aws.ToString(createDeployOut.JobId))

			startOut, err := client.StartDeployment(ctx, &amplifysdk.StartDeploymentInput{
				AppId: appID, BranchName: aws.String("main"), JobId: createDeployOut.JobId,
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.JobSummary)
			assert.Equal(t, aws.ToString(createDeployOut.JobId), aws.ToString(startOut.JobSummary.JobId))
			jobID := startOut.JobSummary.JobId

			getJobOut, err := client.GetJob(ctx, &amplifysdk.GetJobInput{
				AppId: appID, BranchName: aws.String("main"), JobId: jobID,
			})
			require.NoError(t, err)
			require.NotNil(t, getJobOut.Job.Summary)
			assert.Equal(t, aws.ToString(jobID), aws.ToString(getJobOut.Job.Summary.JobId))

			stopOut, err := client.StopJob(ctx, &amplifysdk.StopJobInput{
				AppId: appID, BranchName: aws.String("main"), JobId: jobID,
			})
			require.NoError(t, err)
			assert.Equal(t, amplifytypes.JobStatusCancelled, stopOut.JobSummary.Status)

			delOut, err := client.DeleteJob(ctx, &amplifysdk.DeleteJobInput{
				AppId: appID, BranchName: aws.String("main"), JobId: jobID,
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(jobID), aws.ToString(delOut.JobSummary.JobId))

			_, err = client.GetJob(ctx, &amplifysdk.GetJobInput{
				AppId: appID, BranchName: aws.String("main"), JobId: jobID,
			})
			require.Error(t, err, "job no longer exists after delete")
		}},
		{name: "webhook CRUD", run: func(t *testing.T) {
			t.Helper()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			appOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("s23-webhook-app")})
			require.NoError(t, err)

			_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			createOut, err := client.CreateWebhook(ctx, &amplifysdk.CreateWebhookInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"), Description: aws.String("original"),
			})
			require.NoError(t, err)
			webhookID := createOut.Webhook.WebhookId

			getOut, err := client.GetWebhook(ctx, &amplifysdk.GetWebhookInput{WebhookId: webhookID})
			require.NoError(t, err)
			assert.Equal(t, "original", aws.ToString(getOut.Webhook.Description))

			_, err = client.UpdateWebhook(ctx, &amplifysdk.UpdateWebhookInput{
				WebhookId: webhookID, Description: aws.String("updated"),
			})
			require.NoError(t, err)

			getAfterUpdate, err := client.GetWebhook(ctx, &amplifysdk.GetWebhookInput{WebhookId: webhookID})
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(getAfterUpdate.Webhook.Description))

			_, err = client.DeleteWebhook(ctx, &amplifysdk.DeleteWebhookInput{WebhookId: webhookID})
			require.NoError(t, err)

			_, err = client.GetWebhook(ctx, &amplifysdk.GetWebhookInput{WebhookId: webhookID})
			require.Error(t, err, "webhook no longer exists after delete")
		}},
		{name: "backend environment CRUD", run: func(t *testing.T) {
			t.Helper()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			appOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("s23-env-app")})
			require.NoError(t, err)

			_, err = client.CreateBackendEnvironment(ctx, &amplifysdk.CreateBackendEnvironmentInput{
				AppId: appOut.App.AppId, EnvironmentName: aws.String("staging"), StackName: aws.String("stack-1"),
			})
			require.NoError(t, err)

			getOut, err := client.GetBackendEnvironment(ctx, &amplifysdk.GetBackendEnvironmentInput{
				AppId: appOut.App.AppId, EnvironmentName: aws.String("staging"),
			})
			require.NoError(t, err)
			assert.Equal(t, "stack-1", aws.ToString(getOut.BackendEnvironment.StackName))

			_, err = client.DeleteBackendEnvironment(ctx, &amplifysdk.DeleteBackendEnvironmentInput{
				AppId: appOut.App.AppId, EnvironmentName: aws.String("staging"),
			})
			require.NoError(t, err)

			_, err = client.GetBackendEnvironment(ctx, &amplifysdk.GetBackendEnvironmentInput{
				AppId: appOut.App.AppId, EnvironmentName: aws.String("staging"),
			})
			require.Error(t, err, "backend environment no longer exists after delete")
		}},
		{name: "domain association update and delete", run: func(t *testing.T) {
			t.Helper()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			appOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{Name: aws.String("s23-domain-app")})
			require.NoError(t, err)

			_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)

			_, err = client.CreateDomainAssociation(ctx, &amplifysdk.CreateDomainAssociationInput{
				AppId:      appOut.App.AppId,
				DomainName: aws.String("example.com"),
				SubDomainSettings: []amplifytypes.SubDomainSetting{
					{BranchName: aws.String("main"), Prefix: aws.String("www")},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateDomainAssociation(ctx, &amplifysdk.UpdateDomainAssociationInput{
				AppId:      appOut.App.AppId,
				DomainName: aws.String("example.com"),
				SubDomainSettings: []amplifytypes.SubDomainSetting{
					{BranchName: aws.String("main"), Prefix: aws.String("api")},
				},
				EnableAutoSubDomain: aws.Bool(true),
			})
			require.NoError(t, err)

			getOut, err := client.GetDomainAssociation(ctx, &amplifysdk.GetDomainAssociationInput{
				AppId: appOut.App.AppId, DomainName: aws.String("example.com"),
			})
			require.NoError(t, err)
			require.Len(t, getOut.DomainAssociation.SubDomains, 1)
			assert.Equal(t, "api", aws.ToString(getOut.DomainAssociation.SubDomains[0].SubDomainSetting.Prefix))
			assert.True(t, aws.ToBool(getOut.DomainAssociation.EnableAutoSubDomain))

			logsOut, err := client.GenerateAccessLogs(ctx, &amplifysdk.GenerateAccessLogsInput{
				AppId: appOut.App.AppId, DomainName: aws.String("example.com"),
			})
			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(logsOut.LogUrl))

			_, err = client.DeleteDomainAssociation(ctx, &amplifysdk.DeleteDomainAssociationInput{
				AppId: appOut.App.AppId, DomainName: aws.String("example.com"),
			})
			require.NoError(t, err)

			_, err = client.GetDomainAssociation(ctx, &amplifysdk.GetDomainAssociationInput{
				AppId: appOut.App.AppId, DomainName: aws.String("example.com"),
			})
			require.Error(t, err, "domain association no longer exists after delete")
		}},
		{name: "UpdateApp and UpdateBranch", run: func(t *testing.T) {
			t.Helper()

			backend := amplify.NewInMemoryBackend("000000000000", tagsRTRegion)
			client := newTestAmplifyClient(t, amplify.NewHandler(backend))
			ctx := t.Context()

			appOut, err := client.CreateApp(ctx, &amplifysdk.CreateAppInput{
				Name: aws.String("s23-update-app"), Description: aws.String("original app description"),
			})
			require.NoError(t, err)

			_, err = client.UpdateApp(ctx, &amplifysdk.UpdateAppInput{
				AppId: appOut.App.AppId, Description: aws.String("updated app description"),
			})
			require.NoError(t, err)

			getAppOut, err := client.GetApp(ctx, &amplifysdk.GetAppInput{AppId: appOut.App.AppId})
			require.NoError(t, err)
			assert.Equal(t, "updated app description", aws.ToString(getAppOut.App.Description))

			_, err = client.CreateBranch(ctx, &amplifysdk.CreateBranchInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"), Description: aws.String("original branch"),
			})
			require.NoError(t, err)

			_, err = client.UpdateBranch(ctx, &amplifysdk.UpdateBranchInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"), Description: aws.String("updated branch"),
			})
			require.NoError(t, err)

			getBranchOut, err := client.GetBranch(ctx, &amplifysdk.GetBranchInput{
				AppId: appOut.App.AppId, BranchName: aws.String("main"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated branch", aws.ToString(getBranchOut.Branch.Description))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
