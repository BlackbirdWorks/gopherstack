package emrserverless_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	emrserverlesstypes "github.com/aws/aws-sdk-go-v2/service/emrserverless/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

// TestRealClient_ApplicationAndSessionLifecycle drives emrserverless's remaining
// typed-coverage-blind ops (gopherstack-n3zi) through the real
// aws-sdk-go-v2 client: GetDashboardForJobRun, GetResourceDashboard,
// GetSession, GetSessionEndpoint, ListSessions, StopApplication,
// TagResource, TerminateSession, UntagResource, UpdateApplication.
func TestRealClient_ApplicationAndSessionLifecycle(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "application lifecycle", run: func(t *testing.T) {
			t.Helper()

			backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
			client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
			ctx := t.Context()

			createOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
				Name:         aws.String("s21-app"),
				ReleaseLabel: aws.String("emr-6.6.0"),
				Type:         aws.String("SPARK"),
			})
			require.NoError(t, err)
			appID := aws.ToString(createOut.ApplicationId)

			tagOut, err := client.TagResource(ctx, &emrserverlesssdk.TagResourceInput{
				ResourceArn: createOut.Arn,
				Tags:        map[string]string{"env": "s21"},
			})
			require.NoError(t, err)
			_ = tagOut

			listTagsOut, err := client.ListTagsForResource(ctx, &emrserverlesssdk.ListTagsForResourceInput{
				ResourceArn: createOut.Arn,
			})
			require.NoError(t, err)
			assert.Equal(t, "s21", listTagsOut.Tags["env"])

			_, err = client.UntagResource(ctx, &emrserverlesssdk.UntagResourceInput{
				ResourceArn: createOut.Arn,
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			listTagsOut2, err := client.ListTagsForResource(ctx, &emrserverlesssdk.ListTagsForResourceInput{
				ResourceArn: createOut.Arn,
			})
			require.NoError(t, err)
			assert.NotContains(t, listTagsOut2.Tags, "env")

			updOut, err := client.UpdateApplication(ctx, &emrserverlesssdk.UpdateApplicationInput{
				ApplicationId: createOut.ApplicationId,
				ClientToken:   aws.String("s21-update-token"),
				ReleaseLabel:  aws.String("emr-6.9.0"),
			})
			require.NoError(t, err)
			require.NotNil(t, updOut.Application)
			assert.Equal(t, "emr-6.9.0", aws.ToString(updOut.Application.ReleaseLabel))
			assert.Equal(t, appID, aws.ToString(updOut.Application.ApplicationId))

			_, err = client.StartApplication(ctx, &emrserverlesssdk.StartApplicationInput{
				ApplicationId: createOut.ApplicationId,
			})
			require.NoError(t, err)

			_, err = client.StopApplication(ctx, &emrserverlesssdk.StopApplicationInput{
				ApplicationId: createOut.ApplicationId,
			})
			require.NoError(t, err)

			getAppOut, err := client.GetApplication(ctx, &emrserverlesssdk.GetApplicationInput{
				ApplicationId: createOut.ApplicationId,
			})
			require.NoError(t, err)
			assert.Equal(t, "STOPPED", string(getAppOut.Application.State))
		}},
		{name: "sessions", run: func(t *testing.T) {
			t.Helper()

			backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
			client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
			ctx := t.Context()

			createOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
				Name:         aws.String("s21-session-app"),
				ReleaseLabel: aws.String("emr-6.6.0"),
				Type:         aws.String("SPARK"),
			})
			require.NoError(t, err)

			_, err = client.StartApplication(ctx, &emrserverlesssdk.StartApplicationInput{
				ApplicationId: createOut.ApplicationId,
			})
			require.NoError(t, err)

			startOut, err := client.StartSession(ctx, &emrserverlesssdk.StartSessionInput{
				ApplicationId:    createOut.ApplicationId,
				ClientToken:      aws.String("s21-session-token"),
				ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
				Name:             aws.String("s21-session"),
			})
			require.NoError(t, err)
			sessionID := aws.ToString(startOut.SessionId)

			getOut, err := client.GetSession(ctx, &emrserverlesssdk.GetSessionInput{
				ApplicationId: createOut.ApplicationId,
				SessionId:     aws.String(sessionID),
			})
			require.NoError(t, err)
			require.NotNil(t, getOut.Session)
			assert.Equal(t, sessionID, aws.ToString(getOut.Session.SessionId))
			assert.Equal(t, "s21-session", aws.ToString(getOut.Session.Name))
			assert.NotEmpty(t, string(getOut.Session.State))

			listOut, err := client.ListSessions(ctx, &emrserverlesssdk.ListSessionsInput{
				ApplicationId: createOut.ApplicationId,
			})
			require.NoError(t, err)
			require.Len(t, listOut.Sessions, 1)
			assert.Equal(t, sessionID, aws.ToString(listOut.Sessions[0].SessionId))

			dashOut, err := client.GetResourceDashboard(ctx, &emrserverlesssdk.GetResourceDashboardInput{
				ApplicationId: createOut.ApplicationId,
				ResourceId:    aws.String(sessionID),
				ResourceType:  emrserverlesstypes.ResourceTypeSession,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(dashOut.Url), sessionID)

			endpointOut, err := client.GetSessionEndpoint(ctx, &emrserverlesssdk.GetSessionEndpointInput{
				ApplicationId: createOut.ApplicationId,
				SessionId:     aws.String(sessionID),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(endpointOut.AuthToken))
			assert.False(t, aws.ToTime(endpointOut.AuthTokenExpiresAt).IsZero())

			_, err = client.TerminateSession(ctx, &emrserverlesssdk.TerminateSessionInput{
				ApplicationId: createOut.ApplicationId,
				SessionId:     aws.String(sessionID),
			})
			require.NoError(t, err)

			getAfterTerminate, err := client.GetSession(ctx, &emrserverlesssdk.GetSessionInput{
				ApplicationId: createOut.ApplicationId,
				SessionId:     aws.String(sessionID),
			})
			require.NoError(t, err)
			assert.Equal(t, "TERMINATED", string(getAfterTerminate.Session.State))
		}},
		{name: "job run dashboard", run: func(t *testing.T) {
			t.Helper()

			backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
			client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))
			ctx := t.Context()

			createOut, err := client.CreateApplication(ctx, &emrserverlesssdk.CreateApplicationInput{
				Name:         aws.String("s21-jobrun-app"),
				ReleaseLabel: aws.String("emr-6.6.0"),
				Type:         aws.String("SPARK"),
			})
			require.NoError(t, err)

			jobOut, err := client.StartJobRun(ctx, &emrserverlesssdk.StartJobRunInput{
				ApplicationId:    createOut.ApplicationId,
				ExecutionRoleArn: aws.String("arn:aws:iam::" + emrRTAccountID + ":role/test-role"),
			})
			require.NoError(t, err)

			dashOut, err := client.GetDashboardForJobRun(ctx, &emrserverlesssdk.GetDashboardForJobRunInput{
				ApplicationId: createOut.ApplicationId,
				JobRunId:      jobOut.JobRunId,
			})
			require.NoError(t, err)
			assert.Contains(t, aws.ToString(dashOut.Url), aws.ToString(jobOut.JobRunId))
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
