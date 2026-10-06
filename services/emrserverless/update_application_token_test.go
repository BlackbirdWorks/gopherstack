package emrserverless_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	emrserverlesssdk "github.com/aws/aws-sdk-go-v2/service/emrserverless"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emrserverless"
)

func TestUpdateApplicationClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		replayLabel string
		wantCode    string
		wantLabel   string
	}{
		{name: "changed_params_conflict", replayLabel: "emr-7.0.0", wantCode: "ConflictException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := emrserverless.NewInMemoryBackend(emrRTAccountID, emrRTRegion)
			client := newTestEMRServerlessSDKClient(t, emrserverless.NewHandler(backend))

			app, err := client.CreateApplication(t.Context(), &emrserverlesssdk.CreateApplicationInput{
				Name: aws.String("tok-app"), ReleaseLabel: aws.String("emr-6.6.0"), Type: aws.String("SPARK"),
				ClientToken: aws.String("create-tok"),
			})
			require.NoError(t, err)

			_, err = client.UpdateApplication(t.Context(), &emrserverlesssdk.UpdateApplicationInput{
				ApplicationId: app.ApplicationId, ClientToken: aws.String("u1"), ReleaseLabel: aws.String("emr-6.9.0"),
			})
			require.NoError(t, err)

			_, err = client.UpdateApplication(t.Context(), &emrserverlesssdk.UpdateApplicationInput{
				ApplicationId: app.ApplicationId, ClientToken: aws.String("u2"), ReleaseLabel: aws.String("emr-7.1.0"),
			})
			require.NoError(t, err)

			out, err := client.UpdateApplication(t.Context(), &emrserverlesssdk.UpdateApplicationInput{
				ApplicationId: app.ApplicationId,
				ClientToken:   aws.String("u1"),
				ReleaseLabel:  aws.String(tt.replayLabel),
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantLabel, aws.ToString(out.Application.ReleaseLabel))

			got, err := client.GetApplication(
				t.Context(), &emrserverlesssdk.GetApplicationInput{ApplicationId: app.ApplicationId},
			)
			require.NoError(t, err)
			assert.Equal(t, "emr-7.1.0", aws.ToString(got.Application.ReleaseLabel))
		})
	}
}

func TestGetJobRunOmitsSummaryOnlyID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	appID := createApp(t, h, "jr-no-id-app")
	jobRunID := startJobRun(t, h, appID)

	rec := doRequest(t, h, http.MethodGet, "/applications/"+appID+"/jobruns/"+jobRunID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]map[string]any
	mustUnmarshal(t, rec, &out)
	assert.NotContains(t, out["jobRun"], "id")
	assert.Equal(t, jobRunID, out["jobRun"]["jobRunId"])
}
