package emr_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	emrtypes "github.com/aws/aws-sdk-go-v2/service/emr/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// postEMRJSON sends an EMR JSON-protocol request with the given target and
// body, returning the raw response for wire-shape assertions.
func postEMRJSON(t *testing.T, h *emr.Handler, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "ElasticMapReduce."+target)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestListSummaryShapes proves this pass's over-wide-response audit
// (gopherstack-dv4s, 2026-09-19) for emr's six flagged List ops.
// ListClusters, ListSecurityConfigurations and ListStudios were already
// exact. ListNotebookExecutions and ListSteps were missing real, sourceable
// Summary members (NotebookS3Location; EncryptionKeyArn/LogUri) -- now
// wired via StartNotebookExecution's NotebookS3Location and
// AddJobFlowSteps/RunJobFlow's per-step StepMonitoringConfiguration.
// ListStudioSessionMappings leaked SessionMappingDetail's LastModifiedTime
// (Get-only, no case on the real SessionMappingSummary) -- now projected
// through a narrow wire type.
func TestListSummaryShapes(t *testing.T) {
	t.Parallel()

	t.Run("clusters exact", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		_, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      awssdk.String("lss-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{},
		})
		require.NoError(t, err)

		out, err := client.ListClusters(ctx, &emrsdk.ListClustersInput{})
		require.NoError(t, err)
		require.Len(t, out.Clusters, 1)
		assert.Equal(t, "lss-cluster", awssdk.ToString(out.Clusters[0].Name))
		assert.NotNil(t, out.Clusters[0].Status)
	})

	t.Run("notebook executions gained s3 location", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		_, err := client.StartNotebookExecution(ctx, &emrsdk.StartNotebookExecutionInput{
			EditorId:    awssdk.String("e-lss"),
			ServiceRole: awssdk.String("arn:aws:iam::000000000000:role/notebook-service-role"),
			ExecutionEngine: &emrtypes.ExecutionEngineConfig{
				Id: awssdk.String("j-lss"),
			},
			NotebookS3Location: &emrtypes.NotebookS3LocationFromInput{
				Bucket: awssdk.String("nb-bucket"),
				Key:    awssdk.String("nb-key.ipynb"),
			},
		})
		require.NoError(t, err)

		out, err := client.ListNotebookExecutions(ctx, &emrsdk.ListNotebookExecutionsInput{})
		require.NoError(t, err)
		require.Len(t, out.NotebookExecutions, 1)
		loc := out.NotebookExecutions[0].NotebookS3Location
		require.NotNil(t, loc)
		assert.Equal(t, "nb-bucket", awssdk.ToString(loc.Bucket))
		assert.Equal(t, "nb-key.ipynb", awssdk.ToString(loc.Key))
	})

	t.Run("security configurations exact", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		_, err := client.CreateSecurityConfiguration(ctx, &emrsdk.CreateSecurityConfigurationInput{
			Name:                  awssdk.String("lss-secconfig"),
			SecurityConfiguration: awssdk.String(`{"EncryptionConfiguration":{}}`),
		})
		require.NoError(t, err)

		out, err := client.ListSecurityConfigurations(ctx, &emrsdk.ListSecurityConfigurationsInput{})
		require.NoError(t, err)
		require.Len(t, out.SecurityConfigurations, 1)
		assert.Equal(t, "lss-secconfig", awssdk.ToString(out.SecurityConfigurations[0].Name))
		assert.NotNil(t, out.SecurityConfigurations[0].CreationDateTime)
	})

	t.Run("steps gained encryption key arn and log uri", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		runOut, err := client.RunJobFlow(ctx, &emrsdk.RunJobFlowInput{
			Name:      awssdk.String("lss-step-cluster"),
			Instances: &emrtypes.JobFlowInstancesConfig{},
		})
		require.NoError(t, err)

		_, err = client.AddJobFlowSteps(ctx, &emrsdk.AddJobFlowStepsInput{
			JobFlowId: runOut.JobFlowId,
			Steps: []emrtypes.StepConfig{
				{
					Name: awssdk.String("lss-step"),
					HadoopJarStep: &emrtypes.HadoopJarStepConfig{
						Jar: awssdk.String("s3://bucket/job.jar"),
					},
					StepMonitoringConfiguration: &emrtypes.StepMonitoringConfiguration{
						S3MonitoringConfiguration: &emrtypes.S3MonitoringConfiguration{
							EncryptionKeyArn: awssdk.String("arn:aws:kms:us-east-1:000000000000:key/lss"),
							LogUri:           awssdk.String("s3://bucket/logs/"),
						},
					},
				},
			},
		})
		require.NoError(t, err)

		out, err := client.ListSteps(ctx, &emrsdk.ListStepsInput{ClusterId: runOut.JobFlowId})
		require.NoError(t, err)
		require.Len(t, out.Steps, 1)
		assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/lss", awssdk.ToString(out.Steps[0].EncryptionKeyArn))
		assert.Equal(t, "s3://bucket/logs/", awssdk.ToString(out.Steps[0].LogUri))
	})

	t.Run("studio session mappings dropped last modified time", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		studioOut, err := client.CreateStudio(ctx, &emrsdk.CreateStudioInput{
			Name:                     awssdk.String("lss-studio"),
			AuthMode:                 emrtypes.AuthModeIam,
			DefaultS3Location:        awssdk.String("s3://bucket/studio"),
			EngineSecurityGroupId:    awssdk.String("sg-engine"),
			ServiceRole:              awssdk.String("arn:aws:iam::000000000000:role/studio-service"),
			VpcId:                    awssdk.String("vpc-1"),
			WorkspaceSecurityGroupId: awssdk.String("sg-workspace"),
			SubnetIds:                []string{"subnet-1"},
		})
		require.NoError(t, err)

		_, err = client.CreateStudioSessionMapping(ctx, &emrsdk.CreateStudioSessionMappingInput{
			StudioId:         studioOut.StudioId,
			IdentityType:     emrtypes.IdentityTypeUser,
			IdentityName:     awssdk.String("alice"),
			SessionPolicyArn: awssdk.String("arn:aws:iam::000000000000:policy/session-policy"),
		})
		require.NoError(t, err)

		out, err := client.ListStudioSessionMappings(ctx, &emrsdk.ListStudioSessionMappingsInput{
			StudioId: studioOut.StudioId,
		})
		require.NoError(t, err)
		require.Len(t, out.SessionMappings, 1)
		m := out.SessionMappings[0]
		assert.Equal(t, "alice", awssdk.ToString(m.IdentityName))
		assert.NotNil(t, m.CreationTime)

		rec := postEMRJSON(t, h, "ListStudioSessionMappings", `{"StudioId":"`+awssdk.ToString(studioOut.StudioId)+`"}`)
		assert.NotContains(t, rec.Body.String(), "LastModifiedTime")
	})

	t.Run("studios exact", func(t *testing.T) {
		t.Parallel()

		h := emr.NewHandler(emr.NewInMemoryBackend(testAccountID, testRegion))
		client := newTestEMRClient(t, h)
		ctx := t.Context()

		_, err := client.CreateStudio(ctx, &emrsdk.CreateStudioInput{
			Name:                     awssdk.String("lss-studio2"),
			AuthMode:                 emrtypes.AuthModeIam,
			DefaultS3Location:        awssdk.String("s3://bucket/studio"),
			EngineSecurityGroupId:    awssdk.String("sg-engine"),
			ServiceRole:              awssdk.String("arn:aws:iam::000000000000:role/studio-service"),
			VpcId:                    awssdk.String("vpc-1"),
			WorkspaceSecurityGroupId: awssdk.String("sg-workspace"),
			SubnetIds:                []string{"subnet-1"},
		})
		require.NoError(t, err)

		out, err := client.ListStudios(ctx, &emrsdk.ListStudiosInput{})
		require.NoError(t, err)
		require.Len(t, out.Studios, 1)
		assert.Equal(t, "lss-studio2", awssdk.ToString(out.Studios[0].Name))
	})
}
