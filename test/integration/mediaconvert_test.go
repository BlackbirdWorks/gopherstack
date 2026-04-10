//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaConvertClient returns a MediaConvert client pointed at the shared test container.
func createMediaConvertClient(t *testing.T) *mediaconvertsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediaconvertsdk.NewFromConfig(cfg, func(o *mediaconvertsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaConvert_QueueLifecycle tests the full queue CRUD lifecycle.
func TestIntegration_MediaConvert_QueueLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
	}{
		{
			name:      "full_lifecycle",
			queueName: "integration-test-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)
			queueName := tt.queueName + "-" + t.Name()

			// Create queue.
			createOut, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "CreateQueue should succeed")
			require.NotNil(t, createOut.Queue)
			assert.Equal(t, queueName, aws.ToString(createOut.Queue.Name))
			assert.NotEmpty(t, aws.ToString((*string)(createOut.Queue.Arn)))

			// Get queue.
			getOut, err := client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "GetQueue should succeed")
			require.NotNil(t, getOut.Queue)
			assert.Equal(t, queueName, aws.ToString(getOut.Queue.Name))

			// List queues — should contain the created one.
			listOut, err := client.ListQueues(ctx, &mediaconvertsdk.ListQueuesInput{})
			require.NoError(t, err, "ListQueues should succeed")

			found := false

			for _, q := range listOut.Queues {
				if aws.ToString(q.Name) == queueName {
					found = true

					break
				}
			}

			assert.True(t, found, "created queue should appear in list")

			// Delete queue.
			_, err = client.DeleteQueue(ctx, &mediaconvertsdk.DeleteQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "DeleteQueue should succeed")

			// Verify deletion.
			_, err = client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{
				Name: aws.String(queueName),
			})
			require.Error(t, err, "GetQueue after delete should fail")
		})
	}
}

// TestIntegration_MediaConvert_PresetLifecycle tests the full preset CRUD lifecycle.
func TestIntegration_MediaConvert_PresetLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		presetName string
	}{
		{
			name:       "full_lifecycle",
			presetName: "integration-test-preset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)
			presetName := tt.presetName + "-" + t.Name()

			// Create preset.
			createOut, err := client.CreatePreset(ctx, &mediaconvertsdk.CreatePresetInput{
				Name:        aws.String(presetName),
				Description: aws.String("integration test preset"),
				Category:    aws.String("Standard"),
			})
			require.NoError(t, err, "CreatePreset should succeed")
			require.NotNil(t, createOut.Preset)
			assert.Equal(t, presetName, aws.ToString(createOut.Preset.Name))
			assert.NotEmpty(t, aws.ToString((*string)(createOut.Preset.Arn)))

			// Get preset.
			getOut, err := client.GetPreset(ctx, &mediaconvertsdk.GetPresetInput{
				Name: aws.String(presetName),
			})
			require.NoError(t, err, "GetPreset should succeed")
			require.NotNil(t, getOut.Preset)
			assert.Equal(t, presetName, aws.ToString(getOut.Preset.Name))

			// List presets — should contain the created one.
			listOut, err := client.ListPresets(ctx, &mediaconvertsdk.ListPresetsInput{})
			require.NoError(t, err, "ListPresets should succeed")

			found := false

			for _, p := range listOut.Presets {
				if aws.ToString(p.Name) == presetName {
					found = true

					break
				}
			}

			assert.True(t, found, "created preset should appear in list")

			// Delete preset.
			_, err = client.DeletePreset(ctx, &mediaconvertsdk.DeletePresetInput{
				Name: aws.String(presetName),
			})
			require.NoError(t, err, "DeletePreset should succeed")

			// Verify deletion.
			_, err = client.GetPreset(ctx, &mediaconvertsdk.GetPresetInput{
				Name: aws.String(presetName),
			})
			require.Error(t, err, "GetPreset after delete should fail")
		})
	}
}

// TestIntegration_MediaConvert_AssociateCertificate tests certificate association/disassociation.
func TestIntegration_MediaConvert_AssociateCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		certARN string
	}{
		{
			name:    "associate_and_disassociate",
			certARN: "arn:aws:acm:us-east-1:123456789012:certificate/test-cert-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)

			// Associate certificate.
			_, err := client.AssociateCertificate(ctx, &mediaconvertsdk.AssociateCertificateInput{
				Arn: aws.String(tt.certARN),
			})
			require.NoError(t, err, "AssociateCertificate should succeed")

			// Disassociate certificate.
			_, err = client.DisassociateCertificate(ctx, &mediaconvertsdk.DisassociateCertificateInput{
				Arn: aws.String(tt.certARN),
			})
			require.NoError(t, err, "DisassociateCertificate should succeed")

			// Disassociate again — should fail as not found.
			_, err = client.DisassociateCertificate(ctx, &mediaconvertsdk.DisassociateCertificateInput{
				Arn: aws.String(tt.certARN),
			})
			require.Error(t, err, "DisassociateCertificate of non-associated cert should fail")
		})
	}
}

// TestIntegration_MediaConvert_GetJobsQueryResults tests the jobs query endpoint.
func TestIntegration_MediaConvert_GetJobsQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		queryID string
	}{
		{
			name:    "returns_empty_results",
			queryID: "test-query-id-12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)

			out, err := client.GetJobsQueryResults(ctx, &mediaconvertsdk.GetJobsQueryResultsInput{
				Id: aws.String(tt.queryID),
			})
			require.NoError(t, err, "GetJobsQueryResults should succeed")
			assert.NotNil(t, out)
		})
	}
}

// TestIntegration_MediaConvert_CreateResourceShare tests resource sharing.
func TestIntegration_MediaConvert_CreateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create_share_for_existing_job"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)

			// Create a job first.
			jobOut, err := client.CreateJob(ctx, &mediaconvertsdk.CreateJobInput{
				Role: aws.String("arn:aws:iam::123456789012:role/MediaConvert_Default_Role"),
			})
			require.NoError(t, err, "CreateJob should succeed")
			require.NotNil(t, jobOut.Job)

			jobID := aws.ToString(jobOut.Job.Id)

			// Create resource share for the job.
			_, err = client.CreateResourceShare(ctx, &mediaconvertsdk.CreateResourceShareInput{
				JobId: aws.String(jobID),
			})
			require.NoError(t, err, "CreateResourceShare should succeed")
		})
	}
}
