package sagemaker_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

func asyncRig(t *testing.T, nc *sagemaker.AsyncNotificationConfig) (*engineRig, *modelServer) {
	t.Helper()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	srv := &modelServer{}
	rig.rt.serve = srv.handler
	ctx := t.Context()

	_, err := rig.b.CreateModel(ctx, "m1", "arn:aws:iam::000000000000:role/r", &sagemaker.ContainerDefinition{Image: "img-a"}, nil, nil)
	require.NoError(t, err)
	_, err = rig.b.CreateEndpointConfig(ctx, "cfg", singleVariant("m1"), nil)
	require.NoError(t, err)
	require.NoError(t, rig.b.SetEndpointConfigExtras(ctx, "cfg", nil, &sagemaker.AsyncInferenceConfig{
		OutputConfig: sagemaker.AsyncOutputConfig{
			S3OutputPath: "s3://async/out", S3FailurePath: "s3://async/fail", NotificationConfig: nc,
		},
	}, nil, "", "", nil, false, nil, nil))
	_, err = rig.b.CreateEndpointFSM(ctx, sagemaker.CreateEndpointOptions{Name: "ep", EndpointConfigName: "cfg"})
	require.NoError(t, err)
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	return rig, srv
}

func TestEngineAsync_ResultsAndNotifications(t *testing.T) {
	t.Parallel()

	nc := &sagemaker.AsyncNotificationConfig{SuccessTopic: "arn:sns:ok", ErrorTopic: "arn:sns:err"}

	tests := []struct {
		name         string
		req          sagemaker.AsyncRequest
		inputObject  string
		wantOutput   string
		wantFailure  string
		wantBucketK  [2]string
		wantTopic    string
		wantStatus   string
		wantFailLoc  string
		wantOutValue string
	}{
		{
			name:         "body_success",
			req:          sagemaker.AsyncRequest{InferenceID: "id-1", Body: []byte("payload")},
			wantOutput:   "s3://async/out/id-1.out",
			wantFailLoc:  "s3://async/fail/id-1-error.out",
			wantBucketK:  [2]string{"async", "out/id-1.out"},
			wantTopic:    "arn:sns:ok",
			wantStatus:   "Completed",
			wantOutValue: "img-a|payload|",
		},
		{
			name:         "filename_and_extension",
			req:          sagemaker.AsyncRequest{InferenceID: "id-2", Body: []byte("p"), Filename: "r.json", OutputPathExtension: "/team/a/"},
			wantOutput:   "s3://async/out/team/a/r.json",
			wantFailLoc:  "s3://async/fail/team/a/id-2-error.out",
			wantBucketK:  [2]string{"async", "out/team/a/r.json"},
			wantTopic:    "arn:sns:ok",
			wantStatus:   "Completed",
			wantOutValue: "img-a|p|",
		},
		{
			name:         "input_location",
			req:          sagemaker.AsyncRequest{InferenceID: "id-3", InputLocation: "s3://in/req.json"},
			inputObject:  "from-s3",
			wantOutput:   "s3://async/out/id-3.out",
			wantFailLoc:  "s3://async/fail/id-3-error.out",
			wantBucketK:  [2]string{"async", "out/id-3.out"},
			wantTopic:    "arn:sns:ok",
			wantStatus:   "Completed",
			wantOutValue: "img-a|from-s3|",
		},
		{
			name:         "model_error_writes_failure",
			req:          sagemaker.AsyncRequest{InferenceID: "id-4", Body: []byte("explode")},
			wantOutput:   "s3://async/out/id-4.out",
			wantFailLoc:  "s3://async/fail/id-4-error.out",
			wantBucketK:  [2]string{"async", "fail/id-4-error.out"},
			wantTopic:    "arn:sns:err",
			wantStatus:   "Failed",
			wantOutValue: "model blew up",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rig, _ := asyncRig(t, nc)

			if tt.inputObject != "" {
				requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "in", "req.json", []byte(tt.inputObject)))
			}

			tt.req.EndpointName = "ep"

			acc, err := rig.b.SubmitAsyncInference(t.Context(), tt.req)
			require.NoError(t, err)
			assert.Equal(t, tt.req.InferenceID, acc.InferenceID)
			assert.Equal(t, tt.wantOutput, acc.OutputLocation)
			assert.Equal(t, tt.wantFailLoc, acc.FailureLocation)

			require.Eventually(t, func() bool { return len(rig.sns.messages(tt.wantTopic)) == 1 }, 5*time.Second, 5*time.Millisecond)

			data, ok := rig.s3.get(tt.wantBucketK[0], tt.wantBucketK[1])
			require.True(t, ok)
			assert.Contains(t, string(data), tt.wantOutValue)

			var note map[string]any
			require.NoError(t, json.Unmarshal([]byte(rig.sns.messages(tt.wantTopic)[0]), &note))
			assert.Equal(t, tt.wantStatus, note["invocationStatus"])
			assert.Equal(t, tt.req.InferenceID, note["inferenceId"])
			assert.Equal(t, "us-east-1", note["awsRegion"])
		})
	}
}

func TestEngineAsync_NotAsyncEndpoint(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	rig.rt.serve = (&modelServer{}).handler

	endpointFixture{
		variants: singleVariant("m1"),
		models:   map[string]sagemaker.ContainerDefinition{"m1": {Image: "img-a"}},
	}.create(t, rig, "ep")
	require.Equal(t, "InService", waitEndpoint(t, rig.b, "ep").EndpointStatus)

	_, err := rig.b.SubmitAsyncInference(t.Context(), sagemaker.AsyncRequest{EndpointName: "ep", Body: []byte("x")})
	require.ErrorIs(t, err, sagemaker.ErrNotAsyncEndpoint)
}
