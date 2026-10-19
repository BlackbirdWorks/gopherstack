package gcpcloudtasks_test

import (
	"testing"

	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpcloudtasks"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newEnv(t)
	tg := newTarget(t, always(500))
	q := src.queue(t, "persist", fastRetry(7)).GetName()

	_, err := src.client.PauseQueue(t.Context(), &cloudtaskspb.PauseQueueRequest{Name: q})
	require.NoError(t, err)

	kept := src.task(t, q, httpTask(tg.srv.URL), func(task *cloudtaskspb.Task) {
		task.Name = q + "/tasks/kept"
		task.MessageType = &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{
			Url: tg.srv.URL, Body: []byte("body"),
		}}
	})
	src.task(t, q, httpTask(tg.srv.URL), func(task *cloudtaskspb.Task) { task.Name = q + "/tasks/gone" })
	require.NoError(t, src.client.DeleteTask(t.Context(), &cloudtaskspb.DeleteTaskRequest{Name: q + "/tasks/gone"}))

	_, err = src.client.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
		Resource: q,
		Policy:   &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/x", Members: []string{"user:a@b.c"}}}},
	})
	require.NoError(t, err)

	_, err = src.client.UpdateCmekConfig(t.Context(), &cloudtaskspb.UpdateCmekConfigRequest{
		CmekConfig: &cloudtaskspb.CmekConfig{
			Name: location + "/cmekConfig", KmsKey: "projects/p/locations/l/keyRings/r/cryptoKeys/k",
		},
	})
	require.NoError(t, err)

	data := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, data)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), data))

	gotQ, err := dst.client.GetQueue(t.Context(), &cloudtaskspb.GetQueueRequest{Name: q})
	require.NoError(t, err)
	assert.Equal(t, cloudtaskspb.Queue_PAUSED, gotQ.GetState())
	assert.EqualValues(t, 7, gotQ.GetRetryConfig().GetMaxAttempts())

	gotT, err := dst.client.GetTask(t.Context(), &cloudtaskspb.GetTaskRequest{
		Name: kept.GetName(), ResponseView: cloudtaskspb.Task_FULL,
	})
	require.NoError(t, err)
	assert.Equal(t, "body", string(gotT.GetHttpRequest().GetBody()))

	_, err = dst.client.CreateTask(t.Context(), &cloudtaskspb.CreateTaskRequest{
		Parent: q, Task: &cloudtaskspb.Task{Name: q + "/tasks/gone", MessageType: &cloudtaskspb.Task_HttpRequest{
			HttpRequest: &cloudtaskspb.HttpRequest{Url: tg.srv.URL},
		}},
	})
	assert.Equal(t, codes.AlreadyExists, status.Code(err), "tombstones survive a restore")

	pol, err := dst.client.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: q})
	require.NoError(t, err)
	assert.Len(t, pol.GetBindings(), 1)

	cfg, err := dst.client.GetCmekConfig(
		t.Context(),
		&cloudtaskspb.GetCmekConfigRequest{Name: location + "/cmekConfig"},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.GetKmsKey())

	_, err = dst.client.ResumeQueue(t.Context(), &cloudtaskspb.ResumeQueueRequest{Name: q})
	require.NoError(t, err)

	dst.dispatch()
	assert.Equal(t, 1, tg.count(), "restored tasks dispatch")
}

func TestRestoreRejectsGarbageAndOldVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{"garbage", "not json", true},
		{"old version starts empty", `{"version":0,"queues":{"x":{}}}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := gcpcloudtasks.NewInMemoryBackend().Restore(t.Context(), []byte(tt.data))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	q := e.queue(t, "reset").GetName()

	e.handler.Reset()

	_, err := e.client.GetQueue(t.Context(), &cloudtaskspb.GetQueueRequest{Name: q})
	assert.Equal(t, codes.NotFound, status.Code(err))
}
