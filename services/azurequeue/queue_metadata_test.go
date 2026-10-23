package azurequeue_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azqueue"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

const sdkKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

func newSDKService(t *testing.T) *azqueue.ServiceClient {
	t.Helper()

	e := echo.New()
	e.Any("/*", azurequeue.NewHandler(azurequeue.NewInMemoryBackend()).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cred, err := azqueue.NewSharedKeyCredential(testAccount, sdkKey)
	require.NoError(t, err)

	svc, err := azqueue.NewServiceClientWithSharedKeyCredential(srv.URL+"/"+testAccount, cred, nil)
	require.NoError(t, err)

	return svc
}

func TestQueueNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		queue    string
		wantCode string
		wantHTTP int
	}{
		{name: "valid", queue: "my-queue-1", wantHTTP: http.StatusCreated},
		{name: "too short", queue: "ab", wantCode: "OutOfRangeInput", wantHTTP: http.StatusBadRequest},
		{
			name: "too long", queue: strings.Repeat("a", 64),
			wantCode: "OutOfRangeInput", wantHTTP: http.StatusBadRequest,
		},
		{name: "uppercase", queue: "MyQueue", wantCode: "InvalidResourceName", wantHTTP: http.StatusBadRequest},
		{name: "leading hyphen", queue: "-abc", wantCode: "InvalidResourceName", wantHTTP: http.StatusBadRequest},
		{name: "trailing hyphen", queue: "abc-", wantCode: "InvalidResourceName", wantHTTP: http.StatusBadRequest},
		{name: "double hyphen", queue: "a--b", wantCode: "InvalidResourceName", wantHTTP: http.StatusBadRequest},
		{name: "underscore", queue: "a_b_c", wantCode: "InvalidResourceName", wantHTTP: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPut, "/"+testAccount+"/"+tt.queue, nil)

			assert.Equal(t, tt.wantHTTP, rec.Code)
			assert.Equal(t, tt.wantCode, rec.Header().Get("X-Ms-Error-Code"))
		})
	}
}

func TestQueueMetadataSDK(t *testing.T) {
	t.Parallel()

	svc := newSDKService(t)
	ctx := t.Context()
	qc := svc.NewQueueClient("meta-queue")

	_, err := qc.Create(ctx, &azqueue.CreateOptions{Metadata: map[string]*string{"Env": new("dev")}})
	require.NoError(t, err)

	props, err := qc.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(0), *props.ApproximateMessagesCount)
	require.Contains(t, props.Metadata, "Env")
	assert.Equal(t, "dev", *props.Metadata["Env"])

	_, err = qc.EnqueueMessage(ctx, "hello", nil)
	require.NoError(t, err)
	_, err = qc.EnqueueMessage(ctx, "world", nil)
	require.NoError(t, err)

	_, err = qc.SetMetadata(ctx, &azqueue.SetMetadataOptions{Metadata: map[string]*string{"team": new("core")}})
	require.NoError(t, err)

	props, err = qc.GetProperties(ctx, nil)
	require.NoError(t, err)
	assert.Equal(t, int32(2), *props.ApproximateMessagesCount)
	assert.Len(t, props.Metadata, 1)
	assert.Equal(t, "core", *props.Metadata["Team"])

	_, err = qc.Create(ctx, &azqueue.CreateOptions{Metadata: map[string]*string{"team": new("core")}})
	require.NoError(t, err, "same metadata is idempotent")

	_, err = qc.Create(ctx, &azqueue.CreateOptions{Metadata: map[string]*string{"team": new("other")}})
	require.Error(t, err, "different metadata conflicts")
	assert.Contains(t, err.Error(), "QueueAlreadyExists")

	missing := svc.NewQueueClient("no-such-queue")
	_, err = missing.GetProperties(ctx, nil)
	require.Error(t, err)
}

func TestQueueListIncludeMetadata(t *testing.T) {
	t.Parallel()

	svc := newSDKService(t)
	ctx := t.Context()

	_, err := svc.CreateQueue(ctx, "list-meta", &azqueue.CreateOptions{
		Metadata: map[string]*string{"owner": new("me")},
	})
	require.NoError(t, err)

	pager := svc.NewListQueuesPager(&azqueue.ListQueuesOptions{Include: azqueue.ListQueuesInclude{Metadata: true}})
	page, err := pager.NextPage(ctx)
	require.NoError(t, err)
	require.Len(t, page.Queues, 1)
	require.Contains(t, page.Queues[0].Metadata, "owner")
	assert.Equal(t, "me", *page.Queues[0].Metadata["owner"])
}

func TestQueueMetadataPersistence(t *testing.T) {
	t.Parallel()

	b := azurequeue.NewInMemoryBackend()
	_, err := b.CreateQueueWithMetadata("persisted", map[string]string{"k": "v"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())

	restored := azurequeue.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	props, err := restored.GetQueueProperties("persisted")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"k": "v"}, props.Metadata)
}
