package azureblob_test

import (
	"net/http"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

type blobEventRecorder struct {
	events []azureblob.BlobEvent
	mu     sync.Mutex
}

func (r *blobEventRecorder) OnBlobEvent(ev azureblob.BlobEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, ev)
}

func TestBlobEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run       func(t *testing.T, h *azureblob.Handler)
		name      string
		wantTypes []string
		wantAPIs  []string
	}{
		{
			name: "put blob", wantTypes: []string{azureblob.EventBlobCreated}, wantAPIs: []string{"PutBlob"},
			run: func(t *testing.T, h *azureblob.Handler) {
				t.Helper()
				put(t, h, "/acct/c1/a.txt", "hello")
			},
		},
		{
			name: "block list", wantTypes: []string{azureblob.EventBlobCreated}, wantAPIs: []string{"PutBlockList"},
			run: func(t *testing.T, h *azureblob.Handler) {
				t.Helper()

				id := "YmxvY2sx"
				rec := doRequest(t, h, http.MethodPut, "/acct/c1/b.txt?comp=block&blockid="+id, []byte("x"), nil)
				require.Equal(t, http.StatusCreated, rec.Code)

				body := `<?xml version="1.0"?><BlockList><Latest>` + id + `</Latest></BlockList>`
				rec = doRequest(t, h, http.MethodPut, "/acct/c1/b.txt?comp=blocklist", []byte(body), nil)
				require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
			},
		},
		{
			name: "delete blob", wantTypes: []string{azureblob.EventBlobCreated, azureblob.EventBlobDeleted},
			wantAPIs: []string{"PutBlob", "DeleteBlob"},
			run: func(t *testing.T, h *azureblob.Handler) {
				t.Helper()
				put(t, h, "/acct/c1/a.txt", "hello")

				rec := doRequest(t, h, http.MethodDelete, "/acct/c1/a.txt", nil, nil)
				require.Equal(t, http.StatusAccepted, rec.Code)
			},
		},
		{
			name: "failed delete and reads emit nothing", wantTypes: nil,
			run: func(t *testing.T, h *azureblob.Handler) {
				t.Helper()

				rec := doRequest(t, h, http.MethodDelete, "/acct/c1/missing.txt", nil, nil)
				require.Equal(t, http.StatusNotFound, rec.Code)

				rec = doRequest(t, h, http.MethodGet, "/acct/c1/missing.txt", nil, nil)
				require.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := &blobEventRecorder{}
			h.Events = rec

			require.Equal(
				t,
				http.StatusCreated,
				doRequest(t, h, http.MethodPut, "/acct/c1?restype=container", nil, nil).Code,
			)

			tt.run(t, h)

			types := make([]string, 0, len(rec.events))
			apis := make([]string, 0, len(rec.events))

			for _, ev := range rec.events {
				types, apis = append(types, ev.EventType), append(apis, ev.API)
				assert.Equal(t, "acct", ev.Account)
				assert.Equal(t, "c1", ev.Container)
				assert.NotEmpty(t, ev.RequestID)
			}

			assert.ElementsMatch(t, tt.wantTypes, types)

			if tt.wantAPIs != nil {
				assert.Equal(t, tt.wantAPIs, apis)
			}
		})
	}
}

func put(t *testing.T, h *azureblob.Handler, path, body string) {
	t.Helper()

	rec := doRequest(t, h, http.MethodPut, path, []byte(body), map[string]string{
		"X-Ms-Blob-Type": "BlockBlob", "Content-Type": "text/plain",
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
}
