package gcpserve_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	ltype "google.golang.org/genproto/googleapis/logging/type"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

func TestRequestEntry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trace    string
		wantTrID string
		status   int
		sev      ltype.LogSeverity
		sampled  bool
	}{
		{name: "ok", status: 200, sev: ltype.LogSeverity_INFO},
		{name: "client error", status: 404, sev: ltype.LogSeverity_WARNING},
		{
			name:     "server error with trace",
			status:   503,
			sev:      ltype.LogSeverity_ERROR,
			trace:    "t123/9;o=1",
			wantTrID: "t123",
			sampled:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := httptest.NewRequest(http.MethodPost, "http://svc.localhost/p?q=1", nil)
			r.RemoteAddr = "10.0.0.7:5555"
			r.Header.Set("User-Agent", "ua")

			if tt.trace != "" {
				r.Header.Set("X-Cloud-Trace-Context", tt.trace)
			}

			e := target().RequestEntry(r, tt.status, 12, 3*time.Millisecond)

			assert.Equal(t, "projects/proj/logs/run.googleapis.com%2Frequests", e.GetLogName())
			assert.Equal(t, tt.sev, e.GetSeverity())
			assert.Equal(t, "cloud_run_revision", e.GetResource().GetType())

			h := e.GetHttpRequest()
			assert.Equal(t, "POST", h.GetRequestMethod())
			assert.Equal(t, "http://svc.localhost/p?q=1", h.GetRequestUrl())
			assert.Equal(t, int32(tt.status), h.GetStatus())
			assert.Equal(t, int64(12), h.GetResponseSize())
			assert.Equal(t, "ua", h.GetUserAgent())
			assert.Equal(t, "10.0.0.7", h.GetRemoteIp())
			assert.Equal(t, 3*time.Millisecond, h.GetLatency().AsDuration())

			if tt.wantTrID != "" {
				assert.Equal(t, "projects/proj/traces/"+tt.wantTrID, e.GetTrace())
				assert.Equal(t, tt.sampled, e.GetTraceSampled())
			}
		})
	}
}

func TestRequestRecorder(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	rec := gcpserve.NewRequestRecorder(w)

	assert.Equal(t, http.StatusOK, rec.Status())

	rec.WriteHeader(http.StatusTeapot)
	_, err := rec.Write([]byte("abc"))
	require.NoError(t, err)
	rec.Flush()

	assert.Equal(t, http.StatusTeapot, rec.Status())
	assert.Equal(t, http.StatusTeapot, w.Code)
}
