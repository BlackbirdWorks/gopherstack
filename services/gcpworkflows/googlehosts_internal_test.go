package gcpworkflows

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingTransport struct {
	got    *http.Request
	closed bool
}

func (r *recordingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.got = req

	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

func (r *recordingTransport) CloseIdleConnections() { r.closed = true }

type fixedEndpoints map[string]string

func (f fixedEndpoints) BaseURL(service string) string { return f[service] }

func TestEmulatedHostsRouting(t *testing.T) {
	t.Parallel()

	const emu = "http://127.0.0.1:9999"

	tests := []struct {
		name     string
		url      string
		wantHost string
		routed   bool
	}{
		{"pubsub", "https://pubsub.googleapis.com/v1/x", "127.0.0.1:9999", true},
		{"cloudtasks", "https://cloudtasks.googleapis.com/v2/x", "127.0.0.1:9999", true},
		{"cloudkms", "https://cloudkms.googleapis.com/v1/x", "127.0.0.1:9999", true},
		{"mixed case", "https://PubSub.GoogleAPIs.com/v1/x", "127.0.0.1:9999", true},
		{
			"suffix attacker", "https://evil-pubsub.googleapis.com.attacker.tld/v1/x",
			"evil-pubsub.googleapis.com.attacker.tld", false,
		},
		{"prefix attacker", "https://xpubsub.googleapis.com/v1/x", "xpubsub.googleapis.com", false},
		{"subdomain", "https://a.pubsub.googleapis.com/v1/x", "a.pubsub.googleapis.com", false},
		{"trailing dot", "https://pubsub.googleapis.com./v1/x", "pubsub.googleapis.com.", false},
		{"userinfo host confusion", "https://pubsub.googleapis.com@evil.example/v1/x", "evil.example", false},
		{"other google api", "https://compute.googleapis.com/x", "compute.googleapis.com", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			t.Cleanup(b.Close)
			b.SetEndpoints(fixedEndpoints{svcPubSub: emu, svcCloudTasks: emu, svcKMS: emu})

			rec := &recordingTransport{}
			rt := emulatedHosts{b: b, next: rec}

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, tt.url, nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "Bearer tok")

			_, err = rt.RoundTrip(req)
			require.NoError(t, err)

			assert.Equal(t, tt.wantHost, rec.got.URL.Host)

			if !tt.routed {
				assert.Equal(t, tt.url, rec.got.URL.String())
			}
		})
	}
}

func TestEmulatedHostsForwardsCloseIdleConnections(t *testing.T) {
	t.Parallel()

	rec := &recordingTransport{}
	b := NewInMemoryBackend()
	t.Cleanup(b.Close)

	b.client.Transport = emulatedHosts{b: b, next: rec}
	b.CloseIdleConnections()

	assert.True(t, rec.closed)
}
