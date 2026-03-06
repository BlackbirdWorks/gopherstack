package acm_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/acm"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *acm.InMemoryBackend) string
		verify func(t *testing.T, b *acm.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *acm.InMemoryBackend) string {
				cert, err := b.RequestCertificate("example.com", "AMAZON_ISSUED")
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				cert, err := b.DescribeCertificate(id)
				require.NoError(t, err)
				assert.Equal(t, "example.com", cert.DomainName)
				assert.Equal(t, id, cert.ARN)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *acm.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *acm.InMemoryBackend, _ string) {
				t.Helper()

				certs := b.ListCertificates("", 0).Data
				assert.Empty(t, certs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := acm.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := acm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestACMHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := acm.NewInMemoryBackend("000000000000", "us-east-1")
	h := acm.NewHandler(backend)

	// Create a cert
	_, err := backend.RequestCertificate("example.com", "AMAZON_ISSUED")
	require.NoError(t, err)

	// Test Handler.Snapshot/Restore delegation
	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := acm.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := acm.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	certs := fresh.ListCertificates("", 0).Data
	assert.Len(t, certs, 1)
}

func TestACMHandler_Routing(t *testing.T) {
	t.Parallel()

	h := acm.NewHandler(acm.NewInMemoryBackend("000000000000", "us-east-1"))

	assert.Equal(t, "ACM", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{"acm target", "CertificateManager.ListCertificates", true},
		{"other target", "SQS.SendMessage", false},
		{"no target", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}

	// Test ExtractOperation
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
	req.Header.Set("X-Amz-Target", "CertificateManager.ListCertificates")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "ListCertificates", h.ExtractOperation(c))

	// Test ExtractResource
	req2 := httptest.NewRequest(
		http.MethodPost,
		"/",
		strings.NewReader(`{"CertificateArn":"arn:aws:acm:us-east-1:000000000000:certificate/1234"}`),
	)
	req2.Header.Set("Content-Type", "application/json")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	res := h.ExtractResource(c2)
	assert.Equal(t, "arn:aws:acm:us-east-1:000000000000:certificate/1234", res)
}
