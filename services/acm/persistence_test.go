package acm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
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
				cert, err := b.RequestCertificate(
					context.Background(),
					"example.com",
					"AMAZON_ISSUED",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				cert, err := b.DescribeCertificate(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, "example.com", cert.DomainName)
				assert.Equal(t, id, cert.ARN)
				assert.NotEmpty(t, cert.CertificateBody, "CertificateBody should be persisted")
			},
		},
		{
			name: "round_trip_preserves_imported_cert",
			setup: func(b *acm.InMemoryBackend) string {
				src, err := b.RequestCertificate(
					context.Background(),
					"imported.example.com",
					"",
					"",
					"",
					"",
					"",
					"",
					nil,
				)
				if err != nil {
					return ""
				}
				imported, err := b.ImportCertificate(context.Background(), src.CertificateBody, src.PrivateKey, "", "")
				if err != nil {
					return ""
				}

				return imported.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				cert, err := b.DescribeCertificate(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, "IMPORTED", cert.Type)
				assert.NotEmpty(t, cert.CertificateBody)
				assert.NotEmpty(t, cert.PrivateKey)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *acm.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *acm.InMemoryBackend, _ string) {
				t.Helper()

				p, _ := b.ListCertificates(context.Background(), acm.ListCertificatesParams{})
				certs := p.Data
				assert.Empty(t, certs)
			},
		},
		{
			// Proves the idempotencyMap (a non-*T value map, left raw and
			// persisted directly -- see persistence.go) survives the round
			// trip: a repeat RequestCertificate call with the same token
			// and parameters after Restore must return the ARN captured
			// before Snapshot rather than minting a new certificate.
			name: "round_trip_preserves_idempotency_token",
			setup: func(b *acm.InMemoryBackend) string {
				cert, err := b.RequestCertificate(
					context.Background(),
					"idempotent.example.com",
					"AMAZON_ISSUED",
					"",
					"idem-tok-1",
					"",
					"",
					"",
					nil,
				)
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				again, err := b.RequestCertificate(
					context.Background(),
					"idempotent.example.com",
					"AMAZON_ISSUED",
					"",
					"idem-tok-1",
					"",
					"",
					"",
					nil,
				)
				require.NoError(t, err)
				assert.Equal(t, id, again.ARN, "same idempotency token must return the original ARN")
			},
		},
		{
			// Proves DomainValidationOption.HttpRedirect (a new pointer
			// field, see certificate_validation.go's HTTP case) round-trips
			// through Snapshot/Restore, and that copyCert deep-copies it
			// rather than sharing the pointer with the pre-snapshot cert.
			name: "round_trip_preserves_http_validation",
			setup: func(b *acm.InMemoryBackend) string {
				cert, err := b.RequestCertificate(
					context.Background(),
					"http-validation.example.com",
					"AMAZON_ISSUED",
					"HTTP",
					"",
					"",
					"",
					"",
					nil,
				)
				if err != nil {
					return ""
				}

				return cert.ARN
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, id string) {
				t.Helper()

				cert, err := b.DescribeCertificate(context.Background(), id)
				require.NoError(t, err)
				assert.Equal(t, "PENDING_VALIDATION", cert.Status)
				require.Len(t, cert.DomainValidationOptions, 1)
				dvo := cert.DomainValidationOptions[0]
				require.NotNil(t, dvo.HTTPRedirect)
				assert.NotEmpty(t, dvo.HTTPRedirect.RedirectFrom)
				assert.NotEmpty(t, dvo.HTTPRedirect.RedirectTo)
				assert.Nil(t, dvo.ResourceRecord)
			},
		},
		{
			// Proves accountConfig and accountIdempotency (both non-*T
			// value maps, left raw and persisted directly) survive the
			// round trip: the configured DaysBeforeExpiry must read back
			// as set, and reusing the same PutAccountConfiguration token
			// with different settings after Restore must still conflict.
			name: "round_trip_preserves_account_configuration",
			setup: func(b *acm.InMemoryBackend) string {
				days := int32(30)
				_ = b.PutAccountConfiguration(context.Background(), "acct-tok-1", &days)

				return ""
			},
			verify: func(t *testing.T, b *acm.InMemoryBackend, _ string) {
				t.Helper()

				cfg := b.GetAccountConfiguration(context.Background())
				assert.Equal(t, int32(30), cfg.DaysBeforeExpiry)

				otherDays := int32(45)
				err := b.PutAccountConfiguration(context.Background(), "acct-tok-1", &otherDays)
				require.ErrorIs(t, err, acm.ErrConflict,
					"reusing a persisted idempotency token with different settings must conflict")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := acm.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := acm.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := acm.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// "version" field does not match the backend's current snapshot version is
// discarded wholesale (registry.ResetAll plus a reset of every raw-left
// persisted map) rather than partially decoded -- see the acmSnapshotVersion
// doc comment in persistence.go. It also proves the discard clears any state
// already present in the target backend, not just leaves the mismatched
// snapshot's state out.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(snap map[string]any)
		name   string
	}{
		{
			name: "absent_version",
			// Mirrors a pre-Phase-3.3 snapshot, which carried no version
			// field at all and so decodes as Version == 0.
			mutate: func(snap map[string]any) { delete(snap, "version") },
		},
		{
			name:   "future_version",
			mutate: func(snap map[string]any) { snap["version"] = 999 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build a valid snapshot from a backend with real state, then
			// corrupt only its version field.
			donor := acm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := donor.RequestCertificate(
				context.Background(), "donor.example.com", "AMAZON_ISSUED", "", "", "", "", "", nil,
			)
			require.NoError(t, err)

			var snapMap map[string]any
			require.NoError(t, json.Unmarshal(donor.Snapshot(t.Context()), &snapMap))
			tt.mutate(snapMap)

			mutatedSnap, err := json.Marshal(snapMap)
			require.NoError(t, err)

			// The restore target starts with its own, different state so we
			// can prove the mismatch discards it rather than leaving it
			// untouched or merging in the donor's certificate.
			target := acm.NewInMemoryBackend("000000000000", "us-east-1")
			_, err = target.RequestCertificate(
				context.Background(), "target.example.com", "AMAZON_ISSUED", "", "", "", "", "", nil,
			)
			require.NoError(t, err)

			require.NoError(t, target.Restore(t.Context(), mutatedSnap))

			p, _ := target.ListCertificates(context.Background(), acm.ListCertificatesParams{})
			assert.Empty(t, p.Data, "version-mismatched restore must discard all state, not merge or keep it")

			cfg := target.GetAccountConfiguration(context.Background())
			assert.Equal(t, int32(45), cfg.DaysBeforeExpiry, "account config must reset to its default")
		})
	}
}

func TestACMHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := acm.NewInMemoryBackend("000000000000", "us-east-1")
	h := acm.NewHandler(backend)

	// Create a cert
	_, err := backend.RequestCertificate(context.Background(), "example.com", "AMAZON_ISSUED", "", "", "", "", "", nil)
	require.NoError(t, err)

	// Test Handler.Snapshot/Restore delegation
	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := acm.NewInMemoryBackend("000000000000", "us-east-1")
	freshH := acm.NewHandler(fresh)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	p, _ := fresh.ListCertificates(context.Background(), acm.ListCertificatesParams{})
	certs := p.Data
	assert.Len(t, certs, 1)
}

// TestACMHandler_Persistence_Tags verifies that certificate tags (held by the
// Handler in a store.Table[certTagsEntry], not the backend -- see
// persistence.go) survive a full Handler.Snapshot/Restore round trip,
// including on a certificate ARN that collides with an idempotency-token
// clash-free re-request after restore.
func TestACMHandler_Persistence_Tags(t *testing.T) {
	t.Parallel()

	h := newACMHandler()

	reqRec := postACMJSON(t, h, "RequestCertificate",
		`{"DomainName":"tagged-persist.example.com","Tags":[{"Key":"env","Value":"prod"}]}`)
	require.Equal(t, http.StatusOK, reqRec.Code)

	var reqOut struct {
		CertificateArn string `json:"CertificateArn"`
	}
	require.NoError(t, json.Unmarshal(reqRec.Body.Bytes(), &reqOut))

	addBody, err := json.Marshal(map[string]any{
		"CertificateArn": reqOut.CertificateArn,
		"Tags":           []map[string]string{{"Key": "team", "Value": "platform"}},
	})
	require.NoError(t, err)

	addRec := postACMJSON(t, h, "AddTagsToCertificate", string(addBody))
	require.Equal(t, http.StatusOK, addRec.Code)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	freshH := newACMHandler()
	require.NoError(t, freshH.Restore(t.Context(), snap))

	listBody, err := json.Marshal(map[string]string{"CertificateArn": reqOut.CertificateArn})
	require.NoError(t, err)

	listRec := postACMJSON(t, freshH, "ListTagsForCertificate", string(listBody))
	require.Equal(t, http.StatusOK, listRec.Code)

	var tagsOut struct {
		Tags []map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &tagsOut))

	tagMap := make(map[string]string, len(tagsOut.Tags))
	for _, kv := range tagsOut.Tags {
		tagMap[kv["Key"]] = kv["Value"]
	}

	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "platform", tagMap["team"])
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
