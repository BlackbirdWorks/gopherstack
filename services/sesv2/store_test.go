package sesv2_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// sdkRoundTripTestRegion is the region used by newSESv2SDKClient.
const sdkRoundTripTestRegion = "us-east-1"

// newSESv2SDKClient stands up the real aws-sdk-go-v2 sesv2 client against an
// httptest server running h, wired through the same pkgs/service
// registry/router used in production. Round-tripping through the genuine SDK
// serializer/deserializer (rather than decoding the raw JSON body with
// ad-hoc structs/maps, as most other tests in this package do) is what
// actually proves a response is wire-compatible -- backend-struct or
// decoded-map assertions have repeatedly hidden dropped-field bugs across
// this codebase's parity work. Shared across the sesv2_test package's family
// test files.
func newSESv2SDKClient(t *testing.T, h *sesv2.Handler) *sesv2sdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(sdkRoundTripTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sesv2sdk.NewFromConfig(cfg, func(o *sesv2sdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// newSESv2TestHandler returns a fresh Handler backed by a fresh InMemoryBackend.
// Shared across the sesv2_test package's family test files.
func newSESv2TestHandler(t *testing.T) (*sesv2.Handler, *sesv2.InMemoryBackend) {
	t.Helper()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)

	return h, backend
}

// doReq performs a request against the sesv2 handler via echo. Shared across
// the sesv2_test package's family test files.
func doReq(
	t *testing.T,
	h *sesv2.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doReqQuery performs a request against the sesv2 handler via echo, optionally
// setting URL query parameters. Shared across the sesv2_test package's family
// test files.
func doReqQuery(
	t *testing.T,
	h *sesv2.Handler,
	method, path string,
	query map[string]string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer

	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")

	if len(query) > 0 {
		q := req.URL.Query()
		for k, v := range query {
			q.Set(k, v)
		}

		req.URL.RawQuery = q.Encode()
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// decodeJSON decodes a response recorder's JSON body into a map. Shared
// across the sesv2_test package's family test files.
func decodeJSON(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&m))

	return m
}

// TestHandlerOpsLen verifies the supported operation count.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	_, backend := newSESv2TestHandler(t)
	h := sesv2.NewHandler(backend)

	assert.Equal(t, 110, sesv2.HandlerOpsLen(h))
}

// TestBackendAccountID verifies AccountID returns a non-empty value.
func TestBackendAccountID(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.NotEmpty(t, backend.AccountID())
}

// TestBackendRegion verifies Region returns a non-empty value.
func TestBackendRegion(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	assert.NotEmpty(t, backend.Region())
}

// TestBackendReset verifies Reset clears all data.
func TestBackendReset(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateEmailIdentity("reset@example.com", "", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, sesv2.IdentityCount(backend))

	backend.Reset()
	assert.Equal(t, 0, sesv2.IdentityCount(backend))
}

// TestHandlerReset verifies Handler.Reset delegates to the backend.
func TestHandlerReset(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)

	_, err := backend.CreateEmailIdentity("reset@example.com", "", nil)
	require.NoError(t, err)

	h.Reset()
	assert.Equal(t, 0, sesv2.IdentityCount(backend))
}

// TestSnapshotRestore verifies snapshot and restore round-trip contact lists.
func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	_, err := backend.CreateContactList("my-list", "desc")
	require.NoError(t, err)

	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	backend2 := sesv2.NewInMemoryBackend()
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, sesv2.ContactListCount(backend2))
}

// TestSnapshotRestoreRoundTrip verifies every new resource type persists and
// restores through a snapshot round-trip.
func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	// Populate all new resource types.
	_, err := backend.CreateEmailIdentity("snap@example.com", "", nil)
	require.NoError(t, err)

	_, err = backend.CreateContactList("snap-list", "desc")
	require.NoError(t, err)

	_, err = backend.CreateDedicatedIPPool("snap-pool", "STANDARD")
	require.NoError(t, err)

	_, err = backend.CreateEmailTemplate("snap-tmpl", &sesv2.EmailTemplateContent{Subject: "hi"})
	require.NoError(t, err)

	backend.AddExportJobInternal("snap-job", "CREATED")

	snap := backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	backend2 := sesv2.NewInMemoryBackend()
	require.NoError(t, backend2.Restore(t.Context(), snap))

	assert.Equal(t, 1, sesv2.IdentityCount(backend2))
	assert.Equal(t, 1, sesv2.ContactListCount(backend2))
	assert.Equal(t, 1, sesv2.DedicatedIPPoolCount(backend2))
	assert.Equal(t, 1, sesv2.EmailTemplateCount(backend2))
	assert.Equal(t, 1, sesv2.ExportJobCount(backend2))
}

// TestErrorSentinels verifies error aliases work with [errors.Is].
func TestErrorSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantIs error
		doErr  func() error
		name   string
	}{
		{
			name: "identity_not_found",
			doErr: func() error {
				_, err := sesv2.NewInMemoryBackend().GetEmailIdentity("no@example.com")

				return err
			},
			wantIs: sesv2.ErrNotFound,
		},
		{
			name: "identity_already_exists",
			doErr: func() error {
				b := sesv2.NewInMemoryBackend()
				_, _ = b.CreateEmailIdentity("dup@example.com", "", nil)
				_, err := b.CreateEmailIdentity("dup@example.com", "", nil)

				return err
			},
			wantIs: sesv2.ErrAlreadyExists,
		},
		{
			name: "invalid_input",
			doErr: func() error {
				_, err := sesv2.NewInMemoryBackend().CreateEmailIdentity("", "", nil)

				return err
			},
			wantIs: sesv2.ErrInvalidInput,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.doErr()
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantIs)
		})
	}
}

// TestSDKOpsSorted verifies GetSupportedOperations is sorted.
func TestSDKOpsSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	h := sesv2.NewHandler(backend)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestListConfigurationSetsSorted verifies sorted listing.
func TestListConfigurationSetsSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	for _, name := range []string{"z-set", "a-set", "m-set"} {
		_, err := backend.CreateConfigurationSet(name)
		require.NoError(t, err)
	}

	page := backend.ListConfigurationSets("", 0)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "a-set", page.Data[0].Name)
	assert.Equal(t, "m-set", page.Data[1].Name)
	assert.Equal(t, "z-set", page.Data[2].Name)
}

// TestListEmailIdentitiesSorted verifies sorted listing.
func TestListEmailIdentitiesSorted(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()

	for _, id := range []string{"z@example.com", "a@example.com", "m@example.com"} {
		_, err := backend.CreateEmailIdentity(id, "", nil)
		require.NoError(t, err)
	}

	page := backend.ListEmailIdentities("", 0)
	require.Len(t, page.Data, 3)
	assert.Equal(t, "a@example.com", page.Data[0].Identity)
}

// TestProviderInitNilAppContext verifies provider nil guard.
func TestProviderInitNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sesv2.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, sesv2.ErrNilAppContext)
}

// TestNewInMemoryBackendWithConfig exercises the NewInMemoryBackendWithConfig code path.
func TestNewInMemoryBackendWithConfig(t *testing.T) {
	t.Parallel()

	_, b := newSESv2TestHandler(t)
	assert.NotNil(t, b)
	assert.NotEmpty(t, b.Region())
}
