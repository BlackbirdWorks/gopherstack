package ocistreaming_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	oci "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

func TestSignatureEnforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		opts     envOpts
		other    bool
		wantCode int
	}{
		{"permissive accepts anything", envOpts{}, false, http.StatusOK},
		{"registered key accepted", envOpts{enforce: true, register: true}, false, http.StatusOK},
		{"unregistered key rejected", envOpts{enforce: true}, false, http.StatusUnauthorized},
		{"wrong signing key rejected", envOpts{enforce: true, register: true}, true, http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)
			if tt.other {
				kp := newKeyPair(t)
				prov := common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, e.kp.fprint, kp.privPEM, nil)
				c, err := streaming.NewStreamAdminClientWithConfigurationProvider(prov)
				require.NoError(t, err)

				c.Host = e.base
				e.admin = c
			}

			_, err := e.admin.ListStreamPools(t.Context(), streaming.ListStreamPoolsRequest{CompartmentId: new(testCompartment)})
			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetHTTPStatusCode())
			assert.Equal(t, "NotAuthenticated", se.GetCode())
		})
	}
}

func TestAdminKeyRegistration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})
	req := streaming.ListStreamPoolsRequest{CompartmentId: new(testCompartment)}

	_, err := e.admin.ListStreamPools(t.Context(), req)
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	payload, _ := json.Marshal(map[string]string{"tenancy": testTenancy, "user": testUser, "publicKey": e.kp.pubPEM})
	code, body := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", string(payload))
	require.Equal(t, http.StatusCreated, code, body)

	_, err = e.admin.ListStreamPools(t.Context(), req)
	require.NoError(t, err)
}

func TestSnapshotRestoreFullState(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{})
	s := src.mkStream(t, "persisted", 2)
	c := src.msgs(t, s)

	put := putOne(t, c, s, "k", "payload")
	cur := groupCursor(t, c, s, "g", "i", nil)
	assert.Len(t, getMsgs(t, c, s, cur, 0).Items, 1)

	snap := src.backend.Snapshot(t.Context())

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.admin.GetStream(t.Context(), streaming.GetStreamRequest{StreamId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, "persisted", *got.Name)

	dc := dst.msgs(t, got.Stream)
	tc := plainCursor(t, dc, got.Stream, *put.Partition, streaming.CreateCursorDetailsTypeTrimHorizon, nil)
	assert.Equal(t, []string{"payload"}, values(getMsgs(t, dc, got.Stream, tc, 0).Items))

	grp, err := dc.GetGroup(t.Context(), streaming.GetGroupRequest{StreamId: s.Id, GroupName: new("g")})
	require.NoError(t, err)
	assert.NotEmpty(t, grp.Reservations)
}

func TestRestoreRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		snap    map[string]any
		wantErr bool
	}{
		{"future version starts empty", map[string]any{"version": 99, "streams": map[string]any{"x": map[string]any{}}}, false},
		{"null stream", map[string]any{"version": 1, "streams": map[string]any{"x": nil}}, true},
		{"null pool", map[string]any{"version": 1, "pools": map[string]any{"x": nil}}, true},
		{"null group", map[string]any{"version": 1, "groups": map[string]any{"x": nil}}, true},
		{"empty maps tolerated", map[string]any{"version": 1}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkStream(t, "pre", 1)

			err := e.backend.Restore(t.Context(), persistence.MarshalSnapshot(t.Context(), "ocistreaming", tt.snap))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, lerr := e.admin.ListStreams(t.Context(), streaming.ListStreamsRequest{CompartmentId: new(testCompartment)})
			require.NoError(t, lerr)
			assert.Empty(t, list.Items)
		})
	}
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := oci.NewHandler(oci.NewInMemoryBackend())
	assert.Equal(t, "OCIStreaming", h.Name())
	assert.Len(t, h.GetSupportedOperations(), 30)
	assert.Zero(t, h.MatchPriority())
	assert.False(t, h.RouteMatcher()(nil))

	var _ service.Registerable = h

	h.Reset()
	h.Shutdown(context.Background())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &oci.Provider{}
	assert.Equal(t, "OCIStreaming", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, oci.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{Config: settingsConfig{oci.Settings{Port: 4321, EnforceAuth: true}}})
	require.NoError(t, err)

	h, ok := reg.(*oci.Handler)
	require.True(t, ok)
	assert.Equal(t, 4321, h.Port)
	assert.True(t, h.EnforceAuth)

	reg, err = p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok = reg.(*oci.Handler)
	require.True(t, ok)
	assert.Equal(t, oci.DefaultPort, h.Port)
}

type settingsConfig struct{ s oci.Settings }

func (c settingsConfig) GetOCIStreamingSettings() oci.Settings { return c.s }
