package ocistreaming_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/streaming"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ocistreaming"
)

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, e *env, token string) (id, wr string, err error)
		list   func(t *testing.T, e *env) int
		del    func(t *testing.T, e *env, id string)
		name   string
	}{
		{
			name: "pool",
			create: func(t *testing.T, e *env, token string) (string, string, error) {
				t.Helper()

				r, err := e.admin.CreateStreamPool(t.Context(), streaming.CreateStreamPoolRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testCompartment), Name: new("rp"),
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.OpcWorkRequestId, nil
			},
			list: func(t *testing.T, e *env) int {
				t.Helper()

				l, err := e.admin.ListStreamPools(t.Context(), streaming.ListStreamPoolsRequest{
					CompartmentId: new(testCompartment),
				})
				require.NoError(t, err)

				return len(l.Items)
			},
			del: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.admin.DeleteStreamPool(t.Context(), streaming.DeleteStreamPoolRequest{StreamPoolId: &id})
				require.NoError(t, err)
			},
		},
		{
			name: "harness",
			create: func(t *testing.T, e *env, token string) (string, string, error) {
				t.Helper()

				r, err := e.admin.CreateConnectHarness(t.Context(), streaming.CreateConnectHarnessRequest{
					OpcRetryToken: new(token),
					CompartmentId: new(testCompartment), Name: new("rh"),
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.OpcWorkRequestId, nil
			},
			list: func(t *testing.T, e *env) int {
				t.Helper()

				l, err := e.admin.ListConnectHarnesses(t.Context(), streaming.ListConnectHarnessesRequest{
					CompartmentId: new(testCompartment),
				})
				require.NoError(t, err)

				return len(l.Items)
			},
			del: func(t *testing.T, e *env, id string) {
				t.Helper()

				_, err := e.admin.DeleteConnectHarness(t.Context(), streaming.DeleteConnectHarnessRequest{
					ConnectHarnessId: &id,
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, wr1, err := tt.create(t, e, "tok")
			require.NoError(t, err)

			id2, wr2, err := tt.create(t, e, "tok")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
			assert.Equal(t, wr1, wr2)
			assert.Equal(t, 1, tt.list(t, e))
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, _, err := tt.create(t, e, "tok")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, _, err := tt.create(t, e, "tok")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id, _, err := tt.create(t, e, "tok")
			require.NoError(t, err)

			e.settle()
			tt.del(t, e, id)

			_, _, err = tt.create(t, e, "tok")
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestRetryTokenSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	in := oci.CreatePoolIn{Compartment: testCompartment, Name: "p", RetryToken: "tok"}

	first, _, wr, err := e.backend.CreatePool(in, "h")
	require.NoError(t, err)

	restored := oci.NewInMemoryBackend()
	restored.SetClock(e.clock.Now)
	require.NoError(t, restored.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	again, _, wr2, err := restored.CreatePool(in, "h")
	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID)
	assert.Equal(t, wr, wr2)
}
