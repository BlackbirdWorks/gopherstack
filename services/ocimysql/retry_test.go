package ocimysql_test

import (
	"net/http"
	"testing"
	"time"

	my "github.com/oracle/oci-go-sdk/v65/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type retryCase struct {
	create func(t *testing.T, e *env, token string) (id, wr string, err error)
	remove func(t *testing.T, e *env, id string)
	name   string
}

func retryCases() []retryCase {
	return []retryCase{
		{
			name: "db system",
			create: func(t *testing.T, e *env, token string) (string, string, error) {
				t.Helper()

				r, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{
					OpcRetryToken: new(token), CreateDbSystemDetails: dbDetails("rt"),
				})
				if err != nil {
					return "", "", err
				}

				return *r.Id, *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, id string) {
				t.Helper()

				e.settle()

				_, err := e.db.DeleteDbSystem(t.Context(), my.DeleteDbSystemRequest{DbSystemId: &id})
				require.NoError(t, err)
			},
		},
	}
}

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	for _, tc := range retryCases() {
		t.Run(tc.name+" replay", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, wr1, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			id2, wr2, err := tc.create(t, e, "tok")
			require.NoError(t, err)
			assert.Equal(t, id1, id2)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tc.name+" expired", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id1, _, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			e.clock.Advance(25 * time.Hour)

			id2, _, err := tc.create(t, e, "tok")
			require.NoError(t, err)
			assert.NotEqual(t, id1, id2)
		})

		t.Run(tc.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			id, _, err := tc.create(t, e, "tok")
			require.NoError(t, err)

			tc.remove(t, e, id)

			_, _, err = tc.create(t, e, "tok")
			se := svcErr(t, err)
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "InvalidatedRetryToken", se.GetCode())
		})
	}
}

func TestBackupRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "src", nil)
	mk := func() (string, string) {
		r, err := e.bk.CreateBackup(t.Context(), my.CreateBackupRequest{
			OpcRetryToken: new("tok"), DbSystemId: db.Id,
		})
		require.NoError(t, err)

		return *r.Id, *r.OpcWorkRequestId
	}

	id1, wr1 := mk()
	id2, wr2 := mk()
	assert.Equal(t, id1, id2)
	assert.Equal(t, wr1, wr2)
}

func TestChannelRetryToken(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	db := e.mkDB(t, "tgt", nil)
	mk := func() (string, string) {
		r, err := e.ch.CreateChannel(t.Context(), my.CreateChannelRequest{
			OpcRetryToken: new("tok"), CreateChannelDetails: channelDetails(db.Id),
		})
		require.NoError(t, err)

		return *r.Id, *r.OpcWorkRequestId
	}

	id1, wr1 := mk()
	id2, wr2 := mk()
	assert.Equal(t, id1, id2)
	assert.Equal(t, wr1, wr2)
}

func TestRetryTokenSurvivesRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	first := e.createDBWithToken(t, "tok")

	other := newEnv(t, envOpts{})
	require.NoError(t, other.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

	again := other.createDBWithToken(t, "tok")
	assert.Equal(t, *first.Id, *again.Id)
}

func (e *env) createDBWithToken(t *testing.T, token string) my.CreateDbSystemResponse {
	t.Helper()

	r, err := e.db.CreateDbSystem(t.Context(), my.CreateDbSystemRequest{
		OpcRetryToken: new(token), CreateDbSystemDetails: dbDetails("rt"),
	})
	require.NoError(t, err)

	return r
}

func TestActionRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call      func(t *testing.T, e *env, id *string, token string) (string, error)
		name      string
		isChannel bool
	}{
		{
			name: "stop",
			call: func(t *testing.T, e *env, id *string, token string) (string, error) {
				t.Helper()

				r, err := e.db.StopDbSystem(t.Context(), my.StopDbSystemRequest{
					DbSystemId: id, OpcRetryToken: new(token),
					ShutdownType: my.InnoDbShutdownModeFast,
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
		},
		{
			name: "restart",
			call: func(t *testing.T, e *env, id *string, token string) (string, error) {
				t.Helper()

				r, err := e.db.RestartDbSystem(t.Context(), my.RestartDbSystemRequest{
					DbSystemId: id, OpcRetryToken: new(token),
					ShutdownType: my.InnoDbShutdownModeFast,
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
		},
		{
			name:      "channel reset",
			isChannel: true,
			call: func(t *testing.T, e *env, id *string, token string) (string, error) {
				t.Helper()

				r, err := e.ch.ResetChannel(
					t.Context(),
					my.ResetChannelRequest{ChannelId: id, OpcRetryToken: new(token)},
				)
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			db := e.mkDB(t, "act", nil)
			id := db.Id

			if tt.isChannel {
				ch, err := e.ch.CreateChannel(
					t.Context(),
					my.CreateChannelRequest{CreateChannelDetails: channelDetails(db.Id)},
				)
				require.NoError(t, err)
				e.settle()

				_, err = e.ch.UpdateChannel(
					t.Context(),
					my.UpdateChannelRequest{ChannelId: ch.Id, IsEnabled: new(false)},
				)
				require.NoError(t, err)
				e.settle()

				id = ch.Id
			}

			first, err := tt.call(t, e, id, "tok")
			require.NoError(t, err)

			second, err := tt.call(t, e, id, "tok")
			require.NoError(t, err)
			assert.Equal(t, first, second)
		})
	}
}
