package rdsdata //nolint:testpackage // white-box tests of the unexported placeholder, mapping and engine code.

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testClusterARN = "arn:aws:rds:us-east-1:000000000000:cluster:real"
	testSecretARN  = "arn:aws:secretsmanager:us-east-1:000000000000:secret:creds"
	testCreds      = `{"username":"master","password":"s3cret"}`
)

var errSecretNotFound = errors.New("secret not found")

type fakeResolver struct {
	err    error
	target RealTarget
}

func (f fakeResolver) DataAPITarget(string) (RealTarget, error) { return f.target, f.err }

type fakeSecrets map[string]string

func (f fakeSecrets) SecretString(_ context.Context, id string) (string, error) {
	v, ok := f[id]
	if !ok {
		return "", errSecretNotFound
	}

	return v, nil
}

type realHarness struct {
	b      *InMemoryBackend
	opens  *atomic.Int32
	logins *atomic.Pointer[realLogin]
}

func newRealHarness(t *testing.T, resolverErr error, secrets fakeSecrets) *realHarness {
	t.Helper()

	b := NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	b.WithRealEngine(
		fakeResolver{err: resolverErr, target: RealTarget{Kind: kindMySQL, Addr: "db:3306", DBName: "app"}},
		secrets,
	)

	h := &realHarness{b: b, opens: &atomic.Int32{}, logins: &atomic.Pointer[realLogin]{}}
	file := filepath.Join(t.TempDir(), "real.db")

	b.real.open = func(l realLogin) (*sql.DB, error) {
		h.opens.Add(1)
		h.logins.Store(&l)

		return sql.Open("sqlite", "file:"+file+"?_pragma=busy_timeout(5000)")
	}

	return h
}

func (h *realHarness) ctx(t *testing.T) context.Context {
	t.Helper()

	return withRequestTarget(t.Context(), testSecretARN, "")
}

func (h *realHarness) exec(t *testing.T, stmt, txID string, params ...SQLParameter) (realResult, error) {
	t.Helper()

	recs, cols, updated, gen, err := h.b.ExecuteStatement(h.ctx(t), testClusterARN, stmt, txID, params...)

	return realResult{Records: recs, Columns: cols, Updated: updated, Generated: gen}, err
}

func (h *realHarness) count(t *testing.T) int64 {
	t.Helper()

	res, err := h.exec(t, "SELECT count(*) FROM items", "")
	require.NoError(t, err)

	return *res.Records[0][0].LongValue
}

func newItemsHarness(t *testing.T) *realHarness {
	t.Helper()

	h := newRealHarness(t, nil, fakeSecrets{testSecretARN: testCreds})
	_, err := h.exec(t, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)", "")
	require.NoError(t, err)

	return h
}

func TestRealEngineStatements(t *testing.T) {
	t.Parallel()

	h := newItemsHarness(t)

	res, err := h.exec(t, "INSERT INTO items (id, name) VALUES (:id, :name)", "",
		longParam("id", 1), strParam("name", "alpha"))
	require.NoError(t, err)
	assert.Equal(t, int64(1), res.Updated)

	res, err = h.exec(t, "SELECT id, name FROM items WHERE name = :name", "", strParam("name", "alpha"))
	require.NoError(t, err)
	require.Len(t, res.Records, 1)
	assert.Equal(t, int64(1), *res.Records[0][0].LongValue)
	assert.Equal(t, "alpha", *res.Records[0][1].StringValue)
	assert.Equal(t, []string{"id", "name"}, []string{res.Columns[0].Name, res.Columns[1].Name})

	login := h.logins.Load()
	want := realLogin{Kind: kindMySQL, Addr: "db:3306", User: "master", Password: "s3cret", DBName: "app"}
	assert.Equal(t, want, *login)
}

func TestRealEngineTransactions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		commit    bool
		wantCount int64
	}{
		{name: "commit", commit: true, wantCount: 1},
		{name: "rollback", commit: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newItemsHarness(t)

			id, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
			require.NoError(t, err)

			_, err = h.exec(t, "INSERT INTO items (id, name) VALUES (1, 'a')", id)
			require.NoError(t, err)
			assert.Zero(t, h.count(t), "uncommitted row must be invisible outside the transaction")

			if tt.commit {
				_, err = h.b.CommitTransaction(h.ctx(t), id)
			} else {
				_, err = h.b.RollbackTransaction(h.ctx(t), id)
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantCount, h.count(t))

			_, err = h.exec(t, "SELECT 1", id)
			require.ErrorIs(t, err, ErrTransactionNotFound)
			assert.Empty(t, h.b.ListTransactions(h.ctx(t)))
		})
	}
}

func TestRealEngineBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		sets      [][]SQLParameter
		wantCount int64
		wantErr   bool
	}{
		{
			name:      "all_rows_land",
			sets:      [][]SQLParameter{{longParam("id", 1)}, {longParam("id", 2)}},
			wantCount: 2,
		},
		{
			name:    "failure_rolls_back_whole_batch",
			sets:    [][]SQLParameter{{longParam("id", 1)}, {longParam("id", 1)}},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newItemsHarness(t)

			results, err := h.b.BatchExecuteStatement(h.ctx(t), testClusterARN,
				"INSERT INTO items (id) VALUES (:id)", "", tt.sets)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, results, len(tt.sets))
			}

			assert.Equal(t, tt.wantCount, h.count(t))
			assert.Empty(t, h.b.real.txs)
		})
	}
}

func TestRealEngineRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resolverErr  error
		secrets      fakeSecrets
		wantCode     string
		wantOpens    int32
		wantSQLiteOK bool
	}{
		{name: "stub_cluster_uses_sqlite", resolverErr: ErrNotRealCluster, wantSQLiteOK: true},
		{
			name: "endpoint_disabled", resolverErr: ErrHTTPEndpointNotEnabled,
			wantCode: "HttpEndpointNotEnabledException",
		},
		{name: "not_ready", resolverErr: ErrClusterNotReady, wantCode: "InvalidResourceStateException"},
		{name: "secret_missing", secrets: fakeSecrets{}, wantCode: "SecretsErrorException"},
		{
			name: "secret_not_json", secrets: fakeSecrets{testSecretARN: "plain"},
			wantCode: "InvalidSecretException",
		},
		{
			name: "secret_without_password", secrets: fakeSecrets{testSecretARN: `{"username":"u"}`},
			wantCode: "InvalidSecretException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			secrets := tt.secrets
			if secrets == nil {
				secrets = fakeSecrets{testSecretARN: testCreds}
			}

			h := newRealHarness(t, tt.resolverErr, secrets)

			_, err := h.exec(t, "SELECT 1", "")
			if tt.wantSQLiteOK {
				require.NoError(t, err)
				assert.Zero(t, h.opens.Load())

				return
			}

			var api *apiError

			require.ErrorAs(t, err, &api)
			assert.Equal(t, tt.wantCode, api.code)
			assert.Zero(t, h.opens.Load())

			_, err = h.b.BeginTransaction(h.ctx(t), testClusterARN)
			require.ErrorAs(t, err, &api)
			assert.Empty(t, h.b.ListTransactions(h.ctx(t)))
		})
	}
}

func TestRealEngineTransactionBound(t *testing.T) {
	t.Parallel()

	h := newItemsHarness(t)
	h.b.real.maxTx = 2

	for range 2 {
		_, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
		require.NoError(t, err)
	}

	_, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
	require.ErrorIs(t, err, ErrValidation)
	assert.Len(t, h.b.ListTransactions(h.ctx(t)), 2)
}

func TestRealEngineTransactionExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		touchAt  time.Duration
		advance  time.Duration
		wantKept bool
	}{
		{name: "idle_past_timeout_rolled_back", advance: 4 * time.Minute},
		{name: "recent_activity_keeps_tx", touchAt: 2 * time.Minute, advance: 4 * time.Minute, wantKept: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newItemsHarness(t)
				h.b.WithClock(time.Now)

				id, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
				require.NoError(t, err)

				_, err = h.exec(t, "INSERT INTO items (id) VALUES (1)", id)
				require.NoError(t, err)

				if tt.touchAt > 0 {
					time.Sleep(tt.touchAt)

					_, err = h.exec(t, "SELECT 1", id)
					require.NoError(t, err)
				}

				time.Sleep(tt.advance - tt.touchAt)
				NewJanitor(h.b, 0, 0, 0).SweepOnce(t.Context())

				assert.Equal(t, tt.wantKept, h.b.real.hasTx(realTxKey("us-east-1", id)))

				if !tt.wantKept {
					_, err = h.exec(t, "SELECT 1", id)
					require.ErrorIs(t, err, ErrTransactionNotFound)
					assert.Zero(t, h.count(t), "expired transaction must be rolled back")
				}

				h.b.Close()
			})
		})
	}
}

func TestRealEngineCloseRollsBack(t *testing.T) {
	t.Parallel()

	h := newItemsHarness(t)

	id, err := h.b.BeginTransaction(h.ctx(t), testClusterARN)
	require.NoError(t, err)

	_, err = h.exec(t, "INSERT INTO items (id) VALUES (1)", id)
	require.NoError(t, err)

	h.b.real.rollbackAll()
	assert.Empty(t, h.b.real.txs)

	_, err = h.exec(t, "SELECT 1", id)
	require.ErrorIs(t, err, ErrTransactionNotFound)
	assert.Zero(t, h.count(t))
}
