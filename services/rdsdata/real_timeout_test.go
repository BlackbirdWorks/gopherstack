package rdsdata //nolint:testpackage // white-box access to the real engine timeout.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const slowInsert = "INSERT INTO items (id) WITH RECURSIVE c(x) AS " +
	"(SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<200000) SELECT x FROM c"

func TestRealEngineContinueAfterTimeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resume   bool
		wantRows int64
	}{
		{name: "stops_by_default", wantRows: 0},
		{name: "continues_when_set", resume: true, wantRows: 200000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newItemsHarness(t)
			h.b.real.timeout = 5 * time.Millisecond

			ctx := withContinueAfterTimeout(h.ctx(t), tt.resume)
			_, _, _, _, err := h.b.ExecuteStatement(ctx, testClusterARN, slowInsert, "")
			require.ErrorIs(t, err, errStatementTimeout)

			h.b.real.timeout = realStatementTimeout

			if !tt.resume {
				assert.Zero(t, h.count(t))

				return
			}

			require.Eventually(t, func() bool {
				res, qerr := h.exec(t, "SELECT count(*) FROM items", "")

				return qerr == nil && *res.Records[0][0].LongValue == tt.wantRows
			}, 30*time.Second, 100*time.Millisecond)
		})
	}
}

func TestExecuteSQLRealEngine(t *testing.T) {
	t.Parallel()

	h := newItemsHarness(t)

	res, err := h.b.ExecuteSQL(h.ctx(t), testClusterARN,
		"INSERT INTO items (id, name) VALUES (1, 'a;b'); INSERT INTO items (id, name) VALUES (2, 'c');"+
			" SELECT id FROM items ORDER BY id")
	require.NoError(t, err)
	require.Len(t, res, 3)
	assert.Equal(t, int64(1), res[0].NumberOfRecordsUpdated)
	assert.Equal(t, int64(1), res[1].NumberOfRecordsUpdated)
	require.NotNil(t, res[2].ResultFrame)
	assert.Len(t, res[2].ResultFrame.Records, 2)
}

func TestSplitSQLStatements(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   string
		want []string
	}{
		{name: "two", in: "SELECT 1; SELECT 2;", want: []string{"SELECT 1", "SELECT 2"}},
		{name: "quoted", in: "SELECT ';' ; SELECT \"a;b\"", want: []string{"SELECT ';'", "SELECT \"a;b\""}},
		{name: "empty", in: " ; ;", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, splitSQLStatements(tt.in))
		})
	}
}
