package timestreamquery_test

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

type fakeCatalog map[string][]string

func (f fakeCatalog) DatabaseExists(_, database string) bool {
	_, ok := f[database]

	return ok
}

func (f fakeCatalog) TableExists(_, database, table string) bool {
	return slices.Contains(f[database], table)
}

func TestCatalogTableReferences(t *testing.T) {
	t.Parallel()

	catalog := fakeCatalog{"db": {"tbl"}}

	tests := []struct {
		name    string
		query   string
		wantErr string
		wired   bool
	}{
		{name: "existing", query: "SELECT * FROM db.tbl", wired: true},
		{name: "quoted existing", query: `SELECT * FROM "db"."tbl" WHERE measure_name = 'x'`, wired: true},
		{name: "missing table", query: "SELECT * FROM db.nope", wantErr: "Table 'db.nope' does not exist", wired: true},
		{
			name:    "missing database",
			query:   "SELECT * FROM nodb.tbl",
			wantErr: "Database 'nodb' does not exist",
			wired:   true,
		},
		{name: "join missing", query: "SELECT * FROM db.tbl a JOIN db.nope b ON a.x = b.x",
			wantErr: "Table 'db.nope' does not exist", wired: true},
		{name: "cte unqualified", query: "WITH a AS (SELECT 1) SELECT * FROM a", wired: true},
		{name: "literal ignored", query: "SELECT 'FROM x.y' FROM db.tbl", wired: true},
		{name: "no from", query: "SELECT 1", wired: true},
		{name: "unwired missing", query: "SELECT * FROM nodb.nope"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.wired {
				b.SetCatalog(catalog)
			}

			_, qErr := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: tt.query})
			_, pErr := b.PrepareQuery(t.Context(), tt.query, true)
			_, sErr := b.CreateScheduledQuery(
				t.Context(), "sq", tt.query, "rate(1 hour)", "arn:aws:iam::000000000000:role/r",
				"", "", "", "", "", "", "", "", nil, nil,
			)

			if tt.wantErr == "" {
				require.NoError(t, qErr)
				require.NoError(t, pErr)
				require.NoError(t, sErr)

				return
			}

			for _, err := range []error{qErr, pErr, sErr} {
				require.ErrorIs(t, err, timestreamquery.ErrValidation)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}
