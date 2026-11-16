package gcpspanner_test

import (
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDDL_PartialBatchReportsAppliedTimestamps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		batch []string
		want  int
	}{
		{"second fails", []string{"ALTER TABLE t ADD COLUMN b INT64", "ALTER TABLE nope ADD COLUMN x INT64"}, 1},
		{"first fails", []string{"DROP TABLE nope", "ALTER TABLE t ADD COLUMN b INT64"}, 0},
		{"third fails", []string{"ALTER TABLE t ADD COLUMN b INT64", "ALTER TABLE t ADD COLUMN c INT64", "bad"}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.createInstance(t, "ddl")
			db := e.createDB(t, "ddl", tblT)

			op, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
				Database: db.GetName(), Statements: tt.batch,
			})
			require.NoError(t, err)
			require.Error(t, op.Wait(t.Context()))

			meta, err := op.Metadata()
			require.NoError(t, err)
			assert.Len(t, meta.GetCommitTimestamps(), tt.want)
			assert.Len(t, meta.GetStatements(), len(tt.batch))
		})
	}
}
