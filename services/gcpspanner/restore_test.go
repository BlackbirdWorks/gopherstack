package gcpspanner_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListDatabaseOperations(t *testing.T) {
	t.Parallel()

	const typePrefix = "type.googleapis.com/google.spanner.admin.database.v1."

	e := newEnv(t)
	e.createInstance(t, "ops")
	e.createDB(t, "ops", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")

	ddl, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: parent + "/instances/ops/databases/app", Statements: []string{"CREATE INDEX i ON t (id)"},
	})
	require.NoError(t, err)
	require.NoError(t, ddl.Wait(t.Context()))

	tests := []struct {
		name    string
		parent  string
		filter  string
		want    int
		wantErr codes.Code
	}{
		{"all", parent + "/instances/ops", "", 2, codes.OK},
		{"create", parent + "/instances/ops", "metadata.@type=" + typePrefix + "CreateDatabaseMetadata", 1, codes.OK},
		{
			"ddl",
			parent + "/instances/ops",
			"metadata.@type:UpdateDatabaseDdlMetadata AND metadata.database:app",
			1,
			codes.OK,
		},
		{
			"restore none",
			parent + "/instances/ops",
			"metadata.@type=" + typePrefix + "RestoreDatabaseMetadata",
			0,
			codes.OK,
		},
		{"no instance", parent + "/instances/none", "", 0, codes.NotFound},
		{"bad filter", parent + "/instances/ops", "(", 0, codes.InvalidArgument},
		{"bad parent", "garbage", "", 0, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.databases.ListDatabaseOperations(
				t.Context(),
				&databasepb.ListDatabaseOperationsRequest{Parent: tt.parent, Filter: tt.filter},
			)

			n := 0

			for {
				_, nerr := it.Next()
				if errors.Is(nerr, iterator.Done) {
					break
				}

				if nerr != nil {
					require.Equal(t, tt.wantErr, status.Code(nerr))

					return
				}

				n++
			}

			require.Equal(t, codes.OK, tt.wantErr)
			assert.Equal(t, tt.want, n)
		})
	}
}
