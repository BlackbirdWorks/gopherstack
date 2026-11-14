package glue_test

import (
	"testing"

	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestResourceShareType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		share     types.ResourceShareType
		wantCount int
		wantErr   bool
	}{
		{name: "unset", share: "", wantCount: 1},
		{name: "all", share: types.ResourceShareTypeAll, wantCount: 1},
		{name: "foreign", share: types.ResourceShareTypeForeign, wantCount: 0},
		{name: "federated", share: types.ResourceShareTypeFederated, wantCount: 0},
		{name: "invalid", share: "BOGUS", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			_, err := backend.CreateDatabase(glue.DatabaseInput{Name: "db1"}, nil)
			require.NoError(t, err)
			_, err = backend.CreateTable("db1", glue.TableInput{Name: "widget"})
			require.NoError(t, err)

			client := newTestGlueClient(t, glue.NewHandler(backend))

			dbs, dbErr := client.GetDatabases(t.Context(), &gluesdk.GetDatabasesInput{ResourceShareType: tc.share})
			tbls, tblErr := client.SearchTables(t.Context(), &gluesdk.SearchTablesInput{ResourceShareType: tc.share})

			if tc.wantErr {
				require.Error(t, dbErr)
				require.Error(t, tblErr)

				return
			}

			require.NoError(t, dbErr)
			require.NoError(t, tblErr)
			assert.Len(t, dbs.DatabaseList, tc.wantCount)
			assert.Len(t, tbls.TableList, tc.wantCount)
		})
	}
}
