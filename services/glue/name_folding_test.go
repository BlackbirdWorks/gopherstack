package glue_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestSDKRoundTrip_NamesFoldToLowercase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lookupDB string
		lookupTB string
	}{
		{name: "same case as created", lookupDB: "MyDB", lookupTB: "MyTable"},
		{name: "lowercase", lookupDB: "mydb", lookupTB: "mytable"},
		{name: "upper", lookupDB: "MYDB", lookupTB: "MYTABLE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestGlueClient(t, glue.NewHandler(glue.NewInMemoryBackend("000000000000", "us-east-1")))
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("MyDB")},
			})
			require.NoError(t, err)

			_, err = client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("mydb")},
			})
			var exists *types.AlreadyExistsException
			require.ErrorAs(t, err, &exists)

			db, err := client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{Name: aws.String(tt.lookupDB)})
			require.NoError(t, err)
			assert.Equal(t, "mydb", aws.ToString(db.Database.Name))

			_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
				DatabaseName: aws.String(tt.lookupDB),
				TableInput: &types.TableInput{
					Name:          aws.String("MyTable"),
					PartitionKeys: []types.Column{{Name: aws.String("Day")}},
				},
			})
			require.NoError(t, err)

			tbl, err := client.GetTable(ctx, &gluesdk.GetTableInput{
				DatabaseName: aws.String(tt.lookupDB), Name: aws.String(tt.lookupTB),
			})
			require.NoError(t, err)
			assert.Equal(t, "mytable", aws.ToString(tbl.Table.Name))
			assert.Equal(t, "mydb", aws.ToString(tbl.Table.DatabaseName))

			list, err := client.GetTables(ctx, &gluesdk.GetTablesInput{DatabaseName: aws.String(tt.lookupDB)})
			require.NoError(t, err)
			require.Len(t, list.TableList, 1)

			_, err = client.CreatePartition(ctx, &gluesdk.CreatePartitionInput{
				DatabaseName: aws.String(tt.lookupDB), TableName: aws.String(tt.lookupTB),
				PartitionInput: &types.PartitionInput{Values: []string{"2026-01-01"}},
			})
			require.NoError(t, err)

			part, err := client.GetPartition(ctx, &gluesdk.GetPartitionInput{
				DatabaseName: aws.String("mydb"), TableName: aws.String("mytable"), PartitionValues: []string{"2026-01-01"},
			})
			require.NoError(t, err)
			assert.Equal(t, "mytable", aws.ToString(part.Partition.TableName))

			_, err = client.DeleteTable(ctx, &gluesdk.DeleteTableInput{
				DatabaseName: aws.String(tt.lookupDB), Name: aws.String(tt.lookupTB),
			})
			require.NoError(t, err)

			_, err = client.DeleteDatabase(ctx, &gluesdk.DeleteDatabaseInput{Name: aws.String(tt.lookupDB)})
			require.NoError(t, err)

			_, err = client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{Name: aws.String("mydb")})
			var notFound *types.EntityNotFoundException
			require.ErrorAs(t, err, &notFound)
		})
	}
}

func TestRestore_FoldsMixedCaseNames(t *testing.T) {
	t.Parallel()

	src := glue.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := src.CreateDatabase(glue.DatabaseInput{Name: "legacydb"}, nil)
	require.NoError(t, err)
	_, err = src.CreateTable("legacydb", glue.TableInput{Name: "legacytable", PartitionKeys: []glue.Column{{Name: "d"}}})
	require.NoError(t, err)
	_, errs := src.BatchCreatePartition("legacydb", "legacytable", []glue.PartitionInput{{Values: []string{"v"}}})
	require.Empty(t, errs)

	snap := strings.NewReplacer("legacydb", "LegacyDB", "legacytable", "LegacyTable").Replace(string(src.Snapshot(t.Context())))

	dst := glue.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, dst.Restore(t.Context(), []byte(snap)))

	db, err := dst.GetDatabase("legacydb")
	require.NoError(t, err)
	assert.Equal(t, "legacydb", db.Name)

	tbl, err := dst.GetTable("LegacyDB", "LegacyTable")
	require.NoError(t, err)
	assert.Equal(t, "legacytable", tbl.Name)

	parts, err := dst.GetPartitions("legacydb", "legacytable")
	require.NoError(t, err)
	assert.Len(t, parts, 1)
}
