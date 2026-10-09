package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetTablesAndDatabases_AttributesToGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantType  string
		wantDesc  string
		tableAttr []types.TableAttributes
		dbAttr    []types.DatabaseAttributes
		wantErr   bool
	}{
		{name: "none", wantType: "EXTERNAL_TABLE", wantDesc: "d"},
		{name: "name_only", tableAttr: []types.TableAttributes{types.TableAttributesName},
			dbAttr: []types.DatabaseAttributes{types.DatabaseAttributesName}},
		{name: "name_and_type", tableAttr: []types.TableAttributes{
			types.TableAttributesName, types.TableAttributesTableType,
		}, dbAttr: []types.DatabaseAttributes{types.DatabaseAttributesName}, wantType: "EXTERNAL_TABLE"},
		{name: "missing_name", tableAttr: []types.TableAttributes{types.TableAttributesTableType},
			dbAttr: []types.DatabaseAttributes{types.DatabaseAttributesTargetDatabase}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
				DatabaseInput: &types.DatabaseInput{Name: aws.String("db1"), Description: aws.String("d")},
			})
			require.NoError(t, err)

			_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
				DatabaseName: aws.String("db1"),
				TableInput: &types.TableInput{
					Name: aws.String("t1"), TableType: aws.String("EXTERNAL_TABLE"), Description: aws.String("d"),
				},
			})
			require.NoError(t, err)

			tables, tErr := client.GetTables(ctx, &gluesdk.GetTablesInput{
				DatabaseName: aws.String("db1"), AttributesToGet: tt.tableAttr,
			})
			dbs, dErr := client.GetDatabases(ctx, &gluesdk.GetDatabasesInput{AttributesToGet: tt.dbAttr})

			if tt.wantErr {
				require.Error(t, tErr)
				require.Error(t, dErr)

				return
			}

			require.NoError(t, tErr)
			require.NoError(t, dErr)
			require.Len(t, tables.TableList, 1)
			assert.Equal(t, "t1", aws.ToString(tables.TableList[0].Name))
			assert.Equal(t, tt.wantType, aws.ToString(tables.TableList[0].TableType))
			assert.Equal(t, tt.wantDesc, aws.ToString(tables.TableList[0].Description))
			require.Len(t, dbs.DatabaseList, 1)
			assert.Equal(t, tt.wantDesc, aws.ToString(dbs.DatabaseList[0].Description))
		})
	}
}
