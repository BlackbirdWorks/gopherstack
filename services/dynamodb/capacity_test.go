package dynamodb_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func assertConsumedCapacityBreakdown(
	t *testing.T,
	cc *types.ConsumedCapacity,
	tableName string,
	wantMinTotal float64,
	wantTable, wantGSI, wantLSI bool,
) {
	t.Helper()
	require.NotNil(t, cc)
	assert.Equal(t, tableName, aws.ToString(cc.TableName))
	assert.GreaterOrEqual(t, aws.ToFloat64(cc.CapacityUnits), wantMinTotal)

	if wantTable {
		assert.NotNil(t, cc.Table)
	} else {
		assert.Nil(t, cc.Table)
	}

	if wantGSI {
		require.NotNil(t, cc.GlobalSecondaryIndexes)
		_, hasGSI := cc.GlobalSecondaryIndexes["gsi1"]
		assert.True(t, hasGSI)
	} else {
		assert.Nil(t, cc.GlobalSecondaryIndexes)
	}

	if wantLSI {
		require.NotNil(t, cc.LocalSecondaryIndexes)
		_, hasLSI := cc.LocalSecondaryIndexes["lsi1"]
		assert.True(t, hasLSI)
	} else {
		assert.Nil(t, cc.LocalSecondaryIndexes)
	}
}

func TestConsumedCapacity_Indexes_TableDriven(t *testing.T) {
	t.Parallel()

	type ccArgs struct {
		reqCC types.ReturnConsumedCapacity
	}

	type ccWant struct {
		wantMinTotal float64
		wantTable    bool
		wantGSI      bool
		wantLSI      bool
		wantNil      bool
	}

	tests := []struct {
		name string
		args ccArgs
		want ccWant
	}{
		{
			name: "indexes_requested",
			args: ccArgs{
				reqCC: types.ReturnConsumedCapacityIndexes,
			},
			want: ccWant{
				wantMinTotal: 1.0,
				wantTable:    true,
				wantGSI:      true,
				wantLSI:      true,
				wantNil:      false,
			},
		},
		{
			name: "total_requested",
			args: ccArgs{
				reqCC: types.ReturnConsumedCapacityTotal,
			},
			want: ccWant{
				wantMinTotal: 1.0,
				wantTable:    false,
				wantGSI:      false,
				wantLSI:      false,
				wantNil:      false,
			},
		},
		{
			name: "none_requested",
			args: ccArgs{
				reqCC: types.ReturnConsumedCapacityNone,
			},
			want: ccWant{
				wantNil: true,
			},
		},
	}

	const (
		gsi1Name = "gsi1"
		lsi1Name = "lsi1"
	)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := newInMemoryTestDB(t)

			tableName := "CCTest_" + strings.ReplaceAll(tt.name, " ", "_")
			_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeS},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String(gsi1Name),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				LocalSecondaryIndexes: []types.LocalSecondaryIndex{
					{
						IndexName: aws.String(lsi1Name),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
							{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			// 1. PutItem with GSI and LSI keys
			putOut, err := db.PutItem(ctx, &dynamodb_sdk.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk":     &types.AttributeValueMemberS{Value: "k1"},
					"sk":     &types.AttributeValueMemberS{Value: "s1"},
					"gsi_pk": &types.AttributeValueMemberS{Value: "g1"},
					"lsi_sk": &types.AttributeValueMemberS{Value: "l1"},
					"val":    &types.AttributeValueMemberS{Value: "hello"},
				},
				ReturnConsumedCapacity: tt.args.reqCC,
			})
			require.NoError(t, err)

			if tt.want.wantNil {
				assert.Nil(t, putOut.ConsumedCapacity)
			} else {
				assertConsumedCapacityBreakdown(
					t,
					putOut.ConsumedCapacity,
					tableName,
					tt.want.wantMinTotal,
					tt.want.wantTable,
					tt.want.wantGSI,
					tt.want.wantLSI,
				)
			}

			// 2. Query on GSI
			qOut, err := db.Query(ctx, &dynamodb_sdk.QueryInput{
				TableName:              aws.String(tableName),
				IndexName:              aws.String(gsi1Name),
				KeyConditionExpression: aws.String("gsi_pk = :g"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":g": &types.AttributeValueMemberS{Value: "g1"},
				},
				ReturnConsumedCapacity: tt.args.reqCC,
			})
			require.NoError(t, err)

			if tt.want.wantNil {
				assert.Nil(t, qOut.ConsumedCapacity)
			} else if tt.want.wantGSI {
				require.NotNil(t, qOut.ConsumedCapacity)
				require.NotNil(t, qOut.ConsumedCapacity.GlobalSecondaryIndexes)
				_, hasGSI := qOut.ConsumedCapacity.GlobalSecondaryIndexes[gsi1Name]
				assert.True(t, hasGSI)
			}

			// 3. Scan on LSI
			scanOut, err := db.Scan(ctx, &dynamodb_sdk.ScanInput{
				TableName:              aws.String(tableName),
				IndexName:              aws.String(lsi1Name),
				ReturnConsumedCapacity: tt.args.reqCC,
			})
			require.NoError(t, err)

			if tt.want.wantNil {
				assert.Nil(t, scanOut.ConsumedCapacity)
			} else if tt.want.wantLSI {
				require.NotNil(t, scanOut.ConsumedCapacity)
				require.NotNil(t, scanOut.ConsumedCapacity.LocalSecondaryIndexes)
				_, hasLSI := scanOut.ConsumedCapacity.LocalSecondaryIndexes[lsi1Name]
				assert.True(t, hasLSI)
			}

			// 4. UpdateItem
			updateOut, err := db.UpdateItem(ctx, &dynamodb_sdk.UpdateItemInput{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k1"},
					"sk": &types.AttributeValueMemberS{Value: "s1"},
				},
				UpdateExpression: aws.String("SET val = :newval"),
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":newval": &types.AttributeValueMemberS{Value: "world"},
				},
				ReturnConsumedCapacity: tt.args.reqCC,
			})
			require.NoError(t, err)

			if tt.want.wantNil {
				assert.Nil(t, updateOut.ConsumedCapacity)
			} else {
				require.NotNil(t, updateOut.ConsumedCapacity)
			}

			// 5. DeleteItem
			delOut, err := db.DeleteItem(ctx, &dynamodb_sdk.DeleteItemInput{
				TableName: aws.String(tableName),
				Key: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "k1"},
					"sk": &types.AttributeValueMemberS{Value: "s1"},
				},
				ReturnConsumedCapacity: tt.args.reqCC,
			})
			require.NoError(t, err)

			if tt.want.wantNil {
				assert.Nil(t, delOut.ConsumedCapacity)
			} else {
				require.NotNil(t, delOut.ConsumedCapacity)
			}
		})
	}
}

func TestConsistentRead_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		op             string
		indexName      string
		consistentRead bool
		wantErr        bool
	}{
		{name: "getitem_consistent", op: "GetItem", consistentRead: true, wantErr: false},
		{name: "getitem_eventual", op: "GetItem", consistentRead: false, wantErr: false},
		{name: "query_primary_consistent", op: "Query", indexName: "", consistentRead: true, wantErr: false},
		{name: "query_lsi_consistent", op: "Query", indexName: "lsi1", consistentRead: true, wantErr: false},
		{name: "query_gsi_consistent_rejected", op: "Query", indexName: "gsi1", consistentRead: true, wantErr: true},
		{name: "scan_consistent", op: "Scan", consistentRead: true, wantErr: false},
		{name: "scan_eventual", op: "Scan", consistentRead: false, wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			db := newInMemoryTestDB(t)

			tableName := "CRTable_" + strings.ReplaceAll(tt.name, " ", "_")
			_, err := db.CreateTable(ctx, &dynamodb_sdk.CreateTableInput{
				TableName: aws.String(tableName),
				KeySchema: []types.KeySchemaElement{
					{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
				},
				AttributeDefinitions: []types.AttributeDefinition{
					{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("gsi_pk"), AttributeType: types.ScalarAttributeTypeS},
					{AttributeName: aws.String("lsi_sk"), AttributeType: types.ScalarAttributeTypeS},
				},
				GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
					{
						IndexName: aws.String("gsi1"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("gsi_pk"), KeyType: types.KeyTypeHash},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				LocalSecondaryIndexes: []types.LocalSecondaryIndex{
					{
						IndexName: aws.String("lsi1"),
						KeySchema: []types.KeySchemaElement{
							{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
							{AttributeName: aws.String("lsi_sk"), KeyType: types.KeyTypeRange},
						},
						Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
					},
				},
				BillingMode: types.BillingModePayPerRequest,
			})
			require.NoError(t, err)

			putTestItem(t, db, tableName, map[string]types.AttributeValue{
				"pk":     &types.AttributeValueMemberS{Value: "pk1"},
				"sk":     &types.AttributeValueMemberS{Value: "sk1"},
				"gsi_pk": &types.AttributeValueMemberS{Value: "g1"},
				"lsi_sk": &types.AttributeValueMemberS{Value: "l1"},
			})

			switch tt.op {
			case "GetItem":
				out, gErr := db.GetItem(ctx, &dynamodb_sdk.GetItemInput{
					TableName:      aws.String(tableName),
					ConsistentRead: aws.Bool(tt.consistentRead),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "pk1"},
						"sk": &types.AttributeValueMemberS{Value: "sk1"},
					},
				})
				if tt.wantErr {
					require.Error(t, gErr)
				} else {
					require.NoError(t, gErr)
					require.NotNil(t, out.Item)
				}
			case "Query":
				qInput := &dynamodb_sdk.QueryInput{
					TableName:              aws.String(tableName),
					ConsistentRead:         aws.Bool(tt.consistentRead),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "pk1"},
					},
				}
				if tt.indexName != "" {
					qInput.IndexName = aws.String(tt.indexName)
					if tt.indexName == "gsi1" {
						qInput.KeyConditionExpression = aws.String("gsi_pk = :g")
						qInput.ExpressionAttributeValues = map[string]types.AttributeValue{
							":g": &types.AttributeValueMemberS{Value: "g1"},
						}
					}
				}
				_, qErr := db.Query(ctx, qInput)
				if tt.wantErr {
					require.Error(t, qErr)
					assert.Contains(t, qErr.Error(), "ValidationException")
				} else {
					require.NoError(t, qErr)
				}
			case "Scan":
				_, sErr := db.Scan(ctx, &dynamodb_sdk.ScanInput{
					TableName:      aws.String(tableName),
					ConsistentRead: aws.Bool(tt.consistentRead),
				})
				if tt.wantErr {
					require.Error(t, sErr)
				} else {
					require.NoError(t, sErr)
				}
			}
		})
	}
}

func TestBuildConsumedCapacityWithIndexes_Unit(t *testing.T) {
	t.Parallel()

	type builderArgs struct {
		gsiRCU    map[string]float64
		lsiRCU    map[string]float64
		tableName string
		req       types.ReturnConsumedCapacity
		tableRCU  float64
		tableWCU  float64
	}

	type builderWant struct {
		wantTotal float64
		wantNil   bool
		wantTable bool
		wantGSI   bool
		wantLSI   bool
	}

	tests := []struct {
		name string
		args builderArgs
		want builderWant
	}{
		{
			name: "total_mode",
			args: builderArgs{
				tableName: "myTable",
				req:       types.ReturnConsumedCapacityTotal,
				tableRCU:  1.0,
				gsiRCU:    map[string]float64{"gsi1": 1.0},
			},
			want: builderWant{
				wantNil:   false,
				wantTotal: 2.0,
				wantTable: false,
				wantGSI:   false,
			},
		},
		{
			name: "indexes_mode",
			args: builderArgs{
				tableName: "myTable",
				req:       types.ReturnConsumedCapacityIndexes,
				tableRCU:  1.0,
				gsiRCU:    map[string]float64{"gsi1": 0.5},
				lsiRCU:    map[string]float64{"lsi1": 0.5},
			},
			want: builderWant{
				wantNil:   false,
				wantTotal: 2.0,
				wantTable: true,
				wantGSI:   true,
				wantLSI:   true,
			},
		},
		{
			name: "none_mode",
			args: builderArgs{
				tableName: "myTable",
				req:       types.ReturnConsumedCapacityNone,
				tableRCU:  1.0,
			},
			want: builderWant{
				wantNil: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cc := dynamodb.BuildConsumedCapacityWithIndexes(
				tt.args.tableName,
				tt.args.req,
				tt.args.tableRCU, tt.args.tableWCU,
				tt.args.gsiRCU, nil,
				tt.args.lsiRCU, nil,
			)
			if tt.want.wantNil {
				assert.Nil(t, cc)

				return
			}
			require.NotNil(t, cc)
			assert.Equal(t, tt.args.tableName, aws.ToString(cc.TableName))
			assert.InDelta(t, tt.want.wantTotal, aws.ToFloat64(cc.CapacityUnits), 1e-9)
			if tt.want.wantTable {
				assert.NotNil(t, cc.Table)
			} else {
				assert.Nil(t, cc.Table)
			}
			if tt.want.wantGSI {
				assert.NotNil(t, cc.GlobalSecondaryIndexes)
			} else {
				assert.Nil(t, cc.GlobalSecondaryIndexes)
			}
			if tt.want.wantLSI {
				assert.NotNil(t, cc.LocalSecondaryIndexes)
			} else {
				assert.Nil(t, cc.LocalSecondaryIndexes)
			}
		})
	}
}
