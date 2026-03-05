package dynamodb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	dynamodbstreams "github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddb "github.com/blackbirdworks/gopherstack/dynamodb"
)

// newTestDB creates a minimal DynamoDB backend with a single test table.
func newTestDB(t *testing.T, tableName string) *ddb.InMemoryDB {
	t.Helper()

	db := ddb.NewInMemoryDB()

	_, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	return db
}

func TestUpdateTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T) *ddb.InMemoryDB
		input           *dynamodb.UpdateTableInput
		verify          func(t *testing.T, db *ddb.InMemoryDB, out *dynamodb.UpdateTableOutput)
		name            string
		wantErrContains string
		wantErr         bool
	}{
		{
			name: "provisioned_throughput_update",
			setup: func(t *testing.T) *ddb.InMemoryDB {
				t.Helper()

				return newTestDB(t, "my-table")
			},
			input: &dynamodb.UpdateTableInput{
				TableName: aws.String("my-table"),
				ProvisionedThroughput: &types.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(10),
					WriteCapacityUnits: aws.Int64(20),
				},
			},
			verify: func(t *testing.T, db *ddb.InMemoryDB, out *dynamodb.UpdateTableOutput) {
				t.Helper()
				require.NotNil(t, out.TableDescription)
				require.NotNil(t, out.TableDescription.ProvisionedThroughput)
				assert.EqualValues(t, 10, aws.ToInt64(out.TableDescription.ProvisionedThroughput.ReadCapacityUnits))
				assert.EqualValues(t, 20, aws.ToInt64(out.TableDescription.ProvisionedThroughput.WriteCapacityUnits))

				desc, err := db.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
					TableName: aws.String("my-table"),
				})
				require.NoError(t, err)
				assert.EqualValues(t, 10, aws.ToInt64(desc.Table.ProvisionedThroughput.ReadCapacityUnits))
				assert.EqualValues(t, 20, aws.ToInt64(desc.Table.ProvisionedThroughput.WriteCapacityUnits))
			},
		},
		{
			name: "enable_stream",
			setup: func(t *testing.T) *ddb.InMemoryDB {
				t.Helper()

				return newTestDB(t, "stream-table")
			},
			input: &dynamodb.UpdateTableInput{
				TableName: aws.String("stream-table"),
				StreamSpecification: &types.StreamSpecification{
					StreamEnabled:  aws.Bool(true),
					StreamViewType: types.StreamViewTypeNewImage,
				},
			},
			verify: func(t *testing.T, db *ddb.InMemoryDB, out *dynamodb.UpdateTableOutput) {
				t.Helper()
				assert.Equal(t, "stream-table", aws.ToString(out.TableDescription.TableName))

				outs, err := db.ListStreams(t.Context(), &dynamodbstreams.ListStreamsInput{
					TableName: aws.String("stream-table"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, outs.Streams, "stream should be available after enabling")
			},
		},
		{
			name: "create_gsi",
			setup: func(t *testing.T) *ddb.InMemoryDB {
				t.Helper()
				db := ddb.NewInMemoryDB()
				_, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{
					TableName: aws.String("gsi-table"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					},
					AttributeDefinitions: []types.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
						{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeS},
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
				})
				require.NoError(t, err)

				return db
			},
			input: &dynamodb.UpdateTableInput{
				TableName: aws.String("gsi-table"),
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
					{
						Create: &types.CreateGlobalSecondaryIndexAction{
							IndexName: aws.String("sk-index"),
							KeySchema: []types.KeySchemaElement{
								{AttributeName: aws.String("sk"), KeyType: types.KeyTypeHash},
							},
							Projection: &types.Projection{
								ProjectionType: types.ProjectionTypeAll,
							},
							ProvisionedThroughput: &types.ProvisionedThroughput{
								ReadCapacityUnits:  aws.Int64(5),
								WriteCapacityUnits: aws.Int64(5),
							},
						},
					},
				},
			},
			verify: func(t *testing.T, db *ddb.InMemoryDB, _ *dynamodb.UpdateTableOutput) {
				t.Helper()
				desc, err := db.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
					TableName: aws.String("gsi-table"),
				})
				require.NoError(t, err)
				require.Len(t, desc.Table.GlobalSecondaryIndexes, 1)
				assert.Equal(t, "sk-index", aws.ToString(desc.Table.GlobalSecondaryIndexes[0].IndexName))
			},
		},
		{
			name: "delete_gsi",
			setup: func(t *testing.T) *ddb.InMemoryDB {
				t.Helper()
				db := ddb.NewInMemoryDB()
				_, err := db.CreateTable(t.Context(), &dynamodb.CreateTableInput{
					TableName: aws.String("del-gsi-table"),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
					},
					AttributeDefinitions: []types.AttributeDefinition{
						{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
						{AttributeName: aws.String("gk"), AttributeType: types.ScalarAttributeTypeS},
					},
					GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
						{
							IndexName: aws.String("gk-index"),
							KeySchema: []types.KeySchemaElement{
								{AttributeName: aws.String("gk"), KeyType: types.KeyTypeHash},
							},
							Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
							ProvisionedThroughput: &types.ProvisionedThroughput{
								ReadCapacityUnits:  aws.Int64(5),
								WriteCapacityUnits: aws.Int64(5),
							},
						},
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
				})
				require.NoError(t, err)

				return db
			},
			input: &dynamodb.UpdateTableInput{
				TableName: aws.String("del-gsi-table"),
				GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{
					{
						Delete: &types.DeleteGlobalSecondaryIndexAction{
							IndexName: aws.String("gk-index"),
						},
					},
				},
			},
			verify: func(t *testing.T, db *ddb.InMemoryDB, _ *dynamodb.UpdateTableOutput) {
				t.Helper()
				desc, err := db.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
					TableName: aws.String("del-gsi-table"),
				})
				require.NoError(t, err)
				assert.Empty(t, desc.Table.GlobalSecondaryIndexes, "GSI should be removed after delete")
			},
		},
		{
			name: "not_found",
			setup: func(t *testing.T) *ddb.InMemoryDB {
				t.Helper()

				return ddb.NewInMemoryDB()
			},
			input: &dynamodb.UpdateTableInput{
				TableName: aws.String("no-such-table"),
			},
			wantErr:         true,
			wantErrContains: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := tt.setup(t)
			out, err := db.UpdateTable(t.Context(), tt.input)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrContains != "" {
					require.ErrorContains(t, err, tt.wantErrContains)
				}

				return
			}

			require.NoError(t, err)
			if tt.verify != nil {
				tt.verify(t, db, out)
			}
		})
	}
}
