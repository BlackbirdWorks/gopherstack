package appsync_test

import (
	"context"
	"math"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

type limitCapturingDDB struct {
	appsync.DynamoDBBackend
	limit atomic.Int32
}

func (d *limitCapturingDDB) ScanRaw(_ context.Context, q *appsync.DDBQuery) (*appsync.DDBPage, error) {
	d.limit.Store(q.Limit)

	return &appsync.DDBPage{}, nil
}

func TestExecuteGraphQL_DynamoDBScan_LimitBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		limit string
		want  int32
	}{
		{name: "in range", limit: "25", want: 25},
		{name: "zero uses default", limit: "0", want: 100},
		{name: "negative uses default", limit: "-5", want: 100},
		{name: "max int32", limit: "2147483647", want: math.MaxInt32},
		{name: "clamps instead of truncating to one", limit: "4294967297", want: math.MaxInt32},
		{name: "clamps instead of truncating negative", limit: "4294967295", want: math.MaxInt32},
		{name: "beyond int64", limit: "99999999999999999999", want: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ddb := &limitCapturingDDB{}
			b := newTestBackend()
			b.SetDynamoDBBackend(ddb)

			api, err := b.CreateGraphqlAPI("A", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, `
type Todo { id: ID! }
type Query { listTodos: [Todo] }`)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "ddb", Type: appsync.DataSourceTypeDynamoDB,
				DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "todos", AWSRegion: "us-east-1"},
			})
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "listTodos", DataSourceName: "ddb",
				RequestMappingTemplate:  `{"version":"2018-05-29","operation":"Scan","limit":` + tt.limit + `}`,
				ResponseMappingTemplate: `$util.toJson($ctx.result.items)`,
			})
			require.NoError(t, err)

			_, err = b.ExecuteGraphQL(t.Context(), api.APIID, `query { listTodos { id } }`, "", nil,
				appsync.GraphQLAuth{APIKey: key.ID})
			require.NoError(t, err)

			assert.Equal(t, tt.want, ddb.limit.Load())
		})
	}
}
