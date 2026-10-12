package appsync_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

type idKeyedDDB struct {
	appsync.DynamoDBBackend
	items map[string]map[string]any
	mu    sync.Mutex
}

func idOf(item map[string]any) string {
	id, _ := item["id"].(map[string]any)
	s, _ := id["S"].(string)

	return s
}

func (d *idKeyedDDB) GetItemRaw(_ context.Context, _ string, key map[string]any) (map[string]any, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if it, ok := d.items[idOf(key)]; ok {
		return it, nil
	}

	return map[string]any{}, nil
}

func (d *idKeyedDDB) PutItemRaw(_ context.Context, _ string, item map[string]any, _ *appsync.DDBExpression) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.items == nil {
		d.items = map[string]map[string]any{}
	}

	d.items[idOf(item)] = item

	return nil
}

func TestExecuteGraphQL_DynamoDBResolver_PlainResults(t *testing.T) {
	t.Parallel()

	const (
		putReq = `{"version":"2018-05-29","operation":"PutItem",` +
			`"key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)},` +
			`"attributeValues":{"title":$util.dynamodb.toDynamoDBJson($ctx.args.title),` +
			`"n":{"N":"3"},"ok":{"BOOL":true}}}`
		getReq = `{"version":"2018-05-29","operation":"GetItem",` +
			`"key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)}}`
	)

	tests := []struct {
		want  string
		name  string
		query string
	}{
		{
			name:  "put returns written item",
			query: `mutation { putTodo(id: "1", title: "buy") { id title n ok } }`,
			want:  `{"id":"1","title":"buy","n":3,"ok":true}`,
		},
		{
			name:  "get missing is null",
			query: `query { getTodo(id: "nope") { id title } }`,
			want:  `null`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			b.SetDynamoDBBackend(&idKeyedDDB{})

			api, err := b.CreateGraphqlAPI("A", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, `
type Todo { id: ID! title: String n: Int ok: Boolean }
type Query { getTodo(id: ID!): Todo }
type Mutation { putTodo(id: ID!, title: String): Todo }`)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{
				Name: "ddb", Type: appsync.DataSourceTypeDynamoDB,
				DynamoDBConfig: &appsync.DynamoDBDataSourceConfig{TableName: "todos", AWSRegion: "us-east-1"},
			})
			require.NoError(t, err)

			for _, r := range []*appsync.Resolver{
				{FieldName: "putTodo", DataSourceName: "ddb", RequestMappingTemplate: putReq},
				{FieldName: "getTodo", DataSourceName: "ddb", RequestMappingTemplate: getReq},
			} {
				r.ResponseMappingTemplate = `$util.toJson($ctx.result)`
				typeName := "Query"
				if r.FieldName == "putTodo" {
					typeName = "Mutation"
				}

				_, err = b.CreateResolver(api.APIID, typeName, r)
				require.NoError(t, err)
			}

			auth := appsync.GraphQLAuth{APIKey: key.ID}
			out, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", nil, auth)
			require.NoError(t, err)

			require.Len(t, out, 1)

			for _, v := range out {
				got, mErr := json.Marshal(v)
				require.NoError(t, mErr)
				assert.JSONEq(t, tt.want, string(got))
			}
		})
	}
}

func TestExecuteGraphQL_ProjectsSelectionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "subset", query: `query { todo { id } }`, want: `{"todo":{"id":"1"}}`},
		{name: "alias", query: `query { todo { key: id title } }`, want: `{"todo":{"key":"1","title":"buy"}}`},
		{name: "typename", query: `query { todo { __typename id } }`, want: `{"todo":{"__typename":"Todo","id":"1"}}`},
		{name: "unset member is null", query: `query { todo { done } }`, want: `{"todo":{"done":null}}`},
		{name: "list", query: `query { todos { id } }`, want: `{"todos":[{"id":"1"},{"id":"2"}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("A", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)
			key, err := b.CreateAPIKey(api.APIID, "", 0)
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(api.APIID, `
type Todo { id: ID! title: String done: Boolean }
type Query { todo: Todo todos: [Todo] }`)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "none", Type: appsync.DataSourceTypeNone})
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "todo", DataSourceName: "none",
				RequestMappingTemplate:  `{"version":"2018-05-29","payload":{"id":"1","title":"buy"}}`,
				ResponseMappingTemplate: `$util.toJson($ctx.result)`,
			})
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "todos", DataSourceName: "none",
				RequestMappingTemplate: `{"version":"2018-05-29",` +
					`"payload":[{"id":"1","title":"a"},{"id":"2","title":"b"}]}`,
				ResponseMappingTemplate: `$util.toJson($ctx.result)`,
			})
			require.NoError(t, err)

			out, err := b.ExecuteGraphQL(t.Context(), api.APIID, tt.query, "", nil, appsync.GraphQLAuth{APIKey: key.ID})
			require.NoError(t, err)

			got, mErr := json.Marshal(out)
			require.NoError(t, mErr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
