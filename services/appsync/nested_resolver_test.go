package appsync_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestExecuteGraphQL_NestedResolvers(t *testing.T) {
	t.Parallel()

	const (
		postObj      = `{"id":"p1","authorId":"a7"}`
		postReq      = `{"payload":` + postObj + `}`
		toJSONResult = `$util.toJson($ctx.result)`
		authorIDVTL  = `{"payload":{"id":$util.toJson($ctx.source.authorId)}}`
		authorVTL    = `{"payload":{"id":$util.toJson($ctx.source.authorId),"label":"by $ctx.source.id"}}`
		authorJS     = `export function request(ctx) { return { payload: { id: ctx.source.authorId } }; }` +
			` export function response(ctx) { return ctx.result; }`
	)

	jsRuntime := &appsync.Runtime{Name: "APPSYNC_JS", RuntimeVersion: "1.0.0"}

	tests := []struct {
		author *appsync.Resolver
		name   string
		query  string
		want   string
	}{
		{
			name:   "vtl source",
			author: &appsync.Resolver{RequestMappingTemplate: authorVTL},
			query:  `query { post { id author { id label } } }`,
			want:   `{"post":{"id":"p1","author":{"id":"a7","label":"by p1"}}}`,
		},
		{
			name:   "js source",
			author: &appsync.Resolver{Code: authorJS, Runtime: jsRuntime},
			query:  `query { post { author { id } } }`,
			want:   `{"post":{"author":{"id":"a7"}}}`,
		},
		{
			name:   "list parent runs child per item",
			author: &appsync.Resolver{RequestMappingTemplate: authorIDVTL},
			query:  `query { posts { id author { id } } }`,
			want:   `{"posts":[{"id":"p1","author":{"id":"a7"}},{"id":"p1","author":{"id":"a7"}}]}`,
		},
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
type Author { id: ID label: String }
type Post { id: ID authorId: ID author: Author }
type Query { post: Post posts: [Post] }`)
			require.NoError(t, err)

			_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "none", Type: appsync.DataSourceTypeNone})
			require.NoError(t, err)

			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "post", DataSourceName: "none", RequestMappingTemplate: postReq,
				ResponseMappingTemplate: toJSONResult,
			})
			require.NoError(t, err)
			_, err = b.CreateResolver(api.APIID, "Query", &appsync.Resolver{
				FieldName: "posts", DataSourceName: "none",
				RequestMappingTemplate:  `{"payload":[` + postObj + `,` + postObj + `]}`,
				ResponseMappingTemplate: toJSONResult,
			})
			require.NoError(t, err)

			author := *tt.author
			author.FieldName, author.DataSourceName = "author", "none"
			if author.Code == "" {
				author.ResponseMappingTemplate = toJSONResult
			}
			_, err = b.CreateResolver(api.APIID, "Post", &author)
			require.NoError(t, err)

			out, err := b.ExecuteGraphQL(
				t.Context(), api.APIID, tt.query, "", nil, appsync.GraphQLAuth{APIKey: key.ID},
			)
			require.NoError(t, err)

			got, mErr := json.Marshal(out)
			require.NoError(t, mErr)
			assert.JSONEq(t, tt.want, string(got))
		})
	}
}
