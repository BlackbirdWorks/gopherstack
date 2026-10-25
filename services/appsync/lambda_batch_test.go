package appsync_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appsyncsdk "github.com/aws/aws-sdk-go-v2/service/appsync"
	appsynctypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchInvoker struct {
	respond func(items []any) any
	sizes   []int
	mu      sync.Mutex
}

func (b *batchInvoker) InvokeFunction(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
	if name == "arn:aws:lambda:us-east-1:000000000000:function:posts" {
		return []byte(
			`[{"id":"p1","n":1},{"id":"p2","n":2},{"id":"p3","n":3},{"id":"p4","n":4},{"id":"p5","n":5}]`,
		), 200, nil
	}

	var items []any
	if err := json.Unmarshal(payload, &items); err != nil {
		return nil, 0, err
	}

	b.mu.Lock()
	b.sizes = append(b.sizes, len(items))
	b.mu.Unlock()

	out, err := json.Marshal(b.respond(items))

	return out, 200, err
}

func TestLambdaBatchInvoke(t *testing.T) {
	t.Parallel()

	const (
		toJSON      = `$util.toJson($ctx.result)`
		batchReqVTL = `{"version":"2018-05-29","operation":"BatchInvoke","payload":{"id":$util.toJson($ctx.source.id)}}`
	)

	echoData := func(items []any) any {
		out := make([]any, len(items))
		for i, it := range items {
			id := it.(map[string]any)["source"].(map[string]any)["id"]
			out[i] = map[string]any{"data": "rel-" + id.(string)}
		}

		return out
	}

	tests := []struct {
		respond   func(items []any) any
		resolver  appsyncsdk.CreateResolverInput
		name      string
		wantData  []any
		wantSizes []int
		wantErrs  int
	}{
		{
			name:      "direct batches of two",
			resolver:  appsyncsdk.CreateResolverInput{MaxBatchSize: 2},
			respond:   echoData,
			wantSizes: []int{2, 2, 1},
			wantData:  []any{"rel-p1", "rel-p2", "rel-p3", "rel-p4", "rel-p5"},
		},
		{
			name:     "direct per item error",
			resolver: appsyncsdk.CreateResolverInput{MaxBatchSize: 10},
			respond: func(items []any) any {
				out := echoData(items).([]any)
				out[1] = map[string]any{"data": nil, "errorMessage": "Not found", "errorType": "ERROR"}

				return out
			},
			wantSizes: []int{5},
			wantData:  []any{"rel-p1", nil, "rel-p3", "rel-p4", "rel-p5"},
			wantErrs:  1,
		},
		{
			name: "explicit template",
			resolver: appsyncsdk.CreateResolverInput{
				MaxBatchSize:            3,
				RequestMappingTemplate:  aws.String(batchReqVTL),
				ResponseMappingTemplate: aws.String(`$util.toJson($ctx.result.data)`),
			},
			respond: func(items []any) any {
				out := make([]any, len(items))
				for i, it := range items {
					out[i] = map[string]any{"data": "x-" + it.(map[string]any)["id"].(string)}
				}

				return out
			},
			wantSizes: []int{3, 2},
			wantData:  []any{"x-p1", "x-p2", "x-p3", "x-p4", "x-p5"},
		},
		{
			name:      "length mismatch fails",
			resolver:  appsyncsdk.CreateResolverInput{MaxBatchSize: 10},
			respond:   func([]any) any { return []any{} },
			wantSizes: []int{5},
			wantData:  nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler()
			inv := &batchInvoker{respond: tt.respond}
			b.SetLambdaInvoker(inv)
			client := newTestAppsyncClient(t, h)

			api, err := client.CreateGraphqlApi(t.Context(), &appsyncsdk.CreateGraphqlApiInput{
				Name: aws.String("b"), AuthenticationType: appsynctypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)
			apiID := aws.ToString(api.GraphqlApi.ApiId)

			key, err := client.CreateApiKey(t.Context(), &appsyncsdk.CreateApiKeyInput{ApiId: aws.String(apiID)})
			require.NoError(t, err)

			_, err = b.StartSchemaCreation(apiID, `
type Post { id: ID n: Int related: String }
type Query { posts: [Post] }`)
			require.NoError(t, err)

			for name, fn := range map[string]string{"Posts": "posts", "Rel": "rel"} {
				_, err = client.CreateDataSource(t.Context(), &appsyncsdk.CreateDataSourceInput{
					ApiId: aws.String(apiID), Name: aws.String(name), Type: appsynctypes.DataSourceTypeAwsLambda,
					LambdaConfig: &appsynctypes.LambdaDataSourceConfig{
						LambdaFunctionArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:" + fn),
					},
				})
				require.NoError(t, err)
			}

			_, err = client.CreateResolver(t.Context(), &appsyncsdk.CreateResolverInput{
				ApiId: aws.String(apiID), TypeName: aws.String("Query"), FieldName: aws.String("posts"),
				DataSourceName: aws.String("Posts"),
			})
			require.NoError(t, err)

			in := tt.resolver
			in.ApiId, in.TypeName, in.FieldName = aws.String(apiID), aws.String("Post"), aws.String("related")
			in.DataSourceName = aws.String("Rel")
			_, err = client.CreateResolver(t.Context(), &in)
			require.NoError(t, err)

			rec := doRequestWithHeaders(t, h, "/v1/apis/"+apiID+"/graphql",
				map[string]any{"query": `query { posts { id related } }`},
				map[string]string{"X-Api-Key": aws.ToString(key.ApiKey.Id)})

			var reply struct {
				Data   map[string]any `json:"data"`
				Errors []any          `json:"errors"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &reply), rec.Body.String())

			assert.Equal(t, tt.wantSizes, inv.sizes)
			assert.Len(t, reply.Errors, max(tt.wantErrs, boolInt(tt.wantData == nil)))

			if tt.wantData == nil {
				return
			}

			posts, _ := reply.Data["posts"].([]any)
			got := make([]any, len(posts))
			for i, p := range posts {
				got[i] = p.(map[string]any)["related"]
			}

			assert.Equal(t, tt.wantData, got)
		})
	}
}

func boolInt(b bool) int {
	if b {
		return 1
	}

	return 0
}
