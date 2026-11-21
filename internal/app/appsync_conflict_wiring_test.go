package app

import (
	"context"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
)

const (
	asCFSchema = `
type Item {
  id: ID! title: String tags: [String] labels: [String]
  _version: Int _lastChangedAt: Float _deleted: Boolean
}
type Query { getItem(id: ID!): Item }
type Mutation {
  putItem(id: ID!, title: String, tags: [String], labels: [String], v: Int): Item
  updateItem(id: ID!, title: String, v: Int): Item
  deleteItem(id: ID!, v: Int): Item
  rawPut(id: ID!, attrs: String, v: Int): Item
  rawUpdate(id: ID!, expr: String, names: String, vals: String, v: Int): Item
}`
	asCFKey = `"key":{"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)}`
	asCFPre = `{"version":"2018-05-29",`
	asCFArn = "arn:aws:lambda:us-east-1:000000000000:function:resolver"
)

var errLambdaNotFound = errors.New("function not found")

type asCFLambda struct {
	fn       func(payload []byte) ([]byte, error)
	payloads [][]byte
	mu       sync.Mutex
}

func (f *asCFLambda) InvokeFunction(_ context.Context, _, _ string, payload []byte) ([]byte, int, error) {
	f.mu.Lock()
	f.payloads = append(f.payloads, payload)
	f.mu.Unlock()

	out, err := f.fn(payload)

	return out, http.StatusOK, err
}

func (f *asCFLambda) captured() [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([][]byte(nil), f.payloads...)
}

type asCFFixture struct {
	fx    *sfnFixture
	apiID string
	key   string
}

func newASCFFixture(t *testing.T, sync *astypes.SyncConfig, lambda appsyncbackend.LambdaInvoker) *asCFFixture {
	t.Helper()

	fx := newSFNFixture(t)
	ctx := t.Context()
	ddb := sdkddb.NewFromConfig(fx.cfg)
	as := appsync.NewFromConfig(fx.cfg)

	_, err := ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String("items"), BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbsdktypes.KeyTypeHash}},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbsdktypes.ScalarAttributeTypeS},
		},
	})
	require.NoError(t, err)

	if lambda != nil {
		h, ok := serviceByName(fx.services)["AppSync"].(*appsyncbackend.Handler)
		require.True(t, ok)

		bk, ok := h.Backend.(*appsyncbackend.InMemoryBackend)
		require.True(t, ok)
		bk.SetLambdaInvoker(lambda)
	}

	api, err := as.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
		Name: aws.String("cf"), AuthenticationType: astypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	key, err := as.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: api.GraphqlApi.ApiId})
	require.NoError(t, err)

	_, err = as.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{
		ApiId: api.GraphqlApi.ApiId, Definition: []byte(asCFSchema),
	})
	require.NoError(t, err)

	_, err = as.CreateDataSource(ctx, &appsync.CreateDataSourceInput{
		ApiId: api.GraphqlApi.ApiId, Name: aws.String("items"), Type: astypes.DataSourceTypeAmazonDynamodb,
		ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		DynamodbConfig: &astypes.DynamodbDataSourceConfig{
			TableName: aws.String("items"), AwsRegion: aws.String("us-east-1"), Versioned: true,
		},
	})
	require.NoError(t, err)

	resolvers := map[string]string{
		"getItem": asCFPre + `"operation":"GetItem",` + asCFKey + `}`,
		"putItem": asCFPre + `"operation":"PutItem",` + asCFKey + `,"attributeValues":{` +
			`"title":$util.dynamodb.toDynamoDBJson($ctx.args.title),` +
			`"tags":$util.dynamodb.toDynamoDBJson($ctx.args.tags),` +
			`"labels":$util.dynamodb.toStringSetJson($ctx.args.labels)},"_version":$util.toJson($ctx.args.v)}`,
		"updateItem": asCFPre + `"operation":"UpdateItem",` + asCFKey + `,"update":{"expression":"SET title = :t",` +
			`"expressionValues":{":t":$util.dynamodb.toDynamoDBJson($ctx.args.title)}},"_version":$util.toJson($ctx.args.v)}`,
		"deleteItem": asCFPre + `"operation":"DeleteItem",` + asCFKey + `,"_version":$util.toJson($ctx.args.v)}`,
		"rawPut": asCFPre + `"operation":"PutItem",` + asCFKey + `,"attributeValues":$ctx.args.attrs,` +
			`"_version":$util.toJson($ctx.args.v)}`,
		"rawUpdate": asCFPre + `"operation":"UpdateItem",` + asCFKey + `,"update":{"expression":` +
			`$util.toJson($ctx.args.expr),"expressionNames":$ctx.args.names,"expressionValues":$ctx.args.vals},` +
			`"_version":$util.toJson($ctx.args.v)}`,
	}

	for field, req := range resolvers {
		typeName := "Mutation"
		if field == "getItem" {
			typeName = "Query"
		}

		_, err = as.CreateResolver(ctx, &appsync.CreateResolverInput{
			ApiId: api.GraphqlApi.ApiId, TypeName: aws.String(typeName), FieldName: aws.String(field),
			DataSourceName: aws.String("items"), RequestMappingTemplate: aws.String(req), SyncConfig: sync,
		})
		require.NoError(t, err)
	}

	return &asCFFixture{fx: fx, apiID: aws.ToString(api.GraphqlApi.ApiId), key: aws.ToString(key.ApiKey.Id)}
}

func (f *asCFFixture) do(t *testing.T, query string) asGQLResponse {
	t.Helper()

	body, err := json.Marshal(map[string]any{"query": query}, jsonx.V1())
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+f.apiID+"/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", f.key)

	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out asGQLResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()), rec.Body.String())

	return out
}

const asCFSel = `{ id title tags labels _version }`

// seed writes version 1 then 2 so a write based on version 1 is stale.
func (f *asCFFixture) seed(t *testing.T) {
	t.Helper()

	res := f.do(t, `mutation { putItem(id:"i1", title:"Nadia", tags:["a"], labels:["x","y"]) `+asCFSel+` }`)
	require.Empty(t, res.Errors)
	res = f.do(t, `mutation { updateItem(id:"i1", title:"Nadia", v:1) `+asCFSel+` }`)
	require.Empty(t, res.Errors)
}

func asCFSync(handler string, lambdaARN string) *astypes.SyncConfig {
	cfg := &astypes.SyncConfig{
		ConflictDetection: astypes.ConflictDetectionTypeVersion,
		ConflictHandler:   astypes.ConflictHandlerType(handler),
	}
	if lambdaARN != "" {
		cfg.LambdaConflictHandlerConfig = &astypes.LambdaConflictHandlerConfig{
			LambdaConflictHandlerArn: aws.String(lambdaARN),
		}
	}

	return cfg
}

func TestAppSyncAutomergeConflictHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, f *asCFFixture)
		name  string
	}{
		{
			name: "stale put merges scalars lists and sets",
			check: func(t *testing.T, f *asCFFixture) {
				t.Helper()

				res := f.do(
					t,
					`mutation { putItem(id:"i1", title:"Other", tags:["b"], labels:["y","z"], v:1) `+asCFSel+` }`,
				)
				require.Empty(t, res.Errors)

				got, _ := res.Data["putItem"].(map[string]any)
				assert.Equal(t, "Nadia", got["title"])
				assert.Equal(t, []any{"a", "b"}, got["tags"])
				assert.ElementsMatch(t, []any{"x", "y", "z"}, got["labels"])
				assert.InDelta(t, 3, got["_version"], 0)
			},
		},
		{
			name: "stale update keeps the existing scalar",
			check: func(t *testing.T, f *asCFFixture) {
				t.Helper()

				res := f.do(t, `mutation { updateItem(id:"i1", title:"Changed", v:1) `+asCFSel+` }`)
				require.Empty(t, res.Errors)

				got, _ := res.Data["updateItem"].(map[string]any)
				assert.Equal(t, "Nadia", got["title"])
				assert.InDelta(t, 3, got["_version"], 0)
			},
		},
		{
			name: "matching version writes normally",
			check: func(t *testing.T, f *asCFFixture) {
				t.Helper()

				res := f.do(t, `mutation { updateItem(id:"i1", title:"Changed", v:2) `+asCFSel+` }`)
				require.Empty(t, res.Errors)

				got, _ := res.Data["updateItem"].(map[string]any)
				assert.Equal(t, "Changed", got["title"])
				assert.InDelta(t, 3, got["_version"], 0)
			},
		},
		{
			name: "stale delete is rejected",
			check: func(t *testing.T, f *asCFFixture) {
				t.Helper()

				res := f.do(t, `mutation { deleteItem(id:"i1", v:1) `+asCFSel+` }`)
				require.NotEmpty(t, res.Errors)
				assert.Equal(t, "ConflictUnhandled", res.Errors[0].ErrorType)
				assert.InDelta(t, 2, res.Errors[0].Data["_version"], 0)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASCFFixture(t, asCFSync("AUTOMERGE", ""), nil)
			f.seed(t)
			tt.check(t, f)
		})
	}
}

func TestAppSyncLambdaConflictHandler(t *testing.T) {
	t.Parallel()

	resolve := `{"action":"RESOLVE","item":{"id":"ignored","title":"from-lambda","_version":99}}`

	tests := []struct {
		reply     func([]byte) ([]byte, error)
		check     func(t *testing.T, f *asCFFixture, l *asCFLambda)
		name      string
		mutation  string
		errorType string
	}{
		{
			name:     "resolve replaces the item",
			reply:    func([]byte) ([]byte, error) { return []byte(resolve), nil },
			mutation: `putItem(id:"i1", title:"Other", tags:["b"], v:1)`,
			check: func(t *testing.T, f *asCFFixture, l *asCFLambda) {
				t.Helper()

				res := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
				got, _ := res.Data["getItem"].(map[string]any)
				assert.Equal(t, "from-lambda", got["title"])
				assert.Equal(t, "i1", got["id"])
				assert.InDelta(t, 3, got["_version"], 0)

				calls := l.captured()
				require.Len(t, calls, 1)

				var event map[string]any
				require.NoError(t, json.Unmarshal(calls[0], &event, jsonx.V1()))

				newItem, _ := event["newItem"].(map[string]any)
				existing, _ := event["existingItem"].(map[string]any)
				resolver, _ := event["resolver"].(map[string]any)
				args, _ := event["arguments"].(map[string]any)

				assert.Equal(t, "Other", newItem["title"])
				assert.Equal(t, "Nadia", existing["title"])
				assert.InDelta(t, 2, existing["_version"], 0)
				assert.Equal(t, "Other", args["title"])
				assert.Equal(t, "putItem", resolver["field"])
				assert.Equal(t, "Mutation", resolver["parentType"])
				assert.Equal(t, "items", resolver["tableName"])
			},
		},
		{
			name:      "reject returns the existing item",
			reply:     func([]byte) ([]byte, error) { return []byte(`{"action":"REJECT"}`), nil },
			mutation:  `updateItem(id:"i1", title:"Other", v:1)`,
			errorType: "ConflictUnhandled",
			check: func(t *testing.T, f *asCFFixture, _ *asCFLambda) {
				t.Helper()

				res := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
				got, _ := res.Data["getItem"].(map[string]any)
				assert.Equal(t, "Nadia", got["title"])
			},
		},
		{
			name:     "update preview applies the update to the existing item",
			reply:    func([]byte) ([]byte, error) { return []byte(`{"action":"REJECT"}`), nil },
			mutation: `updateItem(id:"i1", title:"Other", v:1)`,
			check: func(t *testing.T, _ *asCFFixture, l *asCFLambda) {
				t.Helper()

				calls := l.captured()
				require.Len(t, calls, 1)

				var event map[string]any
				require.NoError(t, json.Unmarshal(calls[0], &event, jsonx.V1()))

				newItem, _ := event["newItem"].(map[string]any)
				assert.Equal(t, "Other", newItem["title"])
				assert.Equal(t, []any{"a"}, newItem["tags"])
			},
			errorType: "ConflictUnhandled",
		},
		{
			name:      "remove deletes the item",
			reply:     func([]byte) ([]byte, error) { return []byte(`{"action":"REMOVE"}`), nil },
			mutation:  `deleteItem(id:"i1", v:1)`,
			errorType: "",
			check: func(t *testing.T, f *asCFFixture, _ *asCFLambda) {
				t.Helper()

				res := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
				assert.Nil(t, res.Data["getItem"])
			},
		},
		{
			name:      "resolve on delete is malformed",
			reply:     func([]byte) ([]byte, error) { return []byte(resolve), nil },
			mutation:  `deleteItem(id:"i1", v:1)`,
			errorType: "ConflictError",
			check:     func(*testing.T, *asCFFixture, *asCFLambda) {},
		},
		{
			name:      "malformed response",
			reply:     func([]byte) ([]byte, error) { return []byte(`not json`), nil },
			mutation:  `putItem(id:"i1", title:"x", v:1)`,
			errorType: "ConflictError",
			check:     func(*testing.T, *asCFFixture, *asCFLambda) {},
		},
		{
			name:      "unknown action",
			reply:     func([]byte) ([]byte, error) { return []byte(`{"action":"MERGE"}`), nil },
			mutation:  `putItem(id:"i1", title:"x", v:1)`,
			errorType: "ConflictError",
			check:     func(*testing.T, *asCFFixture, *asCFLambda) {},
		},
		{
			name:      "invocation failure",
			reply:     func([]byte) ([]byte, error) { return nil, errLambdaNotFound },
			mutation:  `putItem(id:"i1", title:"x", v:1)`,
			errorType: "ConflictError",
			check:     func(*testing.T, *asCFFixture, *asCFLambda) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := &asCFLambda{fn: tt.reply}
			f := newASCFFixture(t, asCFSync("LAMBDA", asCFArn), l)
			f.seed(t)

			res := f.do(t, `mutation { `+tt.mutation+` `+asCFSel+` }`)
			if tt.errorType == "" {
				require.Empty(t, res.Errors)
			} else {
				require.NotEmpty(t, res.Errors)
				assert.Equal(t, tt.errorType, res.Errors[0].ErrorType)
			}

			tt.check(t, f, l)
		})
	}
}

func TestAppSyncConflictDetectionNone(t *testing.T) {
	t.Parallel()

	f := newASCFFixture(t, &astypes.SyncConfig{ConflictDetection: astypes.ConflictDetectionTypeNone}, nil)
	f.seed(t)

	res := f.do(t, `mutation { updateItem(id:"i1", title:"Blind", v:1) `+asCFSel+` }`)
	require.Empty(t, res.Errors)

	got, _ := res.Data["updateItem"].(map[string]any)
	assert.Equal(t, "Blind", got["title"])
	assert.InDelta(t, 3, got["_version"], 0)
}

func TestAppSyncSyncConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  *astypes.SyncConfig
		name string
	}{
		{name: "lambda without config", cfg: &astypes.SyncConfig{ConflictHandler: astypes.ConflictHandlerTypeLambda}},
		{name: "unknown handler", cfg: &astypes.SyncConfig{ConflictHandler: "LAST_WRITER"}},
		{name: "unknown detection", cfg: &astypes.SyncConfig{ConflictDetection: "TIMESTAMP"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			as := appsync.NewFromConfig(fx.cfg)

			api, err := as.CreateGraphqlApi(t.Context(), &appsync.CreateGraphqlApiInput{
				Name: aws.String("v"), AuthenticationType: astypes.AuthenticationTypeApiKey,
			})
			require.NoError(t, err)

			_, err = as.CreateResolver(t.Context(), &appsync.CreateResolverInput{
				ApiId: api.GraphqlApi.ApiId, TypeName: aws.String("Mutation"), FieldName: aws.String("f"),
				DataSourceName: aws.String("d"), SyncConfig: tt.cfg,
			})
			require.Error(t, err)

			var bre *astypes.BadRequestException

			assert.ErrorAs(t, err, &bre)
		})
	}
}
