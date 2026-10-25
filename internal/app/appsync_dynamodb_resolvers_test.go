package app

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	astypes "github.com/aws/aws-sdk-go-v2/service/appsync/types"
	sdkddb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbsdktypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const asDDBSchema = `
type Author { id: ID! name: String posts: [Post] }
type Post { authorId: ID! id: ID! title: String n: Int author: Author }
type PostPage { items: [Post] nextToken: String scannedCount: Int }
type PostList { posts: [Post] }
type BatchOut { data: PostList unprocessedKeys: PostList unprocessedItems: PostList }
type PostKey { authorId: ID id: ID }
type TxGetOut { items: [Post] cancellationReasons: [AWSJSON] }
type TxWriteOut { keys: [PostKey] }
type SyncOut { items: [Post] nextToken: String scannedCount: Int startedAt: Float }
type Query {
  transactGetPosts(authorId: ID!, a: ID!, b: ID!): TxGetOut
  syncPosts(limit: Int, nextToken: String, lastSync: Float): SyncOut
  plainSyncPosts: SyncOut
  getPost(authorId: ID!, id: ID!): Post
  queryPosts(authorId: ID!, limit: Int, nextToken: String, fwd: Boolean, minN: Int): PostPage
  queryByTitle(title: String!): PostPage
  scanPosts(minN: Int, limit: Int, nextToken: String): PostPage
  batchGetPosts(authorId: ID!, a: ID!, b: ID!, c: ID!): BatchOut
  postWithAuthor(authorId: ID!, id: ID!): Post
  authorWithPosts(id: ID!): Author
}
type Mutation {
  transactWritePosts(authorId: ID!, a: ID!, expected: Int): TxWriteOut
  updatePost(authorId: ID!, id: ID!, title: String, expected: Int): Post
  putPostIfNew(authorId: ID!, id: ID!, title: String): Post
  deletePost(authorId: ID!, id: ID!, expected: Int): Post
  batchPutPosts(authorId: ID!, a: ID!, b: ID!): BatchOut
  batchDeletePosts(authorId: ID!, a: ID!, b: ID!): BatchOut
}`

const (
	asKeyJSON = `"key":{"authorId":$util.dynamodb.toDynamoDBJson($ctx.args.authorId),` +
		`"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)}`
	asPre = `{"version":"2018-05-29",`
)

func asBatchKey(id string) string {
	return `{"authorId":$util.dynamodb.toDynamoDBJson($ctx.args.authorId),` +
		`"id":$util.dynamodb.toDynamoDBJson($ctx.args.` + id + `)}`
}

func asBatchItem(id string) string {
	return `{"authorId":$util.dynamodb.toDynamoDBJson($ctx.args.authorId),` +
		`"id":$util.dynamodb.toDynamoDBJson($ctx.args.` + id + `),"title":{"S":"batch"},"n":{"N":"1"}}`
}

func asKeyJSON2(id string) string {
	idExpr := `$ctx.args.` + id
	if strings.HasPrefix(id, "\"") {
		idExpr = id
	}

	return `"key":{"authorId":$util.dynamodb.toDynamoDBJson($ctx.args.authorId),` +
		`"id":$util.dynamodb.toDynamoDBJson(` + idExpr + `)}`
}

type asResolverSpec struct {
	typeName, field, ds, request string
}

func asDDBResolvers() []asResolverSpec {
	return []asResolverSpec{
		{"Query", "transactGetPosts", "posts", asPre + `"operation":"TransactGetItems","transactItems":[` +
			`{"table":"posts","key":` + asBatchKey("a") + `},{"table":"posts","key":` + asBatchKey("b") + `}]}`},
		{"Query", "syncPosts", "postsv", asPre + `"operation":"Sync","limit":$util.toJson($ctx.args.limit),` +
			`"nextToken":$util.toJson($ctx.args.nextToken),"lastSync":$util.toJson($ctx.args.lastSync)}`},
		{"Query", "plainSyncPosts", "posts", asPre + `"operation":"Sync"}`},
		{"Mutation", "transactWritePosts", "posts", asPre + `"operation":"TransactWriteItems","transactItems":[` +
			`{"table":"posts","operation":"PutItem",` + asKeyJSON2("a") +
			`,"attributeValues":{"title":{"S":"tx"},"n":{"N":"9"}}},` +
			`{"table":"posts","operation":"ConditionCheck",` + asKeyJSON2("\"p1\"") + `,"condition":{"expression":"n = :e",` +
			`"expressionValues":{":e":$util.dynamodb.toDynamoDBJson($ctx.args.expected)}}}]}`},
		{"Query", "getPost", "posts", asPre + `"operation":"GetItem",` + asKeyJSON + `}`},
		{"Query", "queryPosts", "posts", asPre + `"operation":"Query","query":{"expression":"authorId = :a",` +
			`"expressionValues":{":a":$util.dynamodb.toDynamoDBJson($ctx.args.authorId)}},` +
			`"filter":{"expression":"n >= :m","expressionValues":{":m":$util.dynamodb.toDynamoDBJson($ctx.args.minN)}},` +
			`"limit":$util.toJson($ctx.args.limit),"nextToken":$util.toJson($ctx.args.nextToken),` +
			`"scanIndexForward":$util.toJson($ctx.args.fwd)}`},
		{"Query", "queryByTitle", "posts", asPre + `"operation":"Query","index":"byTitle",` +
			`"query":{"expression":"#t = :t","expressionNames":{"#t":"title"},` +
			`"expressionValues":{":t":$util.dynamodb.toDynamoDBJson($ctx.args.title)}}}`},
		{"Query", "scanPosts", "posts", asPre + `"operation":"Scan",` +
			`"filter":{"expression":"n >= :m","expressionValues":{":m":$util.dynamodb.toDynamoDBJson($ctx.args.minN)}},` +
			`"limit":$util.toJson($ctx.args.limit),"nextToken":$util.toJson($ctx.args.nextToken)}`},
		{"Query", "batchGetPosts", "posts", asPre + `"operation":"BatchGetItem","tables":{"posts":{"keys":[` +
			asBatchKey("a") + `,` + asBatchKey("b") + `,` + asBatchKey("c") + `]}}}`},
		{"Mutation", "updatePost", "posts", asPre + `"operation":"UpdateItem",` + asKeyJSON + `,` +
			`"update":{"expression":"SET title = :t ADD n :one","expressionValues":{` +
			`":t":$util.dynamodb.toDynamoDBJson($ctx.args.title),":one":{"N":"1"}}},` +
			`"condition":{"expression":"attribute_exists(id) AND n = :e",` +
			`"expressionValues":{":e":$util.dynamodb.toDynamoDBJson($ctx.args.expected)}}}`},
		{"Mutation", "putPostIfNew", "posts", asPre + `"operation":"PutItem",` + asKeyJSON + `,` +
			`"attributeValues":{"title":$util.dynamodb.toDynamoDBJson($ctx.args.title)},` +
			`"condition":{"expression":"attribute_not_exists(id)","equalsIgnore":["n"]}}`},
		{"Mutation", "deletePost", "posts", asPre + `"operation":"DeleteItem",` + asKeyJSON + `,` +
			`"condition":{"expression":"n = :e",` +
			`"expressionValues":{":e":$util.dynamodb.toDynamoDBJson($ctx.args.expected)}}}`},
		{"Mutation", "batchPutPosts", "posts", asPre + `"operation":"BatchPutItem","tables":{"posts":[` +
			asBatchItem("a") + `,` + asBatchItem("b") + `]}}`},
		{"Mutation", "batchDeletePosts", "posts", asPre + `"operation":"BatchDeleteItem","tables":{"posts":[` +
			asBatchKey("a") + `,` + asBatchKey("b") + `]}}`},
		{"Query", "postWithAuthor", "posts", asPre + `"operation":"GetItem",` + asKeyJSON + `}`},
		{"Post", "author", "authors", asPre + `"operation":"GetItem","key":{` +
			`"id":$util.dynamodb.toDynamoDBJson($ctx.source.authorId)}}`},
		{"Query", "authorWithPosts", "authors", asPre + `"operation":"GetItem","key":{` +
			`"id":$util.dynamodb.toDynamoDBJson($ctx.args.id)}}`},
		{"Author", "posts", "posts", asPre + `"operation":"Query","query":{"expression":"authorId = :a",` +
			`"expressionValues":{":a":$util.dynamodb.toDynamoDBJson($ctx.source.id)}}}`},
	}
}

type asDDBFixture struct {
	fx    *sfnFixture
	apiID string
	key   string
}

func newASDDBFixture(t *testing.T) *asDDBFixture {
	t.Helper()

	fx := newSFNFixture(t)
	ctx := t.Context()
	ddb := sdkddb.NewFromConfig(fx.cfg)
	as := appsync.NewFromConfig(fx.cfg)

	s := ddbsdktypes.ScalarAttributeTypeS
	_, err := ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String("posts"), BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{
			{AttributeName: aws.String("authorId"), KeyType: ddbsdktypes.KeyTypeHash},
			{AttributeName: aws.String("id"), KeyType: ddbsdktypes.KeyTypeRange},
		},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("authorId"), AttributeType: s},
			{AttributeName: aws.String("id"), AttributeType: s},
			{AttributeName: aws.String("title"), AttributeType: s},
		},
		GlobalSecondaryIndexes: []ddbsdktypes.GlobalSecondaryIndex{
			{
				IndexName: aws.String("byTitle"),
				KeySchema: []ddbsdktypes.KeySchemaElement{
					{AttributeName: aws.String("title"), KeyType: ddbsdktypes.KeyTypeHash},
				},
				Projection: &ddbsdktypes.Projection{ProjectionType: ddbsdktypes.ProjectionTypeAll},
			},
		},
	})
	require.NoError(t, err)

	_, err = ddb.CreateTable(ctx, &sdkddb.CreateTableInput{
		TableName: aws.String("authors"), BillingMode: ddbsdktypes.BillingModePayPerRequest,
		KeySchema: []ddbsdktypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbsdktypes.KeyTypeHash}},
		AttributeDefinitions: []ddbsdktypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: s},
		},
	})
	require.NoError(t, err)

	put := func(table string, item map[string]ddbsdktypes.AttributeValue) {
		_, perr := ddb.PutItem(ctx, &sdkddb.PutItemInput{TableName: aws.String(table), Item: item})
		require.NoError(t, perr)
	}
	str := func(v string) ddbsdktypes.AttributeValue { return &ddbsdktypes.AttributeValueMemberS{Value: v} }
	num := func(v string) ddbsdktypes.AttributeValue { return &ddbsdktypes.AttributeValueMemberN{Value: v} }

	put("authors", map[string]ddbsdktypes.AttributeValue{"id": str("a1"), "name": str("Ann")})

	for _, p := range []struct{ id, title, n string }{
		{"p1", "one", "1"}, {"p2", "two", "2"}, {"p3", "three", "3"},
	} {
		put("posts", map[string]ddbsdktypes.AttributeValue{
			"authorId": str("a1"), "id": str(p.id), "title": str(p.title), "n": num(p.n),
		})
	}

	api, err := as.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{
		Name: aws.String("g"), AuthenticationType: astypes.AuthenticationTypeApiKey,
	})
	require.NoError(t, err)

	apiID := api.GraphqlApi.ApiId

	key, err := as.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: apiID})
	require.NoError(t, err)

	_, err = as.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{
		ApiId: apiID, Definition: []byte(asDDBSchema),
	})
	require.NoError(t, err)

	for _, name := range []string{"posts", "authors", "postsv"} {
		_, err = as.CreateDataSource(ctx, &appsync.CreateDataSourceInput{
			ApiId: apiID, Name: aws.String(name), Type: astypes.DataSourceTypeAmazonDynamodb,
			ServiceRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
			DynamodbConfig: &astypes.DynamodbDataSourceConfig{
				TableName: aws.String(strings.TrimSuffix(name, "v")), AwsRegion: aws.String("us-east-1"),
				Versioned: name == "postsv",
				DeltaSyncConfig: func() *astypes.DeltaSyncConfig {
					if name != "postsv" {
						return nil
					}

					return &astypes.DeltaSyncConfig{
						BaseTableTTL: 0, DeltaSyncTableName: aws.String("posts-delta"), DeltaSyncTableTTL: 60,
					}
				}(),
			},
		})
		require.NoError(t, err)
	}

	for _, r := range asDDBResolvers() {
		in := &appsync.CreateResolverInput{
			ApiId: apiID, TypeName: aws.String(r.typeName), FieldName: aws.String(r.field),
			DataSourceName: aws.String(r.ds), RequestMappingTemplate: aws.String(r.request),
		}
		if r.typeName == "Author" {
			in.ResponseMappingTemplate = aws.String(`$util.toJson($ctx.result.items)`)
		}

		_, err = as.CreateResolver(ctx, in)
		require.NoError(t, err)
	}

	return &asDDBFixture{fx: fx, apiID: aws.ToString(apiID), key: aws.ToString(key.ApiKey.Id)}
}

type asGQLResponse struct {
	Data   map[string]any `json:"data"`
	Errors []struct {
		Data      map[string]any `json:"data"`
		Message   string         `json:"message"`
		ErrorType string         `json:"errorType"`
	} `json:"errors"`
}

func (f *asDDBFixture) do(t *testing.T, query string) asGQLResponse {
	t.Helper()

	body, err := json.Marshal(map[string]any{"query": query})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+f.apiID+"/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", f.key)

	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out asGQLResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out), rec.Body.String())

	return out
}

func TestAppSyncDynamoDBResolvers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(a *assert.Assertions, data map[string]any)
		name  string
		query string
	}{
		{
			name:  "getitem",
			query: `query { getPost(authorId: "a1", id: "p2") { id title n } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Equal(map[string]any{"id": "p2", "title": "two", "n": float64(2)}, d["getPost"])
			},
		},
		{
			name:  "query with filter and limit pages via nextToken",
			query: `query { queryPosts(authorId: "a1", minN: 2, limit: 1, fwd: true) { items { id } nextToken scannedCount } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				page, _ := d["queryPosts"].(map[string]any)
				a.Equal([]any{}, page["items"], "p1 is scanned first and filtered out")
				a.NotEmpty(page["nextToken"])
				a.InDelta(1, page["scannedCount"], 0)
			},
		},
		{
			name:  "query descending",
			query: `query { queryPosts(authorId: "a1", minN: 0, fwd: false) { items { id } scannedCount } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				page, _ := d["queryPosts"].(map[string]any)
				a.Equal([]any{
					map[string]any{"id": "p3"}, map[string]any{"id": "p2"}, map[string]any{"id": "p1"},
				}, page["items"])
				a.Nil(page["nextToken"])
			},
		},
		{
			name:  "query on index with expression names",
			query: `query { queryByTitle(title: "three") { items { id n } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				page, _ := d["queryByTitle"].(map[string]any)
				a.Equal([]any{map[string]any{"id": "p3", "n": float64(3)}}, page["items"])
			},
		},
		{
			name:  "scan with filter",
			query: `query { scanPosts(minN: 2) { items { id } scannedCount } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				page, _ := d["scanPosts"].(map[string]any)
				a.ElementsMatch([]any{map[string]any{"id": "p2"}, map[string]any{"id": "p3"}}, page["items"])
				a.InDelta(3, page["scannedCount"], 0)
			},
		},
		{
			name:  "update item with condition",
			query: `mutation { updatePost(authorId: "a1", id: "p1", title: "uno", expected: 1) { id title n } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Equal(map[string]any{"id": "p1", "title": "uno", "n": float64(2)}, d["updatePost"])
			},
		},
		{
			name:  "delete item with condition",
			query: `mutation { deletePost(authorId: "a1", id: "p3", expected: 3) { id title } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Equal(map[string]any{"id": "p3", "title": "three"}, d["deletePost"])
			},
		},
		{
			name:  "put of an identical item succeeds despite the condition",
			query: `mutation { putPostIfNew(authorId: "a1", id: "p1", title: "one") { id title n } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Equal(map[string]any{"id": "p1", "title": "one", "n": float64(1)}, d["putPostIfNew"])
			},
		},
		{
			name:  "delete of a missing item with a condition is a no-op",
			query: `mutation { deletePost(authorId: "a1", id: "nope", expected: 1) { id } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Nil(d["deletePost"])
			},
		},
		{
			name: "batch get returns null for missing",
			query: `query { batchGetPosts(authorId: "a1", a: "p1", b: "zz", c: "p3") ` +
				`{ data { posts { id } } unprocessedKeys { posts { id } } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				out, _ := d["batchGetPosts"].(map[string]any)
				data, _ := out["data"].(map[string]any)
				a.Equal([]any{map[string]any{"id": "p1"}, nil, map[string]any{"id": "p3"}}, data["posts"])
			},
		},
		{
			name:  "batch put",
			query: `mutation { batchPutPosts(authorId: "a2", a: "x1", b: "x2") { data { posts { id title } } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				out, _ := d["batchPutPosts"].(map[string]any)
				data, _ := out["data"].(map[string]any)
				a.Equal([]any{
					map[string]any{"id": "x1", "title": "batch"}, map[string]any{"id": "x2", "title": "batch"},
				}, data["posts"])
			},
		},
		{
			name:  "batch delete",
			query: `mutation { batchDeletePosts(authorId: "a1", a: "p1", b: "p2") { data { posts { id } } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				out, _ := d["batchDeletePosts"].(map[string]any)
				data, _ := out["data"].(map[string]any)
				a.Equal([]any{map[string]any{"id": "p1"}, map[string]any{"id": "p2"}}, data["posts"])
			},
		},
		{
			name:  "nested child resolver uses source",
			query: `query { postWithAuthor(authorId: "a1", id: "p2") { id author { id name } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				a.Equal(map[string]any{
					"id": "p2", "author": map[string]any{"id": "a1", "name": "Ann"},
				}, d["postWithAuthor"])
			},
		},
		{
			name:  "nested list child resolver",
			query: `query { authorWithPosts(id: "a1") { name posts { id } } }`,
			check: func(a *assert.Assertions, d map[string]any) {
				out, _ := d["authorWithPosts"].(map[string]any)
				a.Equal("Ann", out["name"])
				a.Len(out["posts"], 3)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASDDBFixture(t)
			res := f.do(t, tt.query)
			require.Empty(t, res.Errors)
			tt.check(assert.New(t), res.Data)
		})
	}
}

func TestAppSyncDynamoDBConditionFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
	}{
		{
			name:  "update condition mismatch",
			query: `mutation { updatePost(authorId: "a1", id: "p1", title: "x", expected: 99) { id } }`,
		},
		{
			name:  "update missing item",
			query: `mutation { updatePost(authorId: "a1", id: "nope", title: "x", expected: 1) { id } }`,
		},
		{
			name:  "put onto existing item",
			query: `mutation { putPostIfNew(authorId: "a1", id: "p1", title: "other") { id } }`,
		},
		{
			name:  "delete condition mismatch",
			query: `mutation { deletePost(authorId: "a1", id: "p1", expected: 99) { id } }`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASDDBFixture(t)
			res := f.do(t, tt.query)
			require.Len(t, res.Errors, 1)
			assert.Equal(t, "DynamoDB:ConditionalCheckFailedException", res.Errors[0].ErrorType)
			assert.Contains(t, res.Errors[0].Message, "The conditional request failed")
			assert.Nil(t, res.Data)
		})
	}
}

func TestAppSyncDynamoDBTransactAndSync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check   func(a *assert.Assertions, res asGQLResponse)
		name    string
		query   string
		wantErr string
	}{
		{
			name:  "transact get returns null for missing",
			query: `query { transactGetPosts(authorId: "a1", a: "p1", b: "zz") { items { id title } } }`,
			check: func(a *assert.Assertions, res asGQLResponse) {
				out, _ := res.Data["transactGetPosts"].(map[string]any)
				a.Equal([]any{map[string]any{"id": "p1", "title": "one"}, nil}, out["items"])
			},
		},
		{
			name:  "transact write returns keys",
			query: `mutation { transactWritePosts(authorId: "a1", a: "tx1", expected: 1) { keys { authorId id } } }`,
			check: func(a *assert.Assertions, res asGQLResponse) {
				out, _ := res.Data["transactWritePosts"].(map[string]any)
				a.Equal([]any{
					map[string]any{"authorId": "a1", "id": "tx1"}, map[string]any{"authorId": "a1", "id": "p1"},
				}, out["keys"])
			},
		},
		{
			name:    "transact write cancelled by condition check",
			query:   `mutation { transactWritePosts(authorId: "a1", a: "tx1", expected: 99) { keys { id } } }`,
			wantErr: "DynamoDB:TransactionCanceledException",
			check: func(a *assert.Assertions, res asGQLResponse) {
				reasons, _ := res.Errors[0].Data["cancellationReasons"].([]any)
				a.Len(reasons, 2)
				a.Equal("None", reasons[0].(map[string]any)["type"])
				second, _ := reasons[1].(map[string]any)
				a.Equal("ConditionalCheckFailed", second["type"])
				a.Equal(map[string]any{"authorId": "a1", "id": "p1", "title": "one", "n": float64(1)}, second["item"])
			},
		},
		{
			name:  "sync pages the base table",
			query: `query { syncPosts(limit: 2) { items { id } nextToken startedAt scannedCount } }`,
			check: func(a *assert.Assertions, res asGQLResponse) {
				out, _ := res.Data["syncPosts"].(map[string]any)
				a.Len(out["items"], 2)
				a.NotEmpty(out["nextToken"])
				a.Positive(out["startedAt"])
			},
		},
		{
			name:    "sync needs a versioned data source",
			query:   `query { plainSyncPosts { items { id } } }`,
			wantErr: "requires a versioned",
		},
		{
			name:    "sync with lastSync is unsupported",
			query:   `query { syncPosts(lastSync: 1) { items { id } } }`,
			wantErr: "lastSync (delta table) is not supported",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASDDBFixture(t)
			res := f.do(t, tt.query)

			if tt.wantErr != "" {
				require.NotEmpty(t, res.Errors)
				assert.Contains(t, res.Errors[0].ErrorType+res.Errors[0].Message, tt.wantErr)
			} else {
				require.Empty(t, res.Errors)
			}

			if tt.check != nil {
				tt.check(assert.New(t), res)
			}
		})
	}
}
