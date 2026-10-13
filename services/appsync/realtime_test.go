package appsync_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

const realtimeSchema = `
type Todo { id: ID! title: String }
type Query { todo: Todo }
type Mutation { putTodo(id: ID!, title: String): Todo }
type Subscription {
  onPut(id: ID): Todo @aws_subscribe(mutations: ["putTodo"])
  onNothing: Todo
}`

type rtFixture struct {
	backend *appsync.InMemoryBackend
	wsURL   string
	apiID   string
	apiKey  string
}

func newRealtimeFixture(t *testing.T) *rtFixture {
	t.Helper()

	b := newTestBackend()
	h := appsync.NewHandler(b)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	api, err := b.CreateGraphqlAPI("A", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	key, err := b.CreateAPIKey(api.APIID, "", 0)
	require.NoError(t, err)

	_, err = b.StartSchemaCreation(api.APIID, realtimeSchema)
	require.NoError(t, err)
	_, err = b.CreateDataSource(api.APIID, &appsync.DataSource{Name: "none", Type: appsync.DataSourceTypeNone})
	require.NoError(t, err)
	_, err = b.CreateResolver(api.APIID, "Mutation", &appsync.Resolver{
		FieldName: "putTodo", DataSourceName: "none",
		RequestMappingTemplate:  `{"version":"2018-05-29","payload":{"id":"$ctx.args.id","title":"$ctx.args.title"}}`,
		ResponseMappingTemplate: `$util.toJson($ctx.result)`,
	})
	require.NoError(t, err)

	return &rtFixture{
		backend: b,
		apiID:   api.APIID,
		apiKey:  key.ID,
		wsURL:   "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/apis/" + api.APIID + "/graphql/realtime",
	}
}

func (f *rtFixture) dial(t *testing.T, apiKey string) *websocket.Conn {
	t.Helper()

	hdr, err := json.Marshal(map[string]string{"x-api-key": apiKey})
	require.NoError(t, err)

	d := websocket.Dialer{Subprotocols: []string{"graphql-ws"}}
	conn, resp, err := d.Dial(f.wsURL+"?header="+base64.StdEncoding.EncodeToString(hdr)+"&payload=e30=", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	require.NoError(t, resp.Body.Close())
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))

	return conn
}

func readMsg(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, conn.ReadJSON(&m))

	return m
}

func startMsg(t *testing.T, id, query, apiKey string) map[string]any {
	t.Helper()

	data, err := json.Marshal(map[string]any{"query": query})
	require.NoError(t, err)

	return map[string]any{
		"type": "start", "id": id,
		"payload": map[string]any{
			"data":       string(data),
			"extensions": map[string]any{"authorization": map[string]string{"x-api-key": apiKey}},
		},
	}
}

func TestRealtime_SubscriptionDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		query    string
		mutation string
		want     string
	}{
		{
			name:     "all mutations",
			query:    `subscription { onPut { id title } }`,
			mutation: `mutation { putTodo(id: "1", title: "buy") { id title } }`,
			want:     `{"onPut":{"id":"1","title":"buy"}}`,
		},
		{
			name:     "selection projected",
			query:    `subscription { onPut { title } }`,
			mutation: `mutation { putTodo(id: "1", title: "buy") { id title } }`,
			want:     `{"onPut":{"title":"buy"}}`,
		},
		{
			name:     "argument filter match",
			query:    `subscription { onPut(id: "1") { id } }`,
			mutation: `mutation { putTodo(id: "1", title: "x") { id } }`,
			want:     `{"onPut":{"id":"1"}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealtimeFixture(t)
			conn := f.dial(t, f.apiKey)

			require.NoError(t, conn.WriteJSON(map[string]string{"type": "connection_init"}))
			assert.Equal(t, "connection_ack", readMsg(t, conn)["type"])
			assert.Equal(t, "ka", readMsg(t, conn)["type"])

			require.NoError(t, conn.WriteJSON(startMsg(t, "sub1", tt.query, f.apiKey)))
			ack := readMsg(t, conn)
			assert.Equal(t, "start_ack", ack["type"])
			assert.Equal(t, "sub1", ack["id"])

			_, err := f.backend.ExecuteGraphQL(t.Context(), f.apiID, tt.mutation, "", nil,
				appsync.GraphQLAuth{APIKey: f.apiKey})
			require.NoError(t, err)

			msg := readMsg(t, conn)
			assert.Equal(t, "data", msg["type"])
			assert.Equal(t, "sub1", msg["id"])

			payload, mErr := json.Marshal(msg["payload"])
			require.NoError(t, mErr)
			assert.JSONEq(t, `{"data":`+tt.want+`}`, string(payload))

			require.NoError(t, conn.WriteJSON(map[string]string{"type": "stop", "id": "sub1"}))
			assert.Equal(t, "complete", readMsg(t, conn)["type"])
		})
	}
}

func TestRealtime_StartErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		apiKey  string
		wantTyp string
	}{
		{name: "bad key", query: `subscription { onPut { id } }`, apiKey: "da2-bad", wantTyp: "UnauthorizedException"},
		{name: "no subscribe directive", query: `subscription { onNothing { id } }`, wantTyp: "BadRequestException"},
		{name: "not a subscription", query: `query { todo { id } }`, wantTyp: "BadRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newRealtimeFixture(t)
			conn := f.dial(t, f.apiKey)
			key := tt.apiKey

			if key == "" {
				key = f.apiKey
			}

			require.NoError(t, conn.WriteJSON(startMsg(t, "s", tt.query, key)))

			msg := readMsg(t, conn)
			assert.Equal(t, "error", msg["type"])

			payload, ok := msg["payload"].(map[string]any)
			require.True(t, ok)

			errs, ok := payload["errors"].([]any)
			require.True(t, ok)
			require.Len(t, errs, 1)

			first, ok := errs[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantTyp, first["errorType"])
		})
	}
}

func TestRealtime_URIsAdvertiseWebSocket(t *testing.T) {
	t.Parallel()

	f := newRealtimeFixture(t)
	api, err := f.backend.GetGraphqlAPI(f.apiID)
	require.NoError(t, err)
	assert.Equal(t, "ws://localhost:8000/v1/apis/"+f.apiID+"/graphql/realtime", api.URIs["REALTIME"])
}
