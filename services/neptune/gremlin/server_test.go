package gremlin_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func newServer(t *testing.T, g *opencypher.Graph) *httptest.Server {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	srv := httptest.NewServer(gremlin.NewServer(ctx, g, gremlin.Options{}))

	t.Cleanup(func() {
		cancel()
		srv.Close()
	})

	return srv
}

func dial(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()

	c, resp, err := websocket.DefaultDialer.DialContext(
		t.Context(),
		"ws"+strings.TrimPrefix(srv.URL, "http")+"/gremlin",
		nil,
	)
	require.NoError(t, err)

	_ = resp.Body.Close()

	t.Cleanup(func() { _ = c.Close() })

	return c
}

type wsResponse struct {
	Status struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"status"`
	Result struct {
		Data json.RawMessage `json:"data"`
	} `json:"result"`
}

func frame(mime, body string) []byte {
	return append([]byte{byte(len(mime))}, []byte(mime+body)...)
}

func call(t *testing.T, c *websocket.Conn, mt int, payload []byte) []wsResponse {
	t.Helper()

	require.NoError(t, c.WriteMessage(mt, payload))

	var out []wsResponse

	for {
		_, data, err := c.ReadMessage()
		require.NoError(t, err)

		var r wsResponse
		require.NoError(t, json.Unmarshal(data, &r), string(data))

		out = append(out, r)

		if r.Status.Code != gremlin.StatusPartial {
			return out
		}
	}
}

const evalFrame = `{"requestId":"11111111-1111-1111-1111-111111111111","op":"eval","processor":"",` +
	`"args":{"gremlin":%q,"language":"gremlin-groovy","aliases":{"g":"g"}%s}}`

func evalBody(script, extra string) string {
	b, _ := json.Marshal(script)

	return strings.Replace(strings.Replace(evalFrame, "%q", string(b), 1), "%s", extra, 1)
}

func TestWebSocketGraphSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mime     string
		script   string
		wantData string
		wantCode int
		text     bool
	}{
		{"v3_binary_frame", "application/vnd.gremlin-v3.0+json", `g.V().count()`,
			`{"@type":"g:List","@value":[{"@type":"g:Int64","@value":4}]}`, 200, false},
		{"v3_text_frame", "", `g.V().count()`,
			`{"@type":"g:List","@value":[{"@type":"g:Int64","@value":4}]}`, 200, true},
		{"v2_plain_list", "application/vnd.gremlin-v2.0+json", `g.V().hasLabel('software').values('name')`,
			`["lop"]`, 200, false},
		{"v3_vertex", "application/vnd.gremlin-v3.0+json", `g.V('lop')`,
			`{"@type":"g:List","@value":[{"@type":"g:Vertex","@value":{"id":"lop","label":"software"}}]}`, 200, false},
		{"no_content", "application/vnd.gremlin-v3.0+json", `g.V().has('name','none')`, `null`, 204, false},
		{"syntax_error", "application/vnd.gremlin-v3.0+json", `g.V(`, `null`, 597, false},
		{"unknown_step", "application/vnd.gremlin-v3.0+json", `g.V().nope()`, `null`, 597, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := dial(t, newServer(t, seeded(t)))
			body := evalBody(tt.script, "")

			var resp []wsResponse
			if tt.text {
				resp = call(t, c, websocket.TextMessage, []byte(body))
			} else {
				resp = call(t, c, websocket.BinaryMessage, frame(tt.mime, body))
			}

			require.Len(t, resp, 1)
			assert.Equal(t, tt.wantCode, resp[0].Status.Code)
			assert.JSONEq(t, tt.wantData, string(resp[0].Result.Data))

			if tt.wantCode == 597 {
				assert.Contains(t, resp[0].Status.Message, `"code":"MalformedQueryException"`)
				assert.Contains(t, resp[0].Status.Message, `"requestId":"11111111-1111-1111-1111-111111111111"`)
			}
		})
	}
}

func TestWebSocketBytecode(t *testing.T) {
	t.Parallel()

	c := dial(t, newServer(t, seeded(t)))
	body := `{"requestId":"22222222-2222-2222-2222-222222222222","op":"bytecode","processor":"traversal","args":{` +
		`"gremlin":{"@type":"g:Bytecode","@value":{"step":[["V"],["has","age",{"@type":"g:P","@value":` +
		`{"predicate":"gt","value":{"@type":"g:Int32","@value":28}}}],["order"],["by","age",` +
		`{"@type":"g:Order","@value":"desc"}],["values","name"]]}},"aliases":{"g":"g"}}}`

	resp := call(t, c, websocket.BinaryMessage, frame("application/vnd.gremlin-v3.0+json", body))
	require.Len(t, resp, 1)
	assert.Equal(t, 200, resp[0].Status.Code)
	assert.JSONEq(t, `{"@type":"g:List","@value":["josh","marko"]}`, string(resp[0].Result.Data))
}

func TestWebSocketPartialContent(t *testing.T) {
	t.Parallel()

	g := opencypher.NewGraph()
	_, err := evalAll(t, g, `g.inject(1,2,3,4,5).as('x').addV('n').property('i', select('x'))`)
	require.NoError(t, err)

	c := dial(t, newServer(t, g))

	resp := call(t, c, websocket.BinaryMessage,
		frame("application/vnd.gremlin-v3.0+json", evalBody(`g.V().values('i')`, `,"batchSize":2`)))

	codes := make([]int, len(resp))
	for i, r := range resp {
		codes[i] = r.Status.Code
	}

	assert.Equal(t, []int{206, 206, 200}, codes)
}

const reqPrefix = `{"requestId":"33333333-3333-3333-3333-333333333333",`

func TestWebSocketRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload func() (int, []byte)
		name    string
		want    int
	}{
		{func() (int, []byte) {
			return websocket.TextMessage, []byte(
				reqPrefix + `"op":"nope","args":{}}`,
			)
		}, "unknown_op", 499},
		{func() (int, []byte) {
			return websocket.TextMessage, []byte(
				reqPrefix + `"op":"eval","args":{}}`,
			)
		}, "missing_gremlin", 499},
		{func() (int, []byte) { return websocket.TextMessage, []byte(`{{{`) }, "not_json", 499},
		{func() (int, []byte) { return websocket.BinaryMessage, frame("text/plain", `{}`) }, "bad_mime", 499},
		{func() (int, []byte) { return websocket.BinaryMessage, []byte{200, 'a'} }, "truncated_mime", 499},
		{func() (int, []byte) {
			return websocket.BinaryMessage, frame("application/vnd.graphbinary-v1.0", "\x81\x00\x01")
		}, "garbage_graphbinary", 499},
		{func() (int, []byte) {
			return websocket.TextMessage, []byte(
				reqPrefix + `"op":"bytecode","args":{"gremlin":"x"}}`,
			)
		}, "bytecode_wrong_arg", 499},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := dial(t, newServer(t, opencypher.NewGraph()))
			mt, payload := tt.payload()

			resp := call(t, c, mt, payload)
			require.Len(t, resp, 1)
			assert.Equal(t, tt.want, resp[0].Status.Code)

			// the connection stays usable after a bad request
			resp = call(t, c, websocket.TextMessage, []byte(evalBody(`g.V().count()`, "")))
			assert.Equal(t, 200, resp[0].Status.Code)
		})
	}
}

func TestWebSocketOversizedFrameClosesConnection(t *testing.T) {
	t.Parallel()

	c := dial(t, newServer(t, opencypher.NewGraph()))
	require.NoError(t, c.WriteMessage(websocket.BinaryMessage, make([]byte, 5<<20)))

	_, _, err := c.ReadMessage()
	require.Error(t, err)
}

func TestHTTPEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		body       string
		query      string
		accept     string
		contains   string
		wantStatus int
	}{
		{
			"post",
			http.MethodPost,
			`{"gremlin":"g.V().count()"}`,
			"",
			"",
			`"data":{"@type":"g:List","@value":[{"@type":"g:Int64","@value":4}]}`,
			200,
		},
		{"get", http.MethodGet, "", "?gremlin=g.V('lop').values('name')", "", `"@value":["lop"]`, 200},
		{
			"v2",
			http.MethodPost,
			`{"gremlin":"g.V('lop').values('name')"}`,
			"",
			"application/vnd.gremlin-v2.0+json",
			`"data":["lop"]`,
			200,
		},
		{
			"bindings",
			http.MethodPost,
			`{"gremlin":"g.V(x).values('name')","bindings":{"x":"lop"}}`,
			"",
			"",
			`"lop"`,
			200,
		},
		{"malformed", http.MethodPost, `{"gremlin":"g.V("}`, "", "", `"code":"MalformedQueryException"`, 400},
		{"missing", http.MethodPost, `{}`, "", "", `"code":"InvalidParameterException"`, 400},
		{"bad_json", http.MethodPost, `{`, "", "", `"code":"InvalidParameterException"`, 400},
		{
			"constraint",
			http.MethodPost,
			`{"gremlin":"g.addV('x').property(T.id,'lop')"}`,
			"",
			"",
			`"code":"ConstraintViolationException"`,
			400,
		},
		{"method", http.MethodDelete, "", "", "", `"code":"InvalidParameterException"`, 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := newServer(t, seeded(t))
			req, err := http.NewRequestWithContext(
				t.Context(),
				tt.method,
				srv.URL+"/gremlin"+tt.query,
				strings.NewReader(tt.body),
			)
			require.NoError(t, err)

			if tt.accept != "" {
				req.Header.Set("Accept", tt.accept)
			}

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer func() { _ = resp.Body.Close() }()

			raw, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, resp.StatusCode, string(raw))
			assert.Contains(t, string(raw), tt.contains)
		})
	}
}
