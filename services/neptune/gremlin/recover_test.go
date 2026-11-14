package gremlin_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
)

func TestWebSocketQueryPanicAnswersServerError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		script string
	}{
		{name: "eval", script: `g.V().has('name','boom')`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(t.Context())
			srv := httptest.NewServer(gremlin.NewServer(ctx, seeded(t), gremlin.Options{
				Track: func(parent context.Context, query string) (context.Context, func()) {
					if strings.Contains(query, "boom") {
						panic("injected query panic")
					}

					return parent, func() {}
				},
			}))

			t.Cleanup(func() {
				cancel()
				srv.Close()
			})

			bad, other := dial(t, srv), dial(t, srv)

			resp := call(t, bad, websocket.TextMessage, []byte(evalBody(tt.script, "")))
			require.Len(t, resp, 1)
			assert.Equal(t, gremlin.StatusServerError, resp[0].Status.Code)
			assert.Contains(t, resp[0].Status.Message, gremlin.CodeInternal)

			for _, c := range []*websocket.Conn{bad, other} {
				resp = call(t, c, websocket.TextMessage, []byte(evalBody(`g.V().count()`, "")))
				assert.Equal(t, 200, resp[0].Status.Code)
			}
		})
	}
}
