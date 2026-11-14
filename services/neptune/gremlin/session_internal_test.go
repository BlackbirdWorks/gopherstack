package gremlin

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

func sessionMsg(op, script string) *message {
	args := map[string]any{"session": "s1"}
	if script != "" {
		args["gremlin"] = script
	}

	return &message{id: "r", op: op, processor: "session", args: args}
}

func TestSessionLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		finish    func(t *testing.T, s *Server)
		name      string
		wantNodes int
	}{
		{name: "close_commits_script_session", wantNodes: 2, finish: func(t *testing.T, s *Server) {
			t.Helper()

			_, err := s.execute(t.Context(), sessionMsg("close", ""))
			require.NoError(t, err)
		}},
		{name: "lifetime_expiry_rolls_back", finish: func(*testing.T, *Server) { time.Sleep(2 * time.Minute) }},
		{name: "failed_request_rolls_back_session", wantNodes: 1, finish: func(t *testing.T, s *Server) {
			t.Helper()

			_, err := s.execute(t.Context(), sessionMsg(opEval, "g.V('missing').fail('boom')"))
			require.Error(t, err)
			_, err = s.execute(t.Context(), sessionMsg(opEval, "g.addV('c').next()"))
			require.NoError(t, err)
			_, err = s.execute(t.Context(), sessionMsg("close", ""))
			require.NoError(t, err)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				g := opencypher.NewGraph()
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				s := NewServer(ctx, g, Options{SessionLifetime: time.Minute})

				for _, label := range []string{"a", "b"} {
					_, err := s.execute(ctx, sessionMsg(opEval, "g.addV('"+label+"').next()"))
					require.NoError(t, err)
				}

				tt.finish(t, s)
				synctest.Wait()

				nodes, _ := g.Counts()
				assert.Equal(t, tt.wantNodes, nodes)
			})
		})
	}
}
