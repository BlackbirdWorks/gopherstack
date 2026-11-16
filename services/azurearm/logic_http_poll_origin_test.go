package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicHTTPPollHeadersByOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantKey     string
		wantAuth    string
		crossOrigin bool
	}{
		{name: "same origin keeps credentials", wantKey: "k", wantAuth: "tok"},
		{name: "other origin gets workflow headers only", crossOrigin: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			polled := make(chan http.Header, 1)
			poll := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				polled <- r.Header.Clone()
				_, _ = w.Write([]byte("done"))
			})

			other := httptest.NewServer(poll)
			t.Cleanup(other.Close)

			mux := http.NewServeMux()
			mux.Handle("/status", poll)
			mux.HandleFunc("/start", func(w http.ResponseWriter, _ *http.Request) {
				loc := "/status"
				if tc.crossOrigin {
					loc = other.URL + "/status"
				}

				w.Header().Set("Location", loc)
				w.Header().Set("Retry-After", "0")
				w.WriteHeader(http.StatusAccepted)
			})

			origin := httptest.NewServer(mux)
			t.Cleanup(origin.Close)

			s.create(t, "poller", logicDef(requestTrigger(nil), la{"Call": la{
				"type": "Http", "runAfter": la{},
				"inputs": la{
					"method": "POST", "uri": origin.URL + "/start", "retryPolicy": la{"type": "none"},
					"headers":        la{"X-Api-Key": "k"},
					"authentication": la{"type": "Raw", "value": "tok"},
				},
			}}))
			s.invoke(t, "poller", `{}`)

			h := <-polled
			assert.Equal(t, tc.wantKey, h.Get("X-Api-Key"))
			assert.Equal(t, tc.wantAuth, h.Get("Authorization"))
			require.NotEmpty(t, h.Get("X-Ms-Workflow-Name"))
		})
	}
}
