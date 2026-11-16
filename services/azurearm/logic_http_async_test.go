package azurearm_test

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogicHTTPAsyncPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		options    string
		wantPolls  int
		wantStatus float64
	}{
		{name: "polls location until done", wantStatus: http.StatusOK, wantPolls: 2},
		{name: "disabled returns the 202", options: "DisableAsyncPattern", wantStatus: http.StatusAccepted},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)

			var (
				mu    sync.Mutex
				polls int
				auth  []string
			)

			start := s.clock.Now()
			elapsed := []time.Duration{}
			mux := http.NewServeMux()
			mux.HandleFunc("/start", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", "/status")
				w.Header().Set("Retry-After", "3")
				w.WriteHeader(http.StatusAccepted)
			})
			mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				polls++
				n := polls
				auth = append(auth, r.Header.Get("Authorization"))
				elapsed = append(elapsed, s.clock.Now().Sub(start))
				mu.Unlock()

				if n == 1 {
					w.Header().Set("Location", "/status")
					w.WriteHeader(http.StatusAccepted)

					return
				}

				_, _ = w.Write([]byte("done"))
			})

			backend := httptest.NewServer(mux)
			t.Cleanup(backend.Close)

			call := la{
				"type": "Http", "runAfter": la{},
				"inputs": la{
					"method": "POST", "uri": backend.URL + "/start", "retryPolicy": la{"type": "none"},
					"authentication": la{"type": "Raw", "value": "tok"},
				},
			}
			if tc.options != "" {
				call["operationOptions"] = tc.options
			}

			s.create(t, "poller", logicDef(requestTrigger(nil), la{"Call": call}))

			code, _, _ := s.post(t, s.callback(t, "poller"), `{}`)
			require.Equal(t, http.StatusAccepted, code)

			if tc.wantPolls > 0 {
				s.clock.waitTimer(t)
				s.clock.Advance(3 * time.Second)
				s.clock.waitTimer(t)
				s.clock.Advance(time.Second)
			}

			s.waitRuns(t)

			acts := s.actions(t, "poller")
			assert.Equal(t, armlogic.WorkflowStatusSucceeded, *acts["Call"].Properties.Status)

			out, ok := s.content(t, acts["Call"].Properties.OutputsLink).(map[string]any)
			require.True(t, ok)
			assert.InDelta(t, tc.wantStatus, out["statusCode"], 0)

			mu.Lock()
			defer mu.Unlock()

			assert.Equal(t, tc.wantPolls, polls)

			if tc.wantPolls > 0 {
				assert.Equal(t, []time.Duration{3 * time.Second, 4 * time.Second}, elapsed,
					"Retry-After 3 for the first poll, the default one second after a 202 without it")
				assert.Equal(t, "done", out["body"])
				assert.Equal(t, []string{"tok", "tok"}, auth, "same-origin polls keep the authorization header")
			}
		})
	}
}

func TestLogicHTTPCookieAndOmittedHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers la
		want    http.Header
		name    string
		method  string
		cookie  string
	}{
		{
			name: "cookie property is sent and omitted headers dropped", method: "POST", cookie: "a=b",
			headers: la{
				"Cookie": "x=y", "Origin": "http://o", "Accept-Language": "fr", "Accept-Version": "2",
				"Content-Language": "fr", "Content-Type": "text/plain", "X-Keep": "1", "Expires": "now",
			},
			want: http.Header{
				"Cookie": {"a=b"}, "Accept-Version": {"2"}, "Content-Type": {"text/plain"}, "X-Keep": {"1"},
			},
		},
		{
			name: "cookie header alone is ignored", method: "GET",
			headers: la{"Cookie": "x=y", "Content-Type": "text/plain"},
			want:    http.Header{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newLogicStack(t)
			got := make(chan http.Header, 1)
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got <- r.Header.Clone()
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(backend.Close)

			in := la{
				"method": tc.method, "uri": backend.URL, "headers": tc.headers, "retryPolicy": la{"type": "none"},
			}
			if tc.cookie != "" {
				in["cookie"] = tc.cookie
			}

			s.create(t, "cookies", logicDef(requestTrigger(nil), la{
				"Call": la{"type": "Http", "runAfter": la{}, "inputs": in},
			}))
			s.invoke(t, "cookies", `{}`)

			h := <-got

			keys := []string{
				"Cookie", "Origin", "Accept-Language", "Content-Language", "Expires", "Accept-Version",
				"Content-Type", "X-Keep",
			}

			for _, k := range keys {
				assert.Equal(t, tc.want.Values(k), h.Values(k), k)
			}
		})
	}
}
