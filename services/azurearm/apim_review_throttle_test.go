package azurearm_test

import (
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func (e *gwEnv) parallelStatuses(t *testing.T, n int) map[int]int {
	t.Helper()

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		bad atomic.Int64
		out = map[int]int{}
	)

	for range n {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				e.url+"/_apim/"+apimSvc+"/echo/items/1", nil)
			if err != nil {
				bad.Add(1)

				return
			}

			req.Header.Set("Ocp-Apim-Subscription-Key", e.key)

			resp, err := e.hc.Do(req)
			if err != nil {
				bad.Add(1)

				return
			}

			_ = resp.Body.Close()

			mu.Lock()
			out[resp.StatusCode]++
			mu.Unlock()
		})
	}

	wg.Wait()
	require.Zero(t, bad.Load())

	return out
}

func TestAPIMGateway_CountersUnderConcurrency(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, policy string
		denied       int
	}{
		{"rate limit by key", `<rate-limit-by-key calls="10" renewal-period="60" counter-key="k" />`, 429},
		{"rate limit", `<rate-limit calls="10" renewal-period="60" />`, 429},
		{"quota by key", `<quota-by-key calls="10" renewal-period="3600" counter-key="k" />`, 403},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newGWEnv(t)
			e.policy(t, "/apis/echo", policyDoc(tt.policy, "", ""), http.StatusCreated)

			got := e.parallelStatuses(t, 60)
			assert.Equal(t, 10, got[http.StatusOK], "%v", got)
			assert.Equal(t, 50, got[tt.denied], "%v", got)
		})
	}
}
