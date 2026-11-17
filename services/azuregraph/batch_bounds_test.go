package azuregraph_test

import (
	"net/http"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchMalformedInput(t *testing.T) {
	t.Parallel()

	org := func(id string, deps ...string) batchItem {
		return batchItem{"id": id, "method": "GET", "url": "/organization", "dependsOn": deps}
	}

	tests := []struct {
		body any
		name string
	}{
		{map[string]any{}, "no requests member"},
		{map[string]any{"requests": "x"}, "requests not a list"},
		{map[string]any{"requests": []any{1}}, "request not an object"},
		{map[string]any{"requests": []batchItem{org("1", "1")}}, "self dependency"},
		{map[string]any{"requests": []batchItem{org("1", "3"), org("2", "1"), org("3", "2")}}, "three step cycle"},
		{map[string]any{"requests": []batchItem{{"id": "1", "url": "/organization"}}}, "missing method"},
		{map[string]any{"requests": []batchItem{{"id": "1", "method": "GET"}}}, "missing url"},
		{
			map[string]any{"requests": []batchItem{{"id": "1", "method": "GET", "url": "organization"}}},
			"url without slash",
		},
		{map[string]any{"requests": []batchItem{{"id": "", "method": "GET", "url": "/organization"}}}, "empty id"},
		{`{"requests":[`, "truncated"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertErr(
				t,
				newEnv(t).req(t, http.MethodPost, "/v1.0/$batch", tt.body),
				http.StatusBadRequest,
				"Request_BadRequest",
			)
		})
	}
}

func TestBatchLimitUnderConcurrency(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	build := func(n int) map[string]any {
		reqs := make([]batchItem, 0, n)
		for i := range n {
			reqs = append(reqs, batchItem{"id": strconv.Itoa(i), "method": "GET", "url": "/organization"})
		}

		return map[string]any{"requests": reqs}
	}

	var wg sync.WaitGroup

	statuses := make([]int, 16)

	for i := range statuses {
		wg.Go(func() {
			n := 20
			if i%2 == 1 {
				n = 21
			}

			statuses[i] = e.req(t, http.MethodPost, "/v1.0/$batch", build(n)).status
		})
	}

	wg.Wait()

	for i, s := range statuses {
		want := http.StatusOK
		if i%2 == 1 {
			want = http.StatusBadRequest
		}

		assert.Equal(t, want, s, "request %d", i)
	}
}
