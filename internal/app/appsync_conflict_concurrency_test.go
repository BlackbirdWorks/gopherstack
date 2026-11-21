package app

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

var errUnexpectedStatus = errors.New("unexpected status")

func (f *asCFFixture) post(query string) (asGQLResponse, error) {
	var out asGQLResponse

	body, err := json.Marshal(map[string]any{"query": query}, jsonx.V1())
	if err != nil {
		return out, err
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/apis/"+f.apiID+"/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Api-Key", f.key)

	rec := httptest.NewRecorder()
	f.fx.handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		return out, fmt.Errorf("%w %d: %s", errUnexpectedStatus, rec.Code, rec.Body.String())
	}

	return out, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1())
}

func TestAppSyncAutomergeConcurrentStaleWritesLoseNothing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		writers int
	}{
		{name: "five writers", writers: 5},
		{name: "twelve writers", writers: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newASCFFixture(t, asCFSync("AUTOMERGE", ""), nil)
			f.seed(t)

			var (
				wg  sync.WaitGroup
				mu  sync.Mutex
				won int
			)

			for i := range tt.writers {
				wg.Go(func() {
					q := fmt.Sprintf(`mutation { putItem(id:"i1", title:"T", tags:["w%d"], labels:["l%d"], v:1) %s }`,
						i, i, asCFSel)
					res, err := f.post(q)
					if err != nil || len(res.Errors) > 0 {
						return
					}

					mu.Lock()
					won++
					mu.Unlock()
				})
			}

			wg.Wait()

			got := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
			require.Empty(t, got.Errors)

			item, _ := got.Data["getItem"].(map[string]any)
			tags, _ := item["tags"].([]any)

			if tt.writers <= 5 {
				assert.Equal(t, tt.writers, won, "every writer fits the retry bound")
			}

			assert.InDelta(t, 2+won, item["_version"], 0)
			assert.Len(t, tags, 1+won, "no merged write was lost")
		})
	}
}

func TestAppSyncUpdateConcurrentMatchingVersionOnlyOneWins(t *testing.T) {
	t.Parallel()

	f := newASCFFixture(t, asCFSync("OPTIMISTIC_CONCURRENCY", ""), nil)
	f.seed(t)

	const writers = 10

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		won int
	)

	for i := range writers {
		wg.Go(func() {
			res, err := f.post(fmt.Sprintf(`mutation { updateItem(id:"i1", title:"T%d", v:2) %s }`, i, asCFSel))
			if err != nil || len(res.Errors) > 0 {
				return
			}

			mu.Lock()
			won++
			mu.Unlock()
		})
	}

	wg.Wait()

	got := f.do(t, `query { getItem(id:"i1") `+asCFSel+` }`)
	item, _ := got.Data["getItem"].(map[string]any)

	assert.Equal(t, 1, won)
	assert.InDelta(t, 3, item["_version"], 0)
}
