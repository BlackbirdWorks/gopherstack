package iot_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestListThings_Pagination verifies that GET /things honors maxResults and
// returns a nextToken, walking pages without dropping or duplicating things.
// Previously the op accepted and returned no pagination at all.
func TestListThings_Pagination(t *testing.T) {
	t.Parallel()

	h, b := newRefHandler()

	const total = 5
	for i := range total {
		b.AddThingInternal(iot.Thing{ThingName: fmt.Sprintf("thing-%02d", i)})
	}

	type listResp struct {
		NextToken string           `json:"nextToken"`
		Things    []map[string]any `json:"things"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		path := "/things?maxResults=2"
		if token != "" {
			path += "&nextToken=" + token
		}

		rec := doRefRequest(t, h, http.MethodGet, path, nil, nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.Things), 2, "page exceeds maxResults")

		for _, th := range resp.Things {
			name := th["thingName"].(string)
			assert.False(t, seen[name], "thing %s returned twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10, "pagination did not terminate")

		token = resp.NextToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total, "all things returned exactly once")
	assert.GreaterOrEqual(t, pages, 3, "maxResults=2 over 5 items should span >=3 pages")
}
