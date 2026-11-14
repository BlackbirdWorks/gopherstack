package azuregraph_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type batchItem = map[string]any

func batchResponses(t *testing.T, r resp) map[string]map[string]any {
	t.Helper()

	require.Equal(t, http.StatusOK, r.status, r.raw)

	items, _ := r.body["responses"].([]any)
	out := map[string]map[string]any{}

	for _, it := range items {
		m, _ := it.(map[string]any)
		out[str(m, "id")] = m
	}

	return out
}

func status(m map[string]any) int {
	f, _ := m["status"].(float64)

	return int(f)
}

func TestBatch(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	app := newApp(t, e, "Batched")

	t.Run("mixed methods in request order", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodPost, "/v1.0/$batch", map[string]any{"requests": []batchItem{
			{"id": "1", "method": "GET", "url": "/applications/" + str(app, "id")},
			{"id": "2", "method": "GET", "url": "/applications/" + newID0()},
			{
				"id":      "3",
				"method":  "POST",
				"url":     "/groups",
				"headers": map[string]string{"Content-Type": "application/json"},
				"body": map[string]any{
					"displayName": "BatchGroup", "mailEnabled": false, "mailNickname": "bg", "securityEnabled": true,
				},
			},
			{"id": "4", "method": "GET", "url": "/applications?$top=1&$select=displayName"},
		}})

		got := batchResponses(t, r)
		assert.Equal(t, http.StatusOK, status(got["1"]))
		assert.Equal(
			t,
			"Batched",
			str(got["1"]["body"].(map[string]any), "displayName"),
		) //nolint:forcetypeassert // asserted by status
		assert.Equal(t, http.StatusNotFound, status(got["2"]))
		assert.Equal(
			t,
			"Request_ResourceNotFound",
			str(
				got["2"]["body"].(map[string]any)["error"].(map[string]any),
				"code",
			),
		) //nolint:forcetypeassert // shape asserted
		assert.Equal(t, http.StatusCreated, status(got["3"]))
		assert.Equal(t, http.StatusOK, status(got["4"]))

		items, _ := r.body["responses"].([]any)
		require.Len(t, items, 4)
		assert.Equal(t, "1", str(items[0].(map[string]any), "id")) //nolint:forcetypeassert // shape asserted above
	})

	t.Run("dependsOn orders and short-circuits", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodPost, "/v1.0/$batch", map[string]any{"requests": []batchItem{
			{
				"id":        "get",
				"method":    "GET",
				"url":       "/groups?$filter=displayName%20eq%20'DepGroup'",
				"dependsOn": []string{"create"},
			},
			{"id": "create", "method": "POST", "url": "/groups", "body": map[string]any{
				"displayName": "DepGroup", "mailEnabled": false, "mailNickname": "dg", "securityEnabled": true,
			}},
			{"id": "fail", "method": "GET", "url": "/users/" + newID0()},
			{"id": "blocked", "method": "GET", "url": "/organization", "dependsOn": []string{"fail"}},
		}})

		got := batchResponses(t, r)
		assert.Equal(t, http.StatusCreated, status(got["create"]))
		assert.Equal(t, http.StatusOK, status(got["get"]))
		assert.Len(t, got["get"]["body"].(map[string]any)["value"], 1) //nolint:forcetypeassert // shape asserted
		assert.Equal(t, http.StatusNotFound, status(got["fail"]))
		assert.Equal(t, http.StatusFailedDependency, status(got["blocked"]))
	})

	t.Run("no content and text bodies", func(t *testing.T) {
		t.Parallel()

		r := e.req(t, http.MethodPost, "/v1.0/$batch", map[string]any{"requests": []batchItem{
			{
				"id":     "patch",
				"method": "PATCH",
				"url":    "/applications/" + str(app, "id"),
				"body":   map[string]any{"notes": "n"},
			},
			{
				"id": "count", "method": "GET", "url": "/applications/$count",
				"headers": map[string]string{"ConsistencyLevel": "eventual"},
			},
		}})

		got := batchResponses(t, r)
		assert.Equal(t, http.StatusNoContent, status(got["patch"]))
		assert.NotContains(t, got["patch"], "body")
		assert.Equal(t, http.StatusOK, status(got["count"]))
	})

	tests := []struct {
		body any
		name string
	}{
		{map[string]any{"requests": []batchItem{}}, "empty"},
		{"nope", "not json"},
		{map[string]any{"requests": []batchItem{
			{"id": "1", "method": "GET", "url": "/organization"}, {"id": "1", "method": "GET", "url": "/organization"},
		}}, "duplicate ids"},
		{map[string]any{"requests": []batchItem{{"method": "GET", "url": "/organization"}}}, "missing id"},
		{map[string]any{"requests": []batchItem{{"id": "1", "method": "GET", "url": "https://x/y"}}}, "absolute url"},
		{map[string]any{"requests": []batchItem{
			{"id": "1", "method": "GET", "url": "/organization", "dependsOn": []string{"9"}},
		}}, "unknown dependency"},
		{map[string]any{"requests": []batchItem{
			{"id": "1", "method": "GET", "url": "/organization", "dependsOn": []string{"2"}},
			{"id": "2", "method": "GET", "url": "/organization", "dependsOn": []string{"1"}},
		}}, "cycle"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertErr(
				t,
				e.req(t, http.MethodPost, "/v1.0/$batch", tt.body),
				http.StatusBadRequest,
				"Request_BadRequest",
			)
		})
	}

	t.Run("limit of twenty requests", func(t *testing.T) {
		t.Parallel()

		build := func(n int) map[string]any {
			reqs := make([]batchItem, 0, n)
			for i := range n {
				reqs = append(reqs, batchItem{"id": strconv.Itoa(i), "method": "GET", "url": "/organization"})
			}

			return map[string]any{"requests": reqs}
		}

		assert.Len(t, batchResponses(t, e.req(t, http.MethodPost, "/v1.0/$batch", build(20))), 20)
		assertErr(t, e.req(t, http.MethodPost, "/v1.0/$batch", build(21)), http.StatusBadRequest, "Request_BadRequest")
	})

	t.Run("nested batch and wrong verb", func(t *testing.T) {
		t.Parallel()

		got := batchResponses(t, e.req(t, http.MethodPost, "/v1.0/$batch", map[string]any{"requests": []batchItem{
			{"id": "n", "method": "POST", "url": "/$batch", "body": map[string]any{"requests": []batchItem{}}},
		}}))
		assert.Equal(t, http.StatusBadRequest, status(got["n"]))
		assertErr(t, e.req(t, http.MethodGet, "/v1.0/$batch", nil), http.StatusMethodNotAllowed, "Request_BadRequest")
	})
}
