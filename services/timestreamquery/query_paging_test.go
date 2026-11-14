package timestreamquery_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

const (
	pagingUnnest = "SELECT n FROM UNNEST(SEQUENCE(1, 5)) AS t(n)"
	pagingBig    = "SELECT n, lpad('', 400000, 'a') AS big FROM UNNEST(SEQUENCE(1, 5)) AS t(n)"
)

func pagingToken(t *testing.T, resp map[string]any) string {
	t.Helper()

	tok, _ := resp["NextToken"].(string)

	return tok
}

func TestQueryPagingRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *timestreamquery.Handler)
		name string
	}{
		{name: "newer token invalidates older", run: func(t *testing.T, h *timestreamquery.Handler) {
			t.Helper()

			first := parseResponse(
				t,
				doRequest(t, h, "Query", map[string]any{"QueryString": pagingUnnest, "MaxRows": 1}),
			)
			tokenA := pagingToken(t, first)
			require.NotEmpty(t, tokenA)

			second := doRequest(
				t,
				h,
				"Query",
				map[string]any{"QueryString": pagingUnnest, "MaxRows": 1, "NextToken": tokenA},
			)
			require.Equal(t, http.StatusOK, second.Code)
			tokenB := pagingToken(t, parseResponse(t, second))
			require.NotEmpty(t, tokenB)

			again := doRequest(
				t,
				h,
				"Query",
				map[string]any{"QueryString": pagingUnnest, "MaxRows": 1, "NextToken": tokenA},
			)
			assert.Equal(t, http.StatusOK, again.Code, "the older token still works until a newer one is used")

			third := doRequest(
				t,
				h,
				"Query",
				map[string]any{"QueryString": pagingUnnest, "MaxRows": 1, "NextToken": tokenB},
			)
			require.Equal(t, http.StatusOK, third.Code)

			stale := doRequest(
				t,
				h,
				"Query",
				map[string]any{"QueryString": pagingUnnest, "MaxRows": 1, "NextToken": tokenA},
			)
			assert.Equal(t, http.StatusBadRequest, stale.Code)
			assert.Contains(t, stale.Body.String(), "Invalid pagination token")
		}},
		{name: "other query string", run: func(t *testing.T, h *timestreamquery.Handler) {
			t.Helper()

			first := parseResponse(
				t,
				doRequest(t, h, "Query", map[string]any{"QueryString": pagingUnnest, "MaxRows": 1}),
			)

			rec := doRequest(t, h, "Query", map[string]any{
				"QueryString": "SELECT 1", "MaxRows": 1, "NextToken": pagingToken(t, first),
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "Invalid pagination token")
		}},
		{name: "one megabyte page", run: func(t *testing.T, h *timestreamquery.Handler) {
			t.Helper()

			rows, pages, token := 0, 0, ""

			for {
				body := map[string]any{"QueryString": pagingBig}
				if token != "" {
					body["NextToken"] = token
				}

				rec := doRequest(t, h, "Query", body)
				require.Equal(t, http.StatusOK, rec.Code)
				assert.LessOrEqual(t, rec.Body.Len(), 1<<20)

				resp := parseResponse(t, rec)
				rows += len(resp["Rows"].([]any))
				pages++

				if token = pagingToken(t, resp); token == "" {
					break
				}
			}

			assert.Equal(t, 5, rows)
			assert.Equal(t, 3, pages)
		}},
		{name: "row over one megabyte", run: func(t *testing.T, h *timestreamquery.Handler) {
			t.Helper()

			rec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT lpad('', 1100000, 'a') AS big"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "QueryExecutionException")
			assert.Contains(t, rec.Body.String(), "max page response size has been exceeded by the output result row")
		}},
		{name: "client token rules", run: func(t *testing.T, h *timestreamquery.Handler) {
			t.Helper()

			short := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1", "ClientToken": "short"})
			assert.Equal(t, http.StatusBadRequest, short.Code)

			token := strings.Repeat("t", 40)
			ok := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1", "ClientToken": token})
			require.Equal(t, http.StatusOK, ok.Code)

			changed := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 2", "ClientToken": token})
			assert.Equal(t, http.StatusBadRequest, changed.Code)
			assert.Contains(t, changed.Body.String(), "ValidationException")
		}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestHandler())
		})
	}
}
