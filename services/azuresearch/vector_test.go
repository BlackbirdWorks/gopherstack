package azuresearch_test

import (
	"maps"
	"math"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func vectorIndex() map[string]any {
	return map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{"name": "Body", "type": "Edm.String", "searchable": true},
			map[string]any{
				"name":                "Vec",
				"type":                "Collection(Edm.Single)",
				"dimensions":          3,
				"vectorSearchProfile": "cos",
				"searchable":          true,
				"retrievable":         true,
			},
			map[string]any{
				"name":                "VecE",
				"type":                "Collection(Edm.Single)",
				"dimensions":          3,
				"vectorSearchProfile": "euc",
			},
		},
		"vectorSearch": map[string]any{
			"algorithms": []any{
				map[string]any{
					"name":           "h",
					"kind":           "hnsw",
					"hnswParameters": map[string]any{"metric": "cosine", "m": 4},
				},
				map[string]any{
					"name":                    "e",
					"kind":                    "exhaustiveKnn",
					"exhaustiveKnnParameters": map[string]any{"metric": "euclidean"},
				},
			},
			"profiles": []any{
				map[string]any{"name": "cos", "algorithm": "h"},
				map[string]any{"name": "euc", "algorithm": "e"},
			},
		},
	}
}

func (s *stack) seedVectors(t *testing.T) {
	t.Helper()

	s.must(t, http.MethodPost, "/indexes?"+apiVer, vectorIndex(), http.StatusCreated)

	doc := func(id, body string, v []any) map[string]any {
		return map[string]any{"HotelId": id, "Body": body, "Vec": v, "VecE": v}
	}

	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		doc("a", "apple pie", []any{1, 0, 0}), doc("b", "banana bread", []any{0, 1, 0}),
		doc("c", "apple tart", []any{0.9, 0.1, 0}), doc("d", "cherry cake", []any{0, 0, 1}),
	}}, http.StatusOK)
}

func scoreOf(t *testing.T, r resp, i int) float64 {
	t.Helper()

	rows, _ := r.body["value"].([]any)
	row, _ := rows[i].(map[string]any)
	sc, _ := row["@search.score"].(float64)

	return sc
}

func TestVectorSearch(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedVectors(t)

	vq := func(field string, k int, extra map[string]any) map[string]any {
		q := map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": field, "k": k}
		maps.Copy(q, extra)

		return q
	}

	cases := []struct {
		body  map[string]any
		name  string
		want  []string
		score float64
	}{
		{
			map[string]any{"vectorQueries": []any{vq("Vec", 2, nil)}, "select": "HotelId"},
			"cosine knn",
			[]string{"a", "c"},
			1.0,
		},
		{
			map[string]any{"vectorQueries": []any{vq("VecE", 3, nil)}, "select": "HotelId"},
			"euclidean knn",
			[]string{"a", "c", "b"},
			1.0,
		},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", 1, nil)}, "filter": "HotelId ne 'a'", "select": "HotelId"},
			"prefilter",
			[]string{"c"},
			0,
		},
		{
			map[string]any{
				"vectorQueries":    []any{vq("Vec", 1, nil)},
				"filter":           "HotelId ne 'a'",
				"vectorFilterMode": "postFilter",
				"select":           "HotelId",
			},
			"postfilter",
			nil,
			0,
		},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", 4, nil)}, "top": 2, "select": "HotelId"},
			"top limits",
			[]string{"a", "c"},
			0,
		},
		{
			map[string]any{"search": "banana", "vectorQueries": []any{vq("Vec", 2, nil)}, "select": "HotelId"},
			"hybrid rrf",
			[]string{"b", "a", "c"},
			1.0 / 61,
		},
		{
			map[string]any{"search": "apple", "vectorQueries": []any{vq("Vec", 2, nil)}, "select": "HotelId"},
			"hybrid agreement boosts",
			[]string{"a", "c"},
			2.0 / 61,
		},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", 1, nil), vq("VecE", 1, nil)}, "select": "HotelId"},
			"multi vector",
			[]string{"a"},
			2.0 / 61,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := searchPost(t, s, tc.body)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))
			assert.Equal(t, tc.want, append([]string(nil), idsOf(r)...))

			if tc.score > 0 {
				assert.InDelta(t, tc.score, scoreOf(t, r, 0), 0.0001)
			}
		})
	}
}

func idsOf(r resp) []string { return keysByField(r, "HotelId") }

func keysByField(r resp, field string) []string {
	rows, _ := r.body["value"].([]any)

	out := make([]string, 0, len(rows))

	for _, row := range rows {
		m, _ := row.(map[string]any)
		id, _ := m[field].(string)
		out = append(out, id)
	}

	return out
}

func TestVectorSimilarityValues(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedVectors(t)

	r := searchPost(t, s, map[string]any{
		"vectorQueries": []any{map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": "Vec", "k": 4}},
		"select":        "HotelId",
	})
	require.Equal(t, http.StatusOK, r.status)

	cosC := 0.9 / math.Sqrt(0.9*0.9+0.1*0.1)
	assert.InDelta(t, 1/(2-cosC), scoreOf(t, r, 1), 0.0001, "cosine score is 1/(2-cos)")
	assert.InDelta(t, 0.5, scoreOf(t, r, 3), 0.0001, "orthogonal vectors score 1/(2-0)")

	e := searchPost(t, s, map[string]any{
		"vectorQueries": []any{map[string]any{"kind": "vector", "vector": []any{0, 1, 0}, "fields": "VecE", "k": 4}},
		"select":        "HotelId",
	})
	assert.InDelta(t, 1.0, scoreOf(t, e, 0), 0.0001)
	assert.InDelta(t, 1/(1+math.Sqrt(2)), scoreOf(t, e, 2), 0.0001, "euclidean score is 1/(1+distance)")
}

func TestVectorErrors(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedVectors(t)

	cases := []struct {
		vq   map[string]any
		name string
	}{
		{map[string]any{"kind": "vector", "vector": []any{1, 0}, "fields": "Vec", "k": 1}, "dimension mismatch"},
		{map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": "Body", "k": 1}, "not a vector field"},
		{map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": "Nope", "k": 1}, "unknown field"},
		{map[string]any{"kind": "text", "text": "hello", "fields": "Vec", "k": 1}, "text kind needs a vectorizer"},
		{map[string]any{"kind": "vector", "fields": "Vec", "k": 1}, "empty vector"},
		{map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": "Vec", "k": 0}, "zero k"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := searchPost(t, s, map[string]any{"vectorQueries": []any{tc.vq}})
			assert.Equal(t, http.StatusBadRequest, r.status, string(r.raw))
		})
	}

	bad := s.admin2(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{"HotelId": "x", "Vec": []any{1, 2}},
	}})
	assert.Equal(t, http.StatusMultiStatus, bad.status, "a vector of the wrong length is rejected per document")
}

func TestVectorIndexValidation(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	noProfile := vectorIndex()
	noProfile["vectorSearch"] = map[string]any{"algorithms": []any{}, "profiles": []any{}}

	badAlg := vectorIndex()
	badAlg["vectorSearch"].(map[string]any)["algorithms"] = []any{
		map[string]any{"name": "h", "kind": "ivf"},
	} //nolint:forcetypeassert // test fixture shape

	badMetric := vectorIndex()
	badMetric["vectorSearch"].(map[string]any)["algorithms"] = []any{ //nolint:forcetypeassert // test fixture shape
		map[string]any{"name": "h", "kind": "hnsw", "hnswParameters": map[string]any{"metric": "manhattan"}},
		map[string]any{"name": "e", "kind": "exhaustiveKnn"},
	}

	for name, def := range map[string]map[string]any{"missing profile": noProfile, "bad algorithm": badAlg, "bad metric": badMetric} { //nolint:lll // test fixture
		r := s.admin2(t, http.MethodPost, "/indexes?"+apiVer, def)
		assert.Equal(t, http.StatusBadRequest, r.status, name+": "+string(r.raw))
	}
}
