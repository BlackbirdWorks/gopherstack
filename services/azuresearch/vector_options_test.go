package azuresearch_test

import (
	"encoding/json/v2"
	"maps"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func vq(field string, extra map[string]any) map[string]any {
	q := map[string]any{"kind": "vector", "vector": []any{1, 0, 0}, "fields": field, "k": 4}
	maps.Copy(q, extra)

	return q
}

func TestVectorThresholdAndOverrides(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedVectors(t)

	sim := func(v float64) map[string]any {
		return map[string]any{"threshold": map[string]any{"kind": "vectorSimilarity", "value": v}}
	}
	badThreshold := map[string]any{"threshold": map[string]any{"kind": "x", "value": 1}}
	score := func(v float64) map[string]any {
		return map[string]any{"threshold": map[string]any{"kind": "searchScore", "value": v}}
	}

	cases := []struct {
		body   map[string]any
		name   string
		want   []string
		status int
	}{
		{map[string]any{"vectorQueries": []any{vq("Vec", sim(0.99))}}, "cosine similarity", []string{"a", "c"}, 200},
		{map[string]any{"vectorQueries": []any{vq("Vec", sim(0.999))}}, "cosine strict", []string{"a"}, 200},
		{map[string]any{"vectorQueries": []any{vq("VecE", sim(0.2))}}, "euclid distance", []string{"a", "c"}, 200},
		{map[string]any{"vectorQueries": []any{vq("Vec", score(0.999))}}, "search score", []string{"a"}, 200},
		{map[string]any{"vectorQueries": []any{vq("Vec,VecE", sim(0.5))}}, "mixed metrics", nil, 400},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", badThreshold)}},
			"bad kind", nil, 400,
		},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", map[string]any{"filterOverride": "HotelId eq 'b'"})}},
			"filter override", []string{"b"}, 200,
		},
		{
			map[string]any{
				"filter":        "HotelId eq 'a'",
				"vectorQueries": []any{vq("Vec", map[string]any{"filterOverride": "HotelId eq 'b'"})},
			},
			"override wins", []string{"b"}, 200,
		},
		{
			map[string]any{"vectorQueries": []any{vq("Vec", map[string]any{"oversampling": 2})}},
			"oversampling uncompressed",
			nil,
			400,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tc.body["select"] = "HotelId"
			r := searchPost(t, s, tc.body)
			require.Equal(t, tc.status, r.status, string(r.raw))

			if tc.status == http.StatusOK {
				assert.Equal(t, tc.want, idsOf(r))
			}
		})
	}
}

func TestVectorDebugAndRecall(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedVectors(t)

	r := searchPost(t, s, map[string]any{
		"search": "apple", "debug": "vector", "select": "HotelId",
		"vectorQueries": []any{vq("Vec", map[string]any{"k": 1})},
	})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))

	row := r.body["value"].([]any)[0].(map[string]any)
	sub := row["@search.documentDebugInfo"].(map[string]any)["vectors"].(map[string]any)["subscores"].(map[string]any)
	assert.Contains(t, sub["text"], "searchScore")
	assert.InDelta(t, 1.0, sub["documentBoost"], 0)

	vec := sub["vectors"].([]any)[0].(map[string]any)["Vec"].(map[string]any)
	assert.InDelta(t, 1.0, vec["vectorSimilarity"], 0.0001)
	assert.InDelta(t, 1.0, vec["searchScore"], 0.0001)

	off := searchPost(t, s, map[string]any{"vectorQueries": []any{vq("Vec", nil)}})
	assert.NotContains(t, string(off.raw), "documentDebugInfo")

	cases := []struct {
		hybrid map[string]any
		name   string
		count  float64
		status int
	}{
		{map[string]any{"maxTextRecallSize": 1}, "window all", 3, 200},
		{map[string]any{"maxTextRecallSize": 1, "countAndFacetMode": "countRetrievableResults"}, "window only", 2, 200},
		{map[string]any{"maxTextRecallSize": 0}, "too small", 0, 400},
		{map[string]any{"maxTextRecallSize": 10001}, "too large", 0, 400},
		{map[string]any{"countAndFacetMode": "nope"}, "bad mode", 0, 400},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			res := searchPost(t, s, map[string]any{
				"search": "apple", "count": true, "hybridSearch": tc.hybrid, "select": "HotelId",
				"vectorQueries": []any{vq("Vec", map[string]any{"k": 1, "vector": []any{0, 0, 1}})},
			})
			require.Equal(t, tc.status, res.status, string(res.raw))

			if tc.status == http.StatusOK {
				assert.InDelta(t, tc.count, res.body["@odata.count"], 0)
			}
		})
	}
}

func compressedIndex(comp map[string]any, profile map[string]any) map[string]any {
	ix := vectorIndex()
	vs := ix["vectorSearch"].(map[string]any)
	vs["compressions"] = []any{comp}
	profile = maps.Clone(profile)
	profile["name"], profile["algorithm"] = "cos", "h"
	vs["profiles"] = []any{profile, map[string]any{"name": "euc", "algorithm": "e"}}

	return ix
}

func compression(kind string, extra map[string]any) map[string]any {
	c := map[string]any{"name": "c", "kind": kind}
	maps.Copy(c, extra)

	return c
}

func rescoring(opts map[string]any) map[string]any { return map[string]any{"rescoringOptions": opts} }

func TestVectorCompression(t *testing.T) {
	t.Parallel()

	useC := map[string]any{"compression": "c"}
	noRescore := rescoring(map[string]any{"enableRescoring": false})
	discard := rescoring(map[string]any{"rescoreStorageMethod": "discardOriginals"})
	overNoRescore := rescoring(map[string]any{"enableRescoring": false, "defaultOversampling": 2})

	cases := []struct {
		comp    map[string]any
		profile map[string]any
		oversam any
		name    string
		cScore  float64
		status  int
	}{
		{compression("binaryQuantization", nil), useC, nil, "rescored", 1 / (2 - 0.9/0.9055385138137417), 201},
		{compression("binaryQuantization", noRescore), useC, nil, "binary raw", 1 / (2 - 1/1.7320508075688772), 201},
		{compression("scalarQuantization", discard), useC, nil, "discard originals", 0, 201},
		{compression("other", nil), map[string]any{}, nil, "bad kind", 0, 400},
		{compression("binaryQuantization", nil), map[string]any{"compression": "zz"}, nil, "unknown ref", 0, 400},
		{compression("binaryQuantization", overNoRescore), useC, nil, "oversampling needs rescoring", 0, 400},
		{compression("binaryQuantization", nil), useC, 2, "query oversampling", 1, 201},
		{compression("binaryQuantization", nil), useC, 0.5, "oversampling below one", 0, 201},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			r := s.admin2(t, http.MethodPost, "/indexes?"+apiVer, compressedIndex(tc.comp, tc.profile))
			require.Equal(t, tc.status, r.status, string(r.raw))

			if tc.status != http.StatusCreated {
				return
			}

			s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
				map[string]any{"HotelId": "c", "Vec": []any{0.9, 0.1, 0}},
			}}, http.StatusOK)

			extra := map[string]any{}
			if tc.oversam != nil {
				extra["oversampling"] = tc.oversam
			}

			q := searchPost(t, s, map[string]any{"vectorQueries": []any{vq("Vec", extra)}, "select": "HotelId"})

			switch tc.name {
			case "oversampling below one":
				require.Equal(t, http.StatusBadRequest, q.status, string(q.raw))
			case "discard originals":
				require.Equal(t, http.StatusOK, q.status, string(q.raw))
				assert.Less(t, scoreOf(t, q, 0), 0.9999)
			case "query oversampling":
				require.Equal(t, http.StatusOK, q.status, string(q.raw))
			default:
				require.Equal(t, http.StatusOK, q.status, string(q.raw))
				assert.InDelta(t, tc.cScore, scoreOf(t, q, 0), 0.0001)
			}
		})
	}
}

func TestTextVectorQueryCustomWebApi(t *testing.T) {
	t.Parallel()

	var got map[string]any

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.UnmarshalRead(r.Body, &got, jsonx.V1())
		_, _ = w.Write([]byte(`{"values":[{"recordId":"0","data":{"vector":[0,1,0]}}]}`))
	}))
	t.Cleanup(api.Close)

	mk := func(kind string, params map[string]any) map[string]any {
		ix := vectorIndex()
		vs := ix["vectorSearch"].(map[string]any)
		v := map[string]any{"name": "vz", "kind": kind}

		if params != nil {
			v["customWebApiParameters"] = params
		}

		vs["vectorizers"] = []any{v}
		vs["profiles"] = []any{
			map[string]any{"name": "cos", "algorithm": "h", "vectorizer": "vz"},
			map[string]any{"name": "euc", "algorithm": "e"},
		}

		return ix
	}

	s := newStack(t, "standard")
	s.must(
		t,
		http.MethodPost,
		"/indexes?"+apiVer,
		mk("customWebApi", map[string]any{"uri": api.URL}),
		http.StatusCreated,
	)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{"HotelId": "a", "Vec": []any{1, 0, 0}}, map[string]any{"HotelId": "b", "Vec": []any{0, 1, 0}},
	}}, http.StatusOK)

	r := searchPost(t, s, map[string]any{
		"select":        "HotelId",
		"vectorQueries": []any{map[string]any{"kind": "text", "text": "banana", "fields": "Vec", "k": 1}},
	})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))
	assert.Equal(t, []string{"b"}, idsOf(r))

	data := got["values"].([]any)[0].(map[string]any)["data"].(map[string]any)
	assert.Equal(t, "banana", data["text"])

	noVz := newStack(t, "standard")
	noVz.must(t, http.MethodPost, "/indexes?"+apiVer, vectorIndex(), http.StatusCreated)

	for name, body := range map[string]*stack{"no vectorizer": noVz} {
		r = searchPost(t, body, map[string]any{
			"vectorQueries": []any{map[string]any{"kind": "text", "text": "x", "fields": "Vec"}},
		})
		assert.Equal(t, http.StatusBadRequest, r.status, name)
	}

	oa := newStack(t, "standard")
	oa.must(t, http.MethodPost, "/indexes?"+apiVer, mk("azureOpenAI", nil), http.StatusCreated)
	r = searchPost(t, oa, map[string]any{
		"vectorQueries": []any{map[string]any{"kind": "text", "text": "x", "fields": "Vec"}},
	})
	assert.Equal(t, http.StatusBadRequest, r.status, string(r.raw))
}
