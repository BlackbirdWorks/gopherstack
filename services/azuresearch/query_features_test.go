package azuresearch_test

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSynonymMaps(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	s.must(t, http.MethodPut, "/synonymmaps('syn')?"+apiVer, map[string]any{
		"name": "syn", "format": "solr", "synonyms": "USA, United States, America\nWA => Washington",
	}, http.StatusCreated)

	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{
				"name":        "Description",
				"type":        "Edm.String",
				"searchable":  true,
				"synonymMaps": []any{"syn"},
			},
			map[string]any{"name": "Plain", "type": "Edm.String", "searchable": true},
		},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{
			"HotelId":     "d1",
			"Description": "visit the United States today",
			"Plain":       "visit the United States today",
		},
		map[string]any{"HotelId": "d2", "Description": "Washington state", "Plain": "Washington state"},
		map[string]any{"HotelId": "d3", "Description": "nothing here"},
	}}, http.StatusOK)

	cases := []struct {
		q    url.Values
		name string
		want []string
	}{
		{
			url.Values{"search": {"usa"}, "searchFields": {"Description"}},
			"equivalence expands to phrase",
			[]string{"d1"},
		},
		{
			url.Values{"search": {"america"}, "searchFields": {"Description"}},
			"equivalence from any member",
			[]string{"d1"},
		},
		{url.Values{"search": {"wa"}, "searchFields": {"Description"}}, "explicit mapping", []string{"d2"}},
		{url.Values{"search": {"washington"}, "searchFields": {"Description"}}, "target still matches", []string{"d2"}},
		{url.Values{"search": {"usa"}, "searchFields": {"Plain"}}, "field without synonym map", nil},
		{url.Values{"search": {"wa"}, "searchFields": {"Plain"}}, "explicit mapping needs map", nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := s.admin2(
				t,
				http.MethodGet,
				"/indexes('hotels')/docs?"+apiVer+"&"+tc.q.Encode()+"&$select=HotelId",
				nil,
			)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))
			assert.ElementsMatch(t, tc.want, idsOf(r))
		})
	}

	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "bad", "fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true},
			map[string]any{"name": "d", "type": "Edm.String", "searchable": true, "synonymMaps": []any{"missing"}},
		},
	}, http.StatusBadRequest)
}

func TestScoringProfiles(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{"name": "Title", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "Body", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "Rating", "type": "Edm.Double", "filterable": true},
			map[string]any{"name": "Tags", "type": "Collection(Edm.String)", "filterable": true, "searchable": true},
			map[string]any{"name": "Updated", "type": "Edm.DateTimeOffset", "filterable": true},
			map[string]any{"name": "Location", "type": "Edm.GeographyPoint", "filterable": true},
		},
		"scoringProfiles": []any{
			map[string]any{"name": "weights", "text": map[string]any{"weights": map[string]any{"Title": 5}}},
			map[string]any{"name": "rating", "functions": []any{
				map[string]any{
					"type":          "magnitude",
					"fieldName":     "Rating",
					"boost":         3,
					"interpolation": "linear",
					"magnitude": map[string]any{
						"boostingRangeStart":       1,
						"boostingRangeEnd":         5,
						"constantBoostBeyondRange": true,
					},
				},
			}},
			map[string]any{"name": "tags", "functions": []any{map[string]any{
				"type": "tag", "fieldName": "Tags", "boost": 4, "tag": map[string]any{"tagsParameter": "wanted"},
			}}},
			map[string]any{"name": "fresh", "functions": []any{
				map[string]any{
					"type":      "freshness",
					"fieldName": "Updated",
					"boost":     4,
					"freshness": map[string]any{"boostingDuration": "P30D"},
				},
			}},
			map[string]any{"name": "near", "functions": []any{map[string]any{
				"type": "distance", "fieldName": "Location", "boost": 4,
				"distance": map[string]any{"referencePointParameter": "here", "boostingDistance": 100},
			}}},
		},
	}, http.StatusCreated)

	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{
			"HotelId":  "bodyhit",
			"Title":    "other",
			"Body":     "gamma",
			"Rating":   1,
			"Tags":     []any{"a"},
			"Updated":  "2020-01-01T00:00:00Z",
			"Location": geo(10, 10),
		},
		map[string]any{
			"HotelId":  "titlehit",
			"Title":    "gamma",
			"Body":     "other",
			"Rating":   5,
			"Tags":     []any{"pool"},
			"Updated":  "2026-10-25T00:00:00Z",
			"Location": geo(0, 0),
		},
	}}, http.StatusOK)

	run := func(extra url.Values) []string {
		q := url.Values{"search": {"gamma"}, "$select": {"HotelId"}}
		maps.Copy(q, extra)

		r := searchGet(t, s, q)
		require.Equal(t, http.StatusOK, r.status, string(r.raw))

		return idsOf(r)
	}

	assert.Equal(t, []string{"bodyhit", "titlehit"}, run(nil), "tie keeps insertion order")
	assert.Equal(t, []string{"titlehit", "bodyhit"}, run(url.Values{"scoringProfile": {"weights"}}), "field weights")
	assert.Equal(t, []string{"titlehit", "bodyhit"}, run(url.Values{"scoringProfile": {"rating"}}), "magnitude")
	assert.Equal(
		t,
		[]string{"titlehit", "bodyhit"},
		run(url.Values{"scoringProfile": {"tags"}, "scoringParameter": {"wanted-pool,view"}}),
		"tag",
	)
	assert.Equal(t, []string{"titlehit", "bodyhit"}, run(url.Values{"scoringProfile": {"fresh"}}), "freshness")
	assert.Equal(
		t,
		[]string{"titlehit", "bodyhit"},
		run(url.Values{"scoringProfile": {"near"}, "scoringParameter": {"here--0.5,0.5"}}),
		"distance",
	)
}

func TestScoringBoostSizes(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{"name": "Body", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "Rating", "type": "Edm.Double", "filterable": true},
		},
		"scoringProfiles": []any{map[string]any{"name": "rating", "functions": []any{
			map[string]any{
				"type":      "magnitude",
				"fieldName": "Rating",
				"boost":     3,
				"magnitude": map[string]any{"boostingRangeStart": 1, "boostingRangeEnd": 5},
			},
		}}},
		"defaultScoringProfile": "rating",
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{
			"HotelId": "low",
			"Body":    "x",
			"Rating":  1,
		},
		map[string]any{"HotelId": "high", "Body": "x", "Rating": 5},
	}}, http.StatusOK)

	r := searchGet(t, s, url.Values{"search": {"x"}})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))
	assert.Equal(
		t,
		[]string{"high", "low"},
		idsOf(r),
		"the default scoring profile applies without a request parameter",
	)
	assert.InDelta(t, 3.0, scoreOf(t, r, 0)/scoreOf(t, r, 1), 0.0001, "top of the range gets the full boost")
}

func TestScoringProfileValidation(t *testing.T) {
	t.Parallel()

	base := func(profile map[string]any) map[string]any {
		return map[string]any{
			"name": "scored",
			"fields": []any{
				map[string]any{"name": "id", "type": "Edm.String", "key": true},
				map[string]any{"name": "title", "type": "Edm.String", "searchable": true},
				map[string]any{"name": "rating", "type": "Edm.Int32", "filterable": true},
				map[string]any{"name": "hidden", "type": "Edm.Int32", "filterable": false},
				map[string]any{"name": "tags", "type": "Collection(Edm.String)", "filterable": true},
			},
			"scoringProfiles": []any{profile},
		}
	}
	mag := func(field, interp string) map[string]any {
		return map[string]any{
			"name": "p", "functions": []any{map[string]any{
				"type": "magnitude", "fieldName": field, "boost": 2, "interpolation": interp,
				"magnitude": map[string]any{"boostingRangeStart": 1, "boostingRangeEnd": 5},
			}},
		}
	}

	tests := []struct {
		profile map[string]any
		name    string
		status  int
	}{
		{name: "valid magnitude", profile: mag("rating", "quadratic"), status: http.StatusCreated},
		{name: "non filterable field", profile: mag("hidden", "linear"), status: http.StatusBadRequest},
		{name: "wrong field type", profile: mag("title", "linear"), status: http.StatusBadRequest},
		{name: "unknown interpolation", profile: mag("rating", "cubic"), status: http.StatusBadRequest},
		{name: "tag quadratic", status: http.StatusBadRequest, profile: map[string]any{
			"name": "p", "functions": []any{map[string]any{
				"type": "tag", "fieldName": "tags", "boost": 2, "interpolation": "quadratic",
				"tag": map[string]any{"tagsParameter": "t"},
			}},
		}},
		{name: "tag linear", status: http.StatusCreated, profile: map[string]any{
			"name": "p", "functions": []any{map[string]any{
				"type": "tag", "fieldName": "tags", "boost": 2, "tag": map[string]any{"tagsParameter": "t"},
			}},
		}},
		{name: "unknown aggregation", status: http.StatusBadRequest, profile: map[string]any{
			"name": "p", "functionAggregation": "median",
		}},
		{name: "zero weight", status: http.StatusBadRequest, profile: map[string]any{
			"name": "p", "text": map[string]any{"weights": map[string]any{"title": 0}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			r := s.admin2(t, http.MethodPost, "/indexes?"+apiVer, base(tt.profile))
			assert.Equal(t, tt.status, r.status, string(r.raw))
		})
	}
}

func TestScoringFunctionAggregationProduct(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	fn := func(field string, boost int) map[string]any {
		return map[string]any{
			"type": "magnitude", "fieldName": field, "boost": boost,
			"magnitude": map[string]any{"boostingRangeStart": 1, "boostingRangeEnd": 5},
		}
	}

	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{"name": "Body", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "Rating", "type": "Edm.Double", "filterable": true},
			map[string]any{"name": "Rate", "type": "Edm.Double", "filterable": true},
		},
		"scoringProfiles": []any{map[string]any{
			"name": "both", "functionAggregation": "product", "functions": []any{fn("Rating", 2), fn("Rate", 3)},
		}},
		"defaultScoringProfile": "both",
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{"HotelId": "low", "Body": "x", "Rating": 1, "Rate": 1},
		map[string]any{"HotelId": "high", "Body": "x", "Rating": 5, "Rate": 5},
	}}, http.StatusOK)

	r := searchGet(t, s, url.Values{"search": {"x"}})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))
	assert.InDelta(t, 6.0, scoreOf(t, r, 0)/scoreOf(t, r, 1), 0.0001, "boosts multiply under product")
}
