package azuresearch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func searchGet(t *testing.T, s *stack, q url.Values) resp {
	t.Helper()

	return s.admin2(t, http.MethodGet, "/indexes('hotels')/docs?"+apiVer+"&"+q.Encode(), nil)
}

func searchPost(t *testing.T, s *stack, body map[string]any) resp {
	t.Helper()

	return s.admin2(t, http.MethodPost, "/indexes('hotels')/docs/search.post.search?"+apiVer, body)
}

func TestSearchQueries(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		name  string
		q     url.Values
		want  []string
		exact bool
	}{
		{"term", url.Values{"search": {"motel"}}, []string{"1", "2"}, false},
		{"term in analyzed english field", url.Values{"search": {"hotel"}}, []string{"1", "2", "3", "4"}, false},
		{"stemming", url.Values{"search": {"stands"}, "searchFields": {"Description"}}, []string{"3"}, false},
		{"phrase with stop word", url.Values{"search": {`"heart of"`}}, []string{"1", "4"}, false},
		{"match all", url.Values{"search": {"*"}}, []string{"1", "2", "3", "4"}, false},
		{"empty search", url.Values{}, []string{"1", "2", "3", "4"}, false},
		{
			"any mode",
			url.Values{"search": {"pool concierge"}, "searchFields": {"Tags"}},
			[]string{"1", "2", "4"},
			false,
		},
		{
			"all mode",
			url.Values{"search": {"pool concierge"}, "searchMode": {"all"}, "searchFields": {"Tags"}},
			[]string{"1", "2"},
			false,
		},
		{"negation all mode", url.Values{"search": {"hotel -boutique"}, "searchMode": {"all"}}, []string{"3"}, false},
		{"negation any mode ors", url.Values{"search": {"hotel -boutique"}}, []string{"1", "2", "3", "4"}, false},
		{"simple plus operator", url.Values{"search": {"pool + wifi"}, "searchFields": {"Tags"}}, []string{"2"}, false},
		{
			"simple or operator",
			url.Values{"search": {"wifi | view"}, "searchFields": {"Tags"}},
			[]string{"2", "4"},
			false,
		},
		{"prefix", url.Values{"search": {"mot*"}}, []string{"1", "2"}, false},
		{
			"full fielded and",
			url.Values{"search": {"HotelName:motel AND Tags:wifi"}, "queryType": {"full"}},
			[]string{"2"},
			false,
		},
		{
			"full nested field",
			url.Values{"search": {"Address/City:york"}, "queryType": {"full"}},
			[]string{"1", "4"},
			false,
		},
		{
			"full not",
			url.Values{"search": {"HotelName:motel NOT Tags:wifi"}, "queryType": {"full"}, "searchMode": {"all"}},
			[]string{"1"},
			false,
		},
		{
			"full not any mode ors",
			url.Values{"search": {"HotelName:motel NOT Tags:wifi"}, "queryType": {"full"}},
			[]string{"1", "2", "3", "4"},
			false,
		},
		{"full wildcard", url.Values{"search": {"HotelName:t?in"}, "queryType": {"full"}}, []string{"2"}, false},
		{"full fuzzy", url.Values{"search": {"motle~1"}, "queryType": {"full"}}, []string{"1", "2"}, false},
		{"full fuzzy too far", url.Values{"search": {"mxxxl~1"}, "queryType": {"full"}}, nil, false},
		{
			"full grouping",
			url.Values{"search": {"HotelName:(palace OR landscape)"}, "queryType": {"full"}},
			[]string{"3", "4"},
			false,
		},
		{"full regex", url.Values{"search": {"HotelName:/mo.el/"}, "queryType": {"full"}}, []string{"1", "2"}, false},
		{
			"full proximity",
			url.Values{"search": {`Description:"main artery"~3`}, "queryType": {"full"}},
			[]string{"1"},
			false,
		},
		{"no match", url.Values{"search": {"zzzz"}}, nil, false},
		{"searchFields restricts", url.Values{"search": {"york"}, "searchFields": {"HotelName"}}, nil, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := searchGet(t, s, tc.q)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))
			assert.ElementsMatch(t, tc.want, keysOf(r))
		})
	}
}

func TestSearchFilters(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		name   string
		filter string
		want   []string
	}{
		{"gt", "Rating gt 4", []string{"3", "4"}},
		{"range", "Rating ge 3.6 and Rating lt 4.7", []string{"1", "2", "4"}},
		{"or", "HotelId eq '1' or HotelId eq '3'", []string{"1", "3"}},
		{"ne", "Category ne 'Boutique'", []string{"3"}},
		{"bool eq", "ParkingIncluded eq true", []string{"2", "3", "4"}},
		{"bare bool not", "Category eq 'Boutique' and not ParkingIncluded", []string{"1"}},
		{"not parens", "not (Rating gt 4)", []string{"1", "2"}},
		{"complex path", "Address/City eq 'New York'", []string{"1", "4"}},
		{"any scalar", "Tags/any(t: t eq 'pool')", []string{"1", "2"}},
		{"all scalar", "Tags/all(t: t ne 'pool')", []string{"3", "4"}},
		{"any complex", "Rooms/any(r: r/BaseRate lt 100)", []string{"1", "3"}},
		{"any complex and", "Rooms/any(r: r/Type eq 'Budget Room' and r/BaseRate lt 70)", []string{"3"}},
		{"all complex vacuous", "Rooms/all(r: r/BaseRate gt 100)", []string{"2", "4"}},
		{"any no args", "Rooms/any()", []string{"1", "2", "3"}},
		{"search.in default delimiters", "search.in(HotelId, '1 3')", []string{"1", "3"}},
		{"search.in custom delimiter", "search.in(Category, 'Resort and Spa|Nope', '|')", []string{"3"}},
		{"search.in collection", "search.in(Tags, 'bar,view')", []string{"3", "4"}},
		{"geo.distance", "geo.distance(Location, geography'POINT(-122.131577 47.678581)') le 10", []string{"3", "4"}},
		{
			"geo.intersects",
			"geo.intersects(Location, geography'POLYGON((-123 47, -122 47, -122 48, -123 48, -123 47))')",
			[]string{"3", "4"},
		},
		{"date literal", "LastRenovationDate ge 2010-01-01T00:00:00Z", []string{"3"}},
		{"date lt", "LastRenovationDate lt 1975-01-01T00:00:00Z", []string{"1", "4"}},
		{"null", "Rating eq null", nil},
		{"ne null", "Rating ne null", []string{"1", "2", "3", "4"}},
		{"ismatch", "search.ismatch('motel', 'HotelName')", []string{"1", "2"}},
		{"ismatch full", "search.ismatch('t*', 'HotelName', 'full', 'any')", []string{"2", "3"}},
		{"quoted quote", "HotelName eq 'it''s'", nil},
		{"true literal", "true", []string{"1", "2", "3", "4"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := searchGet(t, s, url.Values{"$filter": {tc.filter}})
			require.Equal(t, http.StatusOK, r.status, string(r.raw))
			assert.ElementsMatch(t, tc.want, keysOf(r))
		})
	}
}

func TestSearchFilterErrors(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	for _, f := range []string{
		"Nope eq 1", "Rating eq", "Rating gt 'a' b", "Rooms/any(r: r/Nope eq 1)", "Rating bogus 4", "(Rating gt 4",
		"geo.distance(Location, 'x') lt 1", "search.in(Rating)", "HotelName eq 'unterminated", "search.ismatch('x', 'Rating')", //nolint:lll // test fixture
	} {
		r := searchGet(t, s, url.Values{"$filter": {f}})
		assert.Equal(t, http.StatusBadRequest, r.status, f)
	}
}

func TestSearchOrderPagingSelect(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		name string
		q    url.Values
		want []string
	}{
		{
			"orderby two keys",
			url.Values{"search": {"*"}, "$orderby": {"Rating desc,HotelName asc"}},
			[]string{"3", "4", "1", "2"},
		},
		{
			"orderby asc",
			url.Values{"search": {"*"}, "$orderby": {"Rating asc,HotelName desc"}},
			[]string{"2", "1", "4", "3"},
		},
		{
			"orderby geo",
			url.Values{"search": {"*"}, "$orderby": {"geo.distance(Location, geography'POINT(-122.13 47.67)') asc"}},
			[]string{"4", "3", "2", "1"},
		},
		{
			"orderby score then field",
			url.Values{"search": {"motel"}, "$orderby": {"search.score() desc, HotelName desc"}},
			[]string{"2", "1"},
		},
		{"top skip", url.Values{"search": {"*"}, "$top": {"2"}, "$skip": {"1"}}, []string{"2", "3"}},
		{"top zero", url.Values{"search": {"*"}, "$top": {"0"}, "$count": {"true"}}, nil},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := searchGet(t, s, tc.q)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))
			assert.Equal(t, tc.want, append([]string(nil), keysOf(r)...))
		})
	}

	r := searchGet(t, s, url.Values{"search": {"*"}, "$top": {"2"}, "$count": {"true"}})
	assert.EqualValues(t, 4, r.body["@odata.count"])

	sel := searchGet(t, s, url.Values{"search": {"motel"}, "$select": {"HotelId,Address/City"}})
	row := sel.body["value"].([]any)[0].(map[string]any) //nolint:forcetypeassert // test fixture shape
	assert.Contains(t, row, "@search.score")
	assert.Equal(t, map[string]any{"City": "New York"}, row["Address"])
	assert.NotContains(t, row, "HotelName")

	for _, bad := range []url.Values{
		{"$orderby": {"Tags"}}, {"$orderby": {"Nope"}}, {"$top": {"1001"}}, {"$top": {"x"}}, {"$skip": {"100001"}},
		{"$select": {"Secret"}}, {"searchMode": {"most"}}, {"queryType": {"semantic"}}, {"searchFields": {"Rating"}},
		{"search": {"Rating:4"}, "queryType": {"full"}}, {"scoringProfile": {"nope"}}, {"highlight": {"Rating"}},
	} {
		assert.Equal(t, http.StatusBadRequest, searchGet(t, s, bad).status, bad.Encode())
	}
}

func TestSearchPagination(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name":   "hotels",
		"fields": []any{map[string]any{"name": "HotelId", "type": "Edm.String", "key": true}},
	}, http.StatusCreated)

	docs := make([]any, 0, 120)
	for i := range 120 {
		docs = append(
			docs,
			map[string]any{"@search.action": "upload", "HotelId": string(rune('a'+i%26)) + string(rune('a'+i/26))},
		)
	}

	s.must(
		t,
		http.MethodPost,
		"/indexes('hotels')/docs/search.index?"+apiVer,
		map[string]any{"value": docs},
		http.StatusOK,
	)

	get := searchGet(t, s, url.Values{"search": {"*"}})
	assert.Len(t, get.body["value"], 50)
	assert.NotEmpty(t, get.body["@odata.nextLink"])

	post := searchPost(t, s, map[string]any{"search": "*", "count": true})
	assert.Len(t, post.body["value"], 50)
	assert.EqualValues(t, 120, post.body["@odata.count"])

	next, ok := post.body["@search.nextPageParameters"].(map[string]any)
	require.True(t, ok)
	assert.EqualValues(t, 50, next["skip"])

	explicit := searchPost(t, s, map[string]any{"search": "*", "top": 100})
	assert.Len(t, explicit.body["value"], 100)
	assert.NotContains(t, explicit.body, "@search.nextPageParameters")
}

func TestSearchFacets(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	facet := func(expr string) []any {
		r := searchPost(t, s, map[string]any{"search": "*", "facets": []any{expr}})
		require.Equal(t, http.StatusOK, r.status, string(r.raw))

		facets, _ := r.body["@search.facets"].(map[string]any)
		field := expr

		for i, c := range expr {
			if c == ',' {
				field = expr[:i]

				break
			}
		}

		out, _ := facets[field].([]any)

		return out
	}

	cases := []struct {
		name string
		expr string
		want []any
	}{
		{"value count", "Category", []any{
			map[string]any{"count": 3.0, "value": "Boutique"}, map[string]any{"count": 1.0, "value": "Resort and Spa"},
		}},
		{"number interval", "Rating,interval:1", []any{
			map[string]any{"count": 2.0, "from": 3.0, "to": 4.0}, map[string]any{"count": 2.0, "from": 4.0, "to": 5.0},
		}},
		{"number values", "Rating,values:4", []any{
			map[string]any{"count": 2.0, "to": 4.0}, map[string]any{"count": 2.0, "from": 4.0},
		}},
		{
			"collection sorted by value",
			"Tags,count:2,sort:value",
			[]any{
				map[string]any{
					"count": 1.0,
					"value": "24-hour front desk service",
				},
				map[string]any{"count": 2.0, "value": "air conditioning"},
			},
		},
		{"collection counts docs once", "Tags,count:1", []any{map[string]any{"count": 3.0, "value": "concierge"}}},
		{"date year", "LastRenovationDate,interval:year", []any{
			map[string]any{"count": 1.0, "from": "1960-01-01T00:00:00Z", "to": "1961-01-01T00:00:00Z"},
			map[string]any{"count": 1.0, "from": "1970-01-01T00:00:00Z", "to": "1971-01-01T00:00:00Z"},
			map[string]any{"count": 1.0, "from": "1979-01-01T00:00:00Z", "to": "1980-01-01T00:00:00Z"},
			map[string]any{"count": 1.0, "from": "2015-01-01T00:00:00Z", "to": "2016-01-01T00:00:00Z"},
		}},
		{"complex subfield", "Address/City,sort:-value", []any{
			map[string]any{"count": 1.0, "value": "Sarasota"}, map[string]any{"count": 1.0, "value": "Kirkland"},
			map[string]any{"count": 2.0, "value": "New York"},
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if tc.name == "complex subfield" {
				tc.want = []any{
					map[string]any{
						"count": 1.0,
						"value": "Sarasota",
					},
					map[string]any{"count": 1.0, "value": "Kirkland"},
					map[string]any{"count": 2.0, "value": "New York"},
				}

				got := facet(tc.expr)
				assert.ElementsMatch(t, tc.want, got)

				return
			}

			assert.Equal(t, tc.want, facet(tc.expr))
		})
	}

	filtered := searchPost(t, s, map[string]any{"search": "*", "filter": "Rating gt 4", "facets": []any{"Category"}})
	f, _ := filtered.body["@search.facets"].(map[string]any)
	assert.Equal(t, []any{
		map[string]any{"count": 1.0, "value": "Boutique"}, map[string]any{"count": 1.0, "value": "Resort and Spa"},
	}, f["Category"], "facets reflect the filter")

	for _, bad := range []string{"Location", "Nope", "Category,count:x", "Category,sort:size", "Category,bogus:1"} {
		r := searchPost(t, s, map[string]any{"search": "*", "facets": []any{bad}})
		assert.Equal(t, http.StatusBadRequest, r.status, bad)
	}
}

func TestSearchHighlight(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	r := searchGet(t, s, url.Values{"search": {"motel"}, "highlight": {"HotelName"}})
	require.Equal(t, http.StatusOK, r.status, string(r.raw))

	row := r.body["value"].([]any)[0].(map[string]any) //nolint:forcetypeassert // test fixture shape
	hl := row["@search.highlights"].(map[string]any)   //nolint:forcetypeassert // test fixture shape
	assert.Equal(t, []any{"Secret Point <em>Motel</em>"}, hl["HotelName"])

	custom := searchGet(t, s, url.Values{
		"search": {
			"heart",
		},
		"highlight":        {"Description"},
		"highlightPreTag":  {"<b>"},
		"highlightPostTag": {"</b>"},
		"searchFields":     {"Description"},
	})
	row = custom.body["value"].([]any)[0].(map[string]any)                     //nolint:forcetypeassert // test fixture shape
	frags := row["@search.highlights"].(map[string]any)["Description"].([]any) //nolint:forcetypeassert // test fixture shape
	assert.Contains(t, frags[0], "<b>heart</b>")
}

func TestSearchPostMatchesGet(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	post := searchPost(t, s, map[string]any{
		"search":  "motel",
		"filter":  "Rating gt 3",
		"orderby": "HotelName desc",
		"select":  "HotelId,HotelName",
		"count":   true,
		"top":     5,
	})
	require.Equal(t, http.StatusOK, post.status, string(post.raw))
	assert.Equal(t, []string{"2", "1"}, keysOf(post))
	assert.EqualValues(t, 2, post.body["@odata.count"])
	assert.Contains(t, post.body["@odata.context"], "/indexes('hotels')/$metadata#docs(*)")
}

func TestScoringBM25Ordering(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "hotels",
		"fields": []any{
			map[string]any{"name": "HotelId", "type": "Edm.String", "key": true},
			map[string]any{"name": "Body", "type": "Edm.String", "searchable": true},
		},
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, map[string]any{"value": []any{
		map[string]any{"HotelId": "short", "Body": "azure search"},
		map[string]any{
			"HotelId": "long",
			"Body":    "azure search with a very long tail of unrelated filler words that dilute the field",
		},
		map[string]any{"HotelId": "double", "Body": "search search search"},
		map[string]any{"HotelId": "none", "Body": "nothing relevant"},
	}}, http.StatusOK)

	r := searchGet(t, s, url.Values{"search": {"search"}})
	require.Equal(t, http.StatusOK, r.status)
	assert.Equal(t, []string{"double", "short", "long"}, keysOf(r), "higher tf and shorter fields score higher")

	rows := r.body["value"].([]any) //nolint:forcetypeassert // test fixture shape
	prev := 1e9

	for _, row := range rows {
		score := row.(map[string]any)["@search.score"].(float64) //nolint:forcetypeassert // test fixture shape
		assert.Less(t, score, prev)

		prev = score
	}
}
