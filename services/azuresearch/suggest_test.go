package azuresearch_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSuggest(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		name string
		q    url.Values
		want []string
		text []string
	}{
		{"prefix of word", url.Values{"search": {"mot"}, "suggesterName": {"sg"}, "$select": {"HotelId"}}, []string{"1", "2"}, []string{"Secret Point Motel", "Twin Dome Motel"}},
		{"infix word match", url.Values{"search": {"palace"}, "suggesterName": {"sg"}}, []string{"4"}, []string{"Sublime Palace Hotel"}},
		{"two terms", url.Values{"search": {"twin do"}, "suggesterName": {"sg"}}, []string{"2"}, []string{"Twin Dome Motel"}},
		{"second source field", url.Values{"search": {"kirk"}, "suggesterName": {"sg"}}, []string{"3"}, []string{"Kirkland"}},
		{"fuzzy", url.Values{"search": {"motle"}, "suggesterName": {"sg"}, "fuzzy": {"true"}}, []string{"1", "2"}, []string{"Secret Point Motel", "Twin Dome Motel"}},
		{"no fuzzy no match", url.Values{"search": {"motle"}, "suggesterName": {"sg"}}, nil, nil},
		{"filter", url.Values{"search": {"mot"}, "suggesterName": {"sg"}, "$filter": {"Rating gt 3.7"}}, nil, nil},
		{"top", url.Values{"search": {"h"}, "suggesterName": {"sg"}, "$top": {"1"}}, []string{"3"}, []string{"Triple Landscape Hotel"}},
		{"orderby", url.Values{"search": {"mot"}, "suggesterName": {"sg"}, "$orderby": {"HotelName desc"}}, []string{"2", "1"}, []string{"Twin Dome Motel", "Secret Point Motel"}},
		{"highlight", url.Values{"search": {"palace"}, "suggesterName": {"sg"}, "highlightPreTag": {"<b>"}, "highlightPostTag": {"</b>"}}, []string{"4"}, []string{"Sublime <b>Palace</b> Hotel"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := s.admin2(t, http.MethodGet, "/indexes('hotels')/docs/search.suggest?"+apiVer+"&"+tc.q.Encode(), nil)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))

			if tc.want != nil && tc.name != "top" {
				assert.ElementsMatch(t, tc.want, keysOf(r))
			}

			assert.Equal(t, tc.text, texts(r, "@search.text"))
		})
	}

	post := s.admin2(t, http.MethodPost, "/indexes('hotels')/docs/search.post.suggest?"+apiVer,
		map[string]any{"search": "mot", "suggesterName": "sg", "top": 1, "select": "HotelId,Rating"})
	require.Equal(t, http.StatusOK, post.status, string(post.raw))
	assert.Len(t, post.body["value"], 1)
	assert.Contains(t, post.body["value"].([]any)[0], "Rating") //nolint:forcetypeassert // test fixture shape

	for _, bad := range []url.Values{
		{"suggesterName": {"sg"}}, {"search": {"x"}}, {"search": {"x"}, "suggesterName": {"nope"}},
		{"search": {"x"}, "suggesterName": {"sg"}, "$top": {"101"}}, {"search": {"x"}, "suggesterName": {"sg"}, "searchFields": {"Category"}},
	} {
		r := s.admin2(t, http.MethodGet, "/indexes('hotels')/docs/search.suggest?"+apiVer+"&"+bad.Encode(), nil)
		assert.Equal(t, http.StatusBadRequest, r.status, bad.Encode())
	}
}

func texts(r resp, field string) []string {
	return keysByField(r, field)
}

func TestAutocomplete(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		name string
		q    url.Values
		want []map[string]any
	}{
		{"one term", url.Values{"search": {"mo"}, "suggesterName": {"sg"}}, []map[string]any{{"text": "motel", "queryPlusText": "motel"}}},
		{"one term with context", url.Values{"search": {"twin do"}, "suggesterName": {"sg"}, "autocompleteMode": {"oneTermWithContext"}}, []map[string]any{{"text": "twin dome", "queryPlusText": "twin dome"}}},
		{"one term plain", url.Values{"search": {"twin do"}, "suggesterName": {"sg"}}, []map[string]any{{"text": "dome", "queryPlusText": "twin dome"}}},
		{"two terms", url.Values{"search": {"tw"}, "suggesterName": {"sg"}, "autocompleteMode": {"twoTerms"}}, []map[string]any{{"text": "twin dome", "queryPlusText": "twin dome"}}},
		{"fuzzy", url.Values{"search": {"motle"}, "suggesterName": {"sg"}, "fuzzy": {"true"}}, []map[string]any{{"text": "motel", "queryPlusText": "motel"}}},
		{"nothing", url.Values{"search": {"zzz"}, "suggesterName": {"sg"}}, []map[string]any{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := s.admin2(t, http.MethodGet, "/indexes('hotels')/docs/search.autocomplete?"+apiVer+"&"+tc.q.Encode(), nil)
			require.Equal(t, http.StatusOK, r.status, string(r.raw))

			got := make([]map[string]any, 0)

			for _, v := range r.body["value"].([]any) { //nolint:forcetypeassert // test fixture shape
				got = append(got, v.(map[string]any)) //nolint:forcetypeassert // test fixture shape
			}

			assert.Equal(t, tc.want, got)
		})
	}

	bad := s.admin2(t, http.MethodGet, "/indexes('hotels')/docs/search.autocomplete?"+apiVer+"&search=m&suggesterName=sg&autocompleteMode=wide", nil)
	assert.Equal(t, http.StatusBadRequest, bad.status)

	post := s.admin2(t, http.MethodPost, "/indexes('hotels')/docs/search.post.autocomplete?"+apiVer,
		map[string]any{"search": "mo", "suggesterName": "sg"})
	assert.Equal(t, http.StatusOK, post.status, string(post.raw))
}
