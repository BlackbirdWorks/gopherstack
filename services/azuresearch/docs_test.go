package azuresearch_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocumentBatch(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)

	batch := func(actions ...any) resp {
		return s.admin2(
			t,
			http.MethodPost,
			"/indexes('hotels')/docs/search.index?"+apiVer,
			map[string]any{"value": actions},
		)
	}

	statusOf := func(r resp, i int) (float64, any) {
		rows, _ := r.body["value"].([]any)
		row, _ := rows[i].(map[string]any)
		code, _ := row["statusCode"].(float64)

		return code, row["errorMessage"]
	}

	up := batch(map[string]any{"@search.action": "upload", "HotelId": "1", "HotelName": "A", "Rating": 4.0})
	require.Equal(t, http.StatusOK, up.status)

	code, _ := statusOf(up, 0)
	assert.Equal(t, 201, int(code), "new document")

	again := batch(map[string]any{"@search.action": "upload", "HotelId": "1", "HotelName": "A2"})
	code, _ = statusOf(again, 0)
	assert.Equal(t, 200, int(code), "replace existing")

	doc := s.must(t, http.MethodGet, "/indexes('hotels')/docs('1')?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "A2", doc.body["HotelName"])
	assert.Nil(t, doc.body["Rating"], "upload replaces the whole document")

	merged := batch(map[string]any{"@search.action": "merge", "HotelId": "1", "Rating": 2.5})
	require.Equal(t, http.StatusOK, merged.status)

	doc = s.must(t, http.MethodGet, "/indexes('hotels')/docs('1')?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "A2", doc.body["HotelName"], "merge keeps other fields")
	assert.InEpsilon(t, 2.5, doc.body["Rating"], 0.0001)

	partial := batch(
		map[string]any{"@search.action": "merge", "HotelId": "missing", "Rating": 1.0},
		map[string]any{"@search.action": "mergeOrUpload", "HotelId": "2", "HotelName": "B"},
		map[string]any{"@search.action": "upload", "HotelId": "3", "Rating": "not a number"},
		map[string]any{"@search.action": "upload", "HotelId": "4", "Nope": 1},
		map[string]any{"@search.action": "upload", "HotelName": "no key"},
		map[string]any{"@search.action": "upload", "HotelId": "bad key!"},
	)
	require.Equal(t, http.StatusMultiStatus, partial.status, "any failure yields 207")

	wantCodes := []float64{404, 201, 400, 400, 400, 400}
	for i, want := range wantCodes {
		got, msg := statusOf(partial, i)
		assert.Equal(t, int(want), int(got), "action %d", i)

		if want >= 400 {
			assert.NotNil(t, msg, "action %d has a message", i)
		}
	}

	del := batch(
		map[string]any{"@search.action": "delete", "HotelId": "2"},
		map[string]any{"@search.action": "delete", "HotelId": "ghost"},
	)
	require.Equal(t, http.StatusOK, del.status)

	count := s.must(t, http.MethodGet, "/indexes('hotels')/docs/$count?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "1", string(count.raw))
	s.must(t, http.MethodGet, "/indexes('hotels')/docs('2')?"+apiVer, nil, http.StatusNotFound)
}

func TestDocumentBatchLimits(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)

	s.must(
		t,
		http.MethodPost,
		"/indexes('hotels')/docs/search.index?"+apiVer,
		map[string]any{"value": []any{}},
		http.StatusBadRequest,
	)

	many := make([]any, 1001)
	for i := range many {
		many[i] = map[string]any{"@search.action": "upload", "HotelId": "k"}
	}

	s.must(
		t,
		http.MethodPost,
		"/indexes('hotels')/docs/search.index?"+apiVer,
		map[string]any{"value": many},
		http.StatusRequestEntityTooLarge,
	)
	s.must(t, http.MethodPost, "/indexes('hotels')/docs/search.index?"+apiVer, "{not json", http.StatusBadRequest)
	s.must(
		t,
		http.MethodPost,
		"/indexes('nope')/docs/search.index?"+apiVer,
		map[string]any{"value": []any{map[string]any{"HotelId": "1"}}},
		http.StatusNotFound,
	)
}

func TestDocumentTypes(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)

	cases := []struct {
		fields map[string]any
		name   string
		code   float64
	}{
		{map[string]any{"Rating": "NaN"}, "double NaN literal", 201},
		{map[string]any{"Rating": 1}, "double from int", 201},
		{map[string]any{"ParkingIncluded": "yes"}, "bool as string", 400},
		{map[string]any{"LastRenovationDate": "yesterday"}, "bad date", 400},
		{map[string]any{"Location": map[string]any{"type": "Point", "coordinates": []any{500, 0}}}, "bad geo", 400},
		{map[string]any{"Tags": "single"}, "collection as scalar", 400},
		{map[string]any{"Tags": []any{"a", "b"}}, "collection ok", 201},
		{map[string]any{"Address": map[string]any{"City": "X", "Zip": "1"}}, "unknown complex subfield", 400},
		{map[string]any{"Rooms": []any{map[string]any{"Type": "t", "BaseRate": 1.5}}}, "complex collection", 201},
		{map[string]any{"Rating": nil, "HotelName": nil}, "nulls", 201},
	}

	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			doc := map[string]any{"@search.action": "upload", "HotelId": "t" + string(rune('a'+i))}
			maps.Copy(doc, tc.fields)

			r := s.admin2(
				t,
				http.MethodPost,
				"/indexes('hotels')/docs/search.index?"+apiVer,
				map[string]any{"value": []any{doc}},
			)
			rows, _ := r.body["value"].([]any)
			row, _ := rows[0].(map[string]any)
			assert.InDelta(t, tc.code, row["statusCode"], 0, string(r.raw))
		})
	}
}

func TestLookupSelectAndRetrievable(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	full := s.must(t, http.MethodGet, "/indexes('hotels')/docs('1')?"+apiVer, nil, http.StatusOK)
	assert.NotContains(t, full.body, "Secret", "non-retrievable fields are hidden")
	assert.Equal(
		t,
		"New York",
		full.body["Address"].(map[string]any)["City"],
	) //nolint:forcetypeassert // test fixture shape

	sel := s.must(
		t,
		http.MethodGet,
		"/indexes('hotels')/docs('1')?$select=HotelName,Address/City&"+apiVer,
		nil,
		http.StatusOK,
	)
	assert.Equal(t, "Secret Point Motel", sel.body["HotelName"])
	assert.NotContains(t, sel.body, "Rating")
	assert.Equal(t, map[string]any{"City": "New York"}, sel.body["Address"])

	s.must(t, http.MethodGet, "/indexes('hotels')/docs('1')?$select=Secret&"+apiVer, nil, http.StatusBadRequest)
	s.must(t, http.MethodGet, "/indexes('hotels')/docs('1')?$select=Nope&"+apiVer, nil, http.StatusBadRequest)
	s.must(t, http.MethodGet, "/indexes('hotels')/docs('zzz')?"+apiVer, nil, http.StatusNotFound)
}
