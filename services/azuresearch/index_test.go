package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexCRUD(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	created := s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)
	assert.Equal(t, "hotels", created.body["name"])
	assert.NotEmpty(t, created.body["@odata.etag"])
	assert.NotEmpty(t, created.header.Get("ETag"))

	fields, ok := created.body["fields"].([]any)
	require.True(t, ok)

	name, _ := fields[1].(map[string]any)
	assert.Equal(t, true, name["searchable"], "string defaults to searchable")
	assert.Equal(t, true, name["retrievable"])

	tags, _ := fields[4].(map[string]any)
	assert.Equal(t, false, tags["sortable"], "collections are not sortable")

	got := s.must(t, http.MethodGet, "/indexes('hotels')?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "hotels", got.body["name"])

	list := s.must(t, http.MethodGet, "/indexes?"+apiVer, nil, http.StatusOK)
	assert.Len(t, list.body["value"], 1)

	s.must(t, http.MethodDelete, "/indexes('hotels')?"+apiVer, nil, http.StatusNoContent)
	s.must(t, http.MethodGet, "/indexes('hotels')?"+apiVer, nil, http.StatusNotFound)
	s.must(t, http.MethodDelete, "/indexes('hotels')?"+apiVer, nil, http.StatusNotFound)
}

func TestIndexValidation(t *testing.T) {
	t.Parallel()

	field := func(m map[string]any) map[string]any { return m }
	keyF := field(map[string]any{"name": "id", "type": "Edm.String", "key": true})

	cases := []struct {
		def  map[string]any
		name string
		want string
	}{
		{map[string]any{"name": "Bad_Name", "fields": []any{keyF}}, "uppercase name", "invalid"},
		{map[string]any{"name": "x", "fields": []any{}}, "no fields", "at least one field"},
		{
			map[string]any{"name": "x", "fields": []any{field(map[string]any{"name": "a", "type": "Edm.String"})}},
			"no key",
			"exactly one key",
		},
		{
			map[string]any{
				"name":   "x",
				"fields": []any{keyF, field(map[string]any{"name": "k2", "type": "Edm.String", "key": true})},
			},
			"two keys",
			"exactly one key",
		},
		{
			map[string]any{
				"name":   "x",
				"fields": []any{field(map[string]any{"name": "id", "type": "Edm.Int32", "key": true})},
			},
			"int key",
			"Edm.String",
		},
		{
			map[string]any{
				"name":   "x",
				"fields": []any{keyF, field(map[string]any{"name": "n", "type": "Edm.Int32", "searchable": true})},
			},
			"searchable int",
			"searchable",
		},
		{
			map[string]any{
				"name": "x",
				"fields": []any{
					keyF,
					field(map[string]any{"name": "t", "type": "Collection(Edm.String)", "sortable": true}),
				},
			},
			"sortable collection",
			"sortable",
		},
		{
			map[string]any{"name": "x", "fields": []any{keyF, field(map[string]any{"name": "t", "type": "Edm.Wat"})}},
			"bad type",
			"unsupported type",
		},
		{
			map[string]any{
				"name":   "x",
				"fields": []any{keyF, field(map[string]any{"name": "v", "type": "Collection(Edm.Single)"})},
			},
			"vector without dims",
			"dimensions",
		},
		{
			map[string]any{
				"name": "x",
				"fields": []any{
					keyF,
					field(map[string]any{"name": "s", "type": "Edm.String", "searchable": true, "analyzer": "nope"}),
				},
			},
			"unknown analyzer",
			"not found",
		},
		{
			map[string]any{
				"name":       "x",
				"fields":     []any{keyF},
				"suggesters": []any{map[string]any{"name": "sg", "sourceFields": []any{"id", "zzz"}}},
			},
			"bad suggester",
			"source field",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			r := s.must(t, http.MethodPost, "/indexes?"+apiVer, tc.def, http.StatusBadRequest)

			errObj, _ := r.body["error"].(map[string]any)
			assert.Contains(t, errObj["message"], tc.want)
		})
	}
}

func TestIndexUpdate(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	base := hotelIndex()

	additive := hotelIndex()
	fields, _ := additive["fields"].([]any)
	additive["fields"] = append(fields, map[string]any{"name": "Extra", "type": "Edm.String", "filterable": true})

	changed := hotelIndex()
	cf, _ := changed["fields"].([]any)
	cf[5].(map[string]any)["type"] = "Edm.Int32" //nolint:forcetypeassert // test fixture shape

	cases := []struct {
		def   map[string]any
		query string
		name  string
		docs  string
		want  int
	}{
		{def: base, query: "", name: "same schema", want: http.StatusNoContent, docs: "4"},
		{def: additive, query: "", name: "additive field", want: http.StatusNoContent, docs: "4"},
		{def: changed, query: "", name: "changed type rejected", want: http.StatusBadRequest, docs: "4"},
		{
			def:   changed,
			query: "&allowIndexDowntime=true",
			name:  "downtime drops documents",
			want:  http.StatusNoContent,
			docs:  "0",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s2 := newStack(t, "standard")
			s2.seedHotels(t)

			s2.must(t, http.MethodPut, "/indexes('hotels')?"+apiVer+tc.query, tc.def, tc.want)

			count := s2.must(t, http.MethodGet, "/indexes('hotels')/docs/$count?"+apiVer, nil, http.StatusOK)
			assert.Equal(t, tc.docs, string(count.raw))
		})
	}
}

func TestIndexPreconditions(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	created := s.must(t, http.MethodPost, "/indexes?"+apiVer, hotelIndex(), http.StatusCreated)
	etag := created.header.Get("ETag")

	put := func(ifMatch, ifNone string) int {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPut, s.url+"/indexes('hotels')?"+apiVer, nil)
		require.NoError(t, err)

		raw, _ := jsonBody(hotelIndex())
		req.Body = raw
		req.Header.Set("Api-Key", s.admin)
		req.Header.Set("Content-Type", "application/json")

		if ifMatch != "" {
			req.Header.Set("If-Match", ifMatch)
		}

		if ifNone != "" {
			req.Header.Set("If-None-Match", ifNone)
		}

		res, err := testclient.New(t).Do(req)
		require.NoError(t, err)
		res.Body.Close()

		return res.StatusCode
	}

	assert.Equal(t, http.StatusPreconditionFailed, put("", "*"))
	assert.Equal(t, http.StatusPreconditionFailed, put(`"0xBAD"`, ""))
	assert.Equal(t, http.StatusNoContent, put(etag, ""))
}

func TestIndexQuota(t *testing.T) {
	t.Parallel()

	s := newStack(t, "free")

	for _, n := range []string{"a", "b", "c"} {
		def := map[string]any{
			"name":   n,
			"fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
		}
		s.must(t, http.MethodPost, "/indexes?"+apiVer, def, http.StatusCreated)
	}

	def := map[string]any{"name": "d", "fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}}}
	s.must(t, http.MethodPost, "/indexes?"+apiVer, def, http.StatusForbidden)
}

func TestIndexStatsAndServiceStats(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	st := s.must(t, http.MethodGet, "/indexes('hotels')/search.stats?"+apiVer, nil, http.StatusOK)
	assert.EqualValues(t, 4, st.body["documentCount"])
	assert.Positive(t, st.body["storageSize"])

	svc := s.must(t, http.MethodGet, "/servicestats?"+apiVer, nil, http.StatusOK)
	counters, _ := svc.body["counters"].(map[string]any)
	idx, _ := counters["indexesCount"].(map[string]any)
	assert.EqualValues(t, 1, idx["usage"])
	assert.EqualValues(t, 50, idx["quota"])
}

func TestAnalyze(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.seedHotels(t)

	cases := []struct {
		body map[string]any
		name string
		want []string
	}{
		{
			map[string]any{"text": "The Quick-Brown fox", "analyzer": "standard.lucene"},
			"standard",
			[]string{"quick", "brown", "fox"},
		},
		{map[string]any{"text": "The Quick fox", "analyzer": "keyword"}, "keyword", []string{"The Quick fox"}},
		{map[string]any{"text": "Running dogs", "analyzer": "en.lucene"}, "english stemming", []string{"run", "dog"}},
		{
			map[string]any{"text": "A B", "tokenizer": "whitespace", "tokenFilters": []any{"lowercase"}},
			"tokenizer filters",
			[]string{"a", "b"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			r := s.must(t, http.MethodPost, "/indexes('hotels')/search.analyze?"+apiVer, tc.body, http.StatusOK)

			var got []string

			for _, tk := range r.body["tokens"].([]any) { //nolint:forcetypeassert // test fixture shape
				got = append(got, tk.(map[string]any)["token"].(string)) //nolint:forcetypeassert // test fixture shape
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
