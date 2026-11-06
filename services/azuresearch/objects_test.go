package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func blobSource() map[string]any {
	return map[string]any{
		"name": "ds",
		"type": "azureblob",
		"credentials": map[string]any{
			"connectionString": "DefaultEndpointsProtocol=https;AccountName=acct;AccountKey=c2VjcmV0;",
		},
		"container": map[string]any{"name": "docs"},
	}
}

func TestSynonymMapCRUD(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	created := s.must(
		t,
		http.MethodPost,
		"/synonymmaps?"+apiVer,
		map[string]any{"name": "m1", "synonyms": "a, b"},
		http.StatusCreated,
	)
	assert.Equal(t, "solr", created.body["format"])
	assert.NotEmpty(t, created.body["@odata.etag"])

	s.must(
		t,
		http.MethodPost,
		"/synonymmaps?"+apiVer,
		map[string]any{"name": "m1", "synonyms": "a, b"},
		http.StatusBadRequest,
	)
	s.must(
		t,
		http.MethodPut,
		"/synonymmaps('m1')?"+apiVer,
		map[string]any{"name": "m1", "synonyms": "a, b, c"},
		http.StatusNoContent,
	)

	got := s.must(t, http.MethodGet, "/synonymmaps('m1')?"+apiVer, nil, http.StatusOK)
	assert.Equal(t, "a, b, c", got.body["synonyms"])

	list := s.must(t, http.MethodGet, "/synonymmaps?"+apiVer+"&$select=name", nil, http.StatusOK)
	assert.Equal(t, []any{map[string]any{"name": "m1"}}, list.body["value"])

	s.must(
		t,
		http.MethodPut,
		"/synonymmaps('Bad_Name')?"+apiVer,
		map[string]any{"synonyms": "a, b"},
		http.StatusBadRequest,
	)
	s.must(t, http.MethodPut, "/synonymmaps('m2')?"+apiVer, map[string]any{"synonyms": ""}, http.StatusBadRequest)
	s.must(
		t,
		http.MethodPut,
		"/synonymmaps('m2')?"+apiVer,
		map[string]any{"synonyms": "a", "format": "wordnet"},
		http.StatusBadRequest,
	)
	s.must(t, http.MethodDelete, "/synonymmaps('m1')?"+apiVer, nil, http.StatusNoContent)
	s.must(t, http.MethodGet, "/synonymmaps('m1')?"+apiVer, nil, http.StatusNotFound)
}

func TestDataSourceRedaction(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	created := s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource(), http.StatusCreated)
	cred, _ := created.body["credentials"].(map[string]any)
	assert.Equal(t, "<redacted>", cred["connectionString"], "secrets are never echoed")

	got := s.must(t, http.MethodGet, "/datasources('ds')?"+apiVer, nil, http.StatusOK)
	cred, _ = got.body["credentials"].(map[string]any)
	assert.Equal(t, "<redacted>", cred["connectionString"])

	back := got.body
	delete(back, "@odata.etag")
	delete(back, "@odata.context")
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, back, http.StatusNoContent)

	snap := string(s.b.Snapshot(t.Context()))
	assert.Contains(t, snap, "AccountKey=c2VjcmV0", "echoing the redacted value keeps the stored secret")

	for name, def := range map[string]map[string]any{
		"unsupported type": {"name": "x", "type": "azuresql", "credentials": map[string]any{"connectionString": "c"}, "container": map[string]any{"name": "t"}}, //nolint:lll // test fixture
		"no credentials":   {"name": "x", "type": "azureblob", "container": map[string]any{"name": "t"}},
		"no container":     {"name": "x", "type": "azureblob", "credentials": map[string]any{"connectionString": "c"}},
	} {
		r := s.admin2(t, http.MethodPost, "/datasources?"+apiVer, def)
		assert.Equal(t, http.StatusBadRequest, r.status, name)
	}
}

func TestIndexerValidation(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
		"name": "target", "fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
	}, http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource(), http.StatusCreated)

	base := func() map[string]any {
		return map[string]any{"name": "ix", "dataSourceName": "ds", "targetIndexName": "target"}
	}

	cases := []struct {
		mut  func(map[string]any)
		name string
		want int
	}{
		{func(map[string]any) {}, "valid", http.StatusCreated},
		{func(m map[string]any) { m["dataSourceName"] = "missing" }, "missing data source", http.StatusBadRequest},
		{func(m map[string]any) { m["targetIndexName"] = "missing" }, "missing index", http.StatusBadRequest},
		{func(m map[string]any) { m["skillsetName"] = "missing" }, "missing skillset", http.StatusBadRequest},
		{
			func(m map[string]any) { m["schedule"] = map[string]any{"interval": "PT1M"} },
			"interval too short",
			http.StatusBadRequest,
		},
		{
			func(m map[string]any) { m["schedule"] = map[string]any{"interval": "soon"} },
			"interval malformed",
			http.StatusBadRequest,
		},
		{
			func(m map[string]any) { m["schedule"] = map[string]any{"interval": "PT5M"} },
			"interval ok",
			http.StatusCreated,
		},
		{func(m map[string]any) {
			m["fieldMappings"] = []any{
				map[string]any{"sourceFieldName": "a", "mappingFunction": map[string]any{"name": "rot13"}},
			}
		}, "unknown mapping function", http.StatusBadRequest},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s2 := newStack(t, "standard")
			s2.must(t, http.MethodPost, "/indexes?"+apiVer, map[string]any{
				"name": "target", "fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
			}, http.StatusCreated)
			s2.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource(), http.StatusCreated)

			def := base()
			tc.mut(def)
			s2.must(t, http.MethodPost, "/indexers?"+apiVer, def, tc.want)
		})
	}

	s.must(t, http.MethodGet, "/indexers('absent')/search.status?"+apiVer, nil, http.StatusNotFound)
	s.must(t, http.MethodPost, "/indexers('absent')/search.run?"+apiVer, nil, http.StatusNotFound)
	require.Equal(t, http.StatusNotFound, s.admin2(t, http.MethodGet, "/nonsense?"+apiVer, nil).status)
}

func TestSkillsetValidation(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	split := map[string]any{
		"@odata.type": "#Microsoft.Skills.Text.SplitSkill", "textSplitMode": "pages", "maximumPageLength": 100,
		"inputs":  []any{map[string]any{"name": "text", "source": "/document/content"}},
		"outputs": []any{map[string]any{"name": "textItems", "targetName": "pages"}},
	}

	created := s.must(
		t,
		http.MethodPut,
		"/skillsets('sk')?"+apiVer,
		map[string]any{"name": "sk", "skills": []any{split}},
		http.StatusCreated,
	)
	assert.Equal(
		t,
		"/document",
		created.body["skills"].([]any)[0].(map[string]any)["context"],
		"context defaults to the document",
	) //nolint:forcetypeassert // test fixture shape

	s.must(
		t,
		http.MethodPut,
		"/skillsets('none')?"+apiVer,
		map[string]any{"name": "none", "skills": []any{}},
		http.StatusBadRequest,
	)
	s.must(
		t,
		http.MethodPut,
		"/skillsets('bad')?"+apiVer,
		map[string]any{"name": "bad", "skills": []any{map[string]any{"inputs": []any{}}}},
		http.StatusBadRequest,
	)
	s.must(
		t,
		http.MethodPut,
		"/skillsets('noio')?"+apiVer,
		map[string]any{
			"name":   "noio",
			"skills": []any{map[string]any{"@odata.type": "#Microsoft.Skills.Text.SplitSkill"}},
		},
		http.StatusBadRequest,
	)
	s.must(t, http.MethodDelete, "/skillsets('sk')?"+apiVer, nil, http.StatusNoContent)
}
