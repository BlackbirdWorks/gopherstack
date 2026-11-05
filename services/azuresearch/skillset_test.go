package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func skillIndex() map[string]any {
	return map[string]any{
		"name": "docs",
		"fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true},
			map[string]any{"name": "content", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "chunks", "type": "Collection(Edm.String)", "searchable": true},
			map[string]any{"name": "merged", "type": "Edm.String", "searchable": true},
			map[string]any{"name": "shaped", "type": "Edm.ComplexType", "fields": []any{
				map[string]any{"name": "title", "type": "Edm.String", "searchable": true},
				map[string]any{"name": "count", "type": "Edm.Int32", "filterable": true},
			}},
		},
	}
}

func runSkillIndexer(
	t *testing.T,
	files map[string]string,
	parsing string,
	skills []any,
	outputs []any,
) (*stack, map[string]any) {
	t.Helper()

	s := newStack(t, "standard")
	blobs := newFakeBlobs()

	for n, d := range files {
		blobs.put(n, d)
	}

	s.b.SetBlobSource(blobs)
	s.must(t, http.MethodPost, "/indexes?"+apiVer, skillIndex(), http.StatusCreated)
	s.must(t, http.MethodPut, "/datasources('ds')?"+apiVer, blobSource(), http.StatusCreated)
	s.must(
		t,
		http.MethodPut,
		"/skillsets('sk')?"+apiVer,
		map[string]any{"name": "sk", "skills": skills},
		http.StatusCreated,
	)

	s.must(t, http.MethodPost, "/indexers?"+apiVer, map[string]any{
		"name":            "ix",
		"dataSourceName":  "ds",
		"targetIndexName": "docs",
		"skillsetName":    "sk",
		"parameters": map[string]any{
			"configuration":  map[string]any{"parsingMode": parsing},
			"maxFailedItems": -1,
		},
		"fieldMappings": []any{
			map[string]any{
				"sourceFieldName": "metadata_storage_path",
				"targetFieldName": "id",
				"mappingFunction": map[string]any{"name": "base64Encode"},
			},
		},
		"outputFieldMappings": outputs,
	}, http.StatusCreated)
	s.must(t, http.MethodPost, "/indexers('ix')/search.run?"+apiVer, nil, http.StatusAccepted)

	last, _ := s.status(t, "ix")["lastResult"].(map[string]any)

	return s, last
}

func firstDoc(t *testing.T, s *stack) map[string]any {
	t.Helper()

	r := s.admin2(t, http.MethodGet, "/indexes('docs')/docs?"+apiVer+"&search=*", nil)
	require.Equal(t, http.StatusOK, r.status, string(r.raw))

	rows, _ := r.body["value"].([]any)
	require.NotEmpty(t, rows)

	doc, _ := rows[0].(map[string]any)

	return doc
}

func TestSkillsetSplit(t *testing.T) {
	t.Parallel()

	split := map[string]any{
		"@odata.type": "#Microsoft.Skills.Text.SplitSkill", "textSplitMode": "pages", "maximumPageLength": 20,
		"inputs":  []any{map[string]any{"name": "text", "source": "/document/content"}},
		"outputs": []any{map[string]any{"name": "textItems", "targetName": "pages"}},
	}

	s, last := runSkillIndexer(
		t,
		map[string]string{"a.txt": "One two three. Four five six. Seven eight nine."},
		"default",
		[]any{split},
		[]any{map[string]any{"sourceFieldName": "/document/pages", "targetFieldName": "chunks"}},
	)

	assert.Equal(t, "success", last["status"], "%v", last)

	doc := firstDoc(t, s)
	assert.Equal(t, []any{"One two three.", "Four five six.", "Seven eight nine."}, doc["chunks"])

	sentences := map[string]any{
		"@odata.type": "#Microsoft.Skills.Text.SplitSkill", "textSplitMode": "sentences",
		"inputs":  []any{map[string]any{"name": "text", "source": "/document/content"}},
		"outputs": []any{map[string]any{"name": "textItems", "targetName": "pages"}},
	}

	s2, _ := runSkillIndexer(t, map[string]string{"a.txt": "First one. Second one! Third?"}, "default",
		[]any{sentences}, []any{map[string]any{"sourceFieldName": "/document/pages", "targetFieldName": "chunks"}})
	assert.Equal(t, []any{"First one.", "Second one!", "Third?"}, firstDoc(t, s2)["chunks"])
}

func TestSkillsetMergeAndShaper(t *testing.T) {
	t.Parallel()

	merge := map[string]any{
		"@odata.type": "#Microsoft.Skills.Text.MergeSkill", "insertPreTag": "[", "insertPostTag": "]",
		"inputs": []any{
			map[string]any{"name": "text", "source": "/document/text"},
			map[string]any{"name": "itemsToInsert", "source": "/document/items"},
			map[string]any{"name": "offsets", "source": "/document/offsets"},
		},
		"outputs": []any{map[string]any{"name": "mergedText", "targetName": "mergedText"}},
	}
	shaper := map[string]any{
		"@odata.type": "#Microsoft.Skills.Util.ShaperSkill",
		"inputs": []any{
			map[string]any{"name": "title", "source": "/document/text"},
			map[string]any{"name": "count", "source": "/document/n"},
		},
		"outputs": []any{map[string]any{"name": "output", "targetName": "shape"}},
	}

	s, last := runSkillIndexer(t,
		map[string]string{"a.json": `{"text":"Hello world","items":["BIG"],"offsets":[5],"n":3}`}, "json",
		[]any{merge, shaper}, []any{
			map[string]any{"sourceFieldName": "/document/mergedText", "targetFieldName": "merged"},
			map[string]any{"sourceFieldName": "/document/shape", "targetFieldName": "shaped"},
		})

	assert.Equal(t, "success", last["status"], "%v", last)

	doc := firstDoc(t, s)
	assert.Equal(t, "Hello[BIG] world", doc["merged"])
	assert.Equal(t, map[string]any{"title": "Hello world", "count": 3.0}, doc["shaped"])
}

func TestSkillsetPerItemContext(t *testing.T) {
	t.Parallel()

	shaper := map[string]any{
		"@odata.type": "#Microsoft.Skills.Util.ShaperSkill", "context": "/document/rows/*",
		"inputs":  []any{map[string]any{"name": "title", "source": "/document/rows/*/a"}},
		"outputs": []any{map[string]any{"name": "output", "targetName": "shape"}},
	}

	s, last := runSkillIndexer(
		t,
		map[string]string{"a.json": `{"rows":[{"a":"x"},{"a":"y"}]}`},
		"json",
		[]any{
			shaper,
		},
		[]any{map[string]any{"sourceFieldName": "/document/rows/*/shape/title", "targetFieldName": "chunks"}},
	)

	assert.Equal(t, "success", last["status"], "%v", last)
	assert.Equal(t, []any{"x", "y"}, firstDoc(t, s)["chunks"], "the skill runs once per element of its context")
}

func TestSkillsetUnsupportedSkillWarns(t *testing.T) {
	t.Parallel()

	ocr := map[string]any{
		"@odata.type": "#Microsoft.Skills.Vision.OcrSkill",
		"inputs":      []any{map[string]any{"name": "image", "source": "/document/normalized_images/*"}},
		"outputs":     []any{map[string]any{"name": "text", "targetName": "ocr"}},
	}

	s, last := runSkillIndexer(t, map[string]string{"a.txt": "plain"}, "default", []any{ocr}, nil)

	assert.Equal(t, "success", last["status"])
	assert.EqualValues(t, 1, last["itemsProcessed"])

	warnings, _ := last["warnings"].([]any)
	require.Len(t, warnings, 1)

	w, _ := warnings[0].(map[string]any)
	assert.Equal(t, "#Microsoft.Skills.Vision.OcrSkill", w["name"])
	assert.Contains(t, w["message"], "not supported")
	assert.NotNil(t, firstDoc(t, s), "the document is still indexed")
}
