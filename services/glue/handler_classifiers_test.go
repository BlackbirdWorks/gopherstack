package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifier_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody map[string]any
		updateBody map[string]any
		getName    string
		wantCode   int
	}{
		{
			name: "grok-classifier",
			createBody: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "grok-cl",
					"Classification": "custom",
					"GrokPattern":    "%{GREEDYDATA:message}",
				},
			},
			updateBody: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "grok-cl",
					"Classification": "custom-updated",
					"GrokPattern":    "%{GREEDYDATA:message}",
				},
			},
			getName:  "grok-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "csv-classifier",
			createBody: map[string]any{
				"CsvClassifier": map[string]any{
					"Name":        "csv-cl",
					"Delimiter":   ",",
					"QuoteSymbol": `"`,
					"Header":      []string{"col1", "col2", "col3"},
				},
			},
			updateBody: map[string]any{
				"CsvClassifier": map[string]any{
					"Name":        "csv-cl",
					"Delimiter":   ";",
					"QuoteSymbol": `"`,
				},
			},
			getName:  "csv-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "json-classifier",
			createBody: map[string]any{
				"JSONClassifier": map[string]any{
					"Name":     "json-cl",
					"JsonPath": "$.records[*]",
				},
			},
			updateBody: map[string]any{
				"JSONClassifier": map[string]any{
					"Name":     "json-cl",
					"JsonPath": "$.data[*]",
				},
			},
			getName:  "json-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "xml-classifier",
			createBody: map[string]any{
				"XMLClassifier": map[string]any{
					"Name":           "xml-cl",
					"Classification": "xml",
					"RowTag":         "record",
				},
			},
			updateBody: map[string]any{
				"XMLClassifier": map[string]any{
					"Name":           "xml-cl",
					"Classification": "xml",
					"RowTag":         "item",
				},
			},
			getName:  "xml-cl",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			t.Run("create", func(t *testing.T) {
				t.Parallel()
				h2 := newTestHandler(t)
				rec := doGlueRequest(t, h2, "CreateClassifier", tt.createBody)
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("get", func(t *testing.T) {
				t.Parallel()
				h2 := newTestHandler(t)
				doGlueRequest(t, h2, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h2, "GetClassifier", map[string]any{"Name": tt.getName})
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("update", func(t *testing.T) {
				t.Parallel()
				h2 := newTestHandler(t)
				doGlueRequest(t, h2, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h2, "UpdateClassifier", tt.updateBody)
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("delete", func(t *testing.T) {
				t.Parallel()
				h2 := newTestHandler(t)
				doGlueRequest(t, h2, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h2, "DeleteClassifier", map[string]any{"Name": tt.getName})
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		})
	}
}

func TestClassifier_GetClassifiers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateClassifier", map[string]any{
		"GrokClassifier": map[string]any{"Name": "g1", "Classification": "custom", "GrokPattern": "%{GREEDYDATA}"},
	})
	doGlueRequest(t, h, "CreateClassifier", map[string]any{
		"CsvClassifier": map[string]any{"Name": "c1", "Delimiter": ","},
	})

	rec := doGlueRequest(t, h, "GetClassifiers", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	classifiers := out["Classifiers"].([]any)
	assert.Len(t, classifiers, 2)
}

func TestClassifier_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "create-duplicate",
			op:   "CreateClassifier",
			body: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "dup-cl",
					"Classification": "x",
					"GrokPattern":    "%{GREEDYDATA}",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-missing",
			op:       "GetClassifier",
			body:     map[string]any{"Name": "no-cl"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update-missing",
			op:       "UpdateClassifier",
			body:     map[string]any{"GrokClassifier": map[string]any{"Name": "no-cl", "GrokPattern": "%{GREEDYDATA}"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete-missing",
			op:       "DeleteClassifier",
			body:     map[string]any{"Name": "no-cl"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateClassifier", map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "dup-cl",
					"Classification": "x",
					"GrokPattern":    "%{GREEDYDATA}",
				},
			})

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
