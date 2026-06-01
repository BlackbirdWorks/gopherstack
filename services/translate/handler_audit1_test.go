package translate_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

func newTestHandler(t *testing.T) *translate.Handler {
	t.Helper()

	backend := translate.NewInMemoryBackend("000000000000", "us-east-1")

	return translate.NewHandler(backend)
}

func doRequest(t *testing.T, h *translate.Handler, operation string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSShineFrontendService_20170701."+operation)

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func unmarshalJSON(t *testing.T, data []byte) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(data, &m))

	return m
}

// --- Terminology tests ---

func TestAudit1_ImportTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "creates terminology returns properties",
			body: map[string]any{
				"Name":            "my-terminology",
				"MergeStrategy":   "OVERWRITE",
				"TerminologyData": map[string]any{"File": "en,fr\nhello,bonjour", "Format": "CSV"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				props, ok := m["TerminologyProperties"].(map[string]any)
				require.True(t, ok, "TerminologyProperties missing")
				assert.Equal(t, "my-terminology", props["Name"])
				assert.NotEmpty(t, props["Arn"])
				assert.NotEmpty(t, props["CreatedAt"])
			},
		},
		{
			name:     "missing Name returns error",
			body:     map[string]any{"MergeStrategy": "OVERWRITE", "TerminologyData": map[string]any{"Format": "CSV"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "overwrite existing terminology",
			body: map[string]any{
				"Name":            "my-terminology",
				"MergeStrategy":   "OVERWRITE",
				"Description":     "updated",
				"TerminologyData": map[string]any{"File": "en,de\nhello,hallo", "Format": "CSV"},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				props := m["TerminologyProperties"].(map[string]any)
				assert.Equal(t, "updated", props["Description"])
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ImportTerminology", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_GetTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *translate.Handler)
		body     any
		name     string
		wantCode int
	}{
		{
			name: "returns existing terminology",
			setup: func(t *testing.T, h *translate.Handler) {
				t.Helper()

				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name": "my-term", "MergeStrategy": "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body:     map[string]any{"Name": "my-term"},
			wantCode: http.StatusOK,
		},
		{
			name:     "returns error for missing terminology",
			body:     map[string]any{"Name": "nonexistent"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(t, h)
			}

			rec := doRequest(t, h, "GetTerminology", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAudit1_DeleteTerminology(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		preload  bool
	}{
		{
			name:     "deletes existing terminology",
			wantCode: http.StatusOK,
			preload:  true,
		},
		{
			name:     "error when terminology missing",
			wantCode: http.StatusBadRequest,
			preload:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.preload {
				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name": "to-delete", "MergeStrategy": "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "DeleteTerminology", map[string]any{"Name": "to-delete"})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAudit1_ListTerminologies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
		count    int
	}{
		{
			name:     "empty list returns empty array",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				list, _ := m["TerminologyPropertiesList"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name:     "returns created terminologies",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			count:    2,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				list, _ := m["TerminologyPropertiesList"].([]any)
				assert.Len(t, list, 2)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tc.count {
				rec := doRequest(t, h, "ImportTerminology", map[string]any{
					"Name":            "term-" + string(rune('a'+i)),
					"MergeStrategy":   "OVERWRITE",
					"TerminologyData": map[string]any{"Format": "CSV"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListTerminologies", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- Parallel Data tests ---

func TestAudit1_CreateParallelData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "creates parallel data",
			body: map[string]any{
				"Name":        "my-pd",
				"Description": "test data",
				"ParallelDataConfig": map[string]any{
					"S3Uri":  "s3://bucket/key.tmx",
					"Format": "TMX",
				},
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				assert.Equal(t, "my-pd", m["Name"])
				assert.Equal(t, "ACTIVE", m["Status"])
			},
		},
		{
			name:     "missing Name returns error",
			body:     map[string]any{"Description": "no name"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate name returns conflict",
			body: map[string]any{
				"Name": "dup-pd",
				"ParallelDataConfig": map[string]any{
					"S3Uri":  "s3://bucket/a.tmx",
					"Format": "TMX",
				},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.name == "duplicate name returns conflict" {
				rec := doRequest(t, h, "CreateParallelData", tc.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreateParallelData", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

func TestAudit1_GetParallelData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		preload  bool
	}{
		{
			name:     "returns existing parallel data",
			wantCode: http.StatusOK,
			preload:  true,
		},
		{
			name:     "error when not found",
			wantCode: http.StatusBadRequest,
			preload:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.preload {
				rec := doRequest(t, h, "CreateParallelData", map[string]any{
					"Name":               "pd-1",
					"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "GetParallelData", map[string]any{"Name": "pd-1"})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestAudit1_DeleteParallelData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "pd-del",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "DeleteParallelData", map[string]any{"Name": "pd-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetParallelData", map[string]any{"Name": "pd-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_ListParallelData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"pd-a", "pd-b", "pd-c"} {
		rec := doRequest(t, h, "CreateParallelData", map[string]any{
			"Name":               name,
			"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListParallelData", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	list, _ := m["ParallelDataPropertiesList"].([]any)
	assert.Len(t, list, 3)
}

// --- Translation Job tests ---

func TestAudit1_StartTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "batch-job-1",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr", "de"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig": map[string]any{
			"S3Uri":       "s3://bucket/input/",
			"ContentType": "text/plain",
		},
		"OutputDataConfig": map[string]any{
			"S3Uri": "s3://bucket/output/",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.NotEmpty(t, m["JobId"])
	assert.Equal(t, "IN_PROGRESS", m["JobStatus"])
}

func TestAudit1_DescribeTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "describe-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"es"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	rec = doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	props, ok := m["TextTranslationJobProperties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, jobID, props["JobId"])
	assert.Equal(t, "describe-test", props["JobName"])
}

func TestAudit1_StopTextTranslationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "stop-test",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	rec = doRequest(t, h, "StopTextTranslationJob", map[string]any{"JobId": jobID})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "STOP_REQUESTED", m["JobStatus"])
}

func TestAudit1_ListTextTranslationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
			"JobName":             "job-" + string(rune('a'+i)),
			"SourceLanguageCode":  "en",
			"TargetLanguageCodes": []string{"fr"},
			"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
			"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
			"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListTextTranslationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	jobs, _ := m["TextTranslationJobPropertiesList"].([]any)
	assert.Len(t, jobs, 3)
}

// --- TranslateText tests ---

func TestAudit1_TranslateText(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		check    func(t *testing.T, body []byte)
		name     string
		wantCode int
	}{
		{
			name: "translates text returns source and target",
			body: map[string]any{
				"Text":               "Hello world",
				"SourceLanguageCode": "en",
				"TargetLanguageCode": "fr",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				assert.Equal(t, "Hello world", m["TranslatedText"])
				assert.Equal(t, "en", m["SourceLanguageCode"])
				assert.Equal(t, "fr", m["TargetLanguageCode"])
			},
		},
		{
			name: "auto-detect source language",
			body: map[string]any{
				"Text":               "Bonjour",
				"SourceLanguageCode": "auto",
				"TargetLanguageCode": "en",
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()

				m := unmarshalJSON(t, body)
				assert.NotEmpty(t, m["SourceLanguageCode"])
				assert.Equal(t, "en", m["TargetLanguageCode"])
			},
		},
		{
			name:     "missing Text returns error",
			body:     map[string]any{"SourceLanguageCode": "en", "TargetLanguageCode": "fr"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing TargetLanguageCode returns error",
			body:     map[string]any{"Text": "Hello", "SourceLanguageCode": "en"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "TranslateText", tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// --- TranslateDocument tests ---

func TestAudit1_TranslateDocument(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "TranslateDocument", map[string]any{
		"Document": map[string]any{
			"Content":     "SGVsbG8gd29ybGQ=",
			"ContentType": "text/plain",
		},
		"SourceLanguageCode": "en",
		"TargetLanguageCode": "fr",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.NotNil(t, m["TranslatedDocument"])
	assert.Equal(t, "en", m["SourceLanguageCode"])
	assert.Equal(t, "fr", m["TargetLanguageCode"])
}

// --- ListLanguages tests ---

func TestAudit1_ListLanguages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListLanguages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	langs, _ := m["Languages"].([]any)
	assert.NotEmpty(t, langs)

	first, ok := langs[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, first["LanguageCode"])
	assert.NotEmpty(t, first["LanguageName"])
}

// --- Tag tests ---

func TestAudit1_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "tagged-term", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termProps := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)
	termARN := termProps["Arn"].(string)

	rec = doRequest(t, h, "TagResource", map[string]any{
		"ResourceArn": termARN,
		"Tags": []map[string]any{
			{"Key": "env", "Value": "test"},
			{"Key": "team", "Value": "translate"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": termARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 2)
}

func TestAudit1_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ImportTerminology", map[string]any{
		"Name": "untag-term", "MergeStrategy": "OVERWRITE",
		"TerminologyData": map[string]any{"Format": "CSV"},
		"Tags": []map[string]any{
			{"Key": "keep", "Value": "yes"},
			{"Key": "remove", "Value": "no"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	termARN := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)["Arn"].(string)

	rec = doRequest(t, h, "UntagResource", map[string]any{
		"ResourceArn": termARN,
		"TagKeys":     []string{"remove"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": termARN})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	tags, _ := m["Tags"].([]any)
	assert.Len(t, tags, 1)
}

func TestAudit1_UpdateParallelData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "pd-update",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "UpdateParallelData", map[string]any{
		"Name":               "pd-update",
		"Description":        "updated description",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f2.tmx", "Format": "TMX"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	assert.Equal(t, "pd-update", m["Name"])
}
