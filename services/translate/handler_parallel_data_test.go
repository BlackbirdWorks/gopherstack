package translate_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateParallelData(t *testing.T) {
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

func TestGetParallelData(t *testing.T) {
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

func TestDeleteParallelData(t *testing.T) {
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

func TestListParallelData(t *testing.T) {
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

func TestUpdateParallelData(t *testing.T) {
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

// TestListParallelData_Pagination verifies that MaxResults and NextToken
// paginate correctly through all parallel data resources.
func TestListParallelData_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 4 {
		rec := doRequest(t, h, "CreateParallelData", map[string]any{
			"Name":               "pd-" + string(rune('a'+i)),
			"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	page1, _ := m["ParallelDataPropertiesList"].([]any)
	assert.Len(t, page1, 2)
	nextToken, _ := m["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	rec = doRequest(t, h, "ListParallelData", map[string]any{"MaxResults": 10, "NextToken": nextToken})
	require.Equal(t, http.StatusOK, rec.Code)

	m = unmarshalJSON(t, rec.Body.Bytes())
	page2, _ := m["ParallelDataPropertiesList"].([]any)
	assert.Len(t, page2, 2)
	assert.Nil(t, m["NextToken"])
}

// TestGetParallelData_DataLocationField verifies that GetParallelData returns
// a DataLocation with RepositoryType and Location fields, matching AWS behavior.
func TestGetParallelData_DataLocationField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "loc-pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetParallelData", map[string]any{"Name": "loc-pd"})
	require.Equal(t, http.StatusOK, rec.Code)

	m := unmarshalJSON(t, rec.Body.Bytes())
	loc, ok := m["DataLocation"].(map[string]any)
	require.True(t, ok, "DataLocation must be present")
	assert.Equal(t, "S3", loc["RepositoryType"])
	assert.NotEmpty(t, loc["Location"])
}

// TestUpdateParallelData_NotFound verifies that updating a nonexistent
// parallel data resource returns ResourceNotFoundException.
func TestUpdateParallelData_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UpdateParallelData", map[string]any{
		"Name":               "nonexistent-pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/f.tmx", "Format": "TMX"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"])
}

// TestUpdateParallelData_IncludesLatestUpdateAttemptAt verifies
// UpdateParallelData response includes LatestUpdateAttemptAt.
func TestUpdateParallelData_IncludesLatestUpdateAttemptAt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateParallelData", map[string]any{
		"Name": "pd-update-test",
		"ParallelDataConfig": map[string]any{
			"S3Uri":  "s3://bucket/pd/",
			"Format": "TSV",
		},
	})

	rec := doRequest(t, h, "UpdateParallelData", map[string]any{
		"Name": "pd-update-test",
		"ParallelDataConfig": map[string]any{
			"S3Uri":  "s3://bucket/pd-v2/",
			"Format": "TSV",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := unmarshalJSON(t, rec.Body.Bytes())
	_, hasAt := resp["LatestUpdateAttemptAt"]
	assert.True(t, hasAt, "LatestUpdateAttemptAt must be present in UpdateParallelData response")
}
