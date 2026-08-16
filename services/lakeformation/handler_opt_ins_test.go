package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_CreateLakeFormationOptIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/MyRole"},` +
				`"Resource":{"Database":{"Name":"mydb"}}}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_principal",
			body:       `{"Resource":{"Database":{"Name":"mydb"}}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_resource",
			body:       `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/R"}}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/CreateLakeFormationOptIn", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateLakeFormationOptIn_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"Principal":{"DataLakePrincipalIdentifier":"arn:aws:iam::123:role/R"},` +
		`"Resource":{"Database":{"Name":"db1"}}}`

	rec := doLFRequest(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestCreateLakeFormationOptIn_DuplicateReturns409(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	body := map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	}

	rec1 := postJSON(t, h, "/CreateLakeFormationOptIn", body)
	require.Equal(t, http.StatusOK, rec1.Code)
	assert.Equal(t, 1, b.OptInCount())

	rec2 := postJSON(t, h, "/CreateLakeFormationOptIn", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestDeleteLakeFormationOptIn_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// First create
	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, 1, b.OptInCount())

	// Now delete
	rec := postJSON(t, h, "/DeleteLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, b.OptInCount())
}

func TestDeleteLakeFormationOptIn_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/nobody"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- ListLakeFormationOptIns tests ---

func TestListLakeFormationOptIns(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::000000000000:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})

	rec := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	entries := out["LakeFormationOptInsInfoList"].([]any)
	assert.Len(t, entries, 1)
}

// --- GetDataLakePrincipal tests ---

func TestCreateLakeFormationOptIn_HasTimestamps(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{
		"Principal": map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"},
		"Resource":  map[string]any{"Database": map[string]any{"Name": "db1"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))

	list, ok := out["LakeFormationOptInsInfoList"].([]any)
	require.True(t, ok && len(list) > 0)

	entry := list[0].(map[string]any)
	assert.NotEmpty(t, entry["LastModified"], "LastModified should be set")
	assert.NotEmpty(t, entry["LastUpdatedBy"], "LastUpdatedBy should be set")
}

// --- #20: ListLakeFormationOptIns resource filter ---

func TestListLakeFormationOptIns_ResourceFilter(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	alice := map[string]any{"DataLakePrincipalIdentifier": "arn:aws:iam::123:user/alice"}
	db1 := map[string]any{"Database": map[string]any{"Name": "db1"}}
	db2 := map[string]any{"Database": map[string]any{"Name": "db2"}}

	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{"Principal": alice, "Resource": db1})
	postJSON(t, h, "/CreateLakeFormationOptIn", map[string]any{"Principal": alice, "Resource": db2})

	rec := postJSON(t, h, "/ListLakeFormationOptIns", map[string]any{
		"Resource": db1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))

	list, _ := out["LakeFormationOptInsInfoList"].([]any)
	assert.Len(t, list, 1, "should only return opt-ins for db1")
}
