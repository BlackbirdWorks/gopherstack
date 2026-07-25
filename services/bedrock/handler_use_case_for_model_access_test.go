package bedrock_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UseCaseForModelAccess_PutAndGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		formData []byte
	}{
		{name: "business use case form", formData: []byte(`{"useCase":"BUSINESS"}`)},
		{name: "research use case form", formData: []byte(`{"useCase":"RESEARCH"}`)},
		{name: "empty form data", formData: []byte{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			recPut := doRequest(t, h, http.MethodPost, "/use-case-for-model-access",
				map[string]any{"formData": base64.StdEncoding.EncodeToString(tt.formData)})
			require.Equal(t, http.StatusOK, recPut.Code)

			recGet := doRequest(t, h, http.MethodGet, "/use-case-for-model-access", nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var out map[string]any
			mustUnmarshal(t, recGet, &out)

			decoded, err := base64.StdEncoding.DecodeString(out["formData"].(string))
			require.NoError(t, err)
			assert.Equal(t, tt.formData, decoded)
		})
	}
}

func TestHandler_UseCaseForModelAccess_GetDefaultEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/use-case-for-model-access", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	assert.Empty(t, out["formData"])
}

func TestHandler_UseCaseForModelAccess_PutOverwritesPrevious(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	first := base64.StdEncoding.EncodeToString([]byte("first-submission"))
	rec1 := doRequest(t, h, http.MethodPost, "/use-case-for-model-access", map[string]any{"formData": first})
	require.Equal(t, http.StatusOK, rec1.Code)

	second := base64.StdEncoding.EncodeToString([]byte("second-submission"))
	rec2 := doRequest(t, h, http.MethodPost, "/use-case-for-model-access", map[string]any{"formData": second})
	require.Equal(t, http.StatusOK, rec2.Code)

	recGet := doRequest(t, h, http.MethodGet, "/use-case-for-model-access", nil)
	var out map[string]any
	mustUnmarshal(t, recGet, &out)
	assert.Equal(t, second, out["formData"])
}

func TestHandler_UseCaseForModelAccess_MissingFormDataRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/use-case-for-model-access", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UseCaseForModelAccess_InvalidBase64Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/use-case-for-model-access",
		map[string]any{"formData": "not-valid-base64!!"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_UseCaseForModelAccess_WrongPathAndMethodRejected locks in that
// the real AWS path/method (POST /use-case-for-model-access) is what routes,
// and the previously-invented PUT /usecase-for-model-access does NOT.
func TestHandler_UseCaseForModelAccess_WrongPathAndMethodRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, "/usecase-for-model-access",
		map[string]any{"formData": base64.StdEncoding.EncodeToString([]byte("x"))})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
