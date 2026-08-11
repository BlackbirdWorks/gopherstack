package mediatailor_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPutFunction_RejectsUnknownFunctionType reproduces a sweep finding:
// PutFunction only checked FunctionType for non-empty, never against the
// real FunctionType enum (HTTP_REQUEST, CUSTOM_OUTPUT, SEQUENTIAL_EXECUTOR --
// aws-sdk-go-v2/service/mediatailor@v1.63.4 types/enums.go), so a client
// sending a made-up FunctionType got a 200 instead of the BadRequestException
// a real MediaTailor client would receive.
func TestPutFunction_RejectsUnknownFunctionType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPut, "/function/fn1", map[string]any{
		"FunctionType": "NOT_A_REAL_FUNCTION_TYPE",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}
