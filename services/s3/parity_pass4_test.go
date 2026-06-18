package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeleteObjects_OverLimitReturnsMalformedXML verifies that a DeleteObjects
// request exceeding the 1000-key limit fails with HTTP 400 and the MalformedXML
// error code (matching AWS), rather than a generic InvalidArgument.
func TestDeleteObjects_OverLimitReturnsMalformedXML(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	body := buildDeleteBody(1001)

	req := httptest.NewRequest(http.MethodPost, "/bkt?delete", strings.NewReader(body))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}

// TestDeleteObjects_AtLimitSucceeds verifies a request at exactly the 1000-key
// limit is accepted.
func TestDeleteObjects_AtLimitSucceeds(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	mustCreateBucket(t, backend, "bkt")

	body := buildDeleteBody(1000)

	req := httptest.NewRequest(http.MethodPost, "/bkt?delete", strings.NewReader(body))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
}

func buildDeleteBody(n int) string {
	var sb strings.Builder
	sb.WriteString("<Delete>")
	for i := range n {
		sb.WriteString("<Object><Key>key-")
		sb.WriteString(strconv.Itoa(i))
		sb.WriteString("</Key></Object>")
	}
	sb.WriteString("</Delete>")

	return sb.String()
}
