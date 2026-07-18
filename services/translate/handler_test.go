package translate_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/translate"
)

// newTestBackend returns an InMemoryBackend for backend-level (non-HTTP) tests.
func newTestBackend(t *testing.T) *translate.InMemoryBackend {
	t.Helper()

	return translate.NewInMemoryBackend("000000000000", "us-east-1")
}

// newTestHandler returns a Handler for handler-level (HTTP) tests.
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

// b64 base64-encodes s, matching what the real aws-sdk-go-v2 translate client
// does to every blob field (TerminologyData.File, Document.Content) before
// sending a request. Test bodies that populate those fields must use this
// helper rather than sending literal text, since the handler decodes them the
// same way the real service does.
func b64(s string) string {
	return base64.StdEncoding.EncodeToString([]byte(s))
}
