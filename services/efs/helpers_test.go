package efs_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/efs"
)

// newTestEFSBackend returns a fresh EFS backend for tests.
func newTestEFSBackend() *efs.InMemoryBackend {
	return efs.NewInMemoryBackend("123456789012", config.DefaultRegion)
}

// newTestEFSHandler returns a fresh EFS handler for tests.
func newTestEFSHandler() *efs.Handler {
	return efs.NewHandler(newTestEFSBackend())
}

// doREST fires an HTTP request at the EFS handler and returns the recorder.
func doREST(
	t *testing.T,
	h *efs.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// parseResp decodes an HTTP response recorder body as a generic JSON map.
func parseResp(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// createFS is a helper that creates a file system and returns its ID.
func createFS(t *testing.T, h *efs.Handler, token string) string {
	t.Helper()

	rec := doREST(t, h, http.MethodPost, "/2015-02-01/file-systems", map[string]any{
		"CreationToken": token,
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	resp := parseResp(t, rec)

	id, ok := resp["FileSystemId"].(string)
	require.True(t, ok)

	return id
}

// fsReq is a convenience constructor for CreateFileSystemRequest with just a token.
func fsReq(token string) efs.CreateFileSystemRequest {
	return efs.CreateFileSystemRequest{CreationToken: token}
}

// mtReq is a convenience constructor for CreateMountTargetRequest.
func mtReq(fsID, subnetID string) efs.CreateMountTargetRequest {
	return efs.CreateMountTargetRequest{FileSystemID: fsID, SubnetID: subnetID}
}

// apReq is a convenience constructor for CreateAccessPointRequest.
func apReq(fsID string) efs.CreateAccessPointRequest {
	return efs.CreateAccessPointRequest{FileSystemID: fsID}
}

// generateString creates a string of the given length for test use.
func generateString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'x'
	}

	return string(b)
}
