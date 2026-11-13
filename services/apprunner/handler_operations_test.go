package apprunner_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestListOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	svcArn := createTestService(t, h)

	// Perform some operations
	doRequest(t, h, "PauseService", map[string]any{"ServiceArn": svcArn})
	doRequest(t, h, "ResumeService", map[string]any{"ServiceArn": svcArn})

	rec := doRequest(t, h, "ListOperations", map[string]any{"ServiceArn": svcArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp, jsonx.V1()))
	ops, ok := resp["OperationSummaryList"].([]any)
	require.True(t, ok)
	// CREATE + PAUSE + RESUME = 3 operations
	assert.Len(t, ops, 3)
}
