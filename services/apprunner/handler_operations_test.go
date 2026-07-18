package apprunner_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ops, ok := resp["OperationSummaryList"].([]any)
	require.True(t, ok)
	// CREATE + PAUSE + RESUME = 3 operations
	assert.Len(t, ops, 3)
}
