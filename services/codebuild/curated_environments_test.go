package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodeBuild_CuratedEnvironmentImages verifies ListCuratedEnvironmentImages returns
// the hardcoded platform/image list.
func TestCodeBuild_CuratedEnvironmentImages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListCuratedEnvironmentImages", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Platforms []map[string]any `json:"platforms"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.Platforms)
	assert.Equal(t, "UBUNTU", out.Platforms[0]["platform"])
}
