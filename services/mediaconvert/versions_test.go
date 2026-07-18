package mediaconvert_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListVersions_ReturnsVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	resp, code := parseJSONResponse(t, h, http.MethodGet, "/2017-08-29/versions", nil)
	assert.Equal(t, http.StatusOK, code)

	versions, _ := resp["versions"].([]any)
	assert.NotEmpty(t, versions, "expected at least one version entry")

	first, _ := versions[0].(map[string]any)
	ver, _ := first["version"].(string)
	assert.NotEmpty(t, ver, "version entry must have a non-empty version string")
}

func TestListVersions_ContainsKnownVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/versions", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Versions []struct {
			Version string `json:"version"`
		} `json:"versions"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.Versions)

	found := false

	for _, v := range out.Versions {
		if v.Version == "2017-08-29" {
			found = true
		}
	}

	assert.True(t, found, "expected versions to contain '2017-08-29'")
}
