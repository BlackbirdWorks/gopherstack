package codebuild_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCodeBuild_SourceCredentials covers Import, List, Delete.
func TestCodeBuild_SourceCredentials(t *testing.T) {
	t.Parallel()

	t.Run("import_returns_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
			"token":      "my-token",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Arn string `json:"arn"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		assert.Contains(t, out.Arn, "GITHUB")
	})

	t.Run("import_missing_token", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("list_after_import", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "BITBUCKET",
			"token":      "tok",
		})

		rec := doRequest(t, h, "ListSourceCredentials", nil)
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			SourceCredentialsInfos []map[string]any `json:"sourceCredentialsInfos"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
		require.Len(t, out.SourceCredentialsInfos, 1)
		assert.Equal(t, "BITBUCKET", out.SourceCredentialsInfos[0]["serverType"])
		assert.Equal(t, "PERSONAL_ACCESS_TOKEN", out.SourceCredentialsInfos[0]["authType"])
	})

	t.Run("delete_existing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		importRec := doRequest(t, h, "ImportSourceCredentials", map[string]any{
			"authType":   "PERSONAL_ACCESS_TOKEN",
			"serverType": "GITHUB",
			"token":      "tok",
		})
		require.Equal(t, http.StatusOK, importRec.Code)

		var importOut struct {
			Arn string `json:"arn"`
		}
		require.NoError(t, json.NewDecoder(importRec.Body).Decode(&importOut))

		delRec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{"arn": importOut.Arn})
		assert.Equal(t, http.StatusOK, delRec.Code)

		// Verify gone.
		listRec := doRequest(t, h, "ListSourceCredentials", nil)
		var listOut struct {
			SourceCredentialsInfos []map[string]any `json:"sourceCredentialsInfos"`
		}
		require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
		assert.Empty(t, listOut.SourceCredentialsInfos)
	})

	t.Run("delete_not_found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{
			"arn": "arn:aws:codebuild:us-east-1:000000000000:token/GHOST",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("delete_missing_arn", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRequest(t, h, "DeleteSourceCredentials", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
