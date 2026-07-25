package inspector2_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetClustersForImage exercises the fixed request shape: real
// GetClustersForImageInput nests the required resourceId under a "filter"
// object (ClusterForImageFilterCriteria), not a bare "filterCriteria" map,
// and the response's cluster list wire key is "cluster" (singular), not
// "clusters".
func TestGetClustersForImage(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/cluster/get", map[string]any{
		"filter": map[string]any{"resourceId": "sha256:abcd1234"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	_, ok := resp["cluster"]
	assert.True(t, ok)
}

// TestGetClustersForImageMissingResourceIDRejected verifies the required
// filter.resourceId member is validated: real AWS returns
// ValidationException when it is absent.
func TestGetClustersForImageMissingResourceIDRejected(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)
	rec := auditDo(t, h, http.MethodPost, "/cluster/get", map[string]any{
		"filter": map[string]any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
