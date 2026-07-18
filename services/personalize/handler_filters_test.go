package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Filter_FieldRetention(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	filterExpr := "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)"
	rec := personalizeDo(t, h, "CreateFilter", map[string]any{
		"name":             "my-filter",
		"datasetGroupArn":  "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"filterExpression": filterExpr,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	filterArn := personalizeUnmarshal(t, rec)["filterArn"].(string)

	rec = personalizeDo(t, h, "DescribeFilter", map[string]any{"filterArn": filterArn})
	require.Equal(t, http.StatusOK, rec.Code)
	f := personalizeUnmarshal(t, rec)["filter"].(map[string]any)
	assert.Equal(t, "my-filter", f["name"])
	assert.Equal(t, filterExpr, f["filterExpression"])
	assert.Equal(t, "ACTIVE", f["status"])
}

// --- Recommender ---
