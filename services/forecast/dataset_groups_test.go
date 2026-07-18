package forecast_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDatasetGroups_FieldShapes verifies Create/Describe/Update/List field
// shapes for DatasetGroup, including ARN/timestamp fields populated by the
// generic resource engine.
func TestDatasetGroups_FieldShapes(t *testing.T) {
	t.Parallel()

	h := newHandler()

	code, created := request(t, h, "CreateDatasetGroup", map[string]any{
		"DatasetGroupName": "field-group",
		"Domain":           "RETAIL",
		"DatasetArns":      []any{"arn:aws:forecast:us-east-1:000000000000:dataset/ds1"},
	})
	require.Equal(t, http.StatusOK, code)
	arn := created["DatasetGroupArn"].(string)

	// Advance to ACTIVE
	request(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	code, m := request(t, h, "DescribeDatasetGroup", map[string]any{"DatasetGroupArn": arn})
	require.Equal(t, http.StatusOK, code)

	assert.Equal(t, "field-group", m["DatasetGroupName"])
	assert.Equal(t, "RETAIL", m["Domain"])
	assert.NotEmpty(t, m["DatasetGroupArn"])
	assert.NotEmpty(t, m["CreationTime"])
	assert.NotEmpty(t, m["LastModificationTime"])

	// Update
	code, m = request(t, h, "UpdateDatasetGroup", map[string]any{
		"DatasetGroupArn": arn,
		"DatasetArns":     []any{"arn:aws:forecast:us-east-1:000000000000:dataset/ds2"},
	})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, m["DatasetGroupArn"])

	// List
	code, m = request(t, h, "ListDatasetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, code)
	groups, ok := m["DatasetGroups"].([]any)
	require.True(t, ok)
	require.Len(t, groups, 1)
}
