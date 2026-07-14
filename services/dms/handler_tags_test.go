package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveTagsFromResource(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "ri-for-tags",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	riArn := parseJSON(t, createRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	addRec := doDMS(t, h, "AddTagsToResource", map[string]any{
		"ResourceArn": riArn,
		"Tags": []map[string]any{
			{"Key": "k1", "Value": "v1"},
			{"Key": "k2", "Value": "v2"},
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	rec := doDMS(t, h, "RemoveTagsFromResource", map[string]any{
		"ResourceArn": riArn,
		"TagKeys":     []string{"k1"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	listRec := doDMS(t, h, "ListTagsForResource", map[string]any{"ResourceArn": riArn})
	require.Equal(t, http.StatusOK, listRec.Code)
	tagList := parseJSON(t, listRec)["TagList"].([]any)
	assert.Len(t, tagList, 1)
	assert.Equal(t, "k2", tagList[0].(map[string]any)["Key"])
}
