package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeRegion verifies DescribeRegion is a real op backed by the
// region state AddRegion/RemoveRegion mutate, returning the AWS wire shape
// (top-level RegionName/Status/IsPrimaryRegion/AddedDate -- not nested under
// a "Region" envelope, and not a fixed empty stub).
func TestDescribeRegion(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "describe-region-inst")

	// Never-added region: ResourceNotFoundException.
	rec := doRequest(t, h, "DescribeRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "ap-south-1",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Missing required fields: ValidationException.
	rec = doRequest(t, h, "DescribeRegion", map[string]any{"InstanceArn": instanceArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	rec = doRequest(t, h, "DescribeRegion", map[string]any{"RegionName": "us-west-2"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Nonexistent instance: ResourceNotFoundException.
	rec = doRequest(t, h, "DescribeRegion", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/ssoins-nonexistent",
		"RegionName":  "us-west-2",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Add then describe: real state, ADDING status, correct wire field names.
	rec = doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	addResp := parseResponse(t, rec)
	assert.Equal(t, "ADDING", addResp["Status"])

	rec = doRequest(t, h, "DescribeRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assert.Equal(t, "us-west-2", resp["RegionName"])
	assert.Equal(t, "ACTIVE", resp["Status"], "ADDING must lazily transition to ACTIVE on describe")
	assert.Equal(t, false, resp["IsPrimaryRegion"])
	assert.NotNil(t, resp["AddedDate"])
	_, hasRegionEnvelope := resp["Region"]
	assert.False(t, hasRegionEnvelope, "DescribeRegion fields must be top-level, not nested under Region")

	// A second describe keeps it ACTIVE (idempotent transition).
	rec = doRequest(t, h, "DescribeRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp2 := parseResponse(t, rec)
	assert.Equal(t, "ACTIVE", resp2["Status"])

	// RemoveRegion marks it REMOVING and DescribeRegion/ListRegions prune it.
	rec = doRequest(t, h, "RemoveRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	removeResp := parseResponse(t, rec)
	assert.Equal(t, "REMOVING", removeResp["Status"])

	rec = doRequest(t, h, "DescribeRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	rec = doRequest(t, h, "ListRegions", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	listResp := parseResponse(t, rec)
	regions, ok := listResp["Regions"].([]any)
	require.True(t, ok)
	assert.Empty(t, regions, "REMOVING region must be pruned from ListRegions")

	// Removing an already-removed/never-added region: ResourceNotFoundException.
	rec = doRequest(t, h, "RemoveRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
