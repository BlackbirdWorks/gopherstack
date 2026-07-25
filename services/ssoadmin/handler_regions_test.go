package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		regionName string
		wantStatus int
		badInst    bool
	}{
		{
			name:       "add region to created instance",
			regionName: "us-west-2",
			wantStatus: http.StatusOK,
		},
		{
			name:       "add same region twice is idempotent",
			regionName: "eu-west-1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "add region to nonexistent instance",
			regionName: "us-east-1",
			wantStatus: http.StatusBadRequest,
			badInst:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var instanceArn string
			if tt.badInst {
				instanceArn = "arn:aws:sso:::instance/ssoins-nonexistent"
			} else {
				instanceArn = createInstance(t, h, "region-test-instance")
			}
			rec := doRequest(t, h, "AddRegion", map[string]any{
				"InstanceArn": instanceArn,
				"RegionName":  tt.regionName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAddRegionIdempotent verifies adding the same region twice is idempotent.
func TestAddRegionIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "region-idempotent-inst")

	rec1 := doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "us-west-2",
	})
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestListRegionsReturnsMetadata verifies that ListRegions returns RegionMetadata objects.
func TestListRegionsReturnsMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "regions-inst")

	rec := doRequest(t, h, "AddRegion", map[string]any{"InstanceArn": instanceArn, "RegionName": "eu-west-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListRegions", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	regions, ok := resp["Regions"].([]any)
	require.True(t, ok)
	require.Len(t, regions, 1)

	region := regions[0].(map[string]any)
	assert.Equal(t, "eu-west-1", region["RegionName"])
	// ListRegions lazily transitions ADDING -> ACTIVE on read, mirroring
	// ListInstances' CREATE_IN_PROGRESS -> ACTIVE transition.
	assert.Equal(t, "ACTIVE", region["Status"])
	assert.Equal(t, false, region["IsPrimaryRegion"])
	assert.NotNil(t, region["AddedDate"])
}

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
	assert.Equal(t, http.StatusBadRequest, rec.Code)

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
	assert.Equal(t, http.StatusBadRequest, rec.Code)

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
	assert.Equal(t, http.StatusBadRequest, rec.Code)

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
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
