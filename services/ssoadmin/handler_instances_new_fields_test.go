package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdateInstance_PermissionSetsEnabled covers gopherstack-gt9o: real
// DescribeInstanceOutput/UpdateInstanceInput gained PermissionSetsEnabled
// (aws-sdk-go-v2/service/ssoadmin@v1.43.1 api_op_DescribeInstance.go:77,
// api_op_UpdateInstance.go:64). It must be threaded through UpdateInstance
// and echoed back by DescribeInstance exactly as supplied.
func TestUpdateInstance_PermissionSetsEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		want bool
	}{
		{
			name: "sets true",
			body: map[string]any{"PermissionSetsEnabled": true},
			want: true,
		},
		{
			name: "sets false explicitly",
			body: map[string]any{"PermissionSetsEnabled": false},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "psenabled-inst")

			tt.body["InstanceArn"] = instanceArn
			rec := doRequest(t, h, "UpdateInstance", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, descRec.Code)
			resp := parseResponse(t, descRec)
			assert.Equal(t, tt.want, resp["PermissionSetsEnabled"])
		})
	}
}

// TestUpdateInstance_PermissionSetsEnabledOmittedLeavesUnchanged verifies that
// omitting PermissionSetsEnabled from an UpdateInstance request never resets
// or guesses the stored value -- only Name is applied.
func TestUpdateInstance_PermissionSetsEnabledOmittedLeavesUnchanged(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "psenabled-unchanged-inst")

	rec := doRequest(t, h, "UpdateInstance", map[string]any{
		"InstanceArn":           instanceArn,
		"PermissionSetsEnabled": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "UpdateInstance", map[string]any{
		"InstanceArn": instanceArn,
		"Name":        "renamed-only",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	descRec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, descRec.Code)
	resp := parseResponse(t, descRec)
	assert.Equal(t, true, resp["PermissionSetsEnabled"])
	assert.Equal(t, "renamed-only", resp["Name"])
}

// TestDescribeInstance_PermissionSetsEnabledAbsentWhenUnset asserts on the raw
// response body: an instance that never had PermissionSetsEnabled set must not
// carry the key at all, not a false value that happens to parse the same way.
func TestDescribeInstance_PermissionSetsEnabledAbsentWhenUnset(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "psenabled-absent-inst")

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "PermissionSetsEnabled")
}

// TestListInstances_Regions verifies InstanceMetadata.Regions (real field,
// aws-sdk-go-v2/service/ssoadmin@v1.43.1 types/types.go:522) is populated from
// this instance's real region state (AddRegion), and that PrimaryRegion
// (types/types.go:518) is never present on the wire -- this backend has no
// caller-settable or derivable source for it (see PARITY.md gaps).
func TestListInstances_Regions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "regions-inst")

	rec := doRequest(t, h, "AddRegion", map[string]any{
		"InstanceArn": instanceArn,
		"RegionName":  "eu-west-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)
	// "IsPrimaryRegion" (a real, always-populated RegionMetadata field) contains
	// "PrimaryRegion" as a substring -- check for the quoted key instead.
	assert.NotContains(t, listRec.Body.String(), `"PrimaryRegion":`)

	resp := parseResponse(t, listRec)
	instances, ok := resp["Instances"].([]any)
	require.True(t, ok)

	var found map[string]any

	for _, raw := range instances {
		inst, iok := raw.(map[string]any)
		require.True(t, iok)

		if inst["InstanceArn"] == instanceArn {
			found = inst

			break
		}
	}

	require.NotNil(t, found, "expected instance %s in ListInstances response", instanceArn)

	regions, ok := found["Regions"].([]any)
	require.True(t, ok, "expected Regions array on instance")
	require.Len(t, regions, 1)

	region, ok := regions[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "eu-west-1", region["RegionName"])
}

// TestListInstances_RegionsAbsentWhenNoneAdded asserts on the raw response
// body that an instance with no AddRegion calls omits Regions entirely rather
// than a misleadingly-present empty array.
func TestListInstances_RegionsAbsentWhenNoneAdded(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createInstance(t, h, "no-regions-inst")

	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "\"Regions\"")
}
