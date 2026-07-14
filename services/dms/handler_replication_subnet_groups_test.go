package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationSubnetGroupLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "test group",
		"SubnetIds":                         []string{"subnet-1", "subnet-2"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	sgArn := parseJSON(t, createRec)["ReplicationSubnetGroup"].(map[string]any)["ReplicationSubnetGroupArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "test group",
		"SubnetIds":                         []string{"subnet-1", "subnet-2"},
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Missing identifier.
	missingRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)

	// Missing SubnetIds.
	missingSubnetsRec := doDMS(t, h, "CreateReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-2",
		"ReplicationSubnetGroupDescription": "test group",
	})
	assert.Equal(t, http.StatusBadRequest, missingSubnetsRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeReplicationSubnetGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify: the description is updated and echoed back on the response.
	modRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier":  "subgrp-1",
		"ReplicationSubnetGroupDescription": "updated",
		"SubnetIds":                         []string{"subnet-1"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)
	assert.Equal(t,
		"updated",
		parseJSON(t, modRec)["ReplicationSubnetGroup"].(map[string]any)["ReplicationSubnetGroupDescription"],
	)

	// The description change must be real backend state, not just echoed in
	// the Modify response: a fresh Describe must also see "updated".
	postModifyDescRec := doDMS(t, h, "DescribeReplicationSubnetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, postModifyDescRec.Code)
	postModifyGroups := parseJSON(t, postModifyDescRec)["ReplicationSubnetGroups"].([]any)
	found := false
	for _, g := range postModifyGroups {
		grp, ok := g.(map[string]any)
		if ok && grp["ReplicationSubnetGroupIdentifier"] == "subgrp-1" {
			found = true
			assert.Equal(t, "updated", grp["ReplicationSubnetGroupDescription"])
		}
	}
	assert.True(t, found, "expected subgrp-1 in DescribeReplicationSubnetGroups")

	// Modify missing SubnetIds.
	modMissingSubnetsRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": "subgrp-1",
	})
	assert.Equal(t, http.StatusBadRequest, modMissingSubnetsRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": "nonexistent",
		"SubnetIds":                        []string{"subnet-1"},
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": sgArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteReplicationSubnetGroup", map[string]any{
		"ReplicationSubnetGroupIdentifier": sgArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}
