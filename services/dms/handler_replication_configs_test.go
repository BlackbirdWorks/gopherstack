package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationConfigLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "rc-1",
		"ReplicationType":             "full-load",
		"SourceEndpointArn":           "arn:src",
		"TargetEndpointArn":           "arn:tgt",
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	rcArn := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

	// Duplicate.
	dupRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "rc-1",
		"ReplicationType":             "cdc",
		"SourceEndpointArn":           "arn:src",
		"TargetEndpointArn":           "arn:tgt",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)

	// Describe.
	descRec := doDMS(t, h, "DescribeReplicationConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, descRec.Code)

	// Modify by ARN.
	modRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
		"ReplicationType":      "cdc",
	})
	assert.Equal(t, http.StatusOK, modRec.Code)

	// Modify not found.
	notFoundRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
		"ReplicationConfigArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// Delete by ARN.
	delRec := doDMS(t, h, "DeleteReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	assert.Equal(t, http.StatusOK, delRec.Code)

	delRec2 := doDMS(t, h, "DeleteReplicationConfig", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	assert.Equal(t, http.StatusNotFound, delRec2.Code)
}

func TestModifyReplicationConfig_UpdatesReplicationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lookupByArn bool
	}{
		{name: "lookup_by_identifier", lookupByArn: false},
		{name: "lookup_by_arn", lookupByArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			createRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
				"ReplicationConfigIdentifier": "rc-modify",
				"ReplicationType":             "full-load",
				"SourceEndpointArn":           "arn:aws:dms:us-east-1:123:endpoint:src",
				"TargetEndpointArn":           "arn:aws:dms:us-east-1:123:endpoint:tgt",
			})
			require.Equal(t, http.StatusOK, createRec.Code)
			rc := parseJSON(t, createRec)["ReplicationConfig"].(map[string]any)
			rcIdentifier := rc["ReplicationConfigIdentifier"].(string)
			rcArn := rc["ReplicationConfigArn"].(string)

			lookupKey := rcIdentifier
			if tt.lookupByArn {
				lookupKey = rcArn
			}

			modRec := doDMS(t, h, "ModifyReplicationConfig", map[string]any{
				"ReplicationConfigArn": lookupKey,
				"ReplicationType":      "cdc",
			})
			require.Equal(t, http.StatusOK, modRec.Code)

			updated := parseJSON(t, modRec)["ReplicationConfig"].(map[string]any)
			assert.Equal(t, "cdc", updated["ReplicationType"],
				"ModifyReplicationConfig must persist the updated ReplicationType")
		})
	}
}
