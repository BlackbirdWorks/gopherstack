package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModifyReplicationInstance(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("mod-ri", "dms.t3.medium")

	descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	ris := parseJSON(t, descRec)["ReplicationInstances"].([]any)
	require.Len(t, ris, 1)
	riArn := ris[0].(map[string]any)["ReplicationInstanceArn"].(string)

	rec := doDMS(t, h, "ModifyReplicationInstance", map[string]any{
		"ReplicationInstanceArn":   riArn,
		"ReplicationInstanceClass": "dms.r5.large",
		"EngineVersion":            "3.5.1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyReplicationInstance", map[string]any{
		"ReplicationInstanceArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestRebootReplicationInstance(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("reboot-ri", "dms.t3.medium")

	descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	ris := parseJSON(t, descRec)["ReplicationInstances"].([]any)
	require.Len(t, ris, 1)
	riArn := ris[0].(map[string]any)["ReplicationInstanceArn"].(string)

	rec := doDMS(t, h, "RebootReplicationInstance", map[string]any{
		"ReplicationInstanceArn": riArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "RebootReplicationInstance", map[string]any{
		"ReplicationInstanceArn": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestDescribeReplicationInstances_PrivateIpAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "via_create"},
		{name: "via_describe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			createRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "ip-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var ri map[string]any

			if tt.name == "via_create" {
				body := parseJSON(t, createRec)
				ri = body["ReplicationInstance"].(map[string]any)
			} else {
				descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				body := parseJSON(t, descRec)
				instances := body["ReplicationInstances"].([]any)
				require.Len(t, instances, 1)
				ri = instances[0].(map[string]any)
			}

			privateIPs, hasKey := ri["ReplicationInstancePrivateIpAddresses"]
			assert.True(t, hasKey, "ReplicationInstancePrivateIpAddresses must be present")
			ipList, ok := privateIPs.([]any)
			assert.True(t, ok, "ReplicationInstancePrivateIpAddresses must be an array")
			assert.NotEmpty(t, ipList, "ReplicationInstancePrivateIpAddresses must have at least one IP")
			assert.Equal(t, "10.0.0.1", ipList[0].(string))

			publicIPs, hasPublicKey := ri["ReplicationInstancePublicIpAddresses"]
			assert.True(t, hasPublicKey, "ReplicationInstancePublicIpAddresses must be present")
			pubList, ok := publicIPs.([]any)
			assert.True(t, ok, "ReplicationInstancePublicIpAddresses must be an array")
			assert.Empty(t, pubList, "ReplicationInstancePublicIpAddresses must be [] (no public IPs)")

			vpcSGs, hasSGKey := ri["VpcSecurityGroups"]
			assert.True(t, hasSGKey, "VpcSecurityGroups must be present")
			sgList, ok := vpcSGs.([]any)
			assert.True(t, ok, "VpcSecurityGroups must be an array")
			assert.Empty(t, sgList, "VpcSecurityGroups must be [] when no SGs configured")
		})
	}
}

func TestDescribeReplicationInstancesHMACPagination(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create three instances.
	for i, id := range []string{"aaa", "bbb", "ccc"} {
		rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": id,
			"ReplicationInstanceClass":      "dms.t3.micro",
		})
		require.Equal(t, http.StatusOK, rec.Code, "create instance %d", i)
	}

	// Page 1: request 2 items.
	page1 := parseJSON(t, doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
	}))
	instances1, ok := page1["ReplicationInstances"].([]any)
	require.True(t, ok)
	require.Len(t, instances1, 2)
	marker1, hasMarker := page1["Marker"].(string)
	require.True(t, hasMarker, "first page must include a Marker")
	require.NotEmpty(t, marker1)

	// Page 2: use the marker.
	page2 := parseJSON(t, doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker1,
	}))
	instances2, ok := page2["ReplicationInstances"].([]any)
	require.True(t, ok)
	require.Len(t, instances2, 1)
	_, hasMore := page2["Marker"]
	assert.False(t, hasMore, "last page must not include a Marker")
}
