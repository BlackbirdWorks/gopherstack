package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
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

// TestRebootReplicationInstance_ForceFailoverMutuallyExclusive covers
// gopherstack-4shm's class: RebootReplicationInstanceInput.ForceFailover and
// .ForcePlannedFailover (databasemigrationservice@v1.66.4
// api_op_RebootReplicationInstance.go: "--force-planned-failover and
// --force-failover can't both be set to true") were decoded but never read
// at all -- a client setting both got no rejection.
func TestRebootReplicationInstance_ForceFailoverMutuallyExclusive(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("reboot-ff", "dms.t3.medium")

	descRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	ris := parseJSON(t, descRec)["ReplicationInstances"].([]any)
	require.Len(t, ris, 1)
	riArn := ris[0].(map[string]any)["ReplicationInstanceArn"].(string)

	rec := doDMS(t, h, "RebootReplicationInstance", map[string]any{
		"ReplicationInstanceArn": riArn,
		"ForceFailover":          true,
		"ForcePlannedFailover":   true,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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

func TestHandler_ReplicationInstanceCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "my-rep-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "test"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				ri, ok := resp["ReplicationInstance"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-rep-inst", ri["ReplicationInstanceIdentifier"])
				assert.Equal(t, "dms.t3.medium", ri["ReplicationInstanceClass"])
				assert.Equal(t, "available", ri["ReplicationInstanceStatus"])
				assert.NotEmpty(t, ri["ReplicationInstanceArn"])
				// InstanceCreateTime must be wire-encoded as an epoch-seconds JSON
				// number (awsjson1.1 unixTimestamp format), not an RFC3339 string
				// and not omitted entirely.
				createTime, ok := ri["InstanceCreateTime"].(float64)
				require.True(t, ok, "InstanceCreateTime must be a JSON number")
				assert.Positive(t, createTime)
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "dup-inst",
					"ReplicationInstanceClass":      "dms.t3.micro",
				})
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "dup-inst",
					"ReplicationInstanceClass":      "dms.t3.micro",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "describe_all",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-a",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "describe_by_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "filter-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-id", "Values": []string{"filter-inst"}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "describe_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-id", "Values": []string{"missing"}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Empty(t, list)
			},
		},
		{
			name: "delete_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "del-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ri := createResp["ReplicationInstance"].(map[string]any)
				arn := ri["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": arn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				// Verify gone
				listRec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{})
				listResp := parseJSON(t, listRec)
				list := listResp["ReplicationInstances"].([]any)
				assert.Empty(t, list)
			},
		},
		{
			name: "create_missing_identifier",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceClass": "dms.t3.medium",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_class",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-no-class",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "describe_by_arn_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				create := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "arn-filter-inst",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				ri := createResp["ReplicationInstance"].(map[string]any)
				arn := ri["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
					"Filters": []map[string]any{
						{"Name": "replication-instance-arn", "Values": []string{arn}},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": "arn:aws:dms:us-east-1:000000000000:rep:missing",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

// TestDescribeReplicationInstancesPagination verifies Marker/MaxRecords pagination.
func TestDescribeReplicationInstancesPagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *dms.Handler)
		name       string
		maxRecords int
		wantCount  int
		wantMarker bool
	}{
		{
			name: "first_page_limited",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-a", "inst-b", "inst-c"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 2,
			wantCount:  2,
			wantMarker: true,
		},
		{
			name: "all_results_no_marker",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-x", "inst-y"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 100,
			wantCount:  2,
			wantMarker: false,
		},
		{
			name: "zero_max_records_uses_default",
			setup: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				for _, id := range []string{"inst-p", "inst-q"} {
					doDMS(t, h, "CreateReplicationInstance", map[string]any{
						"ReplicationInstanceIdentifier": id,
						"ReplicationInstanceClass":      "dms.t3.medium",
					})
				}
			},
			maxRecords: 0,
			wantCount:  2,
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			if tt.setup != nil {
				tt.setup(t, h)
			}

			body := map[string]any{}
			if tt.maxRecords > 0 {
				body["MaxRecords"] = tt.maxRecords
			}

			rec := doDMS(t, h, "DescribeReplicationInstances", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseJSON(t, rec)
			list, ok := resp["ReplicationInstances"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// TestDescribeReplicationInstancesContinuation verifies a two-page traversal.
func TestDescribeReplicationInstancesContinuation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	for _, id := range []string{"inst-a", "inst-b", "inst-c"} {
		doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": id,
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
	}

	// First page: 2 of 3.
	rec1 := doDMS(t, h, "DescribeReplicationInstances", map[string]any{"MaxRecords": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseJSON(t, rec1)
	page1, ok := resp1["ReplicationInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	marker, hasMarker := resp1["Marker"].(string)
	require.True(t, hasMarker, "expected Marker in first page response")
	require.NotEmpty(t, marker)

	// Second page: remaining 1.
	rec2 := doDMS(t, h, "DescribeReplicationInstances", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseJSON(t, rec2)
	page2, ok := resp2["ReplicationInstances"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasMarker := resp2["Marker"]
	assert.False(t, stillHasMarker, "last page should have no Marker")

	// All identifiers collectively.
	ids := make([]string, 0, 3)
	for _, item := range append(page1, page2...) {
		ri := item.(map[string]any)
		ids = append(ids, ri["ReplicationInstanceIdentifier"].(string))
	}
	assert.ElementsMatch(t, []string{"inst-a", "inst-b", "inst-c"}, ids)
}

func TestHandler_DeleteInstanceWithTasksFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "delete_instance_with_attached_task_rejected",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createInst := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-with-task",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, createInst.Code)
				instArn := parseJSON(t, createInst)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "iwt-src",
					"EndpointType":       "source",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, srcRec.Code)
				srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "iwt-dst",
					"EndpointType":       "target",
					"EngineName":         "s3",
				})
				require.Equal(t, http.StatusOK, dstRec.Code)
				dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "attached-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})

				delRec := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": instArn,
				})
				assert.Equal(t, http.StatusBadRequest, delRec.Code)
			},
		},
		{
			name: "delete_instance_after_task_deleted_succeeds",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				createInst := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "inst-after-task-del",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, createInst.Code)
				instArn := parseJSON(t, createInst)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "iatd-src",
					"EndpointType":       "source",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, srcRec.Code)
				srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "iatd-dst",
					"EndpointType":       "target",
					"EngineName":         "s3",
				})
				require.Equal(t, http.StatusOK, dstRec.Code)
				dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "iatd-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, taskRec.Code)
				taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				delTask := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				require.Equal(t, http.StatusOK, delTask.Code)

				delInst := doDMS(t, h, "DeleteReplicationInstance", map[string]any{
					"ReplicationInstanceArn": instArn,
				})
				assert.Equal(t, http.StatusOK, delInst.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_DescribeOrderableReplicationInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "returns_multiple_instance_types",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeOrderableReplicationInstances", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				instances, ok := resp["OrderableReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Greater(t, len(instances), 1, "should have more than 1 instance type")
			},
		},
		{
			name: "includes_t3_and_r5_families",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeOrderableReplicationInstances", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				instances := resp["OrderableReplicationInstances"].([]any)

				classes := make([]string, 0, len(instances))
				for _, inst := range instances {
					classes = append(classes, inst.(map[string]any)["ReplicationInstanceClass"].(string))
				}
				assert.Contains(t, classes, "dms.t3.medium")
				assert.Contains(t, classes, "dms.r5.large")
			},
		},
		{
			name: "supports_pagination",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeOrderableReplicationInstances", map[string]any{"MaxRecords": 3})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				instances, ok := resp["OrderableReplicationInstances"].([]any)
				require.True(t, ok)
				assert.Len(t, instances, 3)
				_, hasMarker := resp["Marker"]
				assert.True(t, hasMarker)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
