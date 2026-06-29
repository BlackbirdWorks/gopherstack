package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
)

// -- helpers used only in this file ----------------------------------------------

func doCreateCluster(t *testing.T, h *memorydb.Handler, body map[string]any) (map[string]any, int) {
	t.Helper()
	rec := doRequest(t, h, "CreateCluster", body)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp, rec.Code
}

func doDescribeUsers(t *testing.T, h *memorydb.Handler, userName string) []any {
	t.Helper()
	body := map[string]any{}
	if userName != "" {
		body["UserName"] = userName
	}
	rec := doRequest(t, h, "DescribeUsers", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	users, _ := resp["Users"].([]any)

	return users
}

func doDescribeACLs(t *testing.T, h *memorydb.Handler, aclName string) []any {
	t.Helper()
	body := map[string]any{}
	if aclName != "" {
		body["ACLName"] = aclName
	}
	rec := doRequest(t, h, "DescribeACLs", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	acls, _ := resp["ACLs"].([]any)

	return acls
}

func doDescribeSnapshots(t *testing.T, h *memorydb.Handler, snapshotName string) []any {
	t.Helper()
	body := map[string]any{}
	if snapshotName != "" {
		body["SnapshotName"] = snapshotName
	}
	rec := doRequest(t, h, "DescribeSnapshots", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snaps, _ := resp["Snapshots"].([]any)

	return snaps
}

func doDescribeParameterGroups(t *testing.T, h *memorydb.Handler, pgName string) []any {
	t.Helper()
	body := map[string]any{}
	if pgName != "" {
		body["ParameterGroupName"] = pgName
	}
	rec := doRequest(t, h, "DescribeParameterGroups", body)
	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	pgs, _ := resp["ParameterGroups"].([]any)

	return pgs
}

func doDescribeParameters(t *testing.T, h *memorydb.Handler, pgName string) []any {
	t.Helper()
	rec := doRequest(t, h, "DescribeParameters", map[string]any{"ParameterGroupName": pgName})
	require.Equal(t, http.StatusOK, rec.Code, "DescribeParameters failed: %s", rec.Body)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	params, _ := resp["Parameters"].([]any)

	return params
}

// minimalClusterBody returns a minimal valid CreateCluster request body.
func minimalClusterBody(name string) map[string]any {
	return map[string]any{
		"ClusterName": name,
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	}
}

// -- Finding 5: NumShards/NumReplicas bounds validation --------------------------

func TestAudit2_NumShardsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numShards  int
		wantStatus int
	}{
		{"zero shards rejected", 0, http.StatusBadRequest},
		{"501 shards rejected", 501, http.StatusBadRequest},
		{"1 shard accepted", 1, http.StatusOK},
		{"500 shards accepted", 500, http.StatusOK},
		{"250 shards accepted", 250, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("shardtest")
			body["NumShards"] = tt.numShards
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "NumShards=%d", tt.numShards)
		})
	}
}

func TestAudit2_NumReplicasBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numReplicas int
		wantStatus  int
	}{
		{"negative replicas rejected", -1, http.StatusBadRequest},
		{"6 replicas rejected", 6, http.StatusBadRequest},
		{"0 replicas accepted", 0, http.StatusOK},
		{"5 replicas accepted", 5, http.StatusOK},
		{"3 replicas accepted", 3, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("reptest")
			body["NumReplicasPerShard"] = tt.numReplicas
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "NumReplicasPerShard=%d", tt.numReplicas)
		})
	}
}

// -- Finding 13: Window format validation ----------------------------------------

func TestAudit2_MaintenanceWindowValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		window     string
		wantStatus int
	}{
		{"empty window accepted", "", http.StatusOK},
		{"valid window accepted", "sun:05:00-sun:06:00", http.StatusOK},
		{"no dash rejected", "sun:05:00", http.StatusBadRequest},
		{"empty left part rejected", "-sun:06:00", http.StatusBadRequest},
		{"empty right part rejected", "sun:05:00-", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("mwtest")
			if tt.window != "" {
				body["MaintenanceWindow"] = tt.window
			}
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "MaintenanceWindow=%q", tt.window)
		})
	}
}

func TestAudit2_SnapshotWindowValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		window     string
		wantStatus int
	}{
		{"empty window accepted", "", http.StatusOK},
		{"valid window accepted", "05:00-06:00", http.StatusOK},
		{"no dash rejected", "05:00", http.StatusBadRequest},
		{"empty left part rejected", "-06:00", http.StatusBadRequest},
		{"empty right part rejected", "05:00-", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			body := minimalClusterBody("swtest")
			if tt.window != "" {
				body["SnapshotWindow"] = tt.window
			}
			_, code := doCreateCluster(t, h, body)
			assert.Equal(t, tt.wantStatus, code, "SnapshotWindow=%q", tt.window)
		})
	}
}

// -- Finding 14: SnsTopicStatus stored -------------------------------------------

func TestAudit2_SnsTopicStatusStoredOnUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		snsTopicStatus string
		wantStatus     string
	}{
		{"status active stored", "active", "active"},
		{"status inactive stored", "inactive", "inactive"},
		{"status empty not overwritten", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create cluster first.
			resp, code := doCreateCluster(t, h, minimalClusterBody("sns-test"))
			require.Equal(t, http.StatusOK, code)
			_ = resp

			// Update with SnsTopicStatus.
			updateBody := map[string]any{
				"ClusterName": "sns-test",
			}
			if tt.snsTopicStatus != "" {
				updateBody["SnsTopicStatus"] = tt.snsTopicStatus
			}

			rec := doRequest(t, h, "UpdateCluster", updateBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var updateResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))

			cluster, _ := updateResp["Cluster"].(map[string]any)
			gotStatus, _ := cluster["SnsTopicStatus"].(string)
			assert.Equal(t, tt.wantStatus, gotStatus)
		})
	}
}

// -- Finding 15: EnginePatchVersion lookup ---------------------------------------

func TestAudit2_EnginePatchVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		engine           string
		engineVersion    string
		wantPatchVersion string
	}{
		{"redis 7.0 has patch version", "redis", "7.0", "7.0.7"},
		{"redis 6.2 has patch version", "redis", "6.2", "6.2.6"},
		{"redis 7.1 has patch version", "redis", "7.1", "7.1.0"},
		{"valkey 7.2 has patch version", "valkey", "7.2", "7.2.4"},
		{"valkey 8.0 has patch version", "valkey", "8.0", "8.0.1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			body := minimalClusterBody("patchtest")
			body["Engine"] = tt.engine
			body["EngineVersion"] = tt.engineVersion

			resp, code := doCreateCluster(t, h, body)
			require.Equal(t, http.StatusOK, code)

			cluster, _ := resp["Cluster"].(map[string]any)
			patchVersion, _ := cluster["EnginePatchVersion"].(string)
			assert.Equal(t, tt.wantPatchVersion, patchVersion,
				"Engine=%s EngineVersion=%s", tt.engine, tt.engineVersion)
		})
	}
}

// -- Finding 16: ACL fields ------------------------------------------------------

func TestAudit2_ACL_MinimumEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{"open-access has MinimumEngineVersion", "open-access"},
		{"created ACL has MinimumEngineVersion", "my-acl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tt.aclName != "open-access" {
				rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": tt.aclName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			acls := doDescribeACLs(t, h, tt.aclName)
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			minVersion, _ := acl["MinimumEngineVersion"].(string)
			assert.Equal(t, "6.2", minVersion, "ACL %s should have MinimumEngineVersion=6.2", tt.aclName)
		})
	}
}

func TestAudit2_ACL_PendingChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{"open-access has PendingChanges", "open-access"},
		{"custom ACL has PendingChanges", "custom-acl"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			if tt.aclName != "open-access" {
				rec := doRequest(t, h, "CreateACL", map[string]any{"ACLName": tt.aclName})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			acls := doDescribeACLs(t, h, tt.aclName)
			require.Len(t, acls, 1)

			acl, _ := acls[0].(map[string]any)
			pendingChanges, hasPending := acl["PendingChanges"]
			assert.True(t, hasPending, "ACL %s should have PendingChanges field", tt.aclName)
			pc, _ := pendingChanges.(map[string]any)
			assert.NotNil(t, pc)
		})
	}
}

// -- Finding 17: User fields -----------------------------------------------------

func TestAudit2_User_MinimumEngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		userName string
	}{
		{"created user has MinimumEngineVersion", "test-user"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateUser", map[string]any{
				"UserName":     tt.userName,
				"AccessString": "on ~* &* +@all",
				"AuthenticationMode": map[string]any{
					"Type": "no-password-required",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			users := doDescribeUsers(t, h, tt.userName)
			require.Len(t, users, 1)

			user, _ := users[0].(map[string]any)
			minVersion, _ := user["MinimumEngineVersion"].(string)
			assert.Equal(t, "6.2", minVersion)
		})
	}
}

func TestAudit2_User_ACLNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setupFn      func(h *memorydb.Handler)
		userName     string
		wantACLCount int
	}{
		{
			name: "user in no ACL has empty ACLNames",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "standalone-user",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			userName:     "standalone-user",
			wantACLCount: 0,
		},
		{
			name: "user in one ACL has one ACLName",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "grouped-user",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
				doRequest(t, h, "CreateACL", map[string]any{
					"ACLName":   "test-acl-group",
					"UserNames": []string{"grouped-user"},
				})
			},
			userName:     "grouped-user",
			wantACLCount: 1,
		},
		{
			name: "user in two ACLs has two ACLNames",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "multi-acl-user",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
				doRequest(t, h, "CreateACL", map[string]any{
					"ACLName":   "test-acl-a",
					"UserNames": []string{"multi-acl-user"},
				})
				doRequest(t, h, "CreateACL", map[string]any{
					"ACLName":   "test-acl-b",
					"UserNames": []string{"multi-acl-user"},
				})
			},
			userName:     "multi-acl-user",
			wantACLCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.setupFn(h)

			users := doDescribeUsers(t, h, tt.userName)
			require.Len(t, users, 1)

			user, _ := users[0].(map[string]any)
			aclNames, _ := user["ACLNames"].([]any)
			assert.Len(t, aclNames, tt.wantACLCount)
		})
	}
}

// -- Finding 20: Snapshot cluster configuration ----------------------------------

func TestAudit2_Snapshot_ClusterConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterSetup map[string]any
		wantFields   []string
	}{
		{
			name: "snapshot contains engine field",
			clusterSetup: map[string]any{
				"ClusterName": "snap-engine-test",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Engine":      "redis",
			},
			wantFields: []string{"Engine"},
		},
		{
			name: "snapshot contains maintenance window",
			clusterSetup: map[string]any{
				"ClusterName":       "snap-mw-test",
				"NodeType":          "db.r6g.large",
				"ACLName":           "open-access",
				"MaintenanceWindow": "sun:05:00-sun:06:00",
			},
			wantFields: []string{"MaintenanceWindow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			clusterName := tt.clusterSetup["ClusterName"].(string)
			resp, code := doCreateCluster(t, h, tt.clusterSetup)
			require.Equal(t, http.StatusOK, code, "create cluster failed: %v", resp)

			// Create a snapshot.
			snapshotName := clusterName + "-snap"
			rec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"ClusterName":  clusterName,
				"SnapshotName": snapshotName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe the snapshot.
			snaps := doDescribeSnapshots(t, h, snapshotName)
			require.Len(t, snaps, 1)

			snap, _ := snaps[0].(map[string]any)
			config, hasConfig := snap["ClusterConfiguration"].(map[string]any)
			require.True(t, hasConfig, "snapshot should have ClusterConfiguration")

			for _, field := range tt.wantFields {
				val, ok := config[field]
				assert.True(t, ok, "ClusterConfiguration should have field %q", field)
				assert.NotEmpty(t, val, "ClusterConfiguration.%s should not be empty", field)
			}
		})
	}
}

// -- Findings 25/29: Default parameter groups ------------------------------------

func TestAudit2_DefaultParameterGroupsSeeded(t *testing.T) {
	t.Parallel()

	expectedGroups := []struct {
		name   string
		family string
	}{
		{"default.memorydb-redis6", "memorydb_redis6"},
		{"default.memorydb-redis7", "memorydb_redis7"},
		{"default.memorydb-valkey7", "memorydb_valkey7"},
		{"default.memorydb-valkey8", "memorydb_valkey8"},
	}

	for _, eg := range expectedGroups {
		t.Run(eg.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			pgs := doDescribeParameterGroups(t, h, eg.name)
			require.Len(t, pgs, 1, "default parameter group %q should exist", eg.name)

			pg, _ := pgs[0].(map[string]any)
			assert.Equal(t, eg.name, pg["Name"])
			assert.Equal(t, eg.family, pg["Family"])
		})
	}
}

func TestAudit2_DescribeParameters_NonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		pgName string
	}{
		{"redis6 default params non-empty", "default.memorydb-redis6"},
		{"redis7 default params non-empty", "default.memorydb-redis7"},
		{"valkey7 default params non-empty", "default.memorydb-valkey7"},
		{"valkey8 default params non-empty", "default.memorydb-valkey8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			params := doDescribeParameters(t, h, tt.pgName)
			assert.NotEmpty(t, params, "default parameter group %q should have non-empty parameters", tt.pgName)

			// Verify a known parameter exists.
			found := false
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					found = true
					val, _ := pm["Value"].(string)
					assert.Equal(t, "noeviction", val)

					break
				}
			}
			assert.True(t, found, "expected maxmemory-policy parameter in %q", tt.pgName)
		})
	}
}

// -- Finding 28: ResetParameterGroup honors ParameterNames -----------------------

func TestAudit2_ResetParameterGroup_AllParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		paramNames    []string
		allParameters bool
	}{
		{"AllParameters=true resets all", nil, true},
		{"empty ParameterNames resets all", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create a parameter group and update some values.
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"Family":             "memorydb_redis7",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Change a value.
			rec = doRequest(t, h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": "maxmemory-policy", "ParameterValue": "allkeys-lru"},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify the change.
			params := doDescribeParameters(t, h, "reset-test-pg")
			var currentVal string
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					currentVal, _ = pm["Value"].(string)
				}
			}
			assert.Equal(t, "allkeys-lru", currentVal)

			// Reset.
			resetBody := map[string]any{
				"ParameterGroupName": "reset-test-pg",
				"AllParameters":      tt.allParameters,
			}
			if tt.paramNames != nil {
				resetBody["ParameterNames"] = tt.paramNames
			}
			rec = doRequest(t, h, "ResetParameterGroup", resetBody)
			require.Equal(t, http.StatusOK, rec.Code)

			// Verify reset back to default.
			params = doDescribeParameters(t, h, "reset-test-pg")
			var resetVal string
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					resetVal, _ = pm["Value"].(string)
				}
			}
			assert.Equal(t, "noeviction", resetVal, "maxmemory-policy should be reset to default")
		})
	}
}

func TestAudit2_ResetParameterGroup_SpecificNames(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		paramToChange string
		newValue      string
		paramToReset  string
		otherParam    string
		otherNewValue string
		wantReset     string
		wantOtherKept string
	}{
		{
			name:          "only named parameter is reset",
			paramToChange: "maxmemory-policy",
			newValue:      "allkeys-lru",
			paramToReset:  "maxmemory-policy",
			otherParam:    "hz",
			otherNewValue: "25",
			wantReset:     "noeviction",
			wantOtherKept: "25",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create parameter group.
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"Family":             "memorydb_redis7",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Change two parameters.
			rec = doRequest(t, h, "UpdateParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"ParameterNameValues": []map[string]any{
					{"ParameterName": tt.paramToChange, "ParameterValue": tt.newValue},
					{"ParameterName": tt.otherParam, "ParameterValue": tt.otherNewValue},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Reset only the first parameter.
			rec = doRequest(t, h, "ResetParameterGroup", map[string]any{
				"ParameterGroupName": "selective-reset-pg",
				"ParameterNames":     []string{tt.paramToReset},
				"AllParameters":      false,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Check the reset param.
			params := doDescribeParameters(t, h, "selective-reset-pg")
			vals := make(map[string]string)
			for _, p := range params {
				pm, _ := p.(map[string]any)
				name, _ := pm["Name"].(string)
				val, _ := pm["Value"].(string)
				vals[name] = val
			}

			assert.Equal(t, tt.wantReset, vals[tt.paramToReset], "reset parameter should be at default")
			assert.Equal(t, tt.wantOtherKept, vals[tt.otherParam], "other parameter should keep its value")
		})
	}
}

// -- Finding 22: Events generated ------------------------------------------------

func TestAudit2_Events_Generated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupFn     func(h *memorydb.Handler)
		wantSrcName string
		wantSrcType string
	}{
		{
			name: "CreateCluster emits event",
			setupFn: func(h *memorydb.Handler) {
				doCreateCluster(t, h, minimalClusterBody("evt-cluster"))
			},
			wantSrcName: "evt-cluster",
			wantSrcType: "cluster",
		},
		{
			name: "CreateACL emits event",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateACL", map[string]any{"ACLName": "evt-acl"})
			},
			wantSrcName: "evt-acl",
			wantSrcType: "acl",
		},
		{
			name: "CreateUser emits event",
			setupFn: func(h *memorydb.Handler) {
				doRequest(t, h, "CreateUser", map[string]any{
					"UserName":           "evt-user",
					"AccessString":       "on ~* &* +@all",
					"AuthenticationMode": map[string]any{"Type": "no-password-required"},
				})
			},
			wantSrcName: "evt-user",
			wantSrcType: "user",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.setupFn(h)

			rec := doRequest(t, h, "DescribeEvents", map[string]any{
				"SourceName": tt.wantSrcName,
				"SourceType": tt.wantSrcType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			events, _ := resp["Events"].([]any)
			assert.NotEmpty(t, events, "expected events for %s", tt.wantSrcName)
		})
	}
}

// -- Optimization: ListClusters clone fix ----------------------------------------

func TestAudit2_ListClusters_NoMutation(t *testing.T) {
	t.Parallel()

	b := memorydb.NewInMemoryBackend(testAccountID, testRegion)
	b.AddClusterInternal("cl-clone-test", "db.r6g.large")

	// Call ListClusters and mutate the result; verify backend is not affected.
	clusters := b.ListClusters()
	require.Len(t, clusters, 1)

	// Mutate the returned cluster's tags.
	clusters[0].Tags["mutated"] = "yes"

	// Call again and verify the mutation didn't leak.
	clusters2 := b.ListClusters()
	require.Len(t, clusters2, 1)
	_, leaked := clusters2[0].Tags["mutated"]
	assert.False(t, leaked, "mutation of ListClusters result should not affect backend")
}

// -- DescribeParameters returns metadata -----------------------------------------

func TestAudit2_DescribeParameters_Metadata(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantField string
		wantValue string
	}{
		{"has ChangeType=immediate", "ChangeType", "immediate"},
		{"has Source=system", "Source", "system"},
		{"has MinimumEngineVersion=6.2", "MinimumEngineVersion", "6.2"},
		{"has DataType=string", "DataType", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			params := doDescribeParameters(t, h, "default.memorydb-redis7")
			require.NotEmpty(t, params)

			// All parameters should have the expected field.
			for _, p := range params {
				pm, _ := p.(map[string]any)
				val, _ := pm[tt.wantField].(string)
				assert.Equal(t, tt.wantValue, val,
					"parameter %q should have %s=%q", pm["Name"], tt.wantField, tt.wantValue)
			}
		})
	}
}

// -- CreateParameterGroup seeds defaults -----------------------------------------

func TestAudit2_CreateParameterGroup_SeedsDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		family string
	}{
		{"redis6 family seeded", "memorydb_redis6"},
		{"redis7 family seeded", "memorydb_redis7"},
		{"valkey7 family seeded", "memorydb_valkey7"},
		{"valkey8 family seeded", "memorydb_valkey8"},
	}

	// Each case uses a unique valid resource name (lowercase alphanumeric + hyphens).
	pgNames := map[string]string{
		"memorydb_redis6":  "my-pg-redis6",
		"memorydb_redis7":  "my-pg-redis7",
		"memorydb_valkey7": "my-pg-valkey7",
		"memorydb_valkey8": "my-pg-valkey8",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			pgName := pgNames[tt.family]
			rec := doRequest(t, h, "CreateParameterGroup", map[string]any{
				"ParameterGroupName": pgName,
				"Family":             tt.family,
			})
			require.Equal(t, http.StatusOK, rec.Code, "CreateParameterGroup failed: %s", rec.Body)

			params := doDescribeParameters(t, h, pgName)
			assert.NotEmpty(t, params, "newly created parameter group should have default parameters")

			// Spot-check a known default parameter.
			found := false
			for _, p := range params {
				pm, _ := p.(map[string]any)
				if nm, _ := pm["Name"].(string); nm == "maxmemory-policy" {
					found = true

					break
				}
			}
			assert.True(t, found, "maxmemory-policy should be seeded in new parameter group")
		})
	}
}

// -- Snapshot contains all expanded fields ---------------------------------------

func TestAudit2_Snapshot_ExpandedClusterConfig_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	clusterBody := map[string]any{
		"ClusterName":            "full-config-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"Engine":                 "redis",
		"EngineVersion":          "7.0",
		"MaintenanceWindow":      "sun:05:00-sun:06:00",
		"SnapshotWindow":         "03:00-04:00",
		"SnapshotRetentionLimit": 3,
	}

	resp, code := doCreateCluster(t, h, clusterBody)
	require.Equal(t, http.StatusOK, code, "create cluster: %v", resp)

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "full-config-cluster",
		"SnapshotName": "full-config-snap",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snaps := doDescribeSnapshots(t, h, "full-config-snap")
	require.Len(t, snaps, 1)

	snap, _ := snaps[0].(map[string]any)
	config, _ := snap["ClusterConfiguration"].(map[string]any)
	require.NotNil(t, config)

	assert.Equal(t, "redis", config["Engine"])
	assert.Equal(t, "sun:05:00-sun:06:00", config["MaintenanceWindow"])
	assert.Equal(t, "03:00-04:00", config["SnapshotWindow"])
	assert.EqualValues(t, 3, config["SnapshotRetentionLimit"])
}
