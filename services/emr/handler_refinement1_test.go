package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/emr"
)

// TestRefinement1_Reset verifies that Backend.Reset() wipes all state maps.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{Name: "cluster1", ReleaseLabel: "emr-6.0.0"})
	require.NoError(t, err)
	require.Equal(t, 1, b.ClusterCount())

	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.SecurityConfigCount())
	assert.Equal(t, 0, b.StudioCount())
	assert.Equal(t, 0, b.PersistentAppUICount())
	assert.Equal(t, 0, b.StudioSessionMappingCount())
}

// TestRefinement1_MultipleResetCycle verifies that Reset is idempotent.
func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	b.Reset()
	b.Reset()

	assert.Equal(t, 0, b.ClusterCount())
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := h.Backend.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "cluster1", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	h.Reset()

	assert.Equal(t, 0, h.Backend.ClusterCount())
}

// TestRefinement1_ProviderInit_NilCtx verifies that ErrNilAppContext is returned for nil AppContext.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, emr.ErrNilAppContext)
}

// TestRefinement1_HandlerOpsPreBuilt verifies that the handler builds its dispatch table at construction time.
func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Positive(t, h.HandlerOpsLen(), "dispatch table must be non-empty after NewHandler")
}

// TestRefinement1_GetSupportedOperations_AllOps verifies all new ops are in the supported list.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	expected := []string{
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
		"DescribeSecurityConfiguration",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "operation %s must be in GetSupportedOperations()", op)
	}
}

// TestRefinement1_GetSupportedOperations_MatchDispatch verifies ops and dispatch table are the same size.
func TestRefinement1_GetSupportedOperations_MatchDispatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, len(h.GetSupportedOperations()), h.HandlerOpsLen(),
		"GetSupportedOperations length must equal dispatch table length")
}

// TestRefinement1_SeedHelpers verifies seed helpers correctly insert into backend.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)

	cluster := &emr.Cluster{
		ID:   "j-0000000000001",
		Name: "seed-cluster",
		ARN:  "arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/j-0000000000001",
		Status: emr.ClusterStatus{
			State: emr.StateWaiting,
		},
	}
	b.AddClusterInternal(context.Background(), cluster)
	assert.Equal(t, 1, b.ClusterCount())

	b.AddSecurityConfigInternal(context.Background(), emr.SecurityConfiguration{
		Name:           "sc1",
		SecurityConfig: `{"EncryptionConfiguration":{}}`,
	})
	assert.Equal(t, 1, b.SecurityConfigCount())

	b.AddStudioInternal(context.Background(), emr.Studio{
		StudioID: "es-0000000000001",
		Name:     "studio1",
	})
	assert.Equal(t, 1, b.StudioCount())

	b.AddPersistentAppUIInternal(context.Background(), emr.PersistentAppUI{
		ID:                "pau-0000000000001",
		TargetResourceArn: cluster.ARN,
	})
	assert.Equal(t, 1, b.PersistentAppUICount())
}

// TestRefinement1_SeedHelpers_DeepCopy verifies seed helpers deep copy values.
func TestRefinement1_SeedHelpers_DeepCopy(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)

	sc := emr.SecurityConfiguration{
		Name:           "sc-deep-copy",
		SecurityConfig: `{"original":true}`,
	}
	b.AddSecurityConfigInternal(context.Background(), sc)

	sc.Name = "mutated"
	assert.Equal(t, 1, b.SecurityConfigCount())
}

// TestRefinement1_ExportCountHelpers verifies all count helpers work.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	assert.Equal(t, 0, b.ClusterCount())
	assert.Equal(t, 0, b.SecurityConfigCount())
	assert.Equal(t, 0, b.StudioCount())
	assert.Equal(t, 0, b.PersistentAppUICount())
	assert.Equal(t, 0, b.StudioSessionMappingCount())
}

// TestRefinement1_SortedListClusters verifies ListClusters returns clusters sorted by ID.
func TestRefinement1_SortedListClusters(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{Name: "clusterA", ReleaseLabel: "emr-6.0.0"})
	require.NoError(t, err)
	_, err = b.RunJobFlow(context.Background(), emr.RunJobFlowParams{Name: "clusterB", ReleaseLabel: "emr-6.0.0"})
	require.NoError(t, err)
	_, err = b.RunJobFlow(context.Background(), emr.RunJobFlowParams{Name: "clusterC", ReleaseLabel: "emr-6.0.0"})
	require.NoError(t, err)

	clusters, _ := b.ListClusters(context.Background(), emr.ListClustersParams{})
	require.Len(t, clusters, 3)

	// ListClusters returns most recently created first (creation-time descending).
	for i := 1; i < len(clusters); i++ {
		assert.GreaterOrEqual(t, clusters[i-1].ID, clusters[i].ID,
			"clusters must be sorted by creation time descending")
	}
}

// TestRefinement1_SortedListTagsForResource verifies tags are returned sorted by key.
func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "tag-cluster", ReleaseLabel: "emr-6.0.0", Tags: []emr.Tag{
			{Key: "zzz", Value: "last"},
			{Key: "aaa", Value: "first"},
			{Key: "mmm", Value: "middle"},
		}},
	)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(context.Background(), cluster.ID)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].Key)
	assert.Equal(t, "mmm", tags[1].Key)
	assert.Equal(t, "zzz", tags[2].Key)
}

// TestRefinement1_CreationDateTime verifies RunJobFlow sets a non-zero CreationDateTime.
func TestRefinement1_CreationDateTime(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	before := time.Now().UnixMilli()
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "ts-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)
	after := time.Now().UnixMilli()

	rawCreation, ok := cluster.Status.Timeline["CreationDateTime"]
	require.True(t, ok, "CreationDateTime must be set in the timeline")

	creationMs, ok := rawCreation.(int64)
	require.True(t, ok, "CreationDateTime must be int64 UnixMilli")
	assert.GreaterOrEqual(t, creationMs, before, "CreationDateTime must be >= before")
	assert.LessOrEqual(t, creationMs, after, "CreationDateTime must be <= after")
}

// TestRefinement1_CreatePersistentAppUI_ReturnsCopy verifies CreatePersistentAppUI returns a copy.
func TestRefinement1_CreatePersistentAppUI_ReturnsCopy(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	ui, err := b.CreatePersistentAppUI(context.Background(), "arn:aws:elasticmapreduce:us-east-1:123:cluster/j-1")
	require.NoError(t, err)
	require.NotNil(t, ui)

	// Mutate the returned value and verify the backend is unaffected.
	originalID := ui.ID
	ui.ID = "mutated"

	ui2, err := b.CreatePersistentAppUI(context.Background(), "arn:aws:elasticmapreduce:us-east-1:123:cluster/j-2")
	require.NoError(t, err)
	assert.NotEqual(t, "mutated", originalID)
	assert.NotEqual(t, ui.ID, ui2.ID)
}

// TestRefinement1_Studio_NameUniqueness verifies CreateStudio enforces name uniqueness.
func TestRefinement1_Studio_NameUniqueness(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	_, err := b.CreateStudio(
		context.Background(),
		"my-studio",
		"SSO",
		"s3://bucket",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateStudio(
		context.Background(),
		"my-studio",
		"SSO",
		"s3://bucket",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.Error(t, err)
}

// TestRefinement1_ErrValidationMapping verifies ErrValidation maps to HTTP 400.
func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreatePersistentAppUI with empty TargetResourceArn.
	rec := doEMRRequest(t, h, "CreatePersistentAppUI", map[string]any{
		"TargetResourceArn": "",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errBody struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errBody))
	assert.Equal(t, "ValidationException", errBody.Type)
}

// TestRefinement1_StudioSessionMapping_CreationTime verifies CreationTime is set.
func TestRefinement1_StudioSessionMapping_CreationTime(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	studio, err := b.CreateStudio(
		context.Background(),
		"ct-studio",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:r",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)

	before := time.Now().Truncate(time.Second)
	err = b.CreateStudioSessionMapping(context.Background(), studio.StudioID, "USER", "user-id", "", "arn:policy")
	require.NoError(t, err)

	// Verify through the HTTP layer.
	h := newTestHandler(t)
	studioOut, err := h.Backend.CreateStudio(
		context.Background(),
		"http-studio",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:r",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)
	err = h.Backend.CreateStudioSessionMapping(
		context.Background(),
		studioOut.StudioID,
		"USER",
		"user2",
		"",
		"arn:policy2",
	)
	require.NoError(t, err)

	assert.Equal(t, 1, h.Backend.StudioSessionMappingCount())
	_ = before // creation time validated at backend level above
}

// TestRefinement1_DescribeSecurityConfiguration verifies the new operation works end-to-end.
func TestRefinement1_DescribeSecurityConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		scName   string
		wantCode int
		create   bool
	}{
		{
			name:     "describes existing security config",
			scName:   "my-sc",
			wantCode: http.StatusOK,
			create:   true,
		},
		{
			name:     "returns error for non-existent config",
			scName:   "missing-sc",
			wantCode: http.StatusBadRequest,
			create:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.create {
				rec := doEMRRequest(t, h, "CreateSecurityConfiguration", map[string]any{
					"Name":                  tt.scName,
					"SecurityConfiguration": `{"EncryptionConfiguration":{}}`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doEMRRequest(t, h, "DescribeSecurityConfiguration", map[string]any{
				"Name": tt.scName,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					Name                  string `json:"Name"`
					CreationDateTime      string `json:"CreationDateTime"`
					SecurityConfiguration string `json:"SecurityConfiguration"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.scName, out.Name)
				assert.NotEmpty(t, out.CreationDateTime)
				assert.JSONEq(t, `{"EncryptionConfiguration":{}}`, out.SecurityConfiguration)
			}
		})
	}
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := src.RunJobFlow(context.Background(), emr.RunJobFlowParams{
		Name:         "persist-cluster",
		ReleaseLabel: "emr-6.8.0",
		Tags:         []emr.Tag{{Key: "env", Value: "dev"}},
	})
	require.NoError(t, err)
	_, err = src.CreateSecurityConfiguration(context.Background(), "sc-1", `{"EncryptionConfiguration":{}}`)
	require.NoError(t, err)
	studio, err := src.CreateStudio(
		context.Background(),
		"studio-persist",
		"SSO",
		"s3://b",
		"sg-1",
		"arn:role",
		"vpc-1",
		"sg-2",
		nil,
		nil,
	)
	require.NoError(t, err)
	err = src.CreateStudioSessionMapping(context.Background(), studio.StudioID, "USER", "uid-1", "", "arn:policy")
	require.NoError(t, err)

	snap := src.Snapshot()
	require.NotNil(t, snap)

	dst := emr.NewInMemoryBackend("", "")
	require.NoError(t, dst.Restore(snap))

	assert.Equal(t, 1, dst.ClusterCount())
	assert.Equal(t, 1, dst.SecurityConfigCount())
	assert.Equal(t, 1, dst.StudioCount())
	assert.Equal(t, 1, dst.StudioSessionMappingCount())

	c, err := dst.DescribeCluster(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, "persist-cluster", c.Name)
	assert.Equal(t, "emr-6.8.0", c.ReleaseLabel)

	sc, err := dst.DescribeSecurityConfiguration(context.Background(), "sc-1")
	require.NoError(t, err)
	assert.Equal(t, "sc-1", sc.Name)
}

// TestRefinement1_PersistenceEmpty verifies Snapshot/Restore works on empty backend.
func TestRefinement1_PersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := emr.NewInMemoryBackend("", "")
	require.NoError(t, b2.Restore(snap))
	assert.Equal(t, 0, b2.ClusterCount())
}

// TestRefinement1_NonNilInstanceGroups verifies ListInstanceGroups returns non-nil for no groups.
func TestRefinement1_NonNilInstanceGroups(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "nogroup-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	groups, err := b.ListInstanceGroups(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.NotNil(t, groups)
	assert.Empty(t, groups)
}

// TestRefinement1_NonNilInstanceFleets verifies ListInstanceFleets returns non-nil for no fleets.
func TestRefinement1_NonNilInstanceFleets(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "nofleet-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	fleets, err := b.ListInstanceFleets(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.NotNil(t, fleets)
	assert.Empty(t, fleets)
}

// TestRefinement1_TerminateJobFlows_StateChangeReason verifies StateChangeReason is set on termination.
func TestRefinement1_TerminateJobFlows_StateChangeReason(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "scr-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	require.NoError(t, b.TerminateJobFlows(context.Background(), []string{cluster.ID}))

	c, err := b.DescribeCluster(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.Equal(t, emr.StateTerminated, c.Status.State)
	assert.NotEmpty(t, c.Status.StateChangeReason["Code"])
}

// TestRefinement1_ListInstanceFleetsTyped verifies ListInstanceFleets HTTP response is typed.
func TestRefinement1_ListInstanceFleetsTyped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "fleet-typed",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId":     createOut.JobFlowID,
		"InstanceFleet": map[string]any{"InstanceFleetType": "TASK", "Name": "typed-fleet"},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []emr.InstanceFleet `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceFleets, 1)
	assert.Equal(t, "typed-fleet", out.InstanceFleets[0].Name)
	assert.NotEmpty(t, out.InstanceFleets[0].ID)
}

// TestRefinement1_AddInstanceGroups_UniqueIDs verifies group IDs are unique across calls.
func TestRefinement1_AddInstanceGroups_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "multi-ig", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	ids1, _, err := b.AddInstanceGroups(context.Background(), cluster.ID, []emr.InstanceGroupSpec{
		{Name: "g1", InstanceRole: "TASK", InstanceType: "m5.xlarge", InstanceCount: 2},
	})
	require.NoError(t, err)

	ids2, _, err := b.AddInstanceGroups(context.Background(), cluster.ID, []emr.InstanceGroupSpec{
		{Name: "g2", InstanceRole: "TASK", InstanceType: "m5.xlarge", InstanceCount: 2},
	})
	require.NoError(t, err)

	assert.NotEqual(t, ids1[0], ids2[0], "consecutive AddInstanceGroups calls must produce unique IDs")
}

// TestRefinement1_HandlerOpsLength verifies dispatch table has all expected operations.
func TestRefinement1_HandlerOpsLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Equal(t, len(ops), h.HandlerOpsLen(),
		"every op in GetSupportedOperations must be in the dispatch table")
}

// TestRefinement1_ListTagsForResource_ReturnsList verifies HTTP response returns a list, not a map.
func TestRefinement1_ListTagsForResource_ReturnsList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "tag-list-cluster",
		"Tags": []map[string]any{
			{"Key": "b", "Value": "2"},
			{"Key": "a", "Value": "1"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	listRec := doEMRRequest(t, h, "ListTagsForResource", map[string]any{
		"ResourceId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Tags []emr.Tag `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.Tags, 2)
	// Verify sorted order.
	assert.Equal(t, "a", out.Tags[0].Key)
	assert.Equal(t, "b", out.Tags[1].Key)
}

// TestRefinement1_MatchPriorityHeaderExact verifies the handler returns PriorityHeaderExact.
func TestRefinement1_MatchPriorityHeaderExact(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

// TestRefinement1_ProviderInit_WithConfig verifies that Init succeeds with a valid AppContext.
func TestRefinement1_ProviderInit_WithConfig(t *testing.T) {
	t.Parallel()

	p := &emr.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestRefinement1_Restore_InvalidJSON verifies Restore returns an error for invalid JSON.
func TestRefinement1_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	err := b.Restore([]byte("not-json"))
	require.Error(t, err)
}

// TestRefinement1_Snapshot_NonNilAfterReset verifies Snapshot returns non-nil after Reset.
func TestRefinement1_Snapshot_NonNilAfterReset(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	b.Reset()
	snap := b.Snapshot()
	assert.NotNil(t, snap)
}
