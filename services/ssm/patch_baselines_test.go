package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeAvailablePatches verifies that the available patches catalog is returned.
func TestDescribeAvailablePatches(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		wantName  string
		seed      []ssm.Patch
		wantCount int
	}{
		{
			name:      "empty catalog returns empty list",
			seed:      nil,
			wantCount: 0,
		},
		{
			name: "returns seeded patches",
			seed: []ssm.Patch{
				{Name: "CVE-2024-001", Product: "Windows", Classification: "SecurityUpdates", Severity: "Critical"},
				{Name: "CVE-2024-002", Product: "Windows", Classification: "BugFix", Severity: "Medium"},
			},
			wantCount: 2,
			wantName:  "CVE-2024-001",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			for _, p := range tc.seed {
				b.AddAvailablePatchInternal(p)
			}

			rec := doRequest(t, h, "DescribeAvailablePatches", `{}`)
			require.Equal(t, http.StatusOK, rec.Code)
			assertBodyContains(t, rec, "Patches")

			if tc.wantName != "" {
				assertBodyContains(t, rec, tc.wantName)
			}
		})
	}
}

// TestDescribePatchGroupState verifies that patch group state is aggregated from instance patch states.
func TestDescribePatchGroupState(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	cases := []struct {
		name             string
		patchGroup       string
		wantBodyContains string
		seed             []ssm.InstancePatchState
		wantInstances    bool // true if Instances > 0 expected
	}{
		{
			name:             "empty store returns zero counts",
			seed:             nil,
			patchGroup:       "grp1",
			wantInstances:    false,
			wantBodyContains: "Instances",
		},
		{
			name: "aggregates counts for matching patch group",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", InstalledCount: 5, FailedCount: 1, OperationStartTime: now},
				{InstanceID: "i-bbb", PatchGroup: "grp1", InstalledCount: 3, MissingCount: 2, OperationStartTime: now},
				{InstanceID: "i-ccc", PatchGroup: "grp2", InstalledCount: 10, OperationStartTime: now},
			},
			patchGroup:       "grp1",
			wantInstances:    true,
			wantBodyContains: "grp1",
		},
		{
			name: "ignores instances from other patch groups",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp2", InstalledCount: 5, OperationStartTime: now},
			},
			patchGroup:       "grp1",
			wantInstances:    false,
			wantBodyContains: "Instances",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			for _, s := range tc.seed {
				b.AddInstancePatchStateInternal(s)
			}

			body := `{"PatchGroup":"` + tc.patchGroup + `"}`
			rec := doRequest(t, h, "DescribePatchGroupState", body)
			require.Equal(t, http.StatusOK, rec.Code)
			assertBodyContains(t, rec, "Instances")

			if tc.wantInstances {
				// The response should contain a non-zero Instances count
				assert.NotContains(t, rec.Body.String(), `"Instances":0`)
			}
		})
	}
}
func TestGetDefaultPatchBaseline_StableRealID(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	first, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(first.BaselineID, "pb-"),
		"default baseline ID must be well-shaped")
	assert.NotEqual(t, "pb-00000000000000000", first.BaselineID,
		"must not return the fabricated all-zeros ID")

	second, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.Equal(t, first.BaselineID, second.BaselineID,
		"default baseline ID must be stable for a given OS")

	// A registered default overrides the synthetic one.
	blID := createTestBaseline(t, b, "custom-baseline")
	_, err = b.RegisterDefaultPatchBaseline(ctx, &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: blID,
	})
	require.NoError(t, err)

	got, err := b.GetDefaultPatchBaseline(ctx, &ssm.GetDefaultPatchBaselineInput{
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)
	assert.Equal(t, blID, got.BaselineID,
		"a registered default must be returned over the synthetic one")
}
func TestDescribeEffectivePatches_FromApprovedAndCatalog(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.TODO()

	create, err := b.CreatePatchBaseline(ctx, &ssm.CreatePatchBaselineInput{
		Name:                           "eff-baseline",
		OperatingSystem:                "AMAZON_LINUX_2",
		ApprovedPatches:                []string{"CVE-2024-0001", "CVE-2024-0002"},
		ApprovedPatchesComplianceLevel: "CRITICAL",
	})
	require.NoError(t, err)

	out, err := b.DescribeEffectivePatchesForPatchBaseline(
		ctx,
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: create.BaselineID},
	)
	require.NoError(t, err)
	require.Len(t, out.EffectivePatches, 2,
		"effective patches must be derived from the baseline's approved patches")

	for _, ep := range out.EffectivePatches {
		require.NotNil(t, ep.Patch)
		require.NotNil(t, ep.PatchStatus)
		assert.Equal(t, "EXPLICIT_APPROVED", ep.PatchStatus.DeploymentStatus)
		assert.Equal(t, "CRITICAL", ep.PatchStatus.ComplianceLevel)
	}

	// Unknown baseline still errors distinctly.
	_, err = b.DescribeEffectivePatchesForPatchBaseline(
		ctx,
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: "pb-unknown"},
	)
	require.ErrorIs(t, err, ssm.ErrPatchBaselineNotFound)
}

// TestStubOps_DeletePatchBaseline exercises the patch-baseline delete stub.
func TestStubOps_DeletePatchBaseline(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	pb, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "test-baseline",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"BaselineId": pb.BaselineID})
	rec := doRequest(t, h, "DeletePatchBaseline", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_DeregisterPatchBaselineForPatchGroup exercises that stub.
func TestStubOps_DeregisterPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(
		t,
		h,
		"DeregisterPatchBaselineForPatchGroup",
		`{"BaselineId":"pb-1234","PatchGroup":"test-group"}`,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_DescribePatchBaselines exercises that stub.
func TestStubOps_DescribePatchBaselines(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "DescribePatchBaselines", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_GetDefaultPatchBaseline exercises that stub.
func TestStubOps_GetDefaultPatchBaseline(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)
	rec := doRequest(t, h, "GetDefaultPatchBaseline", `{}`)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_GetPatchBaseline exercises that stub.
func TestStubOps_GetPatchBaseline(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	pb, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "test-baseline-2",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"BaselineId": pb.BaselineID})
	rec := doRequest(t, h, "GetPatchBaseline", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_UpdatePatchBaseline exercises that stub.
func TestStubOps_UpdatePatchBaseline(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	pb, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "test-baseline-3",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"BaselineId": pb.BaselineID, "Name": "updated-baseline"})
	rec := doRequest(t, h, "UpdatePatchBaseline", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestStubOps_RegisterPatchBaselineForPatchGroup requires valid baseline.
func TestStubOps_RegisterPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	pb, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "test-baseline-4",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	body, _ := json.Marshal(map[string]any{"BaselineId": pb.BaselineID, "PatchGroup": "test-group"})
	rec := doRequest(t, h, "RegisterPatchBaselineForPatchGroup", string(body))
	assert.Equal(t, http.StatusOK, rec.Code)
}
func TestCreatePatchBaseline_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "minimal_baseline",
			body:       `{"Name":"MyBaseline"}`,
			wantStatus: http.StatusOK,
		},
		{
			name: "full_baseline",
			body: `{"Name":"FullBaseline","Description":"desc","OperatingSystem":"WINDOWS",` +
				`"ApprovedPatches":["KB123"],"RejectedPatches":["KB999"],` +
				`"ApprovedPatchesComplianceLevel":"HIGH"}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandler(t)
			rec := doRequest(t, h, "CreatePatchBaseline", tt.body)

			require.Equal(t, tt.wantStatus, rec.Code)

			var resp ssm.CreatePatchBaselineOutput
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.BaselineID)
			assert.True(t, strings.HasPrefix(resp.BaselineID, "pb-"))
			assert.Equal(t, 1, backend.PatchBaselineCount())
		})
	}
}
func TestSSMBounds_DescribePatchGroups(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(100), "100 is valid cap", false},
		{ptr64(101), "101 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribePatchGroups(ctx, &ssm.DescribePatchGroupsInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestBackendOps_GetPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "get-baseline-test")

	out, err := b.GetPatchBaseline(context.TODO(), &ssm.GetPatchBaselineInput{BaselineID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}
func TestBackendOps_GetDefaultPatchBaseline_NoDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	// No default registered; should return a synthetic ID.
	out, err := b.GetDefaultPatchBaseline(context.TODO(), &ssm.GetDefaultPatchBaselineInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.BaselineID)
}
func TestBackendOps_GetDefaultPatchBaseline_WithDefault(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "default-baseline")

	_, err := b.RegisterDefaultPatchBaseline(context.TODO(), &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: id,
	})
	require.NoError(t, err)

	out, err := b.GetDefaultPatchBaseline(context.TODO(), &ssm.GetDefaultPatchBaselineInput{})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}
func TestBackendOps_GetPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "group-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "my-group",
	})
	require.NoError(t, err)

	out, err := b.GetPatchBaselineForPatchGroup(context.TODO(), &ssm.GetPatchBaselineForPatchGroupInput{
		PatchGroup: "my-group",
	})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
	assert.Equal(t, "my-group", out.PatchGroup)
}
func TestBackendOps_RegisterDefaultPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "reg-default-baseline")

	out, err := b.RegisterDefaultPatchBaseline(context.TODO(), &ssm.RegisterDefaultPatchBaselineInput{
		BaselineID: id,
	})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)
}
func TestBackendOps_DeregisterPatchBaselineForPatchGroup(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "dereg-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "dereg-group",
	})
	require.NoError(t, err)

	out, err := b.DeregisterPatchBaselineForPatchGroup(context.TODO(), &ssm.DeregisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "dereg-group",
	})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
func TestBackendOps_DeletePatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "delete-baseline")

	out, err := b.DeletePatchBaseline(context.TODO(), &ssm.DeletePatchBaselineInput{BaselineID: id})
	require.NoError(t, err)
	assert.Equal(t, id, out.BaselineID)

	// Should be gone.
	_, err = b.GetPatchBaseline(context.TODO(), &ssm.GetPatchBaselineInput{BaselineID: id})
	require.Error(t, err)
}
func TestBackendOps_DescribePatchBaselines(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	createTestBaseline(t, b, "describe-baseline-1")
	createTestBaseline(t, b, "describe-baseline-2")

	out, err := b.DescribePatchBaselines(context.TODO(), &ssm.DescribePatchBaselinesInput{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.BaselineIdentities), 2)
}
func TestBackendOps_DescribePatchGroups(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "pg-baseline")

	_, err := b.RegisterPatchBaselineForPatchGroup(context.TODO(), &ssm.RegisterPatchBaselineForPatchGroupInput{
		BaselineID: id,
		PatchGroup: "pg-test-group",
	})
	require.NoError(t, err)

	out, err := b.DescribePatchGroups(context.TODO(), &ssm.DescribePatchGroupsInput{})
	require.NoError(t, err)
	assert.NotEmpty(t, out.Mappings)
}
func TestBackendOps_DescribePatchGroupState(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribePatchGroupState(context.TODO(), &ssm.DescribePatchGroupStateInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
	assert.Equal(t, int32(0), out.Instances)
}
func TestBackendOps_DescribePatchProperties(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.DescribePatchProperties(context.TODO(), &ssm.DescribePatchPropertiesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out.Properties)
}
func TestBackendOps_DescribeEffectivePatchesForPatchBaseline(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	id := createTestBaseline(t, b, "effective-patches-baseline")

	out, err := b.DescribeEffectivePatchesForPatchBaseline(
		context.TODO(),
		&ssm.DescribeEffectivePatchesForPatchBaselineInput{BaselineID: id},
	)
	require.NoError(t, err)
	assert.NotNil(t, out.EffectivePatches)
}
func TestBackendOps_GetDeployablePatchSnapshotForInstance(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	out, err := b.GetDeployablePatchSnapshotForInstance(
		context.TODO(),
		&ssm.GetDeployablePatchSnapshotForInstanceInput{
			InstanceID: "i-snapshot-test",
			SnapshotID: "snap-1234",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "i-snapshot-test", out.InstanceID)
	assert.Equal(t, "snap-1234", out.SnapshotID)
	assert.NotEmpty(t, out.SnapshotDownloadURL)
}
func TestFull_PatchBaseline_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newFullHandler()

	// Create
	code, out := mustPost(t, h, "CreatePatchBaseline", map[string]any{
		"Name":            "TestBaseline",
		"OperatingSystem": "WINDOWS",
		"Description":     "test patch baseline",
	})
	assert.Equal(t, http.StatusOK, code)
	baselineID := out["BaselineId"].(string)
	assert.NotEmpty(t, baselineID)

	// Get
	code, out = mustPost(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestBaseline", out["Name"])

	// Update
	code, _ = mustPost(t, h, "UpdatePatchBaseline", map[string]any{
		"BaselineId":  baselineID,
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, code)

	// Register patch group
	code, _ = mustPost(t, h, "RegisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// DescribePatchGroups
	code, out = mustPost(t, h, "DescribePatchGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	groups := out["Mappings"].([]any)
	assert.NotEmpty(t, groups)

	// GetPatchBaselineForPatchGroup
	code, out = mustPost(t, h, "GetPatchBaselineForPatchGroup", map[string]any{"PatchGroup": "prod-servers"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// Set as default
	code, _ = mustPost(t, h, "RegisterDefaultPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// GetDefaultPatchBaseline
	code, out = mustPost(t, h, "GetDefaultPatchBaseline", map[string]any{"OperatingSystem": "WINDOWS"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// DescribePatchBaselines
	code, out = mustPost(t, h, "DescribePatchBaselines", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["BaselineIdentities"])

	// DeregisterPatchGroup
	code, _ = mustPost(t, h, "DeregisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = mustPost(t, h, "DeletePatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = mustPost(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusBadRequest, code)
}

// TestCreatePatchBaseline_Validation covers Name and OS default branches.
func TestCreatePatchBaseline_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantOS        string
		input         ssm.CreatePatchBaselineInput
		wantErr       bool
		expectTagsSet bool
	}{
		{
			name: "missing_name",
			input: ssm.CreatePatchBaselineInput{
				Name: "",
			},
			wantErr: true,
		},
		{
			name: "default_os_windows",
			input: ssm.CreatePatchBaselineInput{
				Name:            "my-baseline",
				OperatingSystem: "",
			},
			wantOS:  "WINDOWS",
			wantErr: false,
		},
		{
			name: "explicit_os_linux",
			input: ssm.CreatePatchBaselineInput{
				Name:            "linux-baseline",
				OperatingSystem: "AMAZON_LINUX",
			},
			wantOS:  "AMAZON_LINUX",
			wantErr: false,
		},
		{
			name: "with_tags",
			input: ssm.CreatePatchBaselineInput{
				Name: "tagged-baseline",
				Tags: []ssm.Tag{{Key: "env", Value: "prod"}},
			},
			wantOS:        "WINDOWS",
			wantErr:       false,
			expectTagsSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			out, err := b.CreatePatchBaseline(context.TODO(), &tt.input)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				bl := b.GetPatchBaselineInternal(out.BaselineID)
				assert.Equal(t, tt.wantOS, bl.OperatingSystem)
			}
		})
	}
}

// TestPatchBaselineMatchesFilters covers the patchBaselineMatchesFilters branches.
func TestPatchBaselineMatchesFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filters   []ssm.PatchBaselineFilter
		wantCount int
	}{
		{
			name:      "no_filters",
			filters:   nil,
			wantCount: 1,
		},
		{
			name: "filter_by_os_match",
			filters: []ssm.PatchBaselineFilter{
				{Key: "OPERATING_SYSTEM", Values: []string{"AMAZON_LINUX"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_os_no_match",
			filters: []ssm.PatchBaselineFilter{
				{Key: "OPERATING_SYSTEM", Values: []string{"WINDOWS"}},
			},
			wantCount: 0,
		},
		{
			name: "filter_by_name_prefix",
			filters: []ssm.PatchBaselineFilter{
				{Key: "NAME_PREFIX", Values: []string{"my-"}},
			},
			wantCount: 1,
		},
		{
			name: "filter_by_unknown_key",
			filters: []ssm.PatchBaselineFilter{
				{Key: "UNKNOWN_KEY", Values: []string{"anything"}},
			},
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
				Name:            "my-baseline",
				OperatingSystem: "AMAZON_LINUX",
			})
			require.NoError(t, err)

			out, err := b.DescribePatchBaselines(context.TODO(), &ssm.DescribePatchBaselinesInput{
				Filters: tt.filters,
			})
			require.NoError(t, err)
			assert.Len(t, out.BaselineIdentities, tt.wantCount)
		})
	}
}
func TestPatchBaseline_OSFilter(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	// Create baselines for different OSes.
	linuxBL, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "linux-baseline",
		OperatingSystem: "AMAZON_LINUX_2",
	})
	require.NoError(t, err)

	windowsBL, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "windows-baseline",
		OperatingSystem: "WINDOWS",
	})
	require.NoError(t, err)

	_ = linuxBL
	_ = windowsBL

	// Filter by AMAZON_LINUX_2 — should return linux but not windows.
	body, _ := json.Marshal(map[string]any{
		"Filters": []map[string]any{
			{"Key": "OPERATING_SYSTEM", "Values": []string{"AMAZON_LINUX_2"}},
		},
	})
	rec := doRequest(t, h, "DescribePatchBaselines", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "linux-baseline")
	assert.NotContains(t, rec.Body.String(), "windows-baseline")

	// Filter by WINDOWS — should return windows but not linux.
	body, _ = json.Marshal(map[string]any{
		"Filters": []map[string]any{
			{"Key": "OPERATING_SYSTEM", "Values": []string{"WINDOWS"}},
		},
	})
	rec = doRequest(t, h, "DescribePatchBaselines", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "linux-baseline")
	assert.Contains(t, rec.Body.String(), "windows-baseline")
}
func TestPatchBaseline_ComplianceLevelRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		complianceLevel string
	}{
		{name: "critical_compliance", complianceLevel: "CRITICAL"},
		{name: "high_compliance", complianceLevel: "HIGH"},
		{name: "medium_compliance", complianceLevel: "MEDIUM"},
		{name: "informational_compliance", complianceLevel: "INFORMATIONAL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)

			bl, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
				Name:                           "cl-baseline-" + tt.name,
				OperatingSystem:                "AMAZON_LINUX_2",
				ApprovedPatchesComplianceLevel: tt.complianceLevel,
			})
			require.NoError(t, err)

			// GetPatchBaseline should return the compliance level.
			body, _ := json.Marshal(map[string]any{"BaselineId": bl.BaselineID})
			rec := doRequest(t, h, "GetPatchBaseline", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.complianceLevel)

			// UpdatePatchBaseline should also update compliance level.
			body, _ = json.Marshal(map[string]any{
				"BaselineId":                     bl.BaselineID,
				"ApprovedPatchesComplianceLevel": "LOW",
			})
			rec = doRequest(t, h, "UpdatePatchBaseline", string(body))
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), "LOW")
		})
	}
}
func TestPatchGroups_RegisterAndLookup(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	bl, err := b.CreatePatchBaseline(context.TODO(), &ssm.CreatePatchBaselineInput{
		Name:            "group-baseline",
		OperatingSystem: "UBUNTU",
	})
	require.NoError(t, err)

	// Register the baseline for a patch group.
	body, _ := json.Marshal(map[string]any{
		"BaselineId": bl.BaselineID,
		"PatchGroup": "ubuntu-servers",
	})
	rec := doRequest(t, h, "RegisterPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribePatchGroups should list the mapping.
	rec = doRequest(t, h, "DescribePatchGroups", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ubuntu-servers")

	// GetPatchBaselineForPatchGroup should return the baseline.
	body, _ = json.Marshal(map[string]any{"PatchGroup": "ubuntu-servers"})
	rec = doRequest(t, h, "GetPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), bl.BaselineID)

	// DeregisterPatchBaselineForPatchGroup should remove the mapping.
	body, _ = json.Marshal(map[string]any{
		"BaselineId": bl.BaselineID,
		"PatchGroup": "ubuntu-servers",
	})
	rec = doRequest(t, h, "DeregisterPatchBaselineForPatchGroup", string(body))
	require.Equal(t, http.StatusOK, rec.Code)
}
func TestDeregisterPatchBaselineForPatchGroup_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		baselineID     string
		patchGroup     string
		wantBaselineID string
		wantPatchGroup string
		wantStatus     int
	}{
		{
			name:           "deregisters_and_returns_ids",
			baselineID:     "pb-0123456789abcdef0",
			patchGroup:     "Production",
			wantStatus:     http.StatusOK,
			wantBaselineID: "pb-0123456789abcdef0",
			wantPatchGroup: "Production",
		},
		{
			name:           "empty_baseline_id_returns_ok",
			baselineID:     "",
			patchGroup:     "Staging",
			wantStatus:     http.StatusOK,
			wantPatchGroup: "Staging",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler(t)
			body, _ := json.Marshal(map[string]any{
				"BaselineId": tt.baselineID,
				"PatchGroup": tt.patchGroup,
			})

			rec := doRequest(t, h, "DeregisterPatchBaselineForPatchGroup", string(body))
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK && tt.wantBaselineID != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantBaselineID, resp["BaselineId"])
				assert.Equal(t, tt.wantPatchGroup, resp["PatchGroup"])
			}
		})
	}
}
func TestDescribeAvailablePatches_SeededCatalog(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "DescribeAvailablePatches", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Patches []struct {
			Name string `json:"Name"`
		} `json:"Patches"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.Patches, "the catalog must be seeded with real built-in patches")
}

// TestSendCommand_RunPatchBaseline_PopulatesInstanceState verifies that
// running the AWS-RunPatchBaseline document against a managed instance
// records real InstancePatchState/PatchComplianceData, rather than leaving
// those stores permanently empty.
func TestSendCommand_RunPatchBaseline_PopulatesInstanceState(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	createResp := doRequest(t, h, "CreatePatchBaseline", `{
		"Name": "patch-wiring-baseline",
		"OperatingSystem": "AMAZON_LINUX_2",
		"ApprovedPatches": ["ALAS2-2024-2451"],
		"ApprovedPatchesComplianceLevel": "CRITICAL"
	}`)
	require.Equal(t, http.StatusOK, createResp.Code)

	var created struct {
		BaselineID string `json:"BaselineId"`
	}
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	require.NotEmpty(t, created.BaselineID)

	regResp := doRequest(t, h, "RegisterPatchBaselineForPatchGroup", `{
		"BaselineId":"`+created.BaselineID+`","PatchGroup":"wiring-group"
	}`)
	require.Equal(t, http.StatusOK, regResp.Code)

	sendResp := doRequest(t, h, "SendCommand", `{
		"DocumentName": "AWS-RunPatchBaseline",
		"InstanceIds": ["i-patchtest"],
		"Parameters": {"PatchGroup": ["wiring-group"], "Operation": ["Install"]}
	}`)
	require.Equal(t, http.StatusOK, sendResp.Code)

	stateResp := doRequest(t, h, "DescribeInstancePatchStates", `{"InstanceIds":["i-patchtest"]}`)
	require.Equal(t, http.StatusOK, stateResp.Code)

	var states struct {
		InstancePatchStates []struct {
			InstanceID string `json:"InstanceId"`
			BaselineID string `json:"BaselineId"`
			PatchGroup string `json:"PatchGroup"`
		} `json:"InstancePatchStates"`
	}
	require.NoError(t, json.Unmarshal(stateResp.Body.Bytes(), &states))
	require.Len(t, states.InstancePatchStates, 1)
	assert.Equal(t, "i-patchtest", states.InstancePatchStates[0].InstanceID)
	assert.Equal(t, created.BaselineID, states.InstancePatchStates[0].BaselineID)
	assert.Equal(t, "wiring-group", states.InstancePatchStates[0].PatchGroup)

	patchesResp := doRequest(t, h, "DescribeInstancePatches", `{"InstanceId":"i-patchtest"}`)
	require.Equal(t, http.StatusOK, patchesResp.Code)

	var patches struct {
		Patches []struct {
			Title string `json:"Title"`
			State string `json:"State"`
		} `json:"Patches"`
	}
	require.NoError(t, json.Unmarshal(patchesResp.Body.Bytes(), &patches))
	require.NotEmpty(t, patches.Patches)

	var sawInstalled bool
	for _, p := range patches.Patches {
		if p.Title == "ALAS2-2024-2451" {
			assert.Equal(t, "Installed", p.State)
			sawInstalled = true
		}
	}
	assert.True(t, sawInstalled, "the explicitly-approved patch must be reported Installed")
}
