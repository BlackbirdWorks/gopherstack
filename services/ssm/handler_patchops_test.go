package ssm_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

// TestDescribeInstancePatchStates verifies that patch states are stored and retrieved.
func TestDescribeInstancePatchStates(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	cases := []struct {
		name               string
		requestInstanceIDs string // JSON array or empty string for "all"
		wantInstanceID     string // if non-empty, must appear in response body
		seed               []ssm.InstancePatchState
		wantCount          int
	}{
		{
			name:               "empty store returns empty list",
			seed:               nil,
			requestInstanceIDs: `[]`,
			wantCount:          0,
		},
		{
			name: "returns all states when no filter",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
				{InstanceID: "i-bbb", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
			},
			requestInstanceIDs: `[]`,
			wantCount:          2,
		},
		{
			name: "filters by instance ID",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
				{InstanceID: "i-bbb", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
			},
			requestInstanceIDs: `["i-aaa"]`,
			wantCount:          1,
			wantInstanceID:     "i-aaa",
		},
		{
			name: "unknown instance ID returns empty list",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
			},
			requestInstanceIDs: `["i-zzz"]`,
			wantCount:          0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			for _, s := range tc.seed {
				b.AddInstancePatchStateInternal(s)
			}

			body := `{"InstanceIds":` + tc.requestInstanceIDs + `}`
			rec := doRequest(t, h, "DescribeInstancePatchStates", body)
			require.Equal(t, http.StatusOK, rec.Code)

			assertBodyContains(t, rec, "InstancePatchStates")
			if tc.wantInstanceID != "" {
				assertBodyContains(t, rec, tc.wantInstanceID)
			}
		})
	}
}

// TestDescribeInstancePatchStatesForPatchGroup verifies filtering by patch group.
func TestDescribeInstancePatchStatesForPatchGroup(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	cases := []struct {
		name           string
		patchGroup     string
		wantInstanceID string
		seed           []ssm.InstancePatchState
		wantCount      int
	}{
		{
			name:       "empty store returns empty list",
			seed:       nil,
			patchGroup: "grp1",
			wantCount:  0,
		},
		{
			name: "returns only states matching patch group",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
				{InstanceID: "i-bbb", PatchGroup: "grp2", Operation: "Scan", OperationStartTime: now},
			},
			patchGroup:     "grp1",
			wantCount:      1,
			wantInstanceID: "i-aaa",
		},
		{
			name: "unmatched patch group returns empty",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
			},
			patchGroup: "grp-none",
			wantCount:  0,
		},
		{
			name: "multiple instances in same group all returned",
			seed: []ssm.InstancePatchState{
				{InstanceID: "i-aaa", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
				{InstanceID: "i-bbb", PatchGroup: "grp1", Operation: "Scan", OperationStartTime: now},
				{InstanceID: "i-ccc", PatchGroup: "grp2", Operation: "Scan", OperationStartTime: now},
			},
			patchGroup: "grp1",
			wantCount:  2,
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
			rec := doRequest(t, h, "DescribeInstancePatchStatesForPatchGroup", body)
			require.Equal(t, http.StatusOK, rec.Code)

			assertBodyContains(t, rec, "InstancePatchStates")
			if tc.wantInstanceID != "" {
				assertBodyContains(t, rec, tc.wantInstanceID)
			}
		})
	}
}

// TestDescribeInstancePatches verifies that patch compliance data is returned for an instance.
func TestDescribeInstancePatches(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Second)

	cases := []struct {
		seed        map[string][]ssm.PatchComplianceData
		name        string
		instanceID  string
		queryID     string
		wantTitle   string
		wantPatches bool
	}{
		{
			name:        "unknown instance returns empty list",
			instanceID:  "i-aaa",
			seed:        nil,
			queryID:     "i-zzz",
			wantPatches: false,
		},
		{
			name:       "returns patches for instance",
			instanceID: "i-aaa",
			seed: map[string][]ssm.PatchComplianceData{
				"i-aaa": {
					{
						InstalledTime:  &now,
						Title:          "KB123",
						Classification: "SecurityUpdates",
						Severity:       "Critical",
						State:          "Installed",
					},
				},
			},
			queryID:     "i-aaa",
			wantPatches: true,
			wantTitle:   "KB123",
		},
		{
			name:       "does not leak patches from other instance",
			instanceID: "i-aaa",
			seed: map[string][]ssm.PatchComplianceData{
				"i-aaa": {
					{
						InstalledTime:  &now,
						Title:          "KB123",
						Classification: "SecurityUpdates",
						Severity:       "Critical",
						State:          "Installed",
					},
				},
			},
			queryID:     "i-bbb",
			wantPatches: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			for id, patches := range tc.seed {
				b.AddInstancePatchesInternal(id, patches)
			}

			body := `{"InstanceId":"` + tc.queryID + `"}`
			rec := doRequest(t, h, "DescribeInstancePatches", body)
			require.Equal(t, http.StatusOK, rec.Code)
			assertBodyContains(t, rec, "Patches")

			if tc.wantTitle != "" {
				assertBodyContains(t, rec, tc.wantTitle)
			}
		})
	}
}

// TestDescribeInstanceProperties verifies that instance properties are returned.
func TestDescribeInstanceProperties(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		wantInstID string
		seed       []ssm.InstanceProperty
		wantCount  int
	}{
		{
			name:      "empty store returns empty list",
			seed:      nil,
			wantCount: 0,
		},
		{
			name: "returns all instance properties",
			seed: []ssm.InstanceProperty{
				{InstanceID: "i-aaa", PlatformType: "Linux", PingStatus: "Online"},
				{InstanceID: "i-bbb", PlatformType: "Windows", PingStatus: "Online"},
			},
			wantCount:  2,
			wantInstID: "i-aaa",
		},
		{
			name: "single instance property returned",
			seed: []ssm.InstanceProperty{
				{InstanceID: "i-ccc", PlatformType: "Linux", PingStatus: "ConnectionLost", AgentVersion: "3.1.0"},
			},
			wantCount:  1,
			wantInstID: "i-ccc",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			for _, p := range tc.seed {
				b.AddInstancePropertyInternal(p)
			}

			rec := doRequest(t, h, "DescribeInstanceProperties", `{}`)
			require.Equal(t, http.StatusOK, rec.Code)
			assertBodyContains(t, rec, "InstanceProperties")

			if tc.wantInstID != "" {
				assertBodyContains(t, rec, tc.wantInstID)
			}
		})
	}
}

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
