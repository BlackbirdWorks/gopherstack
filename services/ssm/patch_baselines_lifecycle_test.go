package ssm_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestFull_PatchBaseline_FullLifecycle(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// Create
	code, out := postJSON(t, h, "CreatePatchBaseline", map[string]any{
		"Name":            "TestBaseline",
		"OperatingSystem": "WINDOWS",
		"Description":     "test patch baseline",
	})
	assert.Equal(t, http.StatusOK, code)
	baselineID := out["BaselineId"].(string)
	assert.NotEmpty(t, baselineID)

	// Get
	code, out = postJSON(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "TestBaseline", out["Name"])

	// Update
	code, _ = postJSON(t, h, "UpdatePatchBaseline", map[string]any{
		"BaselineId":  baselineID,
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, code)

	// Register patch group
	code, _ = postJSON(t, h, "RegisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// DescribePatchGroups
	code, out = postJSON(t, h, "DescribePatchGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	groups := out["Mappings"].([]any)
	assert.NotEmpty(t, groups)

	// GetPatchBaselineForPatchGroup
	code, out = postJSON(t, h, "GetPatchBaselineForPatchGroup", map[string]any{"PatchGroup": "prod-servers"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// Set as default
	code, _ = postJSON(t, h, "RegisterDefaultPatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// GetDefaultPatchBaseline
	code, out = postJSON(t, h, "GetDefaultPatchBaseline", map[string]any{"OperatingSystem": "WINDOWS"})
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, baselineID, out["BaselineId"])

	// DescribePatchBaselines
	code, out = postJSON(t, h, "DescribePatchBaselines", map[string]any{})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, out["BaselineIdentities"])

	// DeregisterPatchGroup
	code, _ = postJSON(t, h, "DeregisterPatchBaselineForPatchGroup", map[string]any{
		"BaselineId": baselineID,
		"PatchGroup": "prod-servers",
	})
	assert.Equal(t, http.StatusOK, code)

	// Delete
	code, _ = postJSON(t, h, "DeletePatchBaseline", map[string]any{"BaselineId": baselineID})
	assert.Equal(t, http.StatusOK, code)

	// Gone
	code, _ = postJSON(t, h, "GetPatchBaseline", map[string]any{"BaselineId": baselineID})
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
