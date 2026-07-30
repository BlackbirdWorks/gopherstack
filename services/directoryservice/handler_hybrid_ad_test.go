package directoryservice_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/directoryservice"
)

// mustStartAssessment starts (and asserts success of) an AD assessment for
// dirID, returning its AssessmentId -- the value real CreateHybridAD requires.
func mustStartAssessment(t *testing.T, h *directoryservice.Handler, dirID string) string {
	t.Helper()

	rec := doRequest(t, h, "StartADAssessment", map[string]any{"DirectoryId": dirID})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	assessmentID, _ := out["AssessmentId"].(string)
	require.NotEmpty(t, assessmentID)

	return assessmentID
}

func TestHybridAD_CreateUpdateDescribeCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	dirID := mustCreateSimpleAD(t, h, "corp.example.com")
	assessmentID := mustStartAssessment(t, h, dirID)

	// CreateHybridAD: real CreateHybridADInput is {AssessmentId, SecretArn,
	// Tags}; real CreateHybridADOutput is just {DirectoryId} -- no RequestId.
	createRec := doRequest(t, h, "CreateHybridAD", map[string]any{
		"AssessmentId": assessmentID,
		"SecretArn":    "arn:aws:secretsmanager:us-east-1:000000000000:secret:hybrid-admin",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
	hybridDirID, _ := createOut["DirectoryId"].(string)
	require.NotEmpty(t, hybridDirID)
	assert.NotContains(t, createOut, "RequestId", "CreateHybridADOutput has no RequestId member")

	// UpdateHybridAD: real UpdateHybridADInput/Output.
	updateRec := doRequest(t, h, "UpdateHybridAD", map[string]any{
		"DirectoryId": hybridDirID,
		"SelfManagedInstancesSettings": map[string]any{
			"CustomerDnsIps": []string{"10.24.34.100"},
			"InstanceIds":    []string{"i-10243410"},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateOut))
	updateAssessmentID, _ := updateOut["AssessmentId"].(string)
	assert.NotEmpty(t, updateAssessmentID)
	assert.Equal(t, hybridDirID, updateOut["DirectoryId"])
	assert.NotContains(t, updateOut, "RequestId", "UpdateHybridADOutput has no RequestId member")

	// DescribeHybridADUpdate: real output is UpdateActivities{
	// HybridAdministratorAccount, SelfManagedInstances}, each a list of
	// HybridUpdateInfoEntry.
	descRec := doRequest(t, h, "DescribeHybridADUpdate", map[string]any{"DirectoryId": hybridDirID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
	activities, ok := descOut["UpdateActivities"].(map[string]any)
	require.True(t, ok, "UpdateActivities must be an object")

	selfManaged, ok := activities["SelfManagedInstances"].([]any)
	require.True(t, ok)
	require.Len(t, selfManaged, 1)

	entry, ok := selfManaged[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, updateAssessmentID, entry["AssessmentId"])
	assert.Equal(t, "Customer", entry["InitiatedBy"])
	assert.Equal(t, "Updated", entry["Status"])
	assert.NotContains(t, entry, "RequestId", "HybridUpdateInfoEntry has no RequestId member")

	newValue, ok := entry["NewValue"].(map[string]any)
	require.True(t, ok, "NewValue must round-trip the submitted SelfManagedInstancesSettings")
	dnsIPs, _ := newValue["DnsIps"].([]any)
	assert.Equal(t, []any{"10.24.34.100"}, dnsIPs)

	adminAccount, ok := activities["HybridAdministratorAccount"].([]any)
	require.True(t, ok)
	assert.Empty(t, adminAccount, "no HybridAdministratorAccountUpdate was submitted")

	// DescribeDirectories: HybridSettings must now reflect the update.
	describeDirRec := doRequest(t, h, "DescribeDirectories", map[string]any{"DirectoryIds": []string{hybridDirID}})
	require.Equal(t, http.StatusOK, describeDirRec.Code)

	var dirOut map[string]any
	require.NoError(t, json.Unmarshal(describeDirRec.Body.Bytes(), &dirOut))
	dirs, ok := dirOut["DirectoryDescriptions"].([]any)
	require.True(t, ok)
	require.Len(t, dirs, 1)

	dir, ok := dirs[0].(map[string]any)
	require.True(t, ok)
	hybridSettings, ok := dir["HybridSettings"].(map[string]any)
	require.True(t, ok, "HybridSettings must be populated once UpdateHybridAD has set self-managed instance data")
	dnsAddrs, _ := hybridSettings["SelfManagedDnsIpAddrs"].([]any)
	assert.Equal(t, []any{"10.24.34.100"}, dnsAddrs)
	instanceIDs, _ := hybridSettings["SelfManagedInstanceIds"].([]any)
	assert.Equal(t, []any{"i-10243410"}, instanceIDs)
}

func TestHybridAD_CreateHybridAD_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildInput func(t *testing.T, h *directoryservice.Handler) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing AssessmentId rejected",
			buildInput: func(t *testing.T, _ *directoryservice.Handler) map[string]any {
				t.Helper()

				return map[string]any{"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:x"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing SecretArn rejected",
			buildInput: func(t *testing.T, h *directoryservice.Handler) map[string]any {
				t.Helper()

				dirID := mustCreateSimpleAD(t, h, "corp2.example.com")
				assessmentID := mustStartAssessment(t, h, dirID)

				return map[string]any{"AssessmentId": assessmentID}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "unknown AssessmentId rejected",
			buildInput: func(t *testing.T, _ *directoryservice.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"AssessmentId": "a-doesnotexist",
					"SecretArn":    "arn:aws:secretsmanager:us-east-1:000000000000:secret:x",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateHybridAD", tt.buildInput(t, h))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHybridAD_UpdateHybridAD_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildInput func(t *testing.T, h *directoryservice.Handler) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing DirectoryId rejected",
			buildInput: func(t *testing.T, _ *directoryservice.Handler) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "non-hybrid directory rejected",
			buildInput: func(t *testing.T, h *directoryservice.Handler) map[string]any {
				t.Helper()

				dirID := mustCreateSimpleAD(t, h, "notreallyhybrid.example.com")

				return map[string]any{
					"DirectoryId": dirID,
					"HybridAdministratorAccountUpdate": map[string]any{
						"SecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret:x",
					},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "neither update group provided rejected",
			buildInput: func(t *testing.T, h *directoryservice.Handler) map[string]any {
				t.Helper()

				dirID := mustCreateSimpleAD(t, h, "corp3.example.com")
				assessmentID := mustStartAssessment(t, h, dirID)
				createRec := doRequest(t, h, "CreateHybridAD", map[string]any{
					"AssessmentId": assessmentID,
					"SecretArn":    "arn:aws:secretsmanager:us-east-1:000000000000:secret:x",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				var createOut map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
				hybridDirID, _ := createOut["DirectoryId"].(string)

				return map[string]any{"DirectoryId": hybridDirID}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "UpdateHybridAD", tt.buildInput(t, h))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
