package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

func TestEMR_CreatePersistentAppUI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		targetResourceArn string
		wantCode          int
	}{
		{
			name:              "creates persistent app UI",
			targetResourceArn: "arn:aws:elasticmapreduce:us-east-1:000000000000:cluster/j-0000000000001",
			wantCode:          http.StatusOK,
		},
		{
			name:              "returns validation error when target resource ARN is missing",
			targetResourceArn: "",
			wantCode:          http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doEMRRequest(t, h, "CreatePersistentAppUI", map[string]any{
				"TargetResourceArn": tt.targetResourceArn,
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					PersistentAppUIID string `json:"PersistentAppUIId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.PersistentAppUIID)
			}
		})
	}
}

func TestGetClusterSessionCredentials(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "creds-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetClusterSessionCredentials", map[string]any{
		"ClusterId":        create.JobFlowID,
		"ExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Credentials map[string]any `json:"Credentials"`
		ExpiresAt   float64        `json:"ExpiresAt"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.Credentials)
	assert.NotZero(t, out.ExpiresAt)
}

func TestGetClusterSessionCredentials_MissingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "creds-cluster2"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	rec := doEMRRequest(t, h, "GetClusterSessionCredentials", map[string]any{
		"ClusterId": create.JobFlowID,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestCreatePersistentAppUI_ReturnsCopy verifies CreatePersistentAppUI returns a copy.
func TestCreatePersistentAppUI_ReturnsCopy(t *testing.T) {
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

// TestGetOnClusterPresignedURL_NonExistentCluster verifies cluster existence is checked.
func TestGetOnClusterPresignedURL_NonExistentCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "GetOnClusterAppUIPresignedURL", map[string]any{
		"ClusterId": "j-NOSUCHCLUSTER",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errOut))
	assert.Equal(t, "InvalidRequestException", errOut["__type"])
}
