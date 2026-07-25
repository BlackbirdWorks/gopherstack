package ecr_test

// registry_policy_test.go — verifies registry_policy.go: Put/Get/Delete
// RegistryPolicy at the registry (account) scope.

import (
	"context"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestRegistryPolicy_CRUD(t *testing.T) {
	t.Parallel()

	b := newBackend(t)

	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::111122223333:root"},"Action":["ecr:CreateRepository","ecr:ReplicateImage"],"Resource":"*"}]}` //nolint:lll // JSON policy exceeds 120 chars; splitting worsens readability

	out, err := b.PutRegistryPolicy(context.Background(), policy)
	require.NoError(t, err)
	assert.Equal(t, policy, out.PolicyText)

	got, err := b.GetRegistryPolicy(context.Background())
	require.NoError(t, err)
	assert.Equal(t, policy, got.PolicyText)

	_, err = b.DeleteRegistryPolicy(context.Background())
	require.NoError(t, err)

	_, err = b.GetRegistryPolicy(context.Background())
	assert.ErrorIs(t, err, ecr.ErrRegistryPolicyNotFound)
}

func TestPutRegistryPolicy_CreatesThenReplaces(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		policy      string
		wantStatus  int
		wantStatus2 int
	}{
		{
			name:        "creates_then_replaces",
			policy:      `{"Version":"2012-10-17","Statement":[]}`,
			wantStatus:  http.StatusOK,
			wantStatus2: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			rec1 := doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})
			assert.Equal(t, tt.wantStatus, rec1.Code)

			rec2 := doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})
			assert.Equal(t, tt.wantStatus2, rec2.Code)
		})
	}
}

func TestDeleteRegistryPolicy_AfterPut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policy     string
		wantStatus int
	}{
		{
			name:       "delete_after_put",
			policy:     `{"Version":"2012-10-17","Statement":[]}`,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyHandler()

			doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": tt.policy})

			rec := doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestECR_DeleteRegistryPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*ecr.InMemoryBackend)
		name       string
		wantStatus int
	}{
		{
			name:       "no registry policy returns not found",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "deletes existing registry policy",
			setup: func(b *ecr.InMemoryBackend) {
				b.SetRegistryPolicyInternal(`{"Version":"2012-10-17","Statement":[]}`)
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
			if tt.setup != nil {
				tt.setup(backend)
			}

			h := ecr.NewHandler(backend, nil)

			rec := doECRRequest(t, h, "DeleteRegistryPolicy", map[string]any{})
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				out := parseAccuracy(t, rec)
				assert.NotEmpty(t, out["policyText"])
				assert.Equal(t, testAccountID, out["registryId"])
			}
		})
	}
}

func TestRegistryPolicy_PutGet_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	policy := `{"Version":"2012-10-17","Statement":[{"Sid":"AllowCrossAccount",` +
		`"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},` +
		`"Action":"ecr:CreateRepository"}]}`

	doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": policy})

	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	out := parseAccuracy(t, rec)
	assert.Equal(t, policy, out["policyText"])
	assert.Equal(t, "123456789012", out["registryId"])
}

func TestRegistryPolicy_Delete_Then_Get_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	doAccuracy(t, h, "PutRegistryPolicy", map[string]any{
		"policyText": `{"Version":"2012-10-17","Statement":[]}`,
	})

	doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})

	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestRegistryPolicy_Get_NoPolicy_Returns404(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, rec.Code,
		"GetRegistryPolicy on fresh backend must return 404")
}

func TestRegistryPolicy_Put_Get_Delete(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	policy := `{"Version":"2012-10-17","Statement":[]}`

	putRec := doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": policy})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)
	out := parseAccuracy(t, getRec)
	assert.Equal(t, policy, out["policyText"])

	delRec := doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, delRec.Code)

	getRec2 := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	assert.Equal(t, http.StatusNotFound, getRec2.Code,
		"GetRegistryPolicy must return 404 after policy is deleted")
}

func TestDescribeRegistry_AccountID(t *testing.T) {
	t.Parallel()

	b := ecr.NewInMemoryBackend("555555555555", "eu-west-1", "")
	h := ecr.NewHandler(b, nil)

	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "555555555555", out["registryId"])
}

func TestDescribeRegistry_InitialReplicationConfig_Empty(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	repCfg, _ := out["replicationConfiguration"].(map[string]any)
	rules, _ := repCfg["rules"].([]any)
	assert.Empty(t, rules, "fresh registry must have no replication rules")
}

func TestDescribeRegistry_ReturnsRegistryID(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	rec := doAccuracy(t, h, "DescribeRegistry", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	out := parseAccuracy(t, rec)
	assert.Equal(t, "123456789012", out["registryId"],
		"DescribeRegistry must return the configured account ID as registryId")
}

// TestRegistryPolicy_OmitsInventedStatusField locks the Put/Get/DeleteRegistryPolicy
// wire shape against the real AWS SetRegistryPolicyOutput/GetRegistryPolicyOutput/
// DeleteRegistryPolicyOutput, which carry only policyText and registryId — no
// "status" field. gopherstack previously fabricated a status string
// ("DELETED"/"ACTIVE"/"SetComplete") that has no counterpart in the real API.
func TestRegistryPolicy_OmitsInventedStatusField(t *testing.T) {
	t.Parallel()

	h := newAccuracyHandler()
	policy := `{"Version":"2012-10-17","Statement":[]}`

	putRec := doAccuracy(t, h, "PutRegistryPolicy", map[string]any{"policyText": policy})
	require.Equal(t, http.StatusOK, putRec.Code)
	putOut := parseAccuracy(t, putRec)
	_, present := putOut["status"]
	assert.False(t, present, "PutRegistryPolicy must not carry an invented status field")

	getRec := doAccuracy(t, h, "GetRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)
	getOut := parseAccuracy(t, getRec)
	_, present = getOut["status"]
	assert.False(t, present, "GetRegistryPolicy must not carry an invented status field")

	deleteRec := doAccuracy(t, h, "DeleteRegistryPolicy", map[string]any{})
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteOut := parseAccuracy(t, deleteRec)
	_, present = deleteOut["status"]
	assert.False(t, present, "DeleteRegistryPolicy must not carry an invented status field")
}
