package eks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestEKS_PodIdentityAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pid-cluster"})

	// List pod identity associations (empty)
	rec := doREST(t, h, http.MethodGet, "/clusters/pid-cluster/pod-identity-associations", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/pod-identity-associations", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Create association via backend
	assoc, err := h.Backend.CreatePodIdentityAssociation(
		"pid-cluster", "default", "my-sa", "arn:aws:iam::123:role/sa-role", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)

	assocID := assoc.AssociationID

	// Describe via handler
	rec = doREST(
		t,
		h,
		http.MethodGet,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe nonexistent
	rec = doREST(
		t,
		h,
		http.MethodGet,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Update via handler (POST, not PUT -- verified against the SDK serializer)
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		map[string]any{
			"roleArn": "arn:aws:iam::123:role/new-role",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Update nonexistent
	rec = doREST(
		t,
		h,
		http.MethodPost,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		map[string]any{
			"roleArn": "arn:aws:iam::123:role/new-role",
		},
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete via handler
	rec = doREST(
		t,
		h,
		http.MethodDelete,
		"/clusters/pid-cluster/pod-identity-associations/"+assocID,
		nil,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete nonexistent
	rec = doREST(
		t,
		h,
		http.MethodDelete,
		"/clusters/pid-cluster/pod-identity-associations/nonexistent",
		nil,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestEKS_CreatePodIdentityAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_pod_identity_association_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"namespace":      "default",
				"serviceAccount": "my-sa",
				"roleArn":        "arn:aws:iam::123456789012:role/pod-role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_pod_identity_missing_namespace",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"serviceAccount": "my-sa", "roleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_pod_identity_missing_service_account",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"namespace": "default", "roleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_pod_identity_cluster_not_found",
			body:       map[string]any{"namespace": "default", "serviceAccount": "sa"},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/pod-identity-associations", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				assoc, ok := resp["association"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", assoc["clusterName"])
				assert.Equal(t, "default", assoc["namespace"])
				assert.Equal(t, "my-sa", assoc["serviceAccount"])
				assert.NotEmpty(t, assoc["associationId"])
				assert.NotEmpty(t, assoc["associationArn"])
			}
		})
	}
}

func TestPodIdentity_CreateDescribeUpdate_Delete(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "pi-lifecycle-cluster")

	assoc, err := b.CreatePodIdentityAssociation(
		"pi-lifecycle-cluster", "kube-system", "aws-node",
		"arn:aws:iam::123:role/pi-role", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.AssociationID)
	assert.Equal(t, "kube-system", assoc.Namespace)
	assert.Equal(t, "aws-node", assoc.ServiceAccount)

	described, err := b.DescribePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, described.AssociationID)

	updated, err := b.UpdatePodIdentityAssociation(
		"pi-lifecycle-cluster", assoc.AssociationID,
		eks.PodIdentityAssociationUpdate{RoleARN: "arn:aws:iam::123:role/pi-role-v2"},
	)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123:role/pi-role-v2", updated.RoleARN)

	deleted, err := b.DeletePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	require.NoError(t, err)
	assert.Equal(t, assoc.AssociationID, deleted.AssociationID)

	_, err = b.DescribePodIdentityAssociation("pi-lifecycle-cluster", assoc.AssociationID)
	assert.Error(t, err, "describing deleted association must return error")
}

func TestPodIdentity_List(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "pi-list-cluster")

	_, _ = b.CreatePodIdentityAssociation(
		"pi-list-cluster", "ns1", "sa1", "arn:aws:iam::123:role/r1", nil, eks.PodIdentityAssociationInput{},
	)
	_, _ = b.CreatePodIdentityAssociation(
		"pi-list-cluster", "ns2", "sa2", "arn:aws:iam::123:role/r2", nil, eks.PodIdentityAssociationInput{},
	)

	assocs, err := b.ListPodIdentityAssociations("pi-list-cluster")
	require.NoError(t, err)
	assert.Len(t, assocs, 2)
}

func TestPodIdentity_AssocID_IsUUID(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pi-uuid-cluster"})

	rec := doREST(t, h, http.MethodPost, "/clusters/pi-uuid-cluster/pod-identity-associations", map[string]any{
		"namespace":      "default",
		"serviceAccount": "my-sa",
		"roleArn":        "arn:aws:iam::123456789012:role/my-pod-role",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	assoc := parseResp(t, rec)["association"].(map[string]any)
	assocID, _ := assoc["associationId"].(string)
	require.NotEmpty(t, assocID)
	assert.Len(t, assocID, 36, "associationId should be a UUID (36 chars with hyphens)")
}

func TestPodIdentity_DifferentAssocIDs_SameNamespaceSA(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	a1, err := b.CreatePodIdentityAssociation(
		"c1", "default", "my-sa", "arn:aws:iam::123:role/r1", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)

	a2, err := b.CreatePodIdentityAssociation(
		"c1", "default", "my-sa", "arn:aws:iam::123:role/r2", nil, eks.PodIdentityAssociationInput{},
	)
	require.NoError(t, err)

	assert.NotEqual(t, a1.AssociationID, a2.AssociationID,
		"each call must produce a unique associationId")
}

// TestPodIdentity_ListReturnsSummaryShape verifies that
// ListPodIdentityAssociations returns the PodIdentityAssociationSummary
// shape (no roleArn/createdAt/tags), distinct from the full
// PodIdentityAssociation shape returned by Describe/Create/Update -- verified
// against aws-sdk-go-v2/service/eks/types.PodIdentityAssociationSummary.
func TestPodIdentity_ListReturnsSummaryShape(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pi-summary-cluster"})

	createRec := doREST(t, h, http.MethodPost, "/clusters/pi-summary-cluster/pod-identity-associations",
		map[string]any{
			"namespace":      "default",
			"serviceAccount": "my-sa",
			"roleArn":        "arn:aws:iam::123456789012:role/pod-role",
		})
	require.Equal(t, http.StatusOK, createRec.Code)

	created := parseResp(t, createRec)["association"].(map[string]any)
	assert.Contains(t, created, "roleArn", "full shape must include roleArn")
	assert.Contains(t, created, "createdAt", "full shape must include createdAt")

	listRec := doREST(t, h, http.MethodGet, "/clusters/pi-summary-cluster/pod-identity-associations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := parseResp(t, listRec)
	assocs, ok := listResp["associations"].([]any)
	require.True(t, ok)
	require.Len(t, assocs, 1)

	summary, ok := assocs[0].(map[string]any)
	require.True(t, ok)
	assert.NotContains(t, summary, "roleArn", "summary shape must NOT include roleArn")
	assert.NotContains(t, summary, "createdAt", "summary shape must NOT include createdAt")
	assert.NotContains(t, summary, "tags", "summary shape must NOT include tags")
	assert.Equal(t, created["associationId"], summary["associationId"])
}

// TestPodIdentity_CreateAndUpdateOptionalFields exercises Policy and
// DisableSessionTags on Create/Update -- both real
// CreatePodIdentityAssociationInput and UpdatePodIdentityAssociationInput
// fields that were previously unmodeled.
func TestPodIdentity_CreateAndUpdateOptionalFields(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "pi-opt-cluster"})

	createRec := doREST(t, h, http.MethodPost, "/clusters/pi-opt-cluster/pod-identity-associations",
		map[string]any{
			"namespace":          "default",
			"serviceAccount":     "my-sa",
			"roleArn":            "arn:aws:iam::123456789012:role/pod-role",
			"disableSessionTags": true,
			"policy":             `{"Version":"2012-10-17"}`,
		})
	require.Equal(t, http.StatusOK, createRec.Code)

	created := parseResp(t, createRec)["association"].(map[string]any)
	assert.Equal(t, true, created["disableSessionTags"])
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, created["policy"].(string))

	assocID := created["associationId"].(string)

	updateRec := doREST(t, h, http.MethodPost,
		"/clusters/pi-opt-cluster/pod-identity-associations/"+assocID,
		map[string]any{"disableSessionTags": false})
	require.Equal(t, http.StatusOK, updateRec.Code)

	updated := parseResp(t, updateRec)["association"].(map[string]any)
	assert.Equal(t, false, updated["disableSessionTags"])
	// Policy was omitted from the update body (nil pointer) -> unchanged.
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, updated["policy"].(string))
}
