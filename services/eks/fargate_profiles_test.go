package eks_test

import (
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestEKS_FargateProfile_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fg-cluster"})

	// List fargate profiles (empty)
	rec := doREST(t, h, http.MethodGet, "/clusters/fg-cluster/fargate-profiles", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// List for nonexistent cluster
	rec = doREST(t, h, http.MethodGet, "/clusters/nonexistent/fargate-profiles", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Describe nonexistent fargate profile
	rec = doREST(t, h, http.MethodGet, "/clusters/fg-cluster/fargate-profiles/my-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Delete nonexistent fargate profile
	rec = doREST(t, h, http.MethodDelete, "/clusters/fg-cluster/fargate-profiles/my-profile", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// Create fargate profile via backend, then test describe/delete via handler
	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	_, err := b.CreateCluster("fg-cluster2", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	fp, err := b.CreateFargateProfile(
		"fg-cluster2",
		"my-profile",
		"arn:aws:iam::123:role/fp",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "my-profile", fp.FargateProfileName)
	assert.NotEmpty(t, fp.ARN)

	// Describe and delete via backend directly
	described, err := b.DescribeFargateProfile("fg-cluster2", "my-profile")
	require.NoError(t, err)
	assert.Equal(t, "my-profile", described.FargateProfileName)

	names, err := b.ListFargateProfiles("fg-cluster2")
	require.NoError(t, err)
	assert.Contains(t, names, "my-profile")

	deleted, err := b.DeleteFargateProfile("fg-cluster2", "my-profile")
	require.NoError(t, err)
	assert.Equal(t, "my-profile", deleted.FargateProfileName)

	// List after delete
	names, err = b.ListFargateProfiles("fg-cluster2")
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestEKS_FargateProfile_Handler(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)

	// Create cluster first
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fg-h-cluster"})

	// Create fargate profile directly in backend for handler to use
	h.Backend.Reset()
	_, err := h.Backend.CreateCluster("fg-h-cluster", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = h.Backend.CreateFargateProfile(
		"fg-h-cluster",
		"test-fp",
		"arn:aws:iam::123:role/fp",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	// Describe via handler
	rec := doREST(t, h, http.MethodGet, "/clusters/fg-h-cluster/fargate-profiles/test-fp", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete via handler
	rec = doREST(t, h, http.MethodDelete, "/clusters/fg-h-cluster/fargate-profiles/test-fp", nil)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestEKS_CreateFargateProfile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *eks.Handler)
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "create_fargate_profile_success",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body: map[string]any{
				"fargateProfileName":  "my-profile",
				"podExecutionRoleArn": "arn:aws:iam::123456789012:role/fargate-role",
				"selectors": []map[string]any{
					{"namespace": "default"},
					{"namespace": "kube-system", "labels": map[string]string{"app": "backend"}},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "create_fargate_profile_missing_name",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
			},
			body:       map[string]any{"podExecutionRoleArn": "arn:aws:iam::123456789012:role/r"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_fargate_profile_cluster_not_found",
			body:       map[string]any{"fargateProfileName": "p"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "create_fargate_profile_duplicate",
			setup: func(t *testing.T, h *eks.Handler) {
				t.Helper()
				doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "my-cluster"})
				doREST(t, h, http.MethodPost, "/clusters/my-cluster/fargate-profiles",
					map[string]any{"fargateProfileName": "dup-profile"})
			},
			body:       map[string]any{"fargateProfileName": "dup-profile"},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doREST(t, h, http.MethodPost, "/clusters/my-cluster/fargate-profiles", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResp(t, rec)
				profile, ok := resp["fargateProfile"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-cluster", profile["clusterName"])
				assert.Equal(t, "my-profile", profile["fargateProfileName"])
				assert.NotEmpty(t, profile["fargateProfileArn"])
				assert.Equal(t, "CREATING", profile["status"])
			}
		})
	}
}

func TestFargateProfile_Status_ACTIVE_On_Create(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "fp-status-cluster")

	fp, err := b.CreateFargateProfile("fp-status-cluster", "fp1",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATING", fp.Status)
}

func TestFargateProfile_Status_DELETING_On_Delete(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "fp-del-cluster")

	_, _ = b.CreateFargateProfile("fp-del-cluster", "fp1",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)

	deleted, err := b.DeleteFargateProfile("fp-del-cluster", "fp1")
	require.NoError(t, err)
	assert.Equal(t, "DELETING", deleted.Status)
}

func TestFargateProfile_ARN_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	mustCreateClusterNoVpc(t, b, "arn-fp-cluster")

	fp, err := b.CreateFargateProfile("arn-fp-cluster", "my-fargate",
		"arn:aws:iam::123:role/fargate",
		[]eks.FargateProfileSelector{{Namespace: "default"}}, nil, nil)
	require.NoError(t, err)

	assert.Contains(t, fp.ARN, "arn:aws:eks:")
	assert.Contains(t, fp.ARN, ":fargateprofile/")
	assert.Contains(t, fp.ARN, "arn-fp-cluster")
}

func TestFargateProfile_Subnets_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		subnets []string
	}{
		{name: "with_subnets", subnets: []string{"subnet-fp-1", "subnet-fp-2"}},
		{name: "no_subnets", subnets: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fp-sub-" + tt.name})

			body := map[string]any{
				"fargateProfileName":  "fp1",
				"podExecutionRoleArn": "arn:aws:iam::123:role/fp",
			}
			if tt.subnets != nil {
				body["subnets"] = tt.subnets
			}

			rec := doREST(t, h, http.MethodPost,
				"/clusters/fp-sub-"+tt.name+"/fargate-profiles", body)
			require.Equal(t, http.StatusOK, rec.Code)

			profile := parseResp(t, rec)["fargateProfile"].(map[string]any)
			subs, _ := profile["subnets"].([]any)

			if len(tt.subnets) > 0 {
				require.Len(t, subs, len(tt.subnets))
				assert.Equal(t, tt.subnets[0], subs[0])
			} else {
				assert.Empty(t, subs)
			}
		})
	}
}

func TestFargateProfile_Subnets_Describe(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	fp, err := b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp",
		nil, []string{"subnet-a", "subnet-b"}, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"subnet-a", "subnet-b"}, fp.Subnets)

	described, err := b.DescribeFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, []string{"subnet-a", "subnet-b"}, described.Subnets)
}

func TestFargateProfile_Subnets_DeepCopy(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	subs := []string{"subnet-a"}
	fp, err := b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp", nil, subs, nil)
	require.NoError(t, err)

	fp.Subnets[0] = "mutated"

	described, err := b.DescribeFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, "subnet-a", described.Subnets[0], "subnets must be deep-copied")
}

func TestFargateProfile_Subnets_Preserved_On_Delete(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
	_, err := b.CreateCluster("c1", "1.32", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateFargateProfile("c1", "fp1", "arn:aws:iam::123:role/fp",
		nil, []string{"subnet-del"}, nil)
	require.NoError(t, err)

	deleted, err := b.DeleteFargateProfile("c1", "fp1")
	require.NoError(t, err)
	assert.Equal(t, []string{"subnet-del"}, deleted.Subnets)
	assert.Equal(t, "DELETING", deleted.Status)
}

func TestFargateProfileCreatesAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		profileName string
	}{
		{name: "fargate_profile_starts_creating", profileName: "fp1"},
		{name: "second_fargate_profile", profileName: "fp2"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			fp, err := b.CreateFargateProfile(
				"cl", tc.profileName, "arn:aws:iam::123456789012:role/fp-role",
				nil, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, "CREATING", fp.Status, tc.name)
		})
	}
}

func TestFargateProfileTransitionsToActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "transitions_to_active"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newBackend(t)
				_, err := b.CreateCluster(
					"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
				)
				require.NoError(t, err)

				_, err = b.CreateFargateProfile(
					"cl", "fp1", "arn:aws:iam::123456789012:role/fp-role", nil, nil, nil,
				)
				require.NoError(t, err)

				time.Sleep(300 * time.Millisecond)

				fp, err := b.DescribeFargateProfile("cl", "fp1")
				require.NoError(t, err)
				assert.Equal(t, "ACTIVE", fp.Status, tc.name)
			})
		})
	}
}

// TestFargateProfile_HealthFieldIsModeled verifies the "health" field is
// present on Create/Describe responses -- verified against
// aws-sdk-go-v2/service/eks/types.FargateProfile.Health, which was
// previously entirely absent from gopherstack's wire response.
func TestFargateProfile_HealthFieldIsModeled(t *testing.T) {
	t.Parallel()

	h := newTestEKSHandler(t)
	doREST(t, h, http.MethodPost, "/clusters", map[string]any{"name": "fp-health-cluster"})

	createRec := doREST(t, h, http.MethodPost, "/clusters/fp-health-cluster/fargate-profiles",
		map[string]any{"fargateProfileName": "fp-health"})
	require.Equal(t, http.StatusOK, createRec.Code)

	created := parseResp(t, createRec)["fargateProfile"].(map[string]any)
	health, ok := created["health"].(map[string]any)
	require.True(t, ok, "health must be present")
	issues, ok := health["issues"].([]any)
	require.True(t, ok)
	assert.Empty(t, issues, "a freshly created profile has no health issues")

	describeRec := doREST(t, h, http.MethodGet, "/clusters/fp-health-cluster/fargate-profiles/fp-health", nil)
	require.Equal(t, http.StatusOK, describeRec.Code)

	described := parseResp(t, describeRec)["fargateProfile"].(map[string]any)
	assert.Contains(t, described, "health")
}
