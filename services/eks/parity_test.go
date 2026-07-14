package eks_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func newParityBackend(t *testing.T) *eks.InMemoryBackend {
	t.Helper()

	return eks.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
}

func TestParity_FargateProfileCreatesAsCreating(t *testing.T) {
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

			b := newParityBackend(t)
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

func TestParity_FargateProfileTransitionsToActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "transitions_to_active"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
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
	}
}

func TestParity_AddonCreatesAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		addonName string
	}{
		{name: "vpc_cni_starts_creating", addonName: "vpc-cni"},
		{name: "coredns_starts_creating", addonName: "coredns"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			addon, err := b.CreateAddon("cl", tc.addonName, "", "", "", "", nil)
			require.NoError(t, err)
			assert.Equal(t, "CREATING", addon.Status, tc.name)
		})
	}
}

func TestParity_AddonTransitionsToActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "transitions_to_active"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			_, err = b.CreateAddon("cl", "vpc-cni", "", "", "", "", nil)
			require.NoError(t, err)

			time.Sleep(300 * time.Millisecond)

			addon, err := b.DescribeAddon("cl", "vpc-cni")
			require.NoError(t, err)
			assert.Equal(t, "ACTIVE", addon.Status, tc.name)
		})
	}
}

func TestParity_IDPConfigCreatesAsCreating(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "idp_config_starts_creating"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.32", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			cfg, err := b.AssociateIdentityProviderConfig(
				"cl", "oidc", "my-idp",
				map[string]string{"issuerUrl": "https://example.com", "clientId": "client1"},
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, "CREATING", cfg.Status, tc.name)
		})
	}
}

func TestParity_UpdateClusterVersionReturnsInProgress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "version_update_is_inprogress"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := newParityBackend(t)
			_, err := b.CreateCluster(
				"cl", "1.31", "arn:aws:iam::123456789012:role/role", nil, nil, nil,
			)
			require.NoError(t, err)

			u, err := b.UpdateClusterVersion("cl", "1.32")
			require.NoError(t, err)
			assert.Equal(t, "InProgress", u.Status, tc.name)
		})
	}
}

func TestParity_ClusterHasCertificateAuthority(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cluster_has_certificate_authority"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			rec := doREST(t, h, http.MethodPost, "/clusters", map[string]any{
				"name":    "cl",
				"version": "1.32",
				"roleArn": "arn:aws:iam::123456789012:role/role",
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			resp := parseResp(t, rec)
			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok, tc.name)

			ca, ok := cluster["certificateAuthority"].(map[string]any)
			require.True(t, ok, tc.name)
			assert.NotEmpty(t, ca["data"], tc.name)
		})
	}
}

func TestParity_NodegroupHasHealthField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "nodegroup_has_health_with_issues"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{
				"name":    "cl",
				"version": "1.32",
				"roleArn": "arn:aws:iam::123456789012:role/role",
			})

			rec := doREST(t, h, http.MethodPost, "/clusters/cl/node-groups", map[string]any{
				"nodegroupName": "ng1",
				"nodeRole":      "arn:aws:iam::123456789012:role/ng-role",
				"scalingConfig": map[string]any{"desiredSize": 1, "minSize": 1, "maxSize": 3},
				"subnets":       []string{"subnet-abc"},
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			resp := parseResp(t, rec)
			ng, ok := resp["nodegroup"].(map[string]any)
			require.True(t, ok, tc.name)

			health, ok := ng["health"].(map[string]any)
			require.True(t, ok, tc.name)
			_, ok = health["issues"]
			assert.True(t, ok, tc.name)
		})
	}
}

func TestParity_RegisterClusterStoresConnectorConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		provider string
	}{
		{name: "gke_provider", provider: "GKE"},
		{name: "eks_anywhere_provider", provider: "EKS_ANYWHERE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			rec := doREST(t, h, http.MethodPost, "/cluster-registrations", map[string]any{
				"name": "ext-cluster",
				"connectorConfig": map[string]any{
					"provider": tc.provider,
					"roleArn":  "arn:aws:iam::123456789012:role/connector-role",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			resp := parseResp(t, rec)
			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok, tc.name)

			cc, ok := cluster["connectorConfig"].(map[string]any)
			require.True(t, ok, tc.name)
			assert.Equal(t, tc.provider, cc["provider"], tc.name)
			assert.NotEmpty(t, cc["activationId"], tc.name)
			assert.NotEmpty(t, cc["activationCode"], tc.name)
		})
	}
}

func TestParity_DescribeClusterVersionsHasBooleanDefaultVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "default_version_is_boolean"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			rec := doREST(t, h, http.MethodGet, "/cluster-versions", nil)
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			resp := parseResp(t, rec)
			versions, ok := resp["clusterVersions"].([]any)
			require.True(t, ok, tc.name)
			require.NotEmpty(t, versions, tc.name)

			first, ok := versions[0].(map[string]any)
			require.True(t, ok, tc.name)

			defaultVer, exists := first["defaultVersion"]
			require.True(t, exists, tc.name)
			_, isBool := defaultVer.(bool)
			assert.True(t, isBool, "defaultVersion should be bool, got %T", defaultVer)
		})
	}
}

func TestParity_AssociateIDPUsesConfigName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configName string
	}{
		{name: "uses_identityProviderConfigName", configName: "my-oidc-provider"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestEKSHandler(t)
			doREST(t, h, http.MethodPost, "/clusters", map[string]any{
				"name":    "cl",
				"version": "1.32",
				"roleArn": "arn:aws:iam::123456789012:role/role",
			})

			rec := doREST(t, h, http.MethodPost, "/clusters/cl/identity-provider-configs/associate", map[string]any{
				"oidc": map[string]any{
					"identityProviderConfigName": tc.configName,
					"clientId":                   "my-client",
					"issuerUrl":                  "https://issuer.example.com",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code, tc.name)

			listRec := doREST(t, h, http.MethodGet, "/clusters/cl/identity-provider-configs", nil)
			require.Equal(t, http.StatusOK, listRec.Code, tc.name)

			listResp := parseResp(t, listRec)
			idpConfigs, ok := listResp["identityProviderConfigs"].([]any)
			require.True(t, ok, tc.name)
			require.Len(t, idpConfigs, 1, tc.name)

			entry, ok := idpConfigs[0].(map[string]any)
			require.True(t, ok, tc.name)
			assert.Equal(t, tc.configName, entry["name"], tc.name)
		})
	}
}
