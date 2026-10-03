package cognitoidp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedPoolScopedState(t *testing.T, b *InMemoryBackend) (string, string) {
	t.Helper()

	pool, err := b.CreateUserPool("leak")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "c")
	require.NoError(t, err)

	poolID, clientID := pool.ID, client.ClientID

	b.mu.Lock("seed")
	defer b.mu.Unlock()

	b.resourceServers.Put(&ResourceServer{UserPoolID: poolID, Identifier: "rs"})
	b.identityProviders.Put(&IdentityProvider{UserPoolID: poolID, ProviderName: "idp"})
	b.terms.Put(&Terms{UserPoolID: poolID, TermsID: "t-" + poolID})
	b.userImportJobs.Put(&UserImportJob{UserPoolID: poolID, JobID: "j"})
	b.managedLoginBrandings.Put(&ManagedLoginBranding{UserPoolID: poolID, ManagedLoginBrandingID: "m"})
	b.userPoolReplicas.Put(&UserPoolReplica{UserPoolID: poolID, RegionName: "eu-west-1", ARN: "arn:replica:" + poolID})
	b.resourceTags["arn:replica:"+poolID] = map[string]string{"k": "v"}
	b.uiCustomizations.Put(&UICustomization{UserPoolID: poolID, ClientID: clientID})
	b.typedRiskConfigurations.Put(&TypedRiskConfiguration{UserPoolID: poolID, ClientID: clientID})
	b.riskConfigurations[riskKey(poolID, clientID)] = &RiskConfiguration{}
	b.riskConfigurations[riskKey(poolID, "")] = &RiskConfiguration{}
	b.logDeliveryConfigs[poolID] = &LogDeliveryConfig{}
	b.poolMfaConfigs[poolID] = &UserPoolMfaFullConfig{}
	b.attrVerificationCodes[poolID+":bob:email"] = &attrVerificationEntry{ExpiresAt: time.Now().Add(time.Hour)}
	b.mfaSessions["s-"+poolID] = &mfaSessionEntry{PoolID: poolID, ExpiresAt: time.Now().Add(time.Hour)}

	return poolID, clientID
}

func TestDeleteUserPool_CascadesPoolScopedState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		delete     func(b *InMemoryBackend, poolID, clientID string) error
		name       string
		clientOnly bool
	}{
		{
			name:   "delete pool",
			delete: func(b *InMemoryBackend, poolID, _ string) error { return b.DeleteUserPool(poolID) },
		},
		{
			name: "delete client",
			delete: func(b *InMemoryBackend, poolID, clientID string) error {
				return b.DeleteUserPoolClient(poolID, clientID)
			},
			clientOnly: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend("000000000000", "us-east-1", "http://localhost")
			poolID, clientID := seedPoolScopedState(t, b)

			require.NoError(t, tc.delete(b, poolID, clientID))

			b.mu.RLock("check")
			defer b.mu.RUnlock()

			assert.Empty(t, b.uiCustomizations.All())
			assert.Empty(t, b.typedRiskConfigurations.All())
			assert.NotContains(t, b.riskConfigurations, riskKey(poolID, clientID))

			if tc.clientOnly {
				assert.Contains(t, b.riskConfigurations, riskKey(poolID, ""))

				return
			}

			assert.Empty(t, b.resourceServers.All())
			assert.Empty(t, b.identityProviders.All())
			assert.Empty(t, b.terms.All())
			assert.Empty(t, b.userImportJobs.All())
			assert.Empty(t, b.managedLoginBrandings.All())
			assert.Empty(t, b.userPoolReplicas.All())
			assert.Empty(t, b.riskConfigurations)
			assert.Empty(t, b.logDeliveryConfigs)
			assert.Empty(t, b.poolMfaConfigs)
			assert.Empty(t, b.attrVerificationCodes)
			assert.Empty(t, b.mfaSessions)
			assert.NotContains(t, b.resourceTags, "arn:replica:"+poolID)
		})
	}
}
