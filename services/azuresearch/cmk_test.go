package azuresearch_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azuresearch"
)

var errNoKey = errors.New("key not found")

type fakeKeys struct{}

func (fakeKeys) ResolveKey(vault, name, _ string) error {
	if vault == "vault1" && name == "good" {
		return nil
	}

	return errNoKey
}

func cmkKey(vault, key string) map[string]any {
	return map[string]any{"keyVaultKeyName": key, "keyVaultUri": "https://" + vault + ".vault.azure.net"}
}

func TestEncryptionKeyResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  any
		name string
		want int
	}{
		{cmkKey("vault1", "good"), "usable key", http.StatusCreated},
		{cmkKey("vault1", "missing"), "missing key", http.StatusBadRequest},
		{cmkKey("other", "good"), "missing vault", http.StatusBadRequest},
		{map[string]any{"keyVaultUri": "https://vault1.vault.azure.net"}, "no key name", http.StatusBadRequest},
		{nil, "unencrypted", http.StatusCreated},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			s.b.SetKeyResolver(fakeKeys{})

			ix := map[string]any{
				"name":   "enc",
				"fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
			}
			if tt.key != nil {
				ix["encryptionKey"] = tt.key
			}

			assert.Equal(t, tt.want, s.admin2(t, http.MethodPost, "/indexes?"+apiVer, ix).status)

			syn := map[string]any{"name": "sm", "synonyms": "a, b"}
			if tt.key != nil {
				syn["encryptionKey"] = tt.key
			}

			assert.Equal(t, tt.want, s.admin2(t, http.MethodPut, "/synonymmaps('sm')?"+apiVer, syn).status)
		})
	}
}

func TestEncryptionCompliance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		enforcement string
		want        string
		encrypted   bool
	}{
		{name: "enabled unencrypted", enforcement: "Enabled", encrypted: false, want: "NonCompliant"},
		{name: "enabled encrypted", enforcement: "Enabled", encrypted: true, want: "Compliant"},
		{name: "disabled unencrypted", enforcement: "Disabled", encrypted: false, want: "Compliant"},
		{name: "unspecified unencrypted", enforcement: "Unspecified", encrypted: false, want: "Compliant"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newStack(t, "standard")
			s.b.SetKeyResolver(fakeKeys{})

			_, err := s.b.PutService(testService, azuresearch.ServiceConfig{
				SKU: "standard", HostingMode: "default", PublicNetworkAccess: "enabled", AuthMode: "apiKeyOnly",
				ReplicaCount: 1, PartitionCount: 1, CMKEnforcement: tt.enforcement,
			})
			require.NoError(t, err)

			assert.Equal(t, "Compliant", s.b.EncryptionCompliance(testService), "an empty service is compliant")

			ix := map[string]any{
				"name":   "enc",
				"fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
			}
			if tt.encrypted {
				ix["encryptionKey"] = cmkKey("vault1", "good")
			}

			s.must(t, http.MethodPost, "/indexes?"+apiVer, ix, http.StatusCreated)
			assert.Equal(t, tt.want, s.b.EncryptionCompliance(testService))
		})
	}
}
