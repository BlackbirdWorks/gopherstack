package ocivault

import (
	"net/http"
	"strconv"
)

// Default limits of an Oracle Universal Credits tenancy, from
// docs.oracle.com/en-us/iaas/Content/General/Concepts/servicelimits.htm.
const (
	limitVaults                = 10
	limitVersionsDefaultVault  = 100
	limitVersionsPrivateVault  = 1000
	limitKeysPrivateVault      = 3000
	limitSecrets               = 5000
	limitActiveSecretVersions  = 30
	limitPendingSecretVersions = 30
	asymmetricVersionWeight    = 2
)

func errLimit(what string, limit int) *APIError {
	return newErr(http.StatusBadRequest, "LimitExceeded",
		"Fulfilling this request exceeds the limit of "+strconv.Itoa(limit)+" for "+what+".")
}

func (b *InMemoryBackend) checkVaultLimit(vaultType string) *APIError {
	if vaultType != vaultTypeDefault {
		return nil
	}

	n := 0

	for _, v := range b.vaults {
		if v.VaultType == vaultTypeDefault && v.State != stateDeleted {
			n++
		}
	}

	if n >= limitVaults {
		return errLimit("vaults per region", limitVaults)
	}

	return nil
}

func versionWeight(s KeyShape) int {
	if s.Algorithm == algAES {
		return 1
	}

	return asymmetricVersionWeight
}

// checkVersionLimit refuses key versions that take a vault over its key version limit for the protection mode.
func (b *InMemoryBackend) checkVersionLimit(v *Vault, mode string, shape KeyShape, newKey bool) *APIError {
	used, keys := 0, 0

	for _, k := range b.keys {
		if k.VaultID != v.ID || k.State == stateDeleted {
			continue
		}

		keys++

		if k.ProtectionMode != mode {
			continue
		}

		for _, kv := range k.Versions {
			if kv.State != stateDeleted {
				used += versionWeight(k.Shape)
			}
		}
	}

	limit := limitVersionsDefaultVault
	if v.VaultType == vaultTypePrivate {
		limit = limitVersionsPrivateVault

		if newKey && keys >= limitKeysPrivateVault {
			return errLimit("keys per private vault", limitKeysPrivateVault)
		}
	}

	if used+versionWeight(shape) > limit {
		return errLimit("key versions per vault", limit)
	}

	return nil
}

func (b *InMemoryBackend) checkSecretLimit() *APIError {
	n := 0

	for _, s := range b.secrets {
		if s.State != stateDeleted {
			n++
		}
	}

	if n >= limitSecrets {
		return errLimit("secrets per region", limitSecrets)
	}

	return nil
}

func checkSecretVersionLimit(s *Secret) *APIError {
	active, pending := 0, 0

	for _, v := range s.Versions {
		if v.TimeOfDeletion != nil {
			pending++
		} else {
			active++
		}
	}

	if active >= limitActiveSecretVersions {
		return errLimit("active secret versions per secret", limitActiveSecretVersions)
	}

	if pending > limitPendingSecretVersions {
		return errLimit("secret versions pending deletion per secret", limitPendingSecretVersions)
	}

	return nil
}
