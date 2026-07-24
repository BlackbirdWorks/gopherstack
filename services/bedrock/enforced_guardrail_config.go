package bedrock

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// enforcedGuardrailInputTagsHonor and enforcedGuardrailInputTagsIgnore mirror
// types.InputTags in aws-sdk-go-v2/service/bedrock/types.
const (
	enforcedGuardrailInputTagsHonor  = "HONOR"
	enforcedGuardrailInputTagsIgnore = "IGNORE"
)

// enforcedGuardrailConfigOwnerAccount mirrors types.ConfigurationOwnerAccount.
const enforcedGuardrailConfigOwnerAccount = "ACCOUNT"

// newEnforcedGuardrailConfigID generates a unique account-enforced-guardrail
// config ID. Callers must hold b.mu.
func (b *InMemoryBackend) newEnforcedGuardrailConfigID() string {
	b.enforcedGuardrailConfigCounter++

	return fmt.Sprintf("egc-%07d", b.enforcedGuardrailConfigCounter)
}

// PutEnforcedGuardrailConfiguration creates or updates an account-level enforced
// guardrail configuration. If configID is empty, a new configuration is created;
// otherwise the existing configuration identified by configID is updated in place
// (real AWS: PutEnforcedGuardrailConfiguration is an upsert keyed by the optional
// ConfigId request field). guardrailIdentifier must resolve to an existing
// guardrail (by ID or ARN) and inputTags must be HONOR or IGNORE.
func (b *InMemoryBackend) PutEnforcedGuardrailConfiguration(
	configID, guardrailIdentifier, guardrailVersion, inputTags string,
	includedModels, excludedModels []string,
) (*AccountEnforcedGuardrailConfig, error) {
	b.mu.Lock("PutEnforcedGuardrailConfiguration")
	defer b.mu.Unlock()

	if guardrailIdentifier == "" {
		return nil, fmt.Errorf("%w: guardrailIdentifier is required", ErrValidation)
	}

	if guardrailVersion == "" {
		return nil, fmt.Errorf("%w: guardrailVersion is required", ErrValidation)
	}

	if inputTags != enforcedGuardrailInputTagsHonor && inputTags != enforcedGuardrailInputTagsIgnore {
		return nil, fmt.Errorf("%w: inputTags must be HONOR or IGNORE", ErrValidation)
	}

	g, ok := b.findGuardrailByIDOrARN(guardrailIdentifier)
	if !ok {
		return nil, fmt.Errorf("%w: guardrail %s not found", ErrNotFound, guardrailIdentifier)
	}

	now := time.Now().UTC()

	var cfg *AccountEnforcedGuardrailConfig
	if configID != "" {
		existing, exists := b.enforcedGuardrailConfigs.Get(configID)
		if !exists {
			return nil, fmt.Errorf("%w: enforced guardrail configuration %s not found", ErrNotFound, configID)
		}

		cfg = existing
	} else {
		configID = b.newEnforcedGuardrailConfigID()
		cfg = &AccountEnforcedGuardrailConfig{
			ConfigID:  configID,
			CreatedAt: now,
			CreatedBy: b.simulatedCallerARN(),
			Owner:     enforcedGuardrailConfigOwnerAccount,
		}
	}

	cfg.GuardrailID = g.GuardrailID
	cfg.GuardrailArn = g.GuardrailArn
	cfg.GuardrailVersion = guardrailVersion
	cfg.InputTags = inputTags
	cfg.IncludedModels = append([]string(nil), includedModels...)
	cfg.ExcludedModels = append([]string(nil), excludedModels...)
	cfg.UpdatedAt = now
	cfg.UpdatedBy = b.simulatedCallerARN()

	b.enforcedGuardrailConfigs.Put(cfg)
	cp := *cfg

	return &cp, nil
}

// ListEnforcedGuardrailsConfiguration returns all account-level enforced
// guardrail configurations, sorted by ConfigID for deterministic pagination.
func (b *InMemoryBackend) ListEnforcedGuardrailsConfiguration(
	nextToken string,
) ([]*AccountEnforcedGuardrailConfig, string) {
	b.mu.RLock("ListEnforcedGuardrailsConfiguration")
	defer b.mu.RUnlock()

	configs := make([]*AccountEnforcedGuardrailConfig, 0, b.enforcedGuardrailConfigs.Len())
	for _, c := range b.enforcedGuardrailConfigs.All() {
		cp := *c
		cp.IncludedModels = append([]string(nil), c.IncludedModels...)
		cp.ExcludedModels = append([]string(nil), c.ExcludedModels...)
		configs = append(configs, &cp)
	}

	sort.Slice(configs, func(i, k int) bool {
		return configs[i].ConfigID < configs[k].ConfigID
	})

	return paginateBedrockSlice(configs, nextToken)
}

// DeleteEnforcedGuardrailConfiguration removes an account-level enforced
// guardrail configuration by ConfigID.
func (b *InMemoryBackend) DeleteEnforcedGuardrailConfiguration(configID string) error {
	b.mu.Lock("DeleteEnforcedGuardrailConfiguration")
	defer b.mu.Unlock()

	if _, ok := b.enforcedGuardrailConfigs.Get(configID); !ok {
		return fmt.Errorf("%w: enforced guardrail configuration %s not found", ErrNotFound, configID)
	}

	b.enforcedGuardrailConfigs.Delete(configID)

	return nil
}

// simulatedCallerARN returns a deterministic placeholder ARN for the
// CreatedBy/UpdatedBy fields real AWS populates with the calling principal's
// ARN. gopherstack does not model IAM caller identity for Bedrock, so this
// returns a stable, wire-shape-valid ARN instead of leaving the field empty.
func (b *InMemoryBackend) simulatedCallerARN() string {
	return arn.Build("iam", "", b.accountID, "root")
}
