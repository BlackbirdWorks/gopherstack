package backup

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// tieringConfigNameRe matches valid tiering configuration names: alphanumeric
// and underscore characters only (AWS: "must consist of only alphanumeric
// characters and underscores").
var tieringConfigNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

const (
	tieringDownSettingsMinDays = 60
	tieringDownSettingsMaxDays = 36500
)

// validateResourceSelection checks a single ResourceSelection entry against
// AWS Backup's documented constraints: ResourceType and Resources are
// required, and TieringDownSettingsInDays must fall in [60, 36500].
func validateResourceSelection(rs ResourceSelection) error {
	if rs.ResourceType == "" {
		return fmt.Errorf("%w: ResourceSelection.ResourceType is required", ErrValidation)
	}
	if len(rs.Resources) == 0 {
		return fmt.Errorf("%w: ResourceSelection.Resources is required", ErrValidation)
	}
	if rs.TieringDownSettingsInDays < tieringDownSettingsMinDays ||
		rs.TieringDownSettingsInDays > tieringDownSettingsMaxDays {
		return fmt.Errorf(
			"%w: TieringDownSettingsInDays must be between %d and %d",
			ErrValidation, tieringDownSettingsMinDays, tieringDownSettingsMaxDays,
		)
	}

	return nil
}

// validateTieringConfigInput checks the fields shared by Create and Update:
// TieringConfigurationName, BackupVaultName, and a non-empty ResourceSelection
// whose entries each pass [validateResourceSelection].
func validateTieringConfigInput(name, vaultName string, sel []ResourceSelection) error {
	if name == "" {
		return fmt.Errorf("%w: TieringConfigurationName is required", ErrValidation)
	}
	if !tieringConfigNameRe.MatchString(name) {
		return fmt.Errorf(
			"%w: TieringConfigurationName must consist of only alphanumeric characters and underscores",
			ErrValidation,
		)
	}
	if vaultName == "" {
		return fmt.Errorf("%w: BackupVaultName is required", ErrValidation)
	}
	if len(sel) == 0 {
		return fmt.Errorf("%w: ResourceSelection is required", ErrValidation)
	}
	for _, rs := range sel {
		if err := validateResourceSelection(rs); err != nil {
			return err
		}
	}

	return nil
}

// CreateTieringConfiguration creates a tiering configuration. name is the
// unique TieringConfigurationName (the real AWS key for this resource
// family); vaultName is "*" to apply to all vaults or a specific vault name.
// Idempotent: a repeat call with the same creatorRequestID returns the
// existing configuration instead of AlreadyExistsException.
func (b *InMemoryBackend) CreateTieringConfiguration(
	name, vaultName string,
	sel []ResourceSelection,
	creatorRequestID string,
) (*TieringConfiguration, error) {
	b.mu.Lock("CreateTieringConfiguration")
	defer b.mu.Unlock()

	if err := validateTieringConfigInput(name, vaultName, sel); err != nil {
		return nil, err
	}

	if existing, ok := b.tieringConfigs.Get(name); ok {
		if creatorRequestID != "" && existing.CreatorRequestID == creatorRequestID {
			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf("%w: tiering configuration %s already exists", ErrAlreadyExists, name)
	}

	tcARN := arn.Build("backup", b.region, b.accountID, "tiering-configuration:"+name)
	tc := &TieringConfiguration{
		TieringConfigurationName: name,
		TieringConfigurationArn:  tcARN,
		BackupVaultName:          vaultName,
		ResourceSelection:        append([]ResourceSelection(nil), sel...),
		CreatorRequestID:         creatorRequestID,
		CreationTime:             time.Now().UTC(),
	}
	b.tieringConfigs.Put(tc)
	cp := *tc

	return &cp, nil
}

// DeleteTieringConfiguration removes a tiering configuration by name.
func (b *InMemoryBackend) DeleteTieringConfiguration(name string) error {
	b.mu.Lock("DeleteTieringConfiguration")
	defer b.mu.Unlock()

	if !b.tieringConfigs.Has(name) {
		return fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	b.tieringConfigs.Delete(name)

	return nil
}

// GetTieringConfiguration returns the tiering configuration for the given name.
func (b *InMemoryBackend) GetTieringConfiguration(name string) (*TieringConfiguration, error) {
	b.mu.RLock("GetTieringConfiguration")
	defer b.mu.RUnlock()

	tc, ok := b.tieringConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	cp := *tc

	return &cp, nil
}

// ListTieringConfigurations returns all tiering configurations sorted by name.
func (b *InMemoryBackend) ListTieringConfigurations() []*TieringConfiguration {
	b.mu.RLock("ListTieringConfigurations")
	defer b.mu.RUnlock()

	all := b.tieringConfigs.All()
	out := make([]*TieringConfiguration, 0, len(all))
	for _, tc := range all {
		cp := *tc
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TieringConfigurationName < out[j].TieringConfigurationName
	})

	return out
}

// UpdateTieringConfiguration replaces the BackupVaultName/ResourceSelection
// body of an existing tiering configuration. TieringConfigurationName cannot
// be changed (it is the resource's identity), matching AWS.
func (b *InMemoryBackend) UpdateTieringConfiguration(
	name, vaultName string,
	sel []ResourceSelection,
) (*TieringConfiguration, error) {
	b.mu.Lock("UpdateTieringConfiguration")
	defer b.mu.Unlock()

	existing, ok := b.tieringConfigs.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, name)
	}

	if err := validateTieringConfigInput(name, vaultName, sel); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	cp := *existing
	cp.BackupVaultName = vaultName
	cp.ResourceSelection = append([]ResourceSelection(nil), sel...)
	cp.LastUpdatedTime = &now
	b.tieringConfigs.Put(&cp)
	out := cp

	return &out, nil
}

// ---- Restore Access Vaults ----
