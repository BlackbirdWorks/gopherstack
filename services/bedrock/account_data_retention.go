package bedrock

import (
	"fmt"
	"time"
)

// Data retention mode constants mirroring types.DataRetentionMode.
const (
	dataRetentionModeDefault           = "default"
	dataRetentionModeNone              = "none"
	dataRetentionModeProviderDataShare = "provider_data_share"
	dataRetentionModeInherit           = "inherit"
)

// validDataRetentionModes is the set of values real AWS accepts for
// PutAccountDataRetentionInput.Mode.
//
//nolint:gochecknoglobals // lookup table, analogous to errCodeLookup-style tables elsewhere in this package
var validDataRetentionModes = map[string]bool{
	dataRetentionModeDefault:           true,
	dataRetentionModeNone:              true,
	dataRetentionModeProviderDataShare: true,
	dataRetentionModeInherit:           true,
}

// GetAccountDataRetention returns the account's current data retention mode.
// Real AWS's GetAccountDataRetentionOutput.Mode is a required field -- an
// account that has never called PutAccountDataRetention still gets a value
// back, defaulting to "default" (types.DataRetentionModeDefault).
func (b *InMemoryBackend) GetAccountDataRetention() *AccountDataRetention {
	b.mu.RLock("GetAccountDataRetention")
	defer b.mu.RUnlock()

	if b.accountDataRetention == nil {
		return &AccountDataRetention{Mode: dataRetentionModeDefault}
	}

	cp := *b.accountDataRetention

	return &cp
}

// PutAccountDataRetention sets the account's data retention mode.
func (b *InMemoryBackend) PutAccountDataRetention(mode string) (*AccountDataRetention, error) {
	b.mu.Lock("PutAccountDataRetention")
	defer b.mu.Unlock()

	if !validDataRetentionModes[mode] {
		return nil, fmt.Errorf(
			"%w: mode must be one of default|none|provider_data_share|inherit",
			ErrValidation,
		)
	}

	b.accountDataRetention = &AccountDataRetention{Mode: mode, UpdatedAt: time.Now().UTC()}
	cp := *b.accountDataRetention

	return &cp, nil
}
