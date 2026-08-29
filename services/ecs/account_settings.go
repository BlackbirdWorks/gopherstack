package ecs

import (
	"fmt"
)

// accountSettingKey builds the map key for an account setting.
func accountSettingKey(name, principalArn string) string {
	return principalArn + ":" + name
}

// ListAccountSettings returns all account settings, optionally filtered by name and principal.
func (b *InMemoryBackend) ListAccountSettings(name, principalArn string) ([]AccountSetting, error) {
	b.mu.RLock("ListAccountSettings")
	defer b.mu.RUnlock()

	all := b.accountSettings.All()
	out := make([]AccountSetting, 0, len(all))

	for _, setting := range all {
		if name != "" && setting.Name != name {
			continue
		}

		if principalArn != "" && setting.PrincipalArn != principalArn {
			continue
		}

		out = append(out, *setting)
	}

	return out, nil
}

// PutAccountSetting creates or updates an account setting for a specific principal.
func (b *InMemoryBackend) PutAccountSetting(
	name, value, principalArn string,
) (*AccountSetting, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	if value == "" {
		return nil, fmt.Errorf("%w: value is required", ErrInvalidParameter)
	}

	b.mu.Lock("PutAccountSetting")
	defer b.mu.Unlock()

	setting := &AccountSetting{
		Name:         name,
		Value:        value,
		PrincipalArn: principalArn,
	}

	b.accountSettings.Put(setting)

	out := *setting

	return &out, nil
}

// PutAccountSettingDefault creates or updates an account-level default setting (no principal).
func (b *InMemoryBackend) PutAccountSettingDefault(name, value string) (*AccountSetting, error) {
	return b.PutAccountSetting(name, value, "")
}

// DeleteAccountSetting deletes an account setting for a principal.
func (b *InMemoryBackend) DeleteAccountSetting(name, principalArn string) (*AccountSetting, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: name is required", ErrInvalidParameter)
	}

	key := accountSettingKey(name, principalArn)

	b.mu.Lock("DeleteAccountSetting")
	defer b.mu.Unlock()

	setting, ok := b.accountSettings.Get(key)
	if !ok {
		return nil, fmt.Errorf("%w: account setting %s not found", ErrInvalidParameter, name)
	}

	b.accountSettings.Delete(key)

	out := *setting

	return &out, nil
}

// AddAccountSettingInternal adds an account setting directly (seed helper for
// tests). key is unused now that the store.Table derives its own key from
// setting's fields (accountSettingsKeyFn); retained in the signature so
// existing call sites (which always pass a key consistent with
// accountSettingKey(setting.Name, setting.PrincipalArn)) do not need updating.
func (b *InMemoryBackend) AddAccountSettingInternal(_ string, setting *AccountSetting) {
	b.mu.Lock("AddAccountSettingInternal")
	defer b.mu.Unlock()

	b.accountSettings.Put(setting)
}
