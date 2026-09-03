package ecs

import (
	"fmt"
)

// accountSettingKey builds the map key for an account setting.
func accountSettingKey(name, principalArn string) string {
	return principalArn + ":" + name
}

// ListAccountSettings returns all account settings, optionally filtered by
// name and principal.
//
// ListAccountSettingsInput.EffectiveSettings's own doc comment: "If true, the
// account settings for the root user or the default setting for the
// principalArn are returned. If false, the account settings for the
// principalArn are returned if they're set. Otherwise, no account settings
// are returned." When true, any setting name for which principalArn has no
// explicit value falls back to the account-level default (the setting stored
// with an empty PrincipalArn, as PutAccountSettingDefault does).
func (b *InMemoryBackend) ListAccountSettings(
	name, principalArn string, effectiveSettings bool,
) ([]AccountSetting, error) {
	b.mu.RLock("ListAccountSettings")
	defer b.mu.RUnlock()

	all := b.accountSettings.All()

	if !effectiveSettings {
		return filterAccountSettings(all, name, principalArn), nil
	}

	return effectiveAccountSettings(all, name, principalArn), nil
}

// filterAccountSettings implements ListAccountSettings' effectiveSettings=false
// path: only settings explicitly stored for principalArn (or every principal,
// if principalArn is empty).
func filterAccountSettings(all []*AccountSetting, name, principalArn string) []AccountSetting {
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

	return out
}

// effectiveAccountSettings implements ListAccountSettings' effectiveSettings=true
// path: principalArn's own explicit setting for each name, falling back to
// the account-level default (PrincipalArn == "") when principalArn has none.
func effectiveAccountSettings(all []*AccountSetting, name, principalArn string) []AccountSetting {
	byPrincipal := map[string]*AccountSetting{}
	byDefault := map[string]*AccountSetting{}

	for _, setting := range all {
		if name != "" && setting.Name != name {
			continue
		}

		switch setting.PrincipalArn {
		case "":
			byDefault[setting.Name] = setting
		case principalArn:
			byPrincipal[setting.Name] = setting
		}
	}

	out := make([]AccountSetting, 0, len(byPrincipal)+len(byDefault))

	for n, setting := range byPrincipal {
		out = append(out, *setting)
		delete(byDefault, n)
	}

	for _, setting := range byDefault {
		out = append(out, AccountSetting{Name: setting.Name, Value: setting.Value, PrincipalArn: principalArn})
	}

	return out
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
