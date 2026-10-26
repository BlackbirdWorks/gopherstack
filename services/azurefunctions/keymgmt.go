package azurefunctions

import (
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// HostKeys are an app's host-level keys.
type HostKeys struct {
	FunctionKeys map[string]string
	SystemKeys   map[string]string
	MasterKey    string
}

func (b *InMemoryBackend) appLocked(name string) (*app, error) {
	a, ok := b.apps[strs.Fold(name)]
	if !ok {
		return nil, ErrAppNotFound
	}

	return a, nil
}

// syncSecretsLocked rewrites the running host's secret files after a key change.
func (b *InMemoryBackend) syncSecretsLocked(a *app) error {
	if a.secretsDir == "" {
		return nil
	}

	return writeSecrets(a.secretsDir, &a.appState)
}

// HostKeys returns the app's master, function and system keys.
func (b *InMemoryBackend) HostKeys(name string) (HostKeys, error) {
	b.mu.RLock("HostKeys")
	defer b.mu.RUnlock()

	a, err := b.appLocked(name)
	if err != nil {
		return HostKeys{}, err
	}

	return HostKeys{
		MasterKey:    a.MasterKey,
		FunctionKeys: maps.Clone(a.HostKeys),
		SystemKeys:   maps.Clone(a.SystemKeys),
	}, nil
}

func (a *app) hostKeySet(keyType string) (map[string]string, error) {
	switch keyType {
	case KeyTypeFunction:
		return a.HostKeys, nil
	case KeyTypeSystem:
		return a.SystemKeys, nil
	}

	return nil, ErrInvalidKeyType
}

// SetHostKey creates or replaces a host key; an empty value generates one. It returns the stored value.
func (b *InMemoryBackend) SetHostKey(name, keyType, keyName, value string) (string, error) {
	b.mu.Lock("SetHostKey")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return "", err
	}

	set, err := a.hostKeySet(keyType)
	if err != nil {
		return "", err
	}

	if value == "" {
		value = newKey()
	}

	set[keyName] = value

	return value, b.syncSecretsLocked(a)
}

// DeleteHostKey removes a host key.
func (b *InMemoryBackend) DeleteHostKey(name, keyType, keyName string) error {
	b.mu.Lock("DeleteHostKey")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return err
	}

	set, err := a.hostKeySet(keyType)
	if err != nil {
		return err
	}

	if _, ok := set[keyName]; !ok {
		return ErrKeyNotFound
	}

	delete(set, keyName)

	return b.syncSecretsLocked(a)
}

// FunctionKeys returns a function's keys, creating its default key on first use as Azure does.
func (b *InMemoryBackend) FunctionKeys(name, fn string) (map[string]string, error) {
	b.mu.Lock("FunctionKeys")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return nil, err
	}

	keys := a.FunctionKeys[fn]
	if keys == nil {
		keys = map[string]string{defaultKeyName: newKey()}
		a.FunctionKeys[fn] = keys

		if err = b.syncSecretsLocked(a); err != nil {
			return nil, err
		}
	}

	return maps.Clone(keys), nil
}

// SetFunctionKey creates or replaces a function key; an empty value generates one.
func (b *InMemoryBackend) SetFunctionKey(name, fn, keyName, value string) (string, error) {
	b.mu.Lock("SetFunctionKey")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return "", err
	}

	if value == "" {
		value = newKey()
	}

	if a.FunctionKeys[fn] == nil {
		a.FunctionKeys[fn] = map[string]string{defaultKeyName: newKey()}
	}

	a.FunctionKeys[fn][keyName] = value

	return value, b.syncSecretsLocked(a)
}

// DeleteFunctionKey removes a function key.
func (b *InMemoryBackend) DeleteFunctionKey(name, fn, keyName string) error {
	b.mu.Lock("DeleteFunctionKey")
	defer b.mu.Unlock()

	a, err := b.appLocked(name)
	if err != nil {
		return err
	}

	if _, ok := a.FunctionKeys[fn][keyName]; !ok {
		return ErrKeyNotFound
	}

	delete(a.FunctionKeys[fn], keyName)

	return b.syncSecretsLocked(a)
}
