package app

import (
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azureappconfig"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

// wireAzureAppConfig gives ARM's Microsoft.AppConfiguration provider the App Configuration data plane.
func wireAzureAppConfig(byName map[string]service.Registerable) {
	armH, ok := byName["AzureARM"].(*azurearm.Handler)
	if !ok {
		return
	}

	if acH, acOk := byName["AzureAppConfig"].(*azureappconfig.Handler); acOk {
		armH.Registry.SetAppConfig(&azureAppConfigAdapter{backend: acH.Backend})
	}
}

// azureAppConfigAdapter adapts services/azureappconfig's backend to services/azurearm.AppConfig.
type azureAppConfigAdapter struct {
	backend *azureappconfig.InMemoryBackend
}

func (a *azureAppConfigAdapter) PutStore(name string) (time.Time, error) {
	info, err := a.backend.PutStore(name)

	return info.Created, err
}

func (a *azureAppConfigAdapter) StoreCreated(name string) (time.Time, bool) {
	info, err := a.backend.Store(name)

	return info.Created, err == nil
}

func (a *azureAppConfigAdapter) DeleteStore(name string) error {
	return ignoreMissing(a.backend.DeleteStore(name), azureappconfig.ErrStoreNotFound)
}

func accessKeyOf(k azureappconfig.AccessKey) azurearm.AppConfigKey {
	return azurearm.AppConfigKey{
		ID: k.ID, Name: k.Name, Value: k.Secret, ReadOnly: k.ReadOnly, LastModified: k.LastModified,
	}
}

func (a *azureAppConfigAdapter) StoreKeys(name string) ([]azurearm.AppConfigKey, error) {
	keys, err := a.backend.AccessKeys(name)
	if err != nil {
		return nil, err
	}

	out := make([]azurearm.AppConfigKey, 0, len(keys))
	for _, k := range keys {
		out = append(out, accessKeyOf(k))
	}

	return out, nil
}

func (a *azureAppConfigAdapter) RegenerateStoreKey(name, keyID string) (azurearm.AppConfigKey, error) {
	k, err := a.backend.RegenerateKey(name, keyID)
	if err != nil {
		return azurearm.AppConfigKey{}, err
	}

	return accessKeyOf(k), nil
}

func kvOf(kv azureappconfig.KeyValue) azurearm.AppConfigKV {
	return azurearm.AppConfigKV{
		Key: kv.Key, Label: kv.Label, Value: kv.Value, ContentType: kv.ContentType, Tags: kv.Tags,
		ETag: kv.ETag, Locked: kv.Locked, LastModified: kv.LastModified,
	}
}

func (a *azureAppConfigAdapter) PutKeyValue(store string, kv azurearm.AppConfigKV) (azurearm.AppConfigKV, error) {
	out, err := a.backend.SetKeyValue(
		store,
		kv.Key,
		kv.Label,
		azureappconfig.KVInput{
			Value:       kv.Value,
			ContentType: kv.ContentType,
			Tags:        kv.Tags,
		},
		azureappconfig.Precondition{},
	)
	if errors.Is(err, azureappconfig.ErrLocked) {
		return azurearm.AppConfigKV{}, azurearm.ErrAppConfigKeyLocked
	}

	if err != nil {
		return azurearm.AppConfigKV{}, err
	}

	return kvOf(out), nil
}

func (a *azureAppConfigAdapter) GetKeyValue(store, key, label string) (azurearm.AppConfigKV, bool) {
	kv, err := a.backend.GetKeyValue(store, key, label, time.Time{})
	if err != nil {
		return azurearm.AppConfigKV{}, false
	}

	return kvOf(kv), true
}

func (a *azureAppConfigAdapter) DeleteKeyValue(store, key, label string) error {
	_, err := a.backend.DeleteKeyValue(store, key, label, azureappconfig.Precondition{})
	if errors.Is(err, azureappconfig.ErrLocked) {
		return azurearm.ErrAppConfigKeyLocked
	}

	return ignoreMissing(err, azureappconfig.ErrKeyNotFound)
}

func (a *azureAppConfigAdapter) ListKeyValues(store string) ([]azurearm.AppConfigKV, error) {
	kvs, err := a.backend.AllKeyValues(store)
	if err != nil {
		return nil, err
	}

	out := make([]azurearm.AppConfigKV, 0, len(kvs))
	for _, kv := range kvs {
		out = append(out, kvOf(kv))
	}

	return out, nil
}
