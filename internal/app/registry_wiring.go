package app

import (
	"encoding/json"
	"errors"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
	gcparbackend "github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

// GetRegistryDataDir keeps container registry blobs under the data dir when --persist is on
// (azurecontainerregistry.DataDirConfig, gcpartifactregistry.DataDirConfig); empty keeps them in memory.
func (c *CLI) GetRegistryDataDir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "registries")
}

// GetAzureACRSettings returns Azure Container Registry settings (azurecontainerregistry.ConfigProvider).
func (c *CLI) GetAzureACRSettings() acr.Settings { return c.AzureACR }

// wireAzureContainerRegistry gives ARM's Microsoft.ContainerRegistry provider the registry data plane.
func wireAzureContainerRegistry(byName map[string]service.Registerable) {
	armH, ok := byName["AzureARM"].(*azurearm.Handler)
	if !ok {
		return
	}

	if h, hOk := byName["AzureContainerRegistry"].(*acr.Handler); hOk {
		armH.Registry.SetContainerRegistries(&azureContainerRegistryAdapter{backend: h.Backend})
	}
}

// azureContainerRegistryAdapter adapts services/azurecontainerregistry's backend to
// services/azurearm.ContainerRegistries.
type azureContainerRegistryAdapter struct {
	backend *acr.InMemoryBackend
}

func (a *azureContainerRegistryAdapter) PutRegistry(
	name string, adminEnabled bool,
) (azurearm.ContainerRegistryInfo, error) {
	info, err := a.backend.PutRegistry(name, adminEnabled)
	if err != nil {
		return azurearm.ContainerRegistryInfo{}, err
	}

	return azurearm.ContainerRegistryInfo{CreatedAt: info.CreatedAt, AdminEnabled: info.AdminEnabled}, nil
}

func (a *azureContainerRegistryAdapter) RegistryInfo(name string) (azurearm.ContainerRegistryInfo, bool) {
	info, ok := a.backend.Registry(name)

	return azurearm.ContainerRegistryInfo{CreatedAt: info.CreatedAt, AdminEnabled: info.AdminEnabled}, ok
}

func (a *azureContainerRegistryAdapter) DeleteRegistry(name string) error {
	return ignoreMissing(a.backend.DeleteRegistry(name), acr.ErrRegistryNotFound)
}

func (a *azureContainerRegistryAdapter) Credentials(name string) (azurearm.ContainerRegistryCredentials, error) {
	creds, err := a.backend.Credentials(name)

	return registryCredentials(creds), mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) RegenerateCredential(
	name, which string,
) (azurearm.ContainerRegistryCredentials, error) {
	creds, err := a.backend.RegenerateCredential(name, which)

	return registryCredentials(creds), mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) SetWebhook(registry string, w azurearm.ContainerRegistryWebhook) error {
	return mapRegistryError(a.backend.SetWebhook(registry, acr.Webhook{
		Name: w.Name, ServiceURI: w.ServiceURI, Scope: w.Scope, Status: w.Status, Actions: w.Actions,
		Headers: w.Headers,
	}))
}

func (a *azureContainerRegistryAdapter) DeleteWebhook(registry, name string) error {
	return mapRegistryError(a.backend.DeleteWebhook(registry, name))
}

func (a *azureContainerRegistryAdapter) PingWebhook(registry, name string) (string, error) {
	id, err := a.backend.PingWebhook(registry, name)

	return id, mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) WebhookEvents(registry, name string) ([]map[string]any, error) {
	events, err := a.backend.WebhookEvents(registry, name)
	if err != nil {
		return nil, mapRegistryError(err)
	}

	out := make([]map[string]any, 0, len(events))

	for _, ev := range events {
		raw, marshalErr := json.Marshal(ev)
		if marshalErr != nil {
			return nil, marshalErr
		}

		var m map[string]any
		if unmarshalErr := json.Unmarshal(raw, &m); unmarshalErr != nil {
			return nil, unmarshalErr
		}

		out = append(out, m)
	}

	return out, nil
}

func registryCredentials(c acr.Credentials) azurearm.ContainerRegistryCredentials {
	return azurearm.ContainerRegistryCredentials{Username: c.Username, Password: c.Password1, Password2: c.Password2}
}

func mapRegistryError(err error) error {
	switch {
	case errors.Is(err, acr.ErrAdminDisabled):
		return azurearm.ErrACRAdminDisabled
	case errors.Is(err, acr.ErrInvalidCred):
		return azurearm.ErrACRInvalidCredential
	case errors.Is(err, acr.ErrRegistryNotFound):
		return azurearm.ErrACRRegistryNotFound
	case errors.Is(err, acr.ErrWebhookNotFound):
		return azurearm.ErrACRWebhookNotFound
	}

	return err
}

// GetGCPArtifactRegistrySettings returns Artifact Registry settings (gcpartifactregistry.ConfigProvider).
func (c *CLI) GetGCPArtifactRegistrySettings() gcparbackend.Settings {
	return c.GCPArtifactRegistry
}
