package app

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
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

		if kv, kvOK := byName["AzureKeyVault"].(*azurekeyvault.Handler); kvOK {
			h.Backend.SetSecretResolver(keyVaultSecretResolver(kv.Backend))
			h.Backend.SetKeyChecker(keyVaultKeyChecker(kv.Backend))
		}

		if armH.Issuer != nil {
			h.ValidateToken = acrTokenValidator(armH.Issuer)
		}
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

func (a *azureContainerRegistryAdapter) SetAnonymousPull(name string, enabled bool) error {
	return mapRegistryError(a.backend.SetAnonymousPull(name, enabled))
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
		raw, marshalErr := json.Marshal(ev, jsonx.V1())
		if marshalErr != nil {
			return nil, marshalErr
		}

		var m map[string]any
		if unmarshalErr := json.Unmarshal(raw, &m, jsonx.V1()); unmarshalErr != nil {
			return nil, unmarshalErr
		}

		out = append(out, m)
	}

	return out, nil
}

func (a *azureContainerRegistryAdapter) ScopeMaps(registry string) ([]azurearm.ContainerRegistryScopeMap, error) {
	maps, err := a.backend.ScopeMaps(registry)
	if err != nil {
		return nil, mapRegistryError(err)
	}

	out := make([]azurearm.ContainerRegistryScopeMap, 0, len(maps))
	for _, sm := range maps {
		out = append(out, azurearm.ContainerRegistryScopeMap{
			Name: sm.Name, Description: sm.Description, Actions: sm.Actions, CreatedAt: sm.CreatedAt, System: sm.System,
		})
	}

	return out, nil
}

func (a *azureContainerRegistryAdapter) SetScopeMap(registry, name, description string, actions []string) error {
	_, err := a.backend.SetScopeMap(registry, name, description, actions)

	return mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) DeleteScopeMap(registry, name string) error {
	return mapRegistryError(a.backend.DeleteScopeMap(registry, name))
}

func (a *azureContainerRegistryAdapter) Tokens(registry string) ([]azurearm.ContainerRegistryToken, error) {
	tokens, err := a.backend.Tokens(registry)
	if err != nil {
		return nil, mapRegistryError(err)
	}

	out := make([]azurearm.ContainerRegistryToken, 0, len(tokens))

	for _, t := range tokens {
		tok := azurearm.ContainerRegistryToken{
			Name: t.Name, ScopeMap: t.ScopeMap, CreatedAt: t.CreatedAt, Enabled: t.Enabled,
		}

		for _, pw := range t.Passwords {
			tok.Passwords = append(tok.Passwords, tokenPassword(pw))
		}

		out = append(out, tok)
	}

	return out, nil
}

func (a *azureContainerRegistryAdapter) SetToken(registry, name, scopeMap string, enabled bool) error {
	return mapRegistryError(a.backend.SetToken(registry, name, scopeMap, enabled))
}

func (a *azureContainerRegistryAdapter) DeleteToken(registry, name string) error {
	return mapRegistryError(a.backend.DeleteToken(registry, name))
}

func (a *azureContainerRegistryAdapter) GenerateTokenPassword(
	registry, token, which string, expiry *time.Time,
) (azurearm.ContainerRegistryTokenPassword, error) {
	pw, err := a.backend.GenerateTokenPassword(registry, token, which, expiry)

	return tokenPassword(pw), mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) ImportImage(
	ctx context.Context, registry string, req azurearm.ContainerRegistryImport,
) error {
	err := a.backend.ImportImage(ctx, registry, acr.ImportRequest{
		Source: acr.ImportSource{
			Registry: req.SourceRegistry, RegistryURI: req.SourceURI, Image: req.SourceImage,
			Username: req.Username, Password: req.Password,
		},
		TargetTags: req.TargetTags, UntaggedTargets: req.UntaggedTargets, Force: req.Force,
	})

	switch {
	case errors.Is(err, acr.ErrImportSourceNotFound):
		return fmt.Errorf("%w: %w", azurearm.ErrACRImportNotFound, err)
	case errors.Is(err, acr.ErrImportTagExists):
		return fmt.Errorf("%w: %w", azurearm.ErrACRImportTagExists, err)
	}

	return mapRegistryError(err)
}

func (a *azureContainerRegistryAdapter) SetNetworkRules(
	registry string, rules azurearm.ContainerRegistryNetworkRules,
) error {
	return mapRegistryError(a.backend.SetNetworkRules(registry, acr.NetworkRules{
		Allow: rules.Allow, PublicDisabled: rules.PublicDisabled, DefaultDeny: rules.DefaultDeny,
	}))
}

func (a *azureContainerRegistryAdapter) SetCacheRule(
	registry string, rule azurearm.ContainerRegistryCacheRule,
) error {
	var creds *acr.CacheCredentials
	if rule.Credentials != nil {
		creds = &acr.CacheCredentials{
			LoginServer: rule.Credentials.LoginServer, UsernameSecret: rule.Credentials.UsernameSecret,
			PasswordSecret: rule.Credentials.PasswordSecret,
		}
	}

	return mapRegistryError(a.backend.SetCacheRule(registry, acr.CacheRule{
		Name: rule.Name, Source: rule.Source, Target: rule.Target, Credentials: creds,
	}))
}

func (a *azureContainerRegistryAdapter) DeleteCacheRule(registry, name string) error {
	return ignoreMissing(a.backend.DeleteCacheRule(registry, name), acr.ErrCacheRuleNotFound)
}

func (a *azureContainerRegistryAdapter) CredentialHealth(usernameID, passwordID string) (bool, string, string) {
	return a.backend.CredentialHealth(usernameID, passwordID)
}

func (a *azureContainerRegistryAdapter) CheckEncryptionKey(keyIdentifier string) error {
	return a.backend.CheckKey(keyIdentifier)
}

func (a *azureContainerRegistryAdapter) SetEncryptionKey(registry, keyIdentifier string) error {
	return mapRegistryError(a.backend.SetEncryptionKey(registry, keyIdentifier))
}

func tokenPassword(pw acr.TokenPassword) azurearm.ContainerRegistryTokenPassword {
	return azurearm.ContainerRegistryTokenPassword{
		Name:    pw.Name,
		Value:   pw.Value,
		Created: pw.Created,
		Expiry:  pw.Expiry,
	}
}

// keyVaultSecretResolver reads https://<vault>.<domain>/secrets/<name>[/<version>] from the emulated Key Vault.
func keyVaultSecretResolver(kv *azurekeyvault.InMemoryBackend) acr.SecretResolver {
	return func(secretURI string) (string, error) {
		u, err := url.Parse(secretURI)
		if err != nil || u.Host == "" {
			return "", fmt.Errorf("invalid secret identifier %q", secretURI)
		}

		vault, _, _ := strings.Cut(u.Hostname(), ".")
		parts := strings.Split(strings.Trim(strings.TrimPrefix(u.Path, "/secrets/"), "/"), "/")

		version := ""
		if len(parts) > 1 {
			version = parts[1]
		}

		secret, err := kv.GetSecret(vault, u.Scheme+"://"+u.Host, parts[0], version)
		if err != nil {
			return "", err
		}

		if secret.Value == nil {
			return "", fmt.Errorf("secret %q has no value", secretURI)
		}

		return *secret.Value, nil
	}
}

// keyVaultKeyChecker accepts enabled RSA keys of the emulated Key Vault; identifiers look like
// https://<vault>.<domain>/keys/<name>[/<version>].
func keyVaultKeyChecker(kv *azurekeyvault.InMemoryBackend) acr.KeyChecker {
	return func(keyIdentifier string) error {
		u, err := url.Parse(keyIdentifier)
		if err != nil || u.Host == "" || !strings.HasPrefix(u.Path, "/keys/") {
			return fmt.Errorf("%w: %q is not a key identifier", acr.ErrKeyUnavailable, keyIdentifier)
		}

		vault, _, _ := strings.Cut(u.Hostname(), ".")
		parts := strings.Split(strings.Trim(strings.TrimPrefix(u.Path, "/keys/"), "/"), "/")

		version := ""
		if len(parts) > 1 {
			version = parts[1]
		}

		key, err := kv.GetKey(vault, u.Scheme+"://"+u.Host, parts[0], version)
		if err != nil {
			return fmt.Errorf("%w: %w", acr.ErrKeyUnavailable, err)
		}

		if key.Attributes != nil && key.Attributes.Enabled != nil && !*key.Attributes.Enabled {
			return fmt.Errorf("%w: key %s is disabled", acr.ErrKeyUnavailable, keyIdentifier)
		}

		if key.Key == nil || !strings.HasPrefix(key.Key.Kty, "RSA") {
			return fmt.Errorf("%w: only RSA keys are supported", acr.ErrKeyUnavailable)
		}

		return nil
	}
}

// acrTokenValidator accepts access tokens signed by the emulator's Entra issuer, for the tenant when one is given.
func acrTokenValidator(iss *aadauth.Issuer) func(token, tenant string) error {
	return func(token, tenant string) error {
		_, err := iss.Validate(token, "", tenant)

		return err
	}
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
	case errors.Is(err, acr.ErrCacheRuleNotFound):
		return azurearm.ErrACRCacheRuleNotFound
	case errors.Is(err, acr.ErrScopeMapNotFound):
		return azurearm.ErrACRScopeMapNotFound
	case errors.Is(err, acr.ErrTokenNotFound):
		return azurearm.ErrACRTokenNotFound
	case errors.Is(err, acr.ErrScopeMapInUse):
		return azurearm.ErrACRScopeMapInUse
	case errors.Is(err, acr.ErrScopeMapReadOnly):
		return azurearm.ErrACRScopeMapReadOnly
	}

	return err
}

// GetGCPArtifactRegistrySettings returns Artifact Registry settings (gcpartifactregistry.ConfigProvider).
func (c *CLI) GetGCPArtifactRegistrySettings() gcparbackend.Settings {
	return c.GCPArtifactRegistry
}
