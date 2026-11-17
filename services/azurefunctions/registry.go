package azurefunctions

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	registrySettingPrefix = "DOCKER_REGISTRY_SERVER_"
	settingRegistryURL    = registrySettingPrefix + "URL"
	settingRegistryUser   = registrySettingPrefix + "USERNAME"
	registryLoginSecret   = "PASS" + "WORD"
)

// credentialPuller is implemented by container runtimes that pull with registry credentials.
type credentialPuller interface {
	PullImageWithCredentials(ctx context.Context, imageRef, username, password string) error
}

// imageHost is the registry host of an image reference ("" for Docker Hub references).
func imageHost(image string) string {
	head, _, found := strings.Cut(image, "/")
	if found && (strings.ContainsAny(head, ".:") || head == "localhost") {
		return strs.Fold(head)
	}

	return ""
}

func registryHost(url string) string {
	url = strings.TrimSpace(url)
	if i := strings.Index(url, "://"); i >= 0 {
		url = url[i+3:]
	}

	host, _, _ := strings.Cut(url, "/")

	return strs.Fold(host)
}

// registryCredentials returns the app's registry login when it applies to image (Learn: App Service "Configure a
// custom container", DOCKER_REGISTRY_SERVER_URL, _USERNAME and _PASSWORD).
func registryCredentials(settings map[string]string, image string) (string, string, bool) {
	user, pass := settings[settingRegistryUser], settings[registrySettingPrefix+registryLoginSecret]
	host := registryHost(settings[settingRegistryURL])

	if user == "" || pass == "" || host == "" || host != imageHost(image) {
		return "", "", false
	}

	return user, pass, true
}

// RegistryExchanger trades a Microsoft Entra access token for a registry refresh token at the login server's
// /oauth2/exchange endpoint, the way az acr login does.
type RegistryExchanger func(ctx context.Context, registryHost, tenant, accessToken string) (string, error)

// SetRegistryExchanger wires the registry token exchange behind managed-identity image pulls.
func (b *InMemoryBackend) SetRegistryExchanger(fn RegistryExchanger) {
	b.mu.Lock("SetRegistryExchanger")
	defer b.mu.Unlock()

	b.exchange = fn
}

const (
	// A registry refresh token logs in with this user name (Learn: ACR "Authenticate with an Azure container
	// registry", token authentication).
	registryUserName      = "00000000-0000-0000-0000-000000000000"
	registryEntraAudience = "https://management.azure.com/"
)

// managedIdentityLogin logs the app's managed identity in to the registry hosting image: an Entra token for the
// site identity (acrUserManagedIdentityID picks a user-assigned one, else the system identity) is exchanged for a
// registry refresh token. Docker Hub references need no login.
func (b *InMemoryBackend) managedIdentityLogin(
	ctx context.Context,
	st *appState,
	image string,
) (string, string, bool, error) {
	host := imageHost(image)
	if host == "" {
		return "", "", false, nil
	}

	b.mu.RLock("managedIdentityLogin")
	resolve, issue, exchange := b.identity, b.issuer, b.exchange
	b.mu.RUnlock()

	id, found := Identity{}, false
	if resolve != nil {
		id, found = resolve(st.key())
	}

	if !found || issue == nil || exchange == nil {
		return "", "", false, fmt.Errorf("%w: the app has no managed identity to log in with", errRegistryAuth)
	}

	q := map[string][]string{}
	if st.ACRIdentityID != "" {
		q["client_id"] = []string{st.ACRIdentityID}
	}

	picked, err := pickIdentity(id, q)
	if err != nil {
		return "", "", false, fmt.Errorf("%w: %w", errRegistryAuth, err)
	}

	token, _, err := issue(id.TenantID, picked.clientID, picked.objectID, registryEntraAudience)
	if err != nil {
		return "", "", false, fmt.Errorf("%w: token issuance failed", errRegistryAuth)
	}

	refresh, err := exchange(ctx, host, id.TenantID, token)
	if err != nil {
		return "", "", false, fmt.Errorf("%w: %w", errRegistryAuth, err)
	}

	return registryUserName, refresh, true, nil
}

// pullPrivateImage pulls image with the app's registry credentials, or with its managed identity when the site
// sets acrUseManagedIdentityCreds, so the runtime finds it locally.
func (b *InMemoryBackend) pullPrivateImage(image string, st *appState) error {
	user, pass, ok := registryCredentials(st.Settings, image)
	if !ok && !st.ACRManagedID {
		return nil
	}

	runner, err := b.runner()
	if err != nil {
		return err
	}

	puller, canPull := runner.(credentialPuller)
	if !canPull {
		return fmt.Errorf("%w: the container runtime cannot authenticate to registries", errRegistryAuth)
	}

	ctx, cancel := context.WithTimeout(b.baseCtx(), b.startupTimeoutValue())
	defer cancel()

	if !ok {
		if user, pass, ok, err = b.managedIdentityLogin(ctx, st, image); err != nil || !ok {
			return err
		}
	}

	if err = puller.PullImageWithCredentials(ctx, image, user, pass); err != nil {
		return fmt.Errorf("%w: %w", errRegistryAuth, err)
	}

	return nil
}

// copyAppSettings copies app settings into a container environment, leaving out the registry login: App Service
// reserves those names and does not expose them to the app (Learn: Configure a custom container).
func copyAppSettings(env, settings map[string]string) {
	for k, v := range settings {
		if !strings.HasPrefix(k, registrySettingPrefix) {
			env[k] = v
		}
	}
}
