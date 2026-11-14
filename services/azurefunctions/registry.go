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

// pullPrivateImage pulls image with the app's registry credentials so the runtime finds it locally.
func (b *InMemoryBackend) pullPrivateImage(image string, settings map[string]string) error {
	user, pass, ok := registryCredentials(settings, image)
	if !ok {
		return nil
	}

	runner, err := b.runner()
	if err != nil {
		return err
	}

	puller, ok := runner.(credentialPuller)
	if !ok {
		return fmt.Errorf("%w: the container runtime cannot authenticate to registries", errRegistryAuth)
	}

	ctx, cancel := context.WithTimeout(b.baseCtx(), b.startupTimeoutValue())
	defer cancel()

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
