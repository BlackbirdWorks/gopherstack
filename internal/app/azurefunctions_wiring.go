package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
	"github.com/blackbirdworks/gopherstack/services/azureblob"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

// GetAzureFunctionsSettings returns Azure Functions settings (azurefunctions.ConfigProvider).
func (c *CLI) GetAzureFunctionsSettings() azurefunctions.Settings { return c.AzureFunctions }

// wireAzureFunctions gives ARM's Microsoft.Web provider the Azure Functions data plane.
func wireAzureFunctions(byName map[string]service.Registerable) {
	h, hOk := byName["AzureFunctions"].(*azurefunctions.Handler)
	if !hOk {
		return
	}

	h.Backend.SetNetwork(azureFunctionsNetwork(byName))

	if armH, ok := byName["AzureARM"].(*azurearm.Handler); ok {
		armH.Registry.SetFunctionApps(&azureFunctionAppsAdapter{backend: h.Backend})
	}
}

// azureFunctionsNetwork reports the storage listener ports function host containers connect to.
func azureFunctionsNetwork(byName map[string]service.Registerable) azurefunctions.Network {
	var n azurefunctions.Network

	if h, ok := byName["AzureBlob"].(*azureblob.Handler); ok {
		n.BlobPort = h.Port
	}

	if h, ok := byName["AzureQueue"].(*azurequeue.Handler); ok {
		n.QueuePort = h.Port
	}

	if h, ok := byName["AzureTable"].(*azuretable.Handler); ok {
		n.TablePort = h.Port
	}

	return n
}

// azureFunctionAppsAdapter adapts services/azurefunctions's backend to services/azurearm.FunctionApps.
type azureFunctionAppsAdapter struct {
	backend *azurefunctions.InMemoryBackend
}

func mapFunctionsError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, azurefunctions.ErrAppNotFound):
		return azurearm.ErrWebSiteNotFound
	case errors.Is(err, azurefunctions.ErrFunctionNotFound):
		return azurearm.ErrWebFunctionNotFound
	case errors.Is(err, azurefunctions.ErrFunctionInCode):
		return azurearm.ErrWebFunctionInCode
	case errors.Is(err, azurefunctions.ErrFunctionAppOnly):
		return azurearm.ErrWebNotFunctionApp
	case errors.Is(err, azurefunctions.ErrKeyNotFound):
		return azurearm.ErrWebKeyNotFound
	case errors.Is(err, azurefunctions.ErrInvalidKeyType):
		return azurearm.ErrWebInvalidKeyType
	}

	return err
}

func functionAppInfo(i azurefunctions.AppInfo) azurearm.FunctionAppInfo {
	return azurearm.FunctionAppInfo{
		Created: i.Created, LastDeploy: i.LastDeploy, HostName: i.HostName, SCMHost: i.SCMHost, Image: i.Image,
		Worker: i.Worker, Failure: i.Failure, Stopped: i.Stopped, Deployed: i.Deployed,
	}
}

func (a *azureFunctionAppsAdapter) PutApp(
	name string, cfg azurearm.FunctionAppConfig,
) (azurearm.FunctionAppInfo, error) {
	appCfg := azurefunctions.AppConfig{
		Kind: cfg.Kind, LinuxFx: cfg.LinuxFx, AppCommandLine: cfg.AppCommandLine, HealthCheckPath: cfg.HealthCheckPath,
		Flex: cfg.Flex,
	}

	if err := azurefunctions.ValidateConfig(appCfg); err != nil && (!isSlotName(name) || cfg.LinuxFx != "") {
		return azurearm.FunctionAppInfo{}, fmt.Errorf("%w: %w", azurearm.ErrWebInvalidSiteConfig, err)
	}

	return functionAppInfo(a.backend.PutApp(name, appCfg)), nil
}

func (a *azureFunctionAppsAdapter) DeleteApp(name string) error {
	a.backend.DeleteApp(name)

	return nil
}

func (a *azureFunctionAppsAdapter) AppInfo(name string) (azurearm.FunctionAppInfo, bool) {
	i, ok := a.backend.App(name)

	return functionAppInfo(i), ok
}

func (a *azureFunctionAppsAdapter) Settings(name string) (map[string]string, error) {
	s, err := a.backend.Settings(name)

	return s, mapFunctionsError(err)
}

func (a *azureFunctionAppsAdapter) SetSettings(name string, settings map[string]string) error {
	return mapFunctionsError(a.backend.SetSettings(name, settings))
}

func (a *azureFunctionAppsAdapter) SetConnectionStrings(name string, env map[string]string) error {
	return mapFunctionsError(a.backend.SetConnectionStrings(name, env))
}

func (a *azureFunctionAppsAdapter) SetStopped(name string, stopped bool) error {
	return mapFunctionsError(a.backend.SetStopped(name, stopped))
}

func (a *azureFunctionAppsAdapter) Restart(name string) error {
	return mapFunctionsError(a.backend.Restart(name))
}

func (a *azureFunctionAppsAdapter) Functions(
	ctx context.Context, name string,
) ([]azurearm.FunctionAppFunction, error) {
	fns, err := a.backend.Functions(ctx, name)
	if err != nil {
		return nil, mapFunctionsError(err)
	}

	out := make([]azurearm.FunctionAppFunction, 0, len(fns))
	for _, f := range fns {
		out = append(out, azurearm.FunctionAppFunction{
			Config: f.Config, Name: f.Name, Language: f.Language, InvokeURL: f.InvokeURL, IsDisabled: f.IsDisabled,
		})
	}

	return out, nil
}

func (a *azureFunctionAppsAdapter) HostKeys(name string) (azurearm.FunctionHostKeys, error) {
	k, err := a.backend.HostKeys(name)
	if err != nil {
		return azurearm.FunctionHostKeys{}, mapFunctionsError(err)
	}

	return azurearm.FunctionHostKeys{
		MasterKey:    k.MasterKey,
		FunctionKeys: k.FunctionKeys,
		SystemKeys:   k.SystemKeys,
	}, nil
}

func (a *azureFunctionAppsAdapter) SetHostKey(name, keyType, keyName, value string) (string, error) {
	v, err := a.backend.SetHostKey(name, keyType, keyName, value)

	return v, mapFunctionsError(err)
}

func (a *azureFunctionAppsAdapter) DeleteHostKey(name, keyType, keyName string) error {
	return mapFunctionsError(a.backend.DeleteHostKey(name, keyType, keyName))
}

func (a *azureFunctionAppsAdapter) FunctionKeys(name, fn string) (map[string]string, error) {
	k, err := a.backend.FunctionKeys(name, fn)

	return k, mapFunctionsError(err)
}

func (a *azureFunctionAppsAdapter) SetFunctionKey(name, fn, keyName, value string) (string, error) {
	v, err := a.backend.SetFunctionKey(name, fn, keyName, value)

	return v, mapFunctionsError(err)
}

func (a *azureFunctionAppsAdapter) DeleteFunctionKey(name, fn, keyName string) error {
	return mapFunctionsError(a.backend.DeleteFunctionKey(name, fn, keyName))
}

func (a *azureFunctionAppsAdapter) PublishingCredentials(name string) (string, string, string, error) {
	user, password, scmURI, err := a.backend.PublishingCredentials(name)

	return user, password, scmURI, mapFunctionsError(err)
}

func isSlotName(name string) bool { return strings.Contains(name, azurefunctions.SlotSep) }

func (a *azureFunctionAppsAdapter) SwapContent(first, second string) error {
	return mapFunctionsError(a.backend.SwapContent(first, second))
}

func (a *azureFunctionAppsAdapter) WarmUp(ctx context.Context, name string) error {
	return mapFunctionsError(a.backend.WarmUp(ctx, name))
}

func (a *azureFunctionAppsAdapter) DeleteFunction(ctx context.Context, name, fn string) error {
	return mapFunctionsError(a.backend.DeleteFunction(ctx, name, fn))
}

func (a *azureFunctionAppsAdapter) AdminToken(name string) (string, error) {
	t, err := a.backend.AdminToken(name)

	return t, mapFunctionsError(err)
}

func (a *azureFunctionAppsAdapter) SyncTriggers(ctx context.Context, name string) error {
	return mapFunctionsError(a.backend.SyncTriggers(ctx, name))
}
