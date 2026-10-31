package azurearm

import (
	"errors"
	"fmt"
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// ErrNilAppContext is returned when Init is called with a nil AppContext.
var ErrNilAppContext = errors.New("azurearm: nil app context")

// ErrStorageVHostPortMismatch is returned when Settings.StorageVHostPort
// (what ARM advertises in primaryEndpoints) disagrees with the actual
// services/azurestoragevhost listener's own configured port. Failing fast
// here, rather than letting the two silently diverge, avoids repeating the
// exact "advertised a port nothing answers on" bug class already found and
// fixed twice in this milestone (AZURE.md section 10.10, M8 bugs (1)/(5)).
var ErrStorageVHostPortMismatch = errors.New(
	"azurearm: --azure-arm-storage-vhost-port must match --azure-storage-vhost-port",
)

// VHostPortProvider is a private interface AppContext.Config may implement
// to expose the actual services/azurestoragevhost listener's configured
// port, so Init can cross-check it against Settings.StorageVHostPort. cli.go
// implements this via CLI.GetAzureStorageVHostPort.
type VHostPortProvider interface {
	GetAzureStorageVHostPort() int
}

// ConfigProvider is a private interface to extract ARM configuration from
// the abstract AppContext Config, mirroring services/cosmosdb.ConfigProvider.
type ConfigProvider interface {
	GetAzureARMSettings() Settings
}

// StorageAccountsProvider is a private interface AppContext.Config may
// implement to supply a real StorageAccounts adapter (wired by cli.go's
// wireCrossServiceDependencies in a future milestone -- see interfaces.go).
// Absent (the common case for M7), NewStorageProvider falls back to the
// nil-safe noopStorageAccounts default.
type StorageAccountsProvider interface {
	GetAzureARMStorageAccounts() StorageAccounts
}

// ErrServiceBusPortMismatch is returned when Settings.ServiceBusPort (what
// ARM advertises as serviceBusEndpoint) disagrees with the actual
// services/azureservicebus listener's own configured port -- the same
// fail-fast rationale as ErrStorageVHostPortMismatch.
var ErrServiceBusPortMismatch = errors.New(
	"azurearm: --azure-arm-servicebus-port must match --azure-servicebus-port",
)

// ServiceBusPortProvider is a private interface AppContext.Config may
// implement to expose the actual services/azureservicebus listener's
// configured port, so Init can cross-check it against
// Settings.ServiceBusPort. cli.go implements this via
// CLI.GetAzureServiceBusPort.
type ServiceBusPortProvider interface {
	GetAzureServiceBusPort() int
}

// ErrKeyVaultPortMismatch is returned when Settings.KeyVaultPort (what ARM
// advertises in vaultUri) disagrees with the Key Vault listener's own port.
var ErrKeyVaultPortMismatch = errors.New(
	"azurearm: --azure-arm-keyvault-port must match --azure-keyvault-port",
)

// KeyVaultPortProvider is a private interface AppContext.Config may implement
// to expose the Key Vault listener's configured port.
type KeyVaultPortProvider interface {
	GetAzureKeyVaultPort() int
}

// checkKeyVaultPort fails fast when the advertised Key Vault port would diverge
// from the Key Vault listener's own.
func checkKeyVaultPort(cfg any, settings Settings) error {
	if settings.AdvertiseKeyVault != "" {
		return nil
	}

	kp, ok := cfg.(KeyVaultPortProvider)
	if !ok {
		return nil
	}

	if actual := kp.GetAzureKeyVaultPort(); actual != 0 && actual != settings.KeyVaultPort {
		return fmt.Errorf("%w: azure-arm-keyvault-port=%d, azure-keyvault-port=%d",
			ErrKeyVaultPortMismatch, settings.KeyVaultPort, actual)
	}

	return nil
}

// ErrGraphPortMismatch is returned when Settings.GraphPort (what ARM advertises as the Graph endpoint) disagrees with
// the Microsoft Graph listener's own port.
var ErrGraphPortMismatch = errors.New("azurearm: --azure-arm-graph-port must match --azure-graph-port")

// GraphPortProvider is a private interface AppContext.Config may implement to expose the Graph listener's port.
type GraphPortProvider interface {
	GetAzureGraphPort() int
}

func checkGraphPort(cfg any, settings Settings) error {
	if settings.AdvertiseGraph != "" {
		return nil
	}

	gp, ok := cfg.(GraphPortProvider)
	if !ok {
		return nil
	}

	if actual := gp.GetAzureGraphPort(); actual != 0 && actual != settings.GraphPort {
		return fmt.Errorf(
			"%w: azure-arm-graph-port=%d, azure-graph-port=%d",
			ErrGraphPortMismatch,
			settings.GraphPort,
			actual,
		)
	}

	return nil
}

// Provider implements service.Provider for the ARM emulation.
//
// Like services/azureblob/azurequeue/azuretable/cosmosdb/azureservicebus,
// AzureARM does not register a RouteMatcher into the shared AWS single-port
// Router -- see handler.go's RouteMatcher doc comment. It is registered in
// cli.go's getMostRecentServiceProviders like every other provider; only its
// RouteMatcher (which always returns false) is inert.
type Provider struct{}

// Name returns the service provider name.
func (p *Provider) Name() string { return "AzureARM" }

// Init initializes the ARM backend, registry (with the Storage RP
// registered), AAD token issuer, and handler. The configured port
// (Settings.Port, default DefaultPort) is only recorded here; the actual TCP
// bind happens synchronously in Handler.StartWorker, so a port-in-use
// failure is returned to the caller directly instead of being discovered
// later from a background goroutine.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	if ctx == nil {
		return nil, ErrNilAppContext
	}

	settings := DefaultSettings()
	if cp, ok := ctx.Config.(ConfigProvider); ok {
		settings = cp.GetAzureARMSettings()
	}

	var dataPlane StorageAccounts
	if sp, ok := ctx.Config.(StorageAccountsProvider); ok {
		dataPlane = sp.GetAzureARMStorageAccounts()
	}

	if settings.AdvertiseStorageVHost == "" {
		if vp, ok := ctx.Config.(VHostPortProvider); ok {
			if actual := vp.GetAzureStorageVHostPort(); actual != 0 && actual != settings.StorageVHostPort {
				return nil, fmt.Errorf("%w: azure-arm-storage-vhost-port=%d, azure-storage-vhost-port=%d",
					ErrStorageVHostPortMismatch, settings.StorageVHostPort, actual)
			}
		}
	}

	if settings.AdvertiseServiceBus == "" {
		if sp, ok := ctx.Config.(ServiceBusPortProvider); ok {
			if actual := sp.GetAzureServiceBusPort(); actual != 0 && actual != settings.ServiceBusPort {
				return nil, fmt.Errorf("%w: azure-arm-servicebus-port=%d, azure-servicebus-port=%d",
					ErrServiceBusPortMismatch, settings.ServiceBusPort, actual)
			}
		}
	}

	if err := checkKeyVaultPort(ctx.Config, settings); err != nil {
		return nil, err
	}

	if err := checkGraphPort(ctx.Config, settings); err != nil {
		return nil, err
	}

	backend := NewInMemoryBackend()
	registry := NewRegistry(backend)

	storageCfg := StorageEndpointConfig{
		VHostOverride: settings.AdvertiseStorageVHost,
		VHostPort:     settings.StorageVHostPort,
	}
	registry.Register(NewStorageProvider(storageCfg, dataPlane))

	sbCfg := ServiceBusEndpointConfig{
		Override: settings.AdvertiseServiceBus,
		Port:     settings.ServiceBusPort,
	}
	registry.Register(NewServiceBusProvider(sbCfg, nil))

	registry.Register(NewKeyVaultProvider(KeyVaultEndpointConfig{
		Override: settings.AdvertiseKeyVault,
		Port:     settings.KeyVaultPort,
	}, nil))

	registry.Register(NewEventHubProvider(EventHubEndpointConfig{
		Override: settings.AdvertiseEventHub,
		Port:     settings.EventHubPort,
		AMQPPort: settings.EventHubAMQPPort,
	}, nil))

	registry.Register(NewEventGridProvider(EventGridEndpointConfig{
		Override: settings.AdvertiseEventGrid,
		Port:     settings.EventGridPort,
	}, nil))

	registry.Register(NewAppConfigProvider(AppConfigEndpointConfig{
		Override: settings.AdvertiseAppConfig,
		Port:     settings.AppConfigPort,
	}, nil))

	registry.Register(NewContainerRegistryProvider(ContainerRegistryEndpointConfig{
		Override: settings.AdvertiseACR,
		Port:     settings.ACRPort,
	}, nil))

	registry.Register(NewMonitorProvider(namespaceMicrosoftOperationalInsights, backend, nil))
	registry.Register(NewMonitorProvider(namespaceMicrosoftInsights, backend, nil))

	ids := registerLocalProviders(ctx, registry, backend, settings)
	registerContainerProviders(registry, backend, newACIEngine(ctx, settings), ids)
	registerContainerService(ctx, registry, backend, ids, settings)

	registry.Register(NewCosmosProvider(CosmosEndpointConfig{Override: settings.AdvertiseCosmosDB}, nil))

	issuer, err := aadauth.NewIssuer()
	if err != nil {
		return nil, err
	}

	handler := NewHandler(backend, registry, issuer, settings)

	return handler, nil
}

// registerFlexProviders registers the PostgreSQL and MySQL flexible server providers; with the docker engine each
// server runs a real container, dialed on first use.
func registerFlexProviders(ctx *service.AppContext, registry *Registry, network *NetworkProvider, settings Settings) {
	for _, p := range []*FlexDBProvider{NewPostgresFlexProvider(), NewMySQLFlexProvider()} {
		p.SetNetwork(network)

		if settings.DBEngine == DBEngineDocker {
			p.SetEngine(NewDockerFlexEngine(p.flavor.kind, DockerFlexConfig{
				Runtime: func() (container.Runtime, error) {
					return container.NewRuntime(container.Config{Logger: ctx.Logger})
				},
				Ports: ctx.PortAlloc, Logger: ctx.Logger,
			}))
		}

		registry.Register(p)
	}
}

func newACIEngine(ctx *service.AppContext, settings Settings) *ACIEngine {
	if settings.ACIEngine == ACIEngineNone {
		return NewACIEngine(nil, aciDisabledReason)
	}

	lr := gcpserve.NewLazyRuntime(ctx.Logger, "azurearm.aci.runtime")
	engine := NewACIEngine(func() (ACIRuntime, error) {
		r, err := lr.Get()
		if err != nil {
			return nil, err
		}

		return NewDockerACIRuntime(r), nil
	}, "")

	engine.SetOnClose(func() { _ = lr.Close() })

	if ctx.PortAlloc != nil {
		engine.SetPortPool(ctx.PortAlloc)
	}

	return engine
}

const aciDisabledReason = "the container engine is disabled (--azure-arm-aci-engine=none); container groups cannot run"

// registerLocalProviders registers the providers that have no data-plane seam to wire up later.
func registerLocalProviders(
	ctx *service.AppContext,
	registry *Registry,
	backend *InMemoryBackend,
	settings Settings,
) *identityService {
	msi := NewManagedIdentityProvider(settings.TenantID)
	ids := newIdentityService(msi, settings.TenantID)
	network := NewNetworkProvider(backend)

	web := NewWebProvider(nil)
	web.ids = ids

	registry.Register(NewRedisProvider(nil))
	registry.Register(NewSQLProvider(nil))
	registerFlexProviders(ctx, registry, network, settings)

	apim := NewAPIMProvider()
	apim.SetAllowExternal(settings.APIMAllowExternal)
	registry.Register(apim)
	registry.Register(web)
	registry.Register(network)
	registry.Register(msi)
	registry.Register(NewAuthorizationProvider(settings.TenantID, msi))
	registry.Register(NewComputeProvider(backend, network, ids))

	return ids
}

// registerContainerProviders registers Container Instances and Container Apps over one shared container engine.
func registerContainerProviders(registry *Registry, backend *InMemoryBackend, engine *ACIEngine, ids *identityService) {
	aci := NewContainerInstanceProvider(engine)
	aci.ids = ids
	registry.Register(aci)

	var network *NetworkProvider
	if np, ok := registry.ProviderNamed(namespaceMicrosoftNetwork); ok {
		network, _ = np.(*NetworkProvider)
	}

	registry.Register(NewContainerAppsProvider(engine, backend, network, ids))
}

// registerContainerService registers Microsoft.ContainerService; the docker engine dials the container runtime on the
// first cluster, not at startup.
func registerContainerService(
	ctx *service.AppContext, registry *Registry, backend *InMemoryBackend, ids *identityService, settings Settings,
) {
	var network *NetworkProvider
	if np, ok := registry.ProviderNamed(namespaceMicrosoftNetwork); ok {
		network, _ = np.(*NetworkProvider)
	}

	aks := NewContainerServiceProvider(backend, network, ids)
	aks.SetResourceGroupCascade(registry.DeleteResourcesInGroup)

	if settings.AKSEngine == AKSEngineDocker {
		aks.SetEngineFactory(func() (*k3sengine.Engine, error) {
			rt, err := container.NewRuntime(container.Config{Logger: ctx.Logger})
			if err != nil {
				return nil, fmt.Errorf("aks engine: container runtime: %w", err)
			}

			eng, err := k3sengine.New(k3sengine.Config{
				Runtime: rt, Ports: ctx.PortAlloc, Logger: ctx.Logger, Prefix: "aks",
				Host: os.Getenv("AKS_CLUSTER_HOST"), Token: os.Getenv("AKS_CLUSTER_TOKEN"),
			})
			if err != nil {
				_ = rt.Close()

				return nil, fmt.Errorf("aks engine: %w", err)
			}

			return eng, nil
		}, os.Getenv("AKS_K3S_IMAGE"))
	}

	registry.Register(aks)
}
