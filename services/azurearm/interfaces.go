package azurearm

import (
	"context"
	"time"
)

// StorageAccounts is the narrow, consumer-defined seam the Microsoft.Storage
// resource provider (rp_storage.go) delegates account lifecycle events
// through, mirroring the wireCrossServiceDependencies adapter pattern
// cli_adapters.go already uses ~60 times for AWS (see AZURE.md section
// 10.6). It is declared here even though M7's Storage RP does not need it to
// serve Blob/Queue/Table traffic today (those backends are already
// account-name-agnostic -- AZURE.md section 10.4) -- so that M10's
// per-account namespacing (RegisterAccount/DeleteAccount keying each data
// plane's top-level map by account name) is additive: rp_storage.go already
// calls these hooks, they're just no-ops until M10 wires a real adapter in
// cli_adapters.go.
//
// CosmosResources (M10's equivalent) is deliberately NOT declared here yet
// -- AZURE.md's own scope note prefers keeping this milestone's interface
// surface to what it actually uses, over speculatively declaring shapes for
// resource providers this milestone doesn't implement. ServiceBusEntities
// below is M9's version of the same pattern, now that M9 actually needs it.
type StorageAccounts interface {
	// RegisterAccount is called when the Storage RP creates a new storage
	// account. A nil-safe no-op default (noopStorageAccounts) is used when no
	// real adapter is wired, so services/azurearm works standalone in unit
	// tests and degrades gracefully if the data plane is disabled.
	RegisterAccount(name string) error
	// DeleteAccount is called when the Storage RP deletes a storage account.
	DeleteAccount(name string) error
}

// noopStorageAccounts is the nil-safe default StorageAccounts implementation.
type noopStorageAccounts struct{}

func (noopStorageAccounts) RegisterAccount(string) error { return nil }
func (noopStorageAccounts) DeleteAccount(string) error   { return nil }

var _ StorageAccounts = noopStorageAccounts{}

// ServiceBusEntities is the narrow, consumer-defined seam the
// Microsoft.ServiceBus resource provider (rp_servicebus.go) delegates
// queue/topic/subscription lifecycle through, mirroring StorageAccounts's
// shape above. Uses only primitive types (time.Duration, int, string)
// rather than services/azureservicebus's own EntityConfig, so this package
// never imports a data-plane service package (AZURE.md section 10.6) --
// the real adapter translating these calls into azureservicebus.EntityConfig
// lives in cli_adapters.go, which already imports across services.
//
// Unlike StorageAccounts, ServiceBusEntities calls are load-bearing for
// M9's test plan: services/azureservicebus (M5) is one-process-one-namespace
// with no ARM-shaped state of its own, so a Terraform-created queue/topic/
// subscription must actually exist in the real M5 backend for the REST
// send->peek-lock->complete liveness round-trip to succeed -- this is not
// an optional best-effort hook the way Storage's RegisterAccount is.
type ServiceBusEntities interface {
	CreateQueue(name string, lockDuration, defaultMessageTTL time.Duration, maxDeliveryCount int) error
	DeleteQueue(name string) error
	QueueExists(name string) bool

	CreateTopic(name string, defaultMessageTTL time.Duration) error
	DeleteTopic(name string) error
	TopicExists(name string) bool

	CreateSubscription(topic, name string, lockDuration time.Duration, maxDeliveryCount int) error
	DeleteSubscription(topic, name string) error
	SubscriptionExists(topic, name string) bool
}

// noopServiceBusEntities is the nil-safe default ServiceBusEntities
// implementation, used until wireAzureARMResourceProviders (cli.go) wires a
// real adapter post-construction.
type noopServiceBusEntities struct{}

func (noopServiceBusEntities) CreateQueue(string, time.Duration, time.Duration, int) error {
	return nil
}
func (noopServiceBusEntities) DeleteQueue(string) error { return nil }
func (noopServiceBusEntities) QueueExists(string) bool  { return false }

func (noopServiceBusEntities) CreateTopic(string, time.Duration) error { return nil }
func (noopServiceBusEntities) DeleteTopic(string) error                { return nil }
func (noopServiceBusEntities) TopicExists(string) bool                 { return false }

func (noopServiceBusEntities) CreateSubscription(string, string, time.Duration, int) error {
	return nil
}
func (noopServiceBusEntities) DeleteSubscription(string, string) error { return nil }
func (noopServiceBusEntities) SubscriptionExists(string, string) bool  { return false }

var _ ServiceBusEntities = noopServiceBusEntities{}

// EventHubInfo is the data plane's view of an event hub.
type EventHubInfo struct {
	CreatedAt    time.Time
	PartitionIDs []string
	Retention    time.Duration
}

// EventHubRule is the data plane's view of an authorization rule.
type EventHubRule struct {
	PrimaryKey   string
	SecondaryKey string
	Rights       []string
}

// EventHubs is the seam the Microsoft.EventHub resource provider uses to keep
// the Event Hubs data plane in step with ARM. Primitive types only, so this
// package never imports services/azureeventhubs. hub is empty for
// namespace-scoped rules.
type EventHubs interface {
	EnsureNamespace(name string) error
	DeleteNamespace(name string) error
	CreateHub(ns, name string, partitionCount int, retention time.Duration) (EventHubInfo, error)
	HubInfo(ns, name string) (EventHubInfo, bool)
	DeleteHub(ns, name string) error
	CreateConsumerGroup(ns, hub, name string) error
	DeleteConsumerGroup(ns, hub, name string) error
	PutRule(ns, hub, name string, rights []string) (EventHubRule, error)
	GetRule(ns, hub, name string) (EventHubRule, bool)
	DeleteRule(ns, hub, name string) error
}

type noopEventHubs struct{}

func (noopEventHubs) EnsureNamespace(string) error { return nil }
func (noopEventHubs) DeleteNamespace(string) error { return nil }
func (noopEventHubs) CreateHub(string, string, int, time.Duration) (EventHubInfo, error) {
	return EventHubInfo{}, ErrEventHubNotFound
}
func (noopEventHubs) HubInfo(string, string) (EventHubInfo, bool) { return EventHubInfo{}, false }
func (noopEventHubs) DeleteHub(string, string) error              { return nil }
func (noopEventHubs) CreateConsumerGroup(string, string, string) error {
	return nil
}
func (noopEventHubs) DeleteConsumerGroup(string, string, string) error { return nil }
func (noopEventHubs) PutRule(string, string, string, []string) (EventHubRule, error) {
	return EventHubRule{}, ErrEventHubRuleNotFound
}
func (noopEventHubs) GetRule(string, string, string) (EventHubRule, bool) {
	return EventHubRule{}, false
}
func (noopEventHubs) DeleteRule(string, string, string) error { return nil }

var _ EventHubs = noopEventHubs{}

// KeyVaults is the seam the Microsoft.KeyVault resource provider uses to
// register and remove vaults in the Key Vault data plane. Primitive types only,
// so this package never imports services/azurekeyvault.
type KeyVaults interface {
	CreateVault(name string, retentionDays int, purgeProtection bool)
	DeleteVault(name string)
	// SoftDeleteVault retires the vault, keeping its contents for RecoverVault.
	SoftDeleteVault(name string)
	// RecoverVault restores a soft-deleted vault with its contents.
	RecoverVault(name string)
	// PurgeVault permanently discards a soft-deleted vault's contents.
	PurgeVault(name string)
}

type noopKeyVaults struct{}

func (noopKeyVaults) CreateVault(string, int, bool) {}
func (noopKeyVaults) DeleteVault(string)            {}
func (noopKeyVaults) SoftDeleteVault(string)        {}
func (noopKeyVaults) RecoverVault(string)           {}
func (noopKeyVaults) PurgeVault(string)             {}

var _ KeyVaults = noopKeyVaults{}

// CosmosDatabase is a Cosmos DB database as the data plane reports it.
type CosmosDatabase struct {
	Modified time.Time
	ID       string
	RID      string
}

// CosmosContainer is a Cosmos DB container as the data plane reports it; the policy fields are raw JSON.
type CosmosContainer struct {
	Modified                 time.Time
	DefaultTTL               *int
	ID                       string
	RID                      string
	PartitionKeyPaths        []string
	IndexingPolicy           []byte
	UniqueKeyPolicy          []byte
	ConflictResolutionPolicy []byte
}

// CosmosThroughput is a provisioned-throughput setting: manual RU/s or an autoscale maximum.
type CosmosThroughput = cosmosThroughput

// CosmosResources is the seam the Microsoft.DocumentDB resource provider uses to reach the Cosmos DB data plane.
// Errors are the package's ErrCosmos* sentinels.
type CosmosResources interface {
	// Endpoint reports the data-plane listener's port and whether it serves TLS.
	Endpoint() (port int, tls bool)
	// MasterKey is the data plane's configured master key.
	MasterKey() string
	// AcceptKey and RevokeKey manage extra keys the data plane authorizes.
	AcceptKey(key string)
	RevokeKey(key string)

	CreateDatabase(id string) (CosmosDatabase, error)
	GetDatabase(id string) (CosmosDatabase, error)
	ListDatabases() []CosmosDatabase
	DeleteDatabase(id string) error

	// PutContainer creates the container, or replaces its mutable settings when it already exists.
	PutContainer(db string, c CosmosContainer) (CosmosContainer, error)
	GetContainer(db, id string) (CosmosContainer, error)
	ListContainers(db string) ([]CosmosContainer, error)
	DeleteContainer(db, id string) error

	// GetThroughput reads the offer of a database (container == "") or container; ErrCosmosThroughputNotFound
	// when none is provisioned.
	GetThroughput(db, container string) (CosmosThroughput, error)
	// PutThroughput creates or replaces that offer.
	PutThroughput(db, container string, t CosmosThroughput) error

	// SetRegions publishes the account's regional topology (the lowest priority is the write region).
	SetRegions(regions []CosmosRegion, multiWrite bool)
}

// CosmosAccountPolicies are the account settings the data plane honours: the default consistency level (with its
// bounded-staleness limits), service-managed failover, and the backup mode ("Periodic", "Continuous7Days" or
// "Continuous30Days"). A data plane that models them implements CosmosPolicyResources.
type CosmosAccountPolicies struct {
	Consistency        string
	Backup             string
	MaxStalenessPrefix int64
	MaxIntervalSeconds int
	AutomaticFailover  bool
}

// CosmosPolicyResources is the optional seam for CosmosAccountPolicies.
type CosmosPolicyResources interface {
	SetAccountPolicies(CosmosAccountPolicies)
}

// CosmosRegion is one account location and its failover priority.
type CosmosRegion struct {
	Name     string
	Priority int
}

type noopCosmosResources struct{}

func (noopCosmosResources) Endpoint() (int, bool) { return DefaultCosmosDBPort, false }
func (noopCosmosResources) MasterKey() string     { return "" }
func (noopCosmosResources) AcceptKey(string)      {}
func (noopCosmosResources) RevokeKey(string)      {}
func (noopCosmosResources) CreateDatabase(string) (CosmosDatabase, error) {
	return CosmosDatabase{}, ErrCosmosUnavailable
}
func (noopCosmosResources) GetDatabase(string) (CosmosDatabase, error) {
	return CosmosDatabase{}, ErrCosmosUnavailable
}
func (noopCosmosResources) ListDatabases() []CosmosDatabase { return nil }
func (noopCosmosResources) DeleteDatabase(string) error     { return ErrCosmosUnavailable }
func (noopCosmosResources) PutContainer(string, CosmosContainer) (CosmosContainer, error) {
	return CosmosContainer{}, ErrCosmosUnavailable
}
func (noopCosmosResources) GetContainer(string, string) (CosmosContainer, error) {
	return CosmosContainer{}, ErrCosmosUnavailable
}
func (noopCosmosResources) ListContainers(string) ([]CosmosContainer, error) {
	return nil, ErrCosmosUnavailable
}
func (noopCosmosResources) DeleteContainer(string, string) error { return ErrCosmosUnavailable }
func (noopCosmosResources) GetThroughput(string, string) (CosmosThroughput, error) {
	return CosmosThroughput{}, ErrCosmosUnavailable
}
func (noopCosmosResources) PutThroughput(string, string, CosmosThroughput) error {
	return ErrCosmosUnavailable
}

func (noopCosmosResources) SetRegions([]CosmosRegion, bool) {}

var _ CosmosResources = noopCosmosResources{}

// EventGridTopic is the data plane's view of a custom topic.
type EventGridTopic struct {
	CreatedAt   time.Time
	InputSchema string
}

// EventGridDomain is the data plane's view of a domain.
type EventGridDomain struct {
	CreatedAt   time.Time
	InputSchema string
	AutoCreate  bool
	AutoDelete  bool
}

// EventGridDomainTopic is the data plane's view of a domain topic.
type EventGridDomainTopic struct {
	CreatedAt time.Time
	Name      string
}

// EventGrid is the seam the Microsoft.EventGrid resource provider uses to keep
// the Event Grid data plane in step with ARM. Subscription and namespace-topic
// subscription properties cross as ARM property maps, so this package never
// imports services/azureeventgrid.
type EventGrid interface {
	PutTopic(name, resourceID, inputSchema string) (EventGridTopic, error)
	TopicInfo(name string) (EventGridTopic, bool)
	DeleteTopic(name string) error
	TopicKeys(name string) (key1, key2 string, err error)
	RegenerateTopicKey(name, keyName string) (key1, key2 string, err error)

	PutSubscription(ctx context.Context, topic, name string, props map[string]any) (map[string]any, error)
	GetSubscription(topic, name string) (map[string]any, bool)
	DeleteSubscription(topic, name string) error
	SubscriptionFullURL(topic, name string) (string, error)

	PutNamespace(name string) error
	DeleteNamespace(name string) error
	NamespaceKeys(name string) (key1, key2 string, err error)
	RegenerateNamespaceKey(name, keyName string) (key1, key2 string, err error)
	PutNamespaceTopic(ns, name string) (time.Time, error)
	NamespaceTopicCreated(ns, name string) (time.Time, bool)
	DeleteNamespaceTopic(ns, name string) error

	PutPullSubscription(ns, topic, name string, props map[string]any) (map[string]any, error)
	GetPullSubscription(ns, topic, name string) (map[string]any, bool)
	DeletePullSubscription(ns, topic, name string) error

	PutDomain(name, resourceID, inputSchema string, autoCreate, autoDelete *bool) (EventGridDomain, error)
	DomainInfo(name string) (EventGridDomain, bool)
	DeleteDomain(name string) error
	DomainKeys(name string) (key1, key2 string, err error)
	RegenerateDomainKey(name, keyName string) (key1, key2 string, err error)
	PutDomainTopic(domain, name string) (time.Time, error)
	DomainTopic(domain, name string) (EventGridDomainTopic, bool)
	DomainTopics(domain string) ([]EventGridDomainTopic, error)
	DeleteDomainTopic(domain, name string) error

	// The scoped calls address a custom topic (domain empty), a domain (topic empty) or a domain topic.
	PutScopedSubscription(ctx context.Context, domain, topic, name string, props map[string]any) (map[string]any, error)
	GetScopedSubscription(domain, topic, name string) (map[string]any, bool)
	DeleteScopedSubscription(domain, topic, name string) error
	ScopedSubscriptionFullURL(domain, topic, name string) (string, error)
	DeliveryAttributes(domain, topic, name string) ([]map[string]any, error)
	// PatchScopedSubscription merges an EventSubscriptionUpdateParameters body into an existing subscription.
	PatchScopedSubscription(
		ctx context.Context,
		domain, topic, name string,
		patch map[string]any,
	) (map[string]any, error)

	// The resource calls address event subscriptions of an ARM scope: a subscription, resource group or resource.
	PutResourceSubscription(ctx context.Context, scopeID, name string, props map[string]any) (map[string]any, error)
	GetResourceSubscription(scopeID, name string) (map[string]any, bool)
	DeleteResourceSubscription(scopeID, name string) error
	ResourceSubscriptionFullURL(scopeID, name string) (string, error)
	ResourceDeliveryAttributes(scopeID, name string) ([]map[string]any, error)
	PatchResourceSubscription(ctx context.Context, scopeID, name string, patch map[string]any) (map[string]any, error)
}

type noopEventGrid struct{}

func (noopEventGrid) PutTopic(string, string, string) (EventGridTopic, error) {
	return EventGridTopic{}, ErrEventGridUnavailable
}
func (noopEventGrid) TopicInfo(string) (EventGridTopic, bool) { return EventGridTopic{}, false }
func (noopEventGrid) DeleteTopic(string) error                { return nil }
func (noopEventGrid) TopicKeys(string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) RegenerateTopicKey(string, string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) PutSubscription(context.Context, string, string, map[string]any) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}
func (noopEventGrid) GetSubscription(string, string) (map[string]any, bool) { return nil, false }
func (noopEventGrid) DeleteSubscription(string, string) error               { return nil }
func (noopEventGrid) SubscriptionFullURL(string, string) (string, error) {
	return "", ErrEventGridUnavailable
}
func (noopEventGrid) PutNamespace(string) error    { return ErrEventGridUnavailable }
func (noopEventGrid) DeleteNamespace(string) error { return nil }
func (noopEventGrid) NamespaceKeys(string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) RegenerateNamespaceKey(string, string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) PutNamespaceTopic(string, string) (time.Time, error) {
	return time.Time{}, ErrEventGridUnavailable
}
func (noopEventGrid) NamespaceTopicCreated(string, string) (time.Time, bool) {
	return time.Time{}, false
}
func (noopEventGrid) DeleteNamespaceTopic(string, string) error { return nil }
func (noopEventGrid) PutPullSubscription(string, string, string, map[string]any) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}
func (noopEventGrid) GetPullSubscription(string, string, string) (map[string]any, bool) {
	return nil, false
}
func (noopEventGrid) DeletePullSubscription(string, string, string) error { return nil }

func (noopEventGrid) PutDomain(string, string, string, *bool, *bool) (EventGridDomain, error) {
	return EventGridDomain{}, ErrEventGridUnavailable
}
func (noopEventGrid) DomainInfo(string) (EventGridDomain, bool) { return EventGridDomain{}, false }
func (noopEventGrid) DeleteDomain(string) error                 { return nil }
func (noopEventGrid) DomainKeys(string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) RegenerateDomainKey(string, string) (string, string, error) {
	return "", "", ErrEventGridUnavailable
}

func (noopEventGrid) PutDomainTopic(string, string) (time.Time, error) {
	return time.Time{}, ErrEventGridUnavailable
}

func (noopEventGrid) DomainTopic(string, string) (EventGridDomainTopic, bool) {
	return EventGridDomainTopic{}, false
}

func (noopEventGrid) DomainTopics(string) ([]EventGridDomainTopic, error) {
	return nil, ErrEventGridUnavailable
}
func (noopEventGrid) DeleteDomainTopic(string, string) error { return nil }
func (noopEventGrid) PutScopedSubscription(
	context.Context, string, string, string, map[string]any,
) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}

func (noopEventGrid) GetScopedSubscription(string, string, string) (map[string]any, bool) {
	return nil, false
}
func (noopEventGrid) DeleteScopedSubscription(string, string, string) error { return nil }
func (noopEventGrid) PatchScopedSubscription(
	context.Context, string, string, string, map[string]any,
) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}
func (noopEventGrid) PatchResourceSubscription(
	context.Context, string, string, map[string]any,
) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}
func (noopEventGrid) ScopedSubscriptionFullURL(string, string, string) (string, error) {
	return "", ErrEventGridUnavailable
}

func (noopEventGrid) DeliveryAttributes(string, string, string) ([]map[string]any, error) {
	return nil, ErrEventGridUnavailable
}

func (noopEventGrid) PutResourceSubscription(context.Context, string, string, map[string]any) (map[string]any, error) {
	return nil, ErrEventGridUnavailable
}

func (noopEventGrid) GetResourceSubscription(string, string) (map[string]any, bool) {
	return nil, false
}
func (noopEventGrid) DeleteResourceSubscription(string, string) error { return nil }
func (noopEventGrid) ResourceSubscriptionFullURL(string, string) (string, error) {
	return "", ErrEventGridUnavailable
}

func (noopEventGrid) ResourceDeliveryAttributes(string, string) ([]map[string]any, error) {
	return nil, ErrEventGridUnavailable
}

var _ EventGrid = noopEventGrid{}

// AppConfigKey is one of a configuration store's access keys.
type AppConfigKey struct {
	LastModified time.Time
	ID           string
	Name         string
	Value        string
	ReadOnly     bool
}

// AppConfigKV is a key-value as the ARM keyValues resources see it.
type AppConfigKV struct {
	LastModified time.Time
	Value        *string
	ContentType  *string
	Tags         map[string]string
	Key          string
	Label        string
	ETag         string
	Locked       bool
}

// AppConfig is the seam the Microsoft.AppConfiguration resource provider uses to keep the
// App Configuration data plane in step with ARM, so stores, access keys and key-values are
// shared between both planes.
type AppConfig interface {
	PutStore(name string) (time.Time, error)
	StoreCreated(name string) (time.Time, bool)
	DeleteStore(name string) error
	StoreKeys(name string) ([]AppConfigKey, error)
	RegenerateStoreKey(name, keyID string) (AppConfigKey, error)

	PutKeyValue(store string, kv AppConfigKV) (AppConfigKV, error)
	GetKeyValue(store, key, label string) (AppConfigKV, bool)
	DeleteKeyValue(store, key, label string) error
	ListKeyValues(store string) ([]AppConfigKV, error)
}

type noopAppConfig struct{}

func (noopAppConfig) PutStore(string) (time.Time, error)    { return time.Time{}, ErrAppConfigUnavailable }
func (noopAppConfig) StoreCreated(string) (time.Time, bool) { return time.Time{}, false }
func (noopAppConfig) DeleteStore(string) error              { return nil }
func (noopAppConfig) StoreKeys(string) ([]AppConfigKey, error) {
	return nil, ErrAppConfigUnavailable
}

func (noopAppConfig) RegenerateStoreKey(string, string) (AppConfigKey, error) {
	return AppConfigKey{}, ErrAppConfigUnavailable
}

func (noopAppConfig) PutKeyValue(string, AppConfigKV) (AppConfigKV, error) {
	return AppConfigKV{}, ErrAppConfigUnavailable
}
func (noopAppConfig) GetKeyValue(string, string, string) (AppConfigKV, bool) {
	return AppConfigKV{}, false
}
func (noopAppConfig) DeleteKeyValue(string, string, string) error { return nil }
func (noopAppConfig) ListKeyValues(string) ([]AppConfigKV, error) { return nil, nil }

var _ AppConfig = noopAppConfig{}

// ContainerRegistryInfo is the data plane's view of a registry.
type ContainerRegistryInfo struct {
	CreatedAt    time.Time
	AdminEnabled bool
}

// ContainerRegistryCredentials are a registry's admin-user credentials.
type ContainerRegistryCredentials struct {
	Username  string
	Password  string
	Password2 string
}

// ContainerRegistries is the seam the Microsoft.ContainerRegistry resource provider uses to keep the registry
// data plane in step with ARM. Primitive types only, so this package never imports the data-plane service.
type ContainerRegistries interface {
	PutRegistry(name string, adminEnabled bool) (ContainerRegistryInfo, error)
	RegistryInfo(name string) (ContainerRegistryInfo, bool)
	DeleteRegistry(name string) error
	Credentials(name string) (ContainerRegistryCredentials, error)
	RegenerateCredential(name, which string) (ContainerRegistryCredentials, error)
	SetWebhook(registry string, w ContainerRegistryWebhook) error
	DeleteWebhook(registry, name string) error
	PingWebhook(registry, name string) (string, error)
	WebhookEvents(registry, name string) ([]map[string]any, error)
}

// ContainerRegistryWebhook is a registry webhook the data plane fires on image and chart pushes and deletes.
type ContainerRegistryWebhook struct {
	Headers    map[string]string
	Name       string
	ServiceURI string
	Scope      string
	Status     string
	Actions    []string
}

type noopContainerRegistries struct{}

func (noopContainerRegistries) PutRegistry(string, bool) (ContainerRegistryInfo, error) {
	return ContainerRegistryInfo{}, ErrACRUnavailable
}

func (noopContainerRegistries) RegistryInfo(string) (ContainerRegistryInfo, bool) {
	return ContainerRegistryInfo{}, false
}

func (noopContainerRegistries) DeleteRegistry(string) error { return nil }

func (noopContainerRegistries) Credentials(string) (ContainerRegistryCredentials, error) {
	return ContainerRegistryCredentials{}, ErrACRUnavailable
}

func (noopContainerRegistries) RegenerateCredential(string, string) (ContainerRegistryCredentials, error) {
	return ContainerRegistryCredentials{}, ErrACRUnavailable
}

func (noopContainerRegistries) SetWebhook(string, ContainerRegistryWebhook) error { return nil }

func (noopContainerRegistries) DeleteWebhook(string, string) error { return nil }

func (noopContainerRegistries) PingWebhook(string, string) (string, error) {
	return "", ErrACRUnavailable
}

func (noopContainerRegistries) WebhookEvents(string, string) ([]map[string]any, error) {
	return nil, ErrACRUnavailable
}

var _ ContainerRegistries = noopContainerRegistries{}
