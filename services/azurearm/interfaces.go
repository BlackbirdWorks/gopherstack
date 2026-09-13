package azurearm

import "time"

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
