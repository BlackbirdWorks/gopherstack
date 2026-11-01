package azurearm

import (
	"errors"
	"net/http"
)

// Sentinel errors for Azure Resource Manager (ARM) emulation. Mapped to ARM
// error codes/HTTP statuses in the errorDetails table (handler.go), modeled
// on services/sqs's errorDetails pattern.
var (
	ErrInvalidResourceID      = errors.New("azurearm: invalid resource ID")
	ErrResourceGroupNotFound  = errors.New("azurearm: resource group not found")
	ErrResourceNotFound       = errors.New("azurearm: resource not found")
	ErrSubscriptionNotFound   = errors.New("azurearm: subscription not found")
	ErrProviderNotFound       = errors.New("azurearm: resource provider not found")
	ErrInvalidRequestBody     = errors.New("azurearm: invalid request body")
	ErrStorageAccountNotFound = errors.New("azurearm: storage account not found")

	ErrServiceBusNamespaceNotFound    = errors.New("azurearm: service bus namespace not found")
	ErrServiceBusQueueNotFound        = errors.New("azurearm: service bus queue not found")
	ErrServiceBusTopicNotFound        = errors.New("azurearm: service bus topic not found")
	ErrServiceBusSubscriptionNotFound = errors.New("azurearm: service bus subscription not found")

	ErrKeyVaultNotFound = errors.New("azurearm: key vault not found")

	ErrEventHubNamespaceNotFound     = errors.New("azurearm: event hubs namespace not found")
	ErrEventHubNotFound              = errors.New("azurearm: event hub not found")
	ErrEventHubConsumerGroupNotFound = errors.New("azurearm: event hubs consumer group not found")
	ErrEventHubRuleNotFound          = errors.New("azurearm: event hubs authorization rule not found")

	ErrEventGridTopicNotFound          = errors.New("azurearm: event grid topic not found")
	ErrEventGridSubscriptionNotFound   = errors.New("azurearm: event subscription not found")
	ErrEventGridNamespaceNotFound      = errors.New("azurearm: event grid namespace not found")
	ErrEventGridNamespaceTopicNotFound = errors.New("azurearm: event grid namespace topic not found")
	ErrEventGridDomainNotFound         = errors.New("azurearm: event grid domain not found")
	ErrEventGridDomainTopicNotFound    = errors.New("azurearm: event grid domain topic not found")
	ErrEventGridUnavailable            = errors.New("azurearm: event grid data plane is not available")

	ErrAppConfigStoreNotFound    = errors.New("azurearm: app configuration store not found")
	ErrAppConfigKeyValueNotFound = errors.New("azurearm: app configuration key-value not found")
	ErrAppConfigReplicaNotFound  = errors.New("azurearm: app configuration replica not found")
	ErrAppConfigDeletedNotFound  = errors.New("azurearm: deleted app configuration store not found")
	ErrAppConfigKeyLocked        = errors.New("azurearm: app configuration key-value is read-only")
	ErrAppConfigNameTaken        = errors.New("azurearm: app configuration store name is already in use")
	ErrAppConfigSoftDeleted      = errors.New("azurearm: app configuration store name is soft-deleted")
	ErrAppConfigPurgeProtected   = errors.New("azurearm: app configuration store has purge protection enabled")
	ErrAppConfigUnavailable      = errors.New("azurearm: app configuration data plane is not available")

	errWebSiteName             = errors.New("site name must be 2-60 letters, digits or hyphens")
	errWebAppSetting           = errors.New("app setting values must be strings")
	errWebAllowRequired        = errors.New("properties.allow is required")
	errWebPlanRequired         = errors.New("properties.serverFarmId is required for a web app")
	errWebPlanName             = errors.New("app service plan name must be 2-60 letters, digits or hyphens")
	ErrWebSiteNotFound         = errors.New("azurearm: web site not found")
	ErrWebPlanNotFound         = errors.New("azurearm: app service plan not found")
	ErrWebFunctionNotFound     = errors.New("azurearm: function not found")
	ErrWebKeyNotFound          = errors.New("azurearm: function key not found")
	ErrWebConfigNotFound       = errors.New("azurearm: site configuration not found")
	ErrWebNameTaken            = errors.New("azurearm: site name is already in use")
	ErrWebPlanInUse            = errors.New("azurearm: app service plan still has sites")
	ErrWebUnsupportedKind      = errors.New("azurearm: only Linux web apps and function apps are supported")
	ErrWebInvalidSiteConfig    = errors.New("azurearm: invalid site configuration")
	ErrWebPlanNotLinux         = errors.New("azurearm: Linux app needs a Linux (reserved) plan")
	ErrWebNotFunctionApp       = errors.New("azurearm: operation is only valid for function apps")
	ErrWebInvalidKeyType       = errors.New("azurearm: key type must be functionKeys or systemKeys")
	ErrWebSwapPending          = errors.New("azurearm: slot is prepared for a swap")
	ErrWebSlotLimit            = errors.New("azurearm: deployment slot limit reached for the plan tier")
	ErrWebSlotName             = errors.New("azurearm: invalid deployment slot name")
	ErrWebSwapFailed           = errors.New("azurearm: slot swap failed")
	ErrWebFunctionsUnavailable = errors.New("azurearm: azure functions data plane is not available")

	ErrACRRegistryNotFound    = errors.New("azurearm: container registry not found")
	ErrACRReplicationNotFound = errors.New("azurearm: container registry replication not found")
	ErrACRWebhookNotFound     = errors.New("azurearm: container registry webhook not found")
	ErrACRNameTaken           = errors.New("azurearm: container registry name is already in use")
	ErrACRAdminDisabled       = errors.New("azurearm: container registry admin user is not enabled")
	ErrACRInvalidSKU          = errors.New("azurearm: container registry sku name must be Basic, Standard or Premium")
	ErrACRPremiumRequired     = errors.New("azurearm: georeplication requires a Premium registry")
	ErrACRInvalidCredential   = errors.New("azurearm: credential name must be password or password2")
	ErrACRUnavailable         = errors.New("azurearm: container registry data plane is not available")

	ErrCosmosAccountNotFound    = errors.New("azurearm: cosmos db account not found")
	ErrCosmosDatabaseNotFound   = errors.New("azurearm: cosmos db database not found")
	ErrCosmosContainerNotFound  = errors.New("azurearm: cosmos db container not found")
	ErrCosmosThroughputNotFound = errors.New("azurearm: cosmos db throughput settings not found")
	ErrCosmosConflict           = errors.New("azurearm: cosmos db resource conflict")
	ErrCosmosBadRequest         = errors.New("azurearm: cosmos db invalid request")
	ErrCosmosUnavailable        = errors.New("azurearm: cosmos db data plane unavailable")
	errCosmosAccountOtherGroup  = errors.New(
		"azurearm: cosmos db account name already exists in another resource group",
	)

	errVaultSoftDeleted     = errors.New("azurearm: key vault name is in use by a soft-deleted vault")
	ErrDeletedVaultNotFound = errors.New("azurearm: deleted key vault not found")
	ErrVaultPurgeProtected  = errors.New("azurearm: key vault has purge protection enabled")

	errVaultExistsInOtherResourceGroup = errors.New("azurearm: key vault name already exists in another resource group")

	// ErrSnapshotResourceGroupNull/ErrSnapshotResourceNull are returned by
	// Restore when a persisted snapshot's map holds a JSON null entry, which
	// decodes to a nil pointer that would panic on first dereference if
	// stored as-is (same class of bug services/cosmosdb's persistence.go
	// guards against).
	ErrSnapshotResourceGroupNull = errors.New("azurearm: restore snapshot: resource group is null")
	ErrSnapshotResourceNull      = errors.New("azurearm: restore snapshot: resource is null")
)

// errorEntry maps a sentinel error to its ARM JSON error code, message, and
// HTTP status.
type errorEntry struct {
	code    string
	message string
	status  int
}

// cosmosErrorDetails maps the Microsoft.DocumentDB sentinels to their ARM error entries.
func cosmosErrorDetails(err error) (errorEntry, bool) {
	table := []struct {
		err   error
		entry errorEntry
	}{
		{
			ErrCosmosAccountNotFound,
			errorEntry{armErrorCodeResourceNotFound, "The Cosmos DB account was not found.", http.StatusNotFound},
		},
		{
			ErrCosmosDatabaseNotFound,
			errorEntry{armErrorCodeResourceNotFound, "The Cosmos DB database was not found.", http.StatusNotFound},
		},
		{
			ErrCosmosContainerNotFound,
			errorEntry{armErrorCodeResourceNotFound, "The Cosmos DB container was not found.", http.StatusNotFound},
		},
		{
			ErrCosmosThroughputNotFound,
			errorEntry{
				armErrorCodeResourceNotFound,
				"Throughput is not configured for the resource.",
				http.StatusNotFound,
			},
		},
		{
			errCosmosAccountOtherGroup,
			errorEntry{"AccountNameAlreadyExists", "The Cosmos DB account name is already taken.", http.StatusConflict},
		},
		{
			ErrCosmosConflict,
			errorEntry{"Conflict", "The Cosmos DB resource conflicts with an existing one.", http.StatusConflict},
		},
		{ErrCosmosBadRequest, errorEntry{"BadRequest", "The Cosmos DB request is invalid.", http.StatusBadRequest}},
		{
			ErrCosmosUnavailable,
			errorEntry{
				"ServiceUnavailable",
				"The Cosmos DB data plane is not available.",
				http.StatusServiceUnavailable,
			},
		},
	}

	for _, e := range table {
		if errors.Is(err, e.err) {
			return e.entry, true
		}
	}

	return errorEntry{}, false
}
