package app

import "github.com/blackbirdworks/gopherstack/pkgs/service"

// GetAzureEventHubsHandler returns the Event Hubs handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureEventHubsHandler() service.Registerable { return c.azureEventHubsHandler }

// GetAzureEventGridHandler returns the Event Grid handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureEventGridHandler() service.Registerable { return c.azureEventGridHandler }

// GetAzureAppConfigHandler returns the App Configuration handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureAppConfigHandler() service.Registerable { return c.azureAppConfigHandler }

// GetGCPSQLHandler returns the Cloud SQL handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPSQLHandler() service.Registerable { return c.gcpSQLHandler }

// GetGCPLoggingHandler returns the Cloud Logging handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPLoggingHandler() service.Registerable { return c.gcpLoggingHandler }

// GetAzureFunctionsHandler returns the Azure Functions handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureFunctionsHandler() service.Registerable { return c.azureFunctionsHandler }

// GetGCPCloudBuildHandler returns the Cloud Build handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPCloudBuildHandler() service.Registerable { return c.gcpCloudBuildHandler }

// GetGCPRedisHandler returns the Memorystore for Redis handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPRedisHandler() service.Registerable { return c.gcpRedisHandler }

// GetGCPMonitoringHandler returns the Cloud Monitoring handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPMonitoringHandler() service.Registerable { return c.gcpMonitoringHandler }

func storeConsoleHandlers(cli *CLI, byName map[string]service.Registerable) {
	cli.azureFunctionsHandler = byName["AzureFunctions"]
	cli.gcpCloudBuildHandler = byName["GCPCloudBuild"]
	cli.gcpRedisHandler = byName["GCPRedis"]
	cli.gcpMonitoringHandler = byName["GCPMonitoring"]
}

// GetGCPWorkflowsHandler returns the GCPWorkflows handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPWorkflowsHandler() service.Registerable { return c.gcpWorkflowsHandler }

// GetGCPDNSHandler returns the GCPDNS handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPDNSHandler() service.Registerable { return c.gcpDNSHandler }

// GetGCPDatastoreHandler returns the GCPDatastore handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPDatastoreHandler() service.Registerable { return c.gcpDatastoreHandler }

// GetGCPComputeHandler returns the GCPCompute handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPComputeHandler() service.Registerable { return c.gcpComputeHandler }

// GetAzureFileHandler returns the AzureFile handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureFileHandler() service.Registerable { return c.azureFileHandler }

// GetOCIObjectStorageHandler returns the OCIObjectStorage handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIObjectStorageHandler() service.Registerable { return c.ociObjectStorageHandler }

// GetOCIVaultHandler returns the OCIVault handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIVaultHandler() service.Registerable { return c.ociVaultHandler }

// GetOCIStreamingHandler returns the OCIStreaming handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIStreamingHandler() service.Registerable { return c.ociStreamingHandler }

// GetOCIIdentityHandler returns the OCIIdentity handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIIdentityHandler() service.Registerable { return c.ociIdentityHandler }

// GetOCIFunctionsHandler returns the OCIFunctions handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIFunctionsHandler() service.Registerable { return c.ociFunctionsHandler }

// GetOCICoreHandler returns the OCICore handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCICoreHandler() service.Registerable { return c.ociCoreHandler }

// GetGCPContainerHandler returns the GCPContainer handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPContainerHandler() service.Registerable { return c.gcpContainerHandler }

// GetOCIQueueHandler returns the OCIQueue handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIQueueHandler() service.Registerable { return c.ociQueueHandler }

// GetOCINotificationsHandler returns the OCINotifications handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCINotificationsHandler() service.Registerable { return c.ociNotificationsHandler }

// GetOCIContainerEngineHandler returns the OCIContainerEngine handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIContainerEngineHandler() service.Registerable { return c.ociContainerEngineHandler }

// GetAzureMonitorHandler returns the AzureMonitor handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetAzureMonitorHandler() service.Registerable { return c.azureMonitorHandler }

// GetOCIMySQLHandler returns the OCIMySQL handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIMySQLHandler() service.Registerable { return c.ociMySQLHandler }

// GetOCILoggingHandler returns the OCILogging handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCILoggingHandler() service.Registerable { return c.ociLoggingHandler }

// GetOCIMonitoringHandler returns the OCIMonitoring handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIMonitoringHandler() service.Registerable { return c.ociMonitoringHandler }

// GetOCINoSQLHandler returns the OCINoSQL handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCINoSQLHandler() service.Registerable { return c.ociNoSQLHandler }

// GetOCIAPIGatewayHandler returns the OCIAPIGateway handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIAPIGatewayHandler() service.Registerable { return c.ociAPIGatewayHandler }

// GetOCILoadBalancerHandler returns the OCILoadBalancer handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCILoadBalancerHandler() service.Registerable { return c.ociLoadBalancerHandler }

// GetOCIArtifactsHandler returns the OCIArtifacts handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIArtifactsHandler() service.Registerable { return c.ociArtifactsHandler }

// GetOCIEventsHandler returns the OCIEvents handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetOCIEventsHandler() service.Registerable { return c.ociEventsHandler }

// GetGCPIdentityToolkitHandler returns the GCPIdentityToolkit handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPIdentityToolkitHandler() service.Registerable { return c.gcpIdentityToolkitHandler }

// GetGCPAPIGatewayHandler returns the GCPAPIGateway handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPAPIGatewayHandler() service.Registerable { return c.gcpAPIGatewayHandler }

// GetGCPResourceManagerHandler returns the GCPResourceManager handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPResourceManagerHandler() service.Registerable { return c.gcpResourceManagerHandler }

// GetGCPBatchHandler returns the GCPBatch handler for the dashboard proxy.
//
//nolint:ireturn // architecturally required to return interface
func (c *CLI) GetGCPBatchHandler() service.Registerable { return c.gcpBatchHandler }
