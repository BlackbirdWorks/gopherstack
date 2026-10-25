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
