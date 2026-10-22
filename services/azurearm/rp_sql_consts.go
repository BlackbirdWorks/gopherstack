package azurearm

const (
	sqlPropVersion       = "version"
	sqlPropMaxSize       = "maxSizeBytes"
	sqlPropState         = "state"
	sqlPropKind          = "kind"
	sqlPropTier          = "tier"
	sqlPropCapacity      = "capacity"
	sqlPropCreationDate  = "creationDate"
	sqlPropRetentionDays = "retentionDays"
	sqlPropMinTLS        = "minimalTlsVersion"
	sqlPropPublic        = "publicNetworkAccess"
	sqlPropEndpoints     = "privateEndpointConnections"
	sqlPropZone          = "zoneRedundant"
	sqlPropStatus        = "status"
	sqlValReady          = "Ready"
	sqlValDisabled       = "Disabled"
	sqlValDefault        = "default"
	sqlValCurrent        = "current"
	sqlValGeo            = "Geo"
	sqlValStandard       = "Standard"
	sqlValNoRetention    = "PT0S"
	sqlCodeUnavailable   = "ServiceUnavailable"

	sqlAuditQueueDelayMs   = 1000
	sqlDefaultPoolCapacity = 50
	sqlShortRetentionDays  = 7
	sqlDiffBackupHours     = 12
	sqlDefaultSKUDTU       = 10
	sqlParentWithDB        = 2
	sqlFixedTypes          = 5
)
