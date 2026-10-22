package gcpmonitoring

// Field names shared by update masks, list filters and condition paths.
const (
	fldType          = "type"
	fldName          = "name"
	fldDisplayName   = "display_name"
	fldEnabled       = "enabled"
	fldLabels        = "labels"
	fldUserLabels    = "user_labels"
	fldDocumentation = "documentation"
	fldSeverity      = "severity"
	fldChannels      = "notification_channels"
	fldDescription   = "description"
)

// Resource and label names used by several catalogs.
const (
	typeGCEInstance      = "gce_instance"
	typeCloudRunRevision = "cloud_run_revision"
	labelResponseCode    = "response_code"
	scopeMetricLabels    = "metric.labels"
	scopeResourceLabels  = "resource.labels"
)
