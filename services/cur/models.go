package cur

import "time"

const (
	timeHourly  = "HOURLY"
	timeDaily   = "DAILY"
	timeMonthly = "MONTHLY"

	formatCSV     = "textORcsv"
	formatParquet = "Parquet"

	compGzip    = "GZIP"
	compZip     = "ZIP"
	compParquet = "Parquet"

	artifactRedshift   = "REDSHIFT"
	artifactQuicksight = "QUICKSIGHT"
	artifactAthena     = "ATHENA"

	schemaResources = "RESOURCES"
	schemaSplit     = "SPLIT_COST_ALLOCATION_DATA"
	schemaDiscount  = "MANUAL_DISCOUNT_COMPATIBILITY"

	versionNew       = "CREATE_NEW_REPORT"
	versionOverwrite = "OVERWRITE_REPORT"

	statusSuccess       = "SUCCESS"
	statusErrPermission = "ERROR_PERMISSIONS"
	statusErrNoBucket   = "ERROR_NO_BUCKET"
)

// Report is a stored report definition plus delivery state.
type Report struct {
	CreatedAt                time.Time         `json:"createdAt"`
	Tags                     map[string]string `json:"tags"`
	ReportName               string            `json:"reportName"`
	TimeUnit                 string            `json:"timeUnit"`
	Format                   string            `json:"format"`
	Compression              string            `json:"compression"`
	S3Bucket                 string            `json:"s3Bucket"`
	S3Prefix                 string            `json:"s3Prefix"`
	S3Region                 string            `json:"s3Region"`
	ReportVersioning         string            `json:"reportVersioning"`
	BillingViewArn           string            `json:"billingViewArn"`
	LastDelivery             string            `json:"lastDelivery"`
	LastStatus               string            `json:"lastStatus"`
	LastSlot                 string            `json:"lastSlot"`
	AdditionalSchemaElements []string          `json:"additionalSchemaElements"`
	AdditionalArtifacts      []string          `json:"additionalArtifacts"`
	RefreshClosedReports     bool              `json:"refreshClosedReports"`
}
