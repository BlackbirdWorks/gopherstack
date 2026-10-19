package bcmdataexports

import (
	"maps"
	"time"
)

const (
	statusInitiation = "INITIATION_IN_PROCESS"
	statusQueued     = "QUERY_QUEUED"
	statusQuerying   = "QUERY_IN_PROCESS"
	statusQueryFail  = "QUERY_FAILURE"
	statusDelivering = "DELIVERY_IN_PROCESS"
	statusSuccess    = "DELIVERY_SUCCESS"
	statusDelivFail  = "DELIVERY_FAILURE"

	exportHealthy   = "HEALTHY"
	exportUnhealthy = "UNHEALTHY"

	reasonPermission = "INSUFFICIENT_PERMISSION"
	reasonInternal   = "INTERNAL_FAILURE"

	outputCustom = "CUSTOM"
	formatCSV    = "TEXT_OR_CSV"
	formatParq   = "PARQUET"
	compGzip     = "GZIP"
	compZip      = "ZIP"
	compParquet  = "PARQUET"
	ovNew        = "CREATE_NEW_REPORT"
	ovOverwrite  = "OVERWRITE_REPORT"
	freqSync     = "SYNCHRONOUS"
)

// Export is a stored data export and its health.
type Export struct {
	CreatedAt       time.Time                    `json:"createdAt"`
	LastUpdatedAt   time.Time                    `json:"lastUpdatedAt"`
	LastRefreshedAt time.Time                    `json:"lastRefreshedAt"`
	TableConfigs    map[string]map[string]string `json:"tableConfigs"`
	Tags            map[string]string            `json:"tags"`
	ARN             string                       `json:"arn"`
	Name            string                       `json:"name"`
	Description     string                       `json:"description"`
	Query           string                       `json:"query"`
	S3Bucket        string                       `json:"s3Bucket"`
	S3Prefix        string                       `json:"s3Prefix"`
	S3Region        string                       `json:"s3Region"`
	S3BucketOwner   string                       `json:"s3BucketOwner"`
	OutputType      string                       `json:"outputType"`
	Format          string                       `json:"format"`
	Compression     string                       `json:"compression"`
	Overwrite       string                       `json:"overwrite"`
	Frequency       string                       `json:"frequency"`
	StatusCode      string                       `json:"statusCode"`
	StatusReason    string                       `json:"statusReason"`
}

// Execution is one run of an export; Export is the definition it ran with.
type Execution struct {
	CreatedAt     time.Time `json:"createdAt"`
	LastUpdatedAt time.Time `json:"lastUpdatedAt"`
	CompletedAt   time.Time `json:"completedAt"`
	Export        Export    `json:"export"`
	Key           string    `json:"key"`
	ID            string    `json:"id"`
	ExportARN     string    `json:"exportArn"`
	StatusCode    string    `json:"statusCode"`
	StatusReason  string    `json:"statusReason"`
}

func (e *Execution) terminal() bool {
	return e.StatusCode == statusSuccess || e.StatusCode == statusQueryFail || e.StatusCode == statusDelivFail
}

func cloneExport(e *Export) Export {
	c := *e
	c.Tags = nil
	c.TableConfigs = make(map[string]map[string]string, len(e.TableConfigs))

	for k, v := range e.TableConfigs {
		m := make(map[string]string, len(v))
		maps.Copy(m, v)

		c.TableConfigs[k] = m
	}

	return c
}
