package bcmdataexports

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type (
	emptyResp struct{}

	s3OutputWire struct {
		OutputType  string `json:"OutputType"`
		Format      string `json:"Format"`
		Compression string `json:"Compression"`
		Overwrite   string `json:"Overwrite"`
	}

	s3DestinationWire struct {
		S3OutputConfigurations *s3OutputWire `json:"S3OutputConfigurations"`
		S3Bucket               string        `json:"S3Bucket"`
		S3Prefix               string        `json:"S3Prefix"`
		S3Region               string        `json:"S3Region"`
		S3BucketOwner          string        `json:"S3BucketOwner,omitempty"`
	}

	destinationWire struct {
		S3Destination *s3DestinationWire `json:"S3Destination"`
	}

	dataQueryWire struct {
		TableConfigurations map[string]map[string]string `json:"TableConfigurations,omitempty"`
		QueryStatement      string                       `json:"QueryStatement"`
	}

	refreshWire struct {
		Frequency string `json:"Frequency"`
	}

	exportWire struct {
		DataQuery                 *dataQueryWire   `json:"DataQuery"`
		DestinationConfigurations *destinationWire `json:"DestinationConfigurations"`
		RefreshCadence            *refreshWire     `json:"RefreshCadence"`
		Name                      string           `json:"Name"`
		Description               string           `json:"Description,omitempty"`
		ExportArn                 string           `json:"ExportArn,omitempty"`
	}

	exportStatusWire struct {
		StatusCode      string `json:"StatusCode"`
		StatusReason    string `json:"StatusReason,omitempty"`
		CreatedAt       string `json:"CreatedAt"`
		LastRefreshedAt string `json:"LastRefreshedAt"`
		LastUpdatedAt   string `json:"LastUpdatedAt"`
	}

	executionStatusWire struct {
		CompletedAt   *string `json:"CompletedAt,omitempty"`
		StatusCode    string  `json:"StatusCode"`
		StatusReason  string  `json:"StatusReason,omitempty"`
		CreatedAt     string  `json:"CreatedAt"`
		LastUpdatedAt string  `json:"LastUpdatedAt"`
	}

	createReq struct {
		Export       *exportWire `json:"Export"`
		ResourceTags []tags.KV   `json:"ResourceTags"`
	}

	arnResp struct {
		ExportArn string `json:"ExportArn"`
	}

	updateReq struct {
		Export    *exportWire `json:"Export"`
		ExportArn string      `json:"ExportArn"`
	}

	getExportReq struct {
		ExportArn string `json:"ExportArn"`
	}

	getExportResp struct {
		Export       exportWire       `json:"Export"`
		ExportStatus exportStatusWire `json:"ExportStatus"`
	}

	pageReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
		ExportArn  string `json:"ExportArn"`
	}

	exportRefWire struct {
		ExportArn    string           `json:"ExportArn"`
		ExportName   string           `json:"ExportName"`
		ExportStatus exportStatusWire `json:"ExportStatus"`
	}

	listExportsResp struct {
		NextToken string          `json:"NextToken,omitempty"`
		Exports   []exportRefWire `json:"Exports"`
	}

	execRefWire struct {
		ExecutionStatus executionStatusWire `json:"ExecutionStatus"`
		ExecutionID     string              `json:"ExecutionId"`
	}

	listExecsResp struct {
		NextToken  string        `json:"NextToken,omitempty"`
		Executions []execRefWire `json:"Executions"`
	}

	getExecReq struct {
		ExecutionID string `json:"ExecutionId"`
		ExportArn   string `json:"ExportArn"`
	}

	getExecResp struct {
		ExecutionID     string              `json:"ExecutionId"`
		ExecutionStatus executionStatusWire `json:"ExecutionStatus"`
		Export          exportWire          `json:"Export"`
	}

	getTableReq struct {
		TableProperties map[string]string `json:"TableProperties"`
		TableName       string            `json:"TableName"`
	}

	columnWire struct {
		Description string `json:"Description"`
		Name        string `json:"Name"`
		Type        string `json:"Type"`
	}

	getTableResp struct {
		TableProperties map[string]string `json:"TableProperties"`
		Description     string            `json:"Description"`
		TableName       string            `json:"TableName"`
		Schema          []columnWire      `json:"Schema"`
	}

	tablePropWire struct {
		DefaultValue string   `json:"DefaultValue"`
		Description  string   `json:"Description"`
		Name         string   `json:"Name"`
		ValidValues  []string `json:"ValidValues"`
	}

	tableWire struct {
		Description     string          `json:"Description"`
		TableName       string          `json:"TableName"`
		TableProperties []tablePropWire `json:"TableProperties"`
	}

	listTablesResp struct {
		NextToken string      `json:"NextToken,omitempty"`
		Tables    []tableWire `json:"Tables"`
	}

	tagReq struct {
		ResourceArn     string    `json:"ResourceArn"`
		MaxResults      *int32    `json:"MaxResults"`
		NextToken       string    `json:"NextToken"`
		ResourceTags    []tags.KV `json:"ResourceTags"`
		ResourceTagKeys []string  `json:"ResourceTagKeys"`
	}

	listTagsResp struct {
		NextToken    string    `json:"NextToken,omitempty"`
		ResourceTags []tags.KV `json:"ResourceTags"`
	}
)

func exportToWire(e *Export) exportWire {
	return exportWire{
		Name:        e.Name,
		Description: e.Description,
		ExportArn:   e.ARN,
		DataQuery:   &dataQueryWire{QueryStatement: e.Query, TableConfigurations: e.TableConfigs},
		DestinationConfigurations: &destinationWire{S3Destination: &s3DestinationWire{
			S3Bucket: e.S3Bucket, S3Prefix: e.S3Prefix, S3Region: e.S3Region, S3BucketOwner: e.S3BucketOwner,
			S3OutputConfigurations: &s3OutputWire{
				OutputType: e.OutputType, Format: e.Format, Compression: e.Compression, Overwrite: e.Overwrite,
			},
		}},
		RefreshCadence: &refreshWire{Frequency: e.Frequency},
	}
}

func exportStatusToWire(e *Export) exportStatusWire {
	return exportStatusWire{
		StatusCode:      e.StatusCode,
		StatusReason:    e.StatusReason,
		CreatedAt:       dateTime(e.CreatedAt),
		LastRefreshedAt: dateTime(e.LastRefreshedAt),
		LastUpdatedAt:   dateTime(e.LastUpdatedAt),
	}
}

func execStatusToWire(x *Execution) executionStatusWire {
	w := executionStatusWire{
		StatusCode:    x.StatusCode,
		StatusReason:  x.StatusReason,
		CreatedAt:     dateTime(x.CreatedAt),
		LastUpdatedAt: dateTime(x.LastUpdatedAt),
	}

	if !x.CompletedAt.IsZero() {
		c := dateTime(x.CompletedAt)
		w.CompletedAt = &c
	}

	return w
}

// dateTime renders the smithy date-time format this API's timestamps use.
func dateTime(t time.Time) string { return t.UTC().Format(time.RFC3339) }
