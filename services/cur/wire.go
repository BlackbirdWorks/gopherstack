package cur

import "github.com/blackbirdworks/gopherstack/pkgs/tags"

type (
	emptyResp struct{}

	reportStatusWire struct {
		LastDelivery string `json:"lastDelivery,omitempty"`
		LastStatus   string `json:"lastStatus,omitempty"`
	}

	reportDefinitionWire struct {
		ReportStatus             *reportStatusWire `json:"ReportStatus,omitempty"`
		RefreshClosedReports     *bool             `json:"RefreshClosedReports,omitempty"`
		ReportName               string            `json:"ReportName"`
		TimeUnit                 string            `json:"TimeUnit"`
		Format                   string            `json:"Format"`
		Compression              string            `json:"Compression"`
		S3Bucket                 string            `json:"S3Bucket"`
		S3Prefix                 string            `json:"S3Prefix"`
		S3Region                 string            `json:"S3Region"`
		ReportVersioning         string            `json:"ReportVersioning,omitempty"`
		BillingViewArn           string            `json:"BillingViewArn,omitempty"`
		AdditionalSchemaElements []string          `json:"AdditionalSchemaElements"`
		AdditionalArtifacts      []string          `json:"AdditionalArtifacts,omitempty"`
	}

	putReq struct {
		ReportDefinition *reportDefinitionWire `json:"ReportDefinition"`
		Tags             []tags.KV             `json:"Tags"`
	}

	modifyReq struct {
		ReportDefinition *reportDefinitionWire `json:"ReportDefinition"`
		ReportName       string                `json:"ReportName"`
	}

	describeReq struct {
		MaxResults *int32 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	describeResp struct {
		NextToken         string                 `json:"NextToken,omitempty"`
		ReportDefinitions []reportDefinitionWire `json:"ReportDefinitions"`
	}

	deleteReq struct {
		ReportName string `json:"ReportName"`
	}

	deleteResp struct {
		ResponseMessage string `json:"ResponseMessage,omitempty"`
	}

	tagReq struct {
		ReportName string    `json:"ReportName"`
		Tags       []tags.KV `json:"Tags"`
		TagKeys    []string  `json:"TagKeys"`
	}

	listTagsResp struct {
		Tags []tags.KV `json:"Tags"`
	}
)

func toWire(r *Report) reportDefinitionWire {
	refresh := r.RefreshClosedReports

	w := reportDefinitionWire{
		ReportName:               r.ReportName,
		TimeUnit:                 r.TimeUnit,
		Format:                   r.Format,
		Compression:              r.Compression,
		S3Bucket:                 r.S3Bucket,
		S3Prefix:                 r.S3Prefix,
		S3Region:                 r.S3Region,
		ReportVersioning:         r.ReportVersioning,
		BillingViewArn:           r.BillingViewArn,
		AdditionalSchemaElements: append([]string{}, r.AdditionalSchemaElements...),
		AdditionalArtifacts:      append([]string(nil), r.AdditionalArtifacts...),
		RefreshClosedReports:     &refresh,
	}

	if r.LastStatus != "" {
		w.ReportStatus = &reportStatusWire{LastDelivery: r.LastDelivery, LastStatus: r.LastStatus}
	}

	return w
}
