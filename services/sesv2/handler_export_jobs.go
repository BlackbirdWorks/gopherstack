package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCancelExportJob(jobID string) (any, error) {
	if err := h.Backend.CancelExportJob(jobID); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// export job handlers

// exportDataSourceInput mirrors types.ExportDataSource: exactly one of
// MetricsDataSource/MessageInsightsDataSource must be set. gopherstack has no
// metrics-aggregation or message-log engine behind an export job, so their
// contents (Dimensions/Metrics/Namespace/StartDate/EndDate for metrics,
// Exclude/Include/MaxResults/StartDate/EndDate for message insights) are
// accepted opaquely via json.RawMessage rather than decoded into a typed
// shape -- only which branch was set is used, to derive ExportSourceType.
type exportDataSourceInput struct {
	MetricsDataSource         json.RawMessage `json:"MetricsDataSource,omitempty"`
	MessageInsightsDataSource json.RawMessage `json:"MessageInsightsDataSource,omitempty"`
}

// sourceType validates that exactly one branch is set (real SES v2 requires
// "either MessageInsightsDataSource or MetricsDataSource, but not both") and
// returns the corresponding ExportSourceType.
func (d exportDataSourceInput) sourceType() (string, error) {
	hasMetrics := len(d.MetricsDataSource) > 0
	hasInsights := len(d.MessageInsightsDataSource) > 0

	switch {
	case hasMetrics == hasInsights:
		return "", fmt.Errorf(
			"%w: ExportDataSource must set exactly one of MetricsDataSource or MessageInsightsDataSource",
			ErrInvalidInput,
		)
	case hasMetrics:
		return ExportSourceTypeMetricsData, nil
	default:
		return ExportSourceTypeMessageInsights, nil
	}
}

// exportDestinationInput mirrors types.ExportDestination. S3Url is accepted
// but not populated back onto the job: gopherstack never actually writes an
// export file, so there is no pre-signed URL to report.
type exportDestinationInput struct {
	DataFormat string `json:"DataFormat"`
	S3Url      string `json:"S3Url,omitempty"`
}

type createExportJobInput struct {
	ExportDataSource  *exportDataSourceInput  `json:"ExportDataSource"`
	ExportDestination *exportDestinationInput `json:"ExportDestination"`
}

func (h *Handler) handleCreateExportJob(c *echo.Context) (any, error) {
	var in createExportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if in.ExportDataSource == nil {
		return nil, fmt.Errorf("%w: ExportDataSource is required", ErrInvalidInput)
	}

	if in.ExportDestination == nil {
		return nil, fmt.Errorf("%w: ExportDestination is required", ErrInvalidInput)
	}

	if in.ExportDestination.DataFormat == "" {
		return nil, fmt.Errorf("%w: ExportDestination.DataFormat is required", ErrInvalidInput)
	}

	sourceType, err := in.ExportDataSource.sourceType()
	if err != nil {
		return nil, err
	}

	job, err := h.Backend.CreateExportJob(sourceType)
	if err != nil {
		return nil, err
	}

	return map[string]any{"JobId": job.JobID}, nil
}

func (h *Handler) handleGetExportJob(jobID string) (any, error) {
	job, err := h.Backend.GetExportJob(jobID)
	if err != nil {
		return nil, err
	}

	return toExportJobOutput(job), nil
}

// listExportJobsInput mirrors ListExportJobsInput -- real SES v2 serves
// ListExportJobs as POST /v2/email/list-export-jobs with filter/pagination in
// the JSON body, not query params.
type listExportJobsInput struct {
	ExportSourceType string `json:"ExportSourceType"`
	JobStatus        string `json:"JobStatus"`
	NextToken        string `json:"NextToken"`
	PageSize         int32  `json:"PageSize"`
}

func (h *Handler) handleListExportJobs(c *echo.Context) (any, error) {
	var in listExportJobsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	pg := h.Backend.ListExportJobs(in.ExportSourceType, in.JobStatus, in.NextToken, int(in.PageSize))

	items := make([]*exportJobOutput, 0, len(pg.Data))
	for _, j := range pg.Data {
		items = append(items, toExportJobOutput(j))
	}

	return map[string]any{
		"ExportJobs": items,
		keyNextToken: pg.Next,
	}, nil
}
