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

type createExportJobInput struct {
	DataSource string `json:"DataSource"`
}

func (h *Handler) handleCreateExportJob(c *echo.Context) (any, error) {
	var in createExportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	job, err := h.Backend.CreateExportJob(in.DataSource)
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
// the JSON body, not query params (ExportSourceType/JobStatus filters aren't
// modelled by the backend yet, so only pagination is honored).
type listExportJobsInput struct {
	NextToken string `json:"NextToken"`
	PageSize  int32  `json:"PageSize"`
}

func (h *Handler) handleListExportJobs(c *echo.Context) (any, error) {
	var in listExportJobsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	pg := h.Backend.ListExportJobs(in.NextToken, int(in.PageSize))

	items := make([]*exportJobOutput, 0, len(pg.Data))
	for _, j := range pg.Data {
		items = append(items, toExportJobOutput(j))
	}

	return map[string]any{
		"ExportJobs": items,
		keyNextToken: pg.Next,
	}, nil
}
