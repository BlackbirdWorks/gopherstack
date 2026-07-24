package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createImportJobInput struct {
	DataSource string `json:"DataSource"`
}

func (h *Handler) handleCreateImportJob(c *echo.Context) (any, error) {
	var in createImportJobInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	job, err := h.Backend.CreateImportJob(in.DataSource)
	if err != nil {
		return nil, err
	}

	return map[string]any{"JobId": job.JobID}, nil
}

func (h *Handler) handleGetImportJob(jobID string) (any, error) {
	job, err := h.Backend.GetImportJob(jobID)
	if err != nil {
		return nil, err
	}

	return toImportJobOutput(job), nil
}

// listImportJobsInput mirrors ListImportJobsInput -- real SES v2 serves
// ListImportJobs as POST /v2/email/import-jobs/list with filter/pagination
// in the JSON body, not query params (ImportDestinationType filter isn't
// modelled by the backend yet, so only pagination is honored).
type listImportJobsInput struct {
	NextToken string `json:"NextToken"`
	PageSize  int32  `json:"PageSize"`
}

func (h *Handler) handleListImportJobs(c *echo.Context) (any, error) {
	var in listImportJobsInput
	if err := decodeSESv2Body(c, &in); err != nil {
		return nil, err
	}

	pg := h.Backend.ListImportJobs(in.NextToken, int(in.PageSize))

	items := make([]*importJobOutput, 0, len(pg.Data))
	for _, j := range pg.Data {
		items = append(items, toImportJobOutput(j))
	}

	return map[string]any{
		"ImportJobs": items,
		keyNextToken: pg.Next,
	}, nil
}
