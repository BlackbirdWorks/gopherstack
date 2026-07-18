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

func (h *Handler) handleListImportJobs(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListImportJobs(nextToken, 0)

	items := make([]*importJobOutput, 0, len(pg.Data))
	for _, j := range pg.Data {
		items = append(items, toImportJobOutput(j))
	}

	return map[string]any{
		"ImportJobs": items,
		keyNextToken: pg.Next,
	}, nil
}
