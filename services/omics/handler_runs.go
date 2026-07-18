package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCreateRunGroup(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		Name        string            `json:"name"`
		MaxCpus     int               `json:"maxCpus"`
		MaxRuns     int               `json:"maxRuns"`
		MaxDuration int               `json:"maxDuration"`
		MaxGpus     int               `json:"maxGpus"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rg, err := h.Backend.CreateRunGroup(
		req.Name,
		req.MaxCpus,
		req.MaxRuns,
		req.MaxDuration,
		req.MaxGpus,
		req.Tags,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rg)
}

func (h *Handler) handleDeleteRunGroup(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunGroup(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunGroup(c *echo.Context, id string) error {
	rg, err := h.Backend.GetRunGroup(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rg)
}

func (h *Handler) handleListRunGroups(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	groups, next, err := h.Backend.ListRunGroups(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runGroups": groups, keyNextToken: next})
}

func (h *Handler) handleUpdateRunGroup(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		MaxCpus     int    `json:"maxCpus"`
		MaxRuns     int    `json:"maxRuns"`
		MaxDuration int    `json:"maxDuration"`
		MaxGpus     int    `json:"maxGpus"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rg, err := h.Backend.UpdateRunGroup(
		id,
		req.Name,
		req.MaxCpus,
		req.MaxRuns,
		req.MaxDuration,
		req.MaxGpus,
	)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rg)
}

func (h *Handler) handleStartRun(c *echo.Context) error {
	var req struct {
		Parameters map[string]any    `json:"parameters"`
		Tags       map[string]string `json:"tags"`
		WorkflowID string            `json:"workflowId"`
		RoleArn    string            `json:"roleArn"`
		Name       string            `json:"name"`
		RunBatchID string            `json:"runBatchId"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	run, err := h.Backend.StartRun(req.WorkflowID, req.RoleArn, req.Name, req.RunBatchID, req.Parameters, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		"arn":    run.Arn,
		"id":     run.ID,
		"status": run.Status,
		keyTags:  run.Tags,
	})
}

func (h *Handler) handleCancelRun(c *echo.Context, id string) error {
	if err := h.Backend.CancelRun(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleDeleteRun(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRun(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRun(c *echo.Context, id string) error {
	run, err := h.Backend.GetRun(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, run)
}

func (h *Handler) handleListRuns(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	runs, next, err := h.Backend.ListRuns(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runs": runs, keyNextToken: next})
}

func (h *Handler) handleGetRunTask(c *echo.Context, runID, taskID string) error {
	task, err := h.Backend.GetRunTask(runID, taskID)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, task)
}

func (h *Handler) handleListRunTasks(c *echo.Context, runID string) error {
	maxResults, nextToken := paginationQueryParams(c)
	tasks, next, err := h.Backend.ListRunTasks(runID, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tasks": tasks, keyNextToken: next})
}

func (h *Handler) handleCreateRunCache(c *echo.Context) error {
	var req struct {
		Tags            map[string]string `json:"tags"`
		Name            string            `json:"name"`
		CacheS3Location string            `json:"cacheS3Location"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rc, err := h.Backend.CreateRunCache(req.Name, req.CacheS3Location, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rc)
}

func (h *Handler) handleDeleteRunCache(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunCache(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunCache(c *echo.Context, id string) error {
	rc, err := h.Backend.GetRunCache(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rc)
}

func (h *Handler) handleListRunCaches(c *echo.Context) error {
	maxResults, nextToken := paginationQueryParams(c)
	caches, next, err := h.Backend.ListRunCaches(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runCaches": caches, keyNextToken: next})
}

func (h *Handler) handleUpdateRunCache(c *echo.Context, id string) error {
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateRunCache(id, req.Name, req.Description); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleStartRunBatch(c *echo.Context) error {
	var req struct {
		WorkflowID string `json:"workflowId"`
		RoleArn    string `json:"roleArn"`
		Name       string `json:"name"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rb, err := h.Backend.StartRunBatch(req.WorkflowID, req.RoleArn, req.Name)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, rb)
}

func (h *Handler) handleCancelRunBatch(c *echo.Context) error {
	var req struct {
		BatchID string `json:"batchId"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.CancelRunBatch(req.BatchID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleDeleteBatch implements real AWS DeleteBatch: DELETE /runBatch/{batchId}
// deletes the run batch resource and its metadata (the individual runs must
// already be deleted via DeleteRunBatch — see handleDeleteRunBatch below).
func (h *Handler) handleDeleteBatch(c *echo.Context, id string) error {
	if err := h.Backend.DeleteRunBatch(id); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetRunBatch(c *echo.Context, id string) error {
	rb, err := h.Backend.GetRunBatch(id)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, rb)
}

func (h *Handler) handleListRunBatches(c *echo.Context) error {
	maxResults, nextToken := batchQueryParams(c)
	batches, next, err := h.Backend.ListRunBatches(maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runBatches": batches, keyNextToken: next})
}

// handleDeleteRunBatch implements real AWS DeleteRunBatch: POST /runBatch/delete
// with a single batchId in the JSON body deletes the individual workflow runs
// belonging to that batch (the batch resource itself is left intact; use
// DeleteBatch — DELETE /runBatch/{batchId} — to remove it afterward).
func (h *Handler) handleDeleteRunBatch(c *echo.Context) error {
	var req struct {
		BatchID string `json:"batchId"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.DeleteRunsInBatch(req.BatchID); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListRunsInBatch(c *echo.Context, batchID string) error {
	maxResults, nextToken := batchQueryParams(c)
	runs, next, err := h.Backend.ListRunsInBatch(batchID, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runs": runs, keyNextToken: next})
}
