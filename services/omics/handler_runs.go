package omics

import (
	"fmt"
	"net/http"
	"time"

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
	filter := &RunGroupFilter{Name: c.QueryParam("name")}
	groups, next, err := h.Backend.ListRunGroups(filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: groups, keyNextToken: next})
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
		Parameters          map[string]any    `json:"parameters"`
		Tags                map[string]string `json:"tags"`
		StorageCapacity     *int              `json:"storageCapacity"`
		OutputURI           string            `json:"outputUri"`
		CacheBehavior       string            `json:"cacheBehavior"`
		RunGroupID          string            `json:"runGroupId"`
		RunBatchID          string            `json:"runBatchId"`
		NetworkingMode      string            `json:"networkingMode"`
		RoleArn             string            `json:"roleArn"`
		CacheID             string            `json:"cacheId"`
		Name                string            `json:"name"`
		RetentionMode       string            `json:"retentionMode"`
		ScratchStorageMode  string            `json:"scratchStorageMode"`
		StorageType         string            `json:"storageType"`
		WorkflowType        string            `json:"workflowType"`
		WorkflowVersionName string            `json:"workflowVersionName"`
		WorkflowID          string            `json:"workflowId"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	run, err := h.Backend.StartRun(StartRunInput{
		WorkflowID:          req.WorkflowID,
		RoleARN:             req.RoleArn,
		Name:                req.Name,
		RunGroupID:          req.RunGroupID,
		RunBatchID:          req.RunBatchID,
		NetworkingMode:      req.NetworkingMode,
		RunOutputURI:        req.OutputURI,
		CacheID:             req.CacheID,
		CacheBehavior:       req.CacheBehavior,
		RetentionMode:       req.RetentionMode,
		ScratchStorageMode:  req.ScratchStorageMode,
		StorageType:         req.StorageType,
		WorkflowType:        req.WorkflowType,
		WorkflowVersionName: req.WorkflowVersionName,
		StorageCapacity:     req.StorageCapacity,
		Params:              req.Parameters,
		Tags:                req.Tags,
	})
	if err != nil {
		return h.mapError(c, err)
	}

	// Real StartRunOutput: arn/id/status/tags plus the optional uuid/
	// configuration/networkingMode/runOutputUri fields (gopherstack-fedo).
	// CacheBehavior/RetentionMode/ScratchStorageMode/StorageCapacity/
	// StorageType/WorkflowType/WorkflowVersionName are not part of
	// StartRunOutput's own wire shape (verified against api_op_StartRun.go)
	// -- only GetRun/ListRuns echo them.
	return c.JSON(http.StatusCreated, map[string]any{
		keyArn:           run.Arn,
		"id":             run.ID,
		keyStatus:        run.Status,
		keyUUID:          run.UUID,
		"networkingMode": run.NetworkingMode,
		"runOutputUri":   run.RunOutputURI,
		"configuration":  run.Configuration,
		keyTags:          run.Tags,
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
	q := c.Request().URL.Query()
	filter := &RunFilter{
		Name:       q.Get("name"),
		RunGroupID: q.Get("runGroupId"),
		BatchID:    q.Get("batchId"),
		Status:     q.Get("status"),
	}
	runs, next, err := h.Backend.ListRuns(filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: runs, keyNextToken: next})
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
	filter := &RunTaskFilter{Status: c.QueryParam("status")}
	tasks, next, err := h.Backend.ListRunTasks(runID, filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: tasks, keyNextToken: next})
}

func (h *Handler) handleCreateRunCache(c *echo.Context) error {
	var req struct {
		Tags            map[string]string `json:"tags"`
		Name            string            `json:"name"`
		CacheS3Location string            `json:"cacheS3Location"`
		CacheBehavior   string            `json:"cacheBehavior"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	rc, err := h.Backend.CreateRunCache(req.Name, req.CacheS3Location, req.CacheBehavior, req.Tags)
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

	return c.JSON(http.StatusOK, map[string]any{keyItems: caches, keyNextToken: next})
}

func (h *Handler) handleUpdateRunCache(c *echo.Context, id string) error {
	var req struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		CacheBehavior string `json:"cacheBehavior"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.UpdateRunCache(id, req.Name, req.Description, req.CacheBehavior); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// inlineRunSettingWire mirrors types.InlineSetting's real JSON keys (confirmed via
// awsRestjson1_serializeDocumentInlineSetting, omics@v1.49.5's serializers.go).
type inlineRunSettingWire struct {
	Priority     *int32            `json:"priority,omitempty"`
	RunTags      map[string]string `json:"runTags,omitempty"`
	RunSettingID string            `json:"runSettingId"`
	Name         string            `json:"name,omitempty"`
	OutputURI    string            `json:"outputUri,omitempty"`
}

// batchRunSettingsWire mirrors the real BatchRunSettings union
// ({"inlineSettings": [...]} | {"s3UriSettings": "..."}), confirmed via
// awsRestjson1_serializeDocumentBatchRunSettings.
type batchRunSettingsWire struct {
	S3URISettings  string                 `json:"s3UriSettings,omitempty"`
	InlineSettings []inlineRunSettingWire `json:"inlineSettings,omitempty"`
}

// defaultRunSettingWire mirrors the subset of types.DefaultRunSetting's real JSON keys
// this backend models (confirmed via awsRestjson1_serializeDocumentDefaultRunSetting;
// see the DefaultRunSetting doc comment in models.go for the fields not modeled).
type defaultRunSettingWire struct {
	RunTags    map[string]string `json:"runTags,omitempty"`
	RoleArn    string            `json:"roleArn"`
	WorkflowID string            `json:"workflowId"`
	Name       string            `json:"name,omitempty"`
	OutputURI  string            `json:"outputUri,omitempty"`
	RunGroupID string            `json:"runGroupId,omitempty"`
	Priority   int32             `json:"priority,omitempty"`
}

// startRunBatchWire mirrors the real StartRunBatchInput's JSON keys, confirmed via
// awsRestjson1_serializeOpDocumentStartRunBatchInput.
type startRunBatchWire struct {
	Tags              map[string]string     `json:"tags,omitempty"`
	RequestID         string                `json:"requestId"`
	BatchName         string                `json:"batchName,omitempty"`
	DefaultRunSetting defaultRunSettingWire `json:"defaultRunSetting"`
	BatchRunSettings  batchRunSettingsWire  `json:"batchRunSettings"`
}

func (h *Handler) handleStartRunBatch(c *echo.Context) error {
	var req startRunBatchWire
	if err := readJSON(c, &req); err != nil {
		return err
	}

	if req.RequestID == "" {
		return h.mapError(c, fmt.Errorf("%w: requestId is required", ErrValidation))
	}

	if req.DefaultRunSetting.RoleArn == "" || req.DefaultRunSetting.WorkflowID == "" {
		return h.mapError(c, fmt.Errorf(
			"%w: defaultRunSetting.roleArn and defaultRunSetting.workflowId are required", ErrValidation,
		))
	}

	hasInline := len(req.BatchRunSettings.InlineSettings) > 0
	hasS3URI := req.BatchRunSettings.S3URISettings != ""

	if hasInline == hasS3URI {
		return h.mapError(c, fmt.Errorf(
			"%w: specify exactly one of batchRunSettings.inlineSettings or batchRunSettings.s3UriSettings",
			ErrValidation,
		))
	}

	if hasS3URI {
		// Real AWS reads and validates access to this S3 object synchronously during
		// the StartRunBatch call. gopherstack has no S3 object content to read here
		// (no cross-service wiring reads real S3 body bytes for this op), so this path
		// cannot be honestly simulated -- rejected explicitly rather than silently
		// creating a batch with zero runs. See PARITY.md.
		return h.mapError(c, fmt.Errorf(
			"%w: batchRunSettings.s3UriSettings is not supported by this emulator; use inlineSettings",
			ErrValidation,
		))
	}

	def := DefaultRunSetting{
		RoleARN:    req.DefaultRunSetting.RoleArn,
		WorkflowID: req.DefaultRunSetting.WorkflowID,
		Name:       req.DefaultRunSetting.Name,
		OutputURI:  req.DefaultRunSetting.OutputURI,
		RunGroupID: req.DefaultRunSetting.RunGroupID,
		Priority:   req.DefaultRunSetting.Priority,
		RunTags:    req.DefaultRunSetting.RunTags,
	}

	inline := make([]InlineRunSetting, len(req.BatchRunSettings.InlineSettings))
	for i, s := range req.BatchRunSettings.InlineSettings {
		inline[i] = InlineRunSetting{
			RunSettingID: s.RunSettingID,
			Name:         s.Name,
			OutputURI:    s.OutputURI,
			Priority:     s.Priority,
			RunTags:      s.RunTags,
		}
	}

	rb, err := h.Backend.StartRunBatch(req.BatchName, def, inline, req.Tags)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusCreated, map[string]any{
		keyArn:    rb.Arn,
		"id":      rb.ID,
		keyStatus: rb.Status,
		keyUUID:   rb.UUID,
		keyTags:   rb.Tags,
	})
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

	summary, err := h.Backend.GetRunBatchSummary(id)
	if err != nil {
		return h.mapError(c, err)
	}

	resp := map[string]any{
		keyArn:         rb.Arn,
		"creationTime": rb.CreationTime,
		"id":           rb.ID,
		"name":         rb.Name,
		keyStatus:      rb.Status,
		keyTags:        rb.Tags,
		"totalRuns":    rb.TotalRuns,
		keyUUID:        rb.UUID,
		"defaultRunSetting": map[string]any{
			"roleArn":    rb.RoleARN,
			"workflowId": rb.WorkflowID,
			"runGroupId": rb.RunGroupID,
			"outputUri":  rb.OutputURI,
		},
		"runSummary": map[string]any{
			"pendingRunCount":   summary.PendingRunCount,
			"runningRunCount":   summary.RunningRunCount,
			"completedRunCount": summary.CompletedRunCount,
			"cancelledRunCount": summary.CancelledRunCount,
			"failedRunCount":    summary.FailedRunCount,
			"deletedRunCount":   rb.DeletedRunCount,
			"startingRunCount":  0,
			"stoppingRunCount":  0,
		},
		"submissionSummary": map[string]any{
			"successfulStartSubmissionCount": rb.SubmissionSuccessCount,
			"failedStartSubmissionCount":     rb.SubmissionFailureCount,
			"pendingStartSubmissionCount":    0,
		},
	}

	if !rb.SubmittedTime.IsZero() {
		resp["submittedTime"] = rb.SubmittedTime
	}

	if !rb.ProcessedTime.IsZero() {
		resp["processedTime"] = rb.ProcessedTime
	}

	return c.JSON(http.StatusOK, resp)
}

// runBatchListItemWire mirrors types.BatchListItem's real, smaller field set (real
// ListBatch responses do NOT include runSummary/submissionSummary/defaultRunSetting --
// those are GetBatch-only, confirmed via awsRestjson1_deserializeDocumentBatchListItem).
type runBatchListItemWire struct {
	CreatedAt  time.Time `json:"createdAt"`
	ID         string    `json:"id"`
	Name       string    `json:"name,omitempty"`
	Status     string    `json:"status"`
	WorkflowID string    `json:"workflowId"`
	TotalRuns  int32     `json:"totalRuns"`
}

func (h *Handler) handleListRunBatches(c *echo.Context) error {
	maxResults, nextToken := batchQueryParams(c)
	q := c.Request().URL.Query()
	filter := &RunBatchFilter{
		Name:       q.Get("name"),
		RunGroupID: q.Get("runGroupId"),
		Status:     q.Get("status"),
	}
	batches, next, err := h.Backend.ListRunBatches(filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	items := make([]runBatchListItemWire, len(batches))
	for i, rb := range batches {
		items[i] = runBatchListItemWire{
			CreatedAt:  rb.CreationTime,
			ID:         rb.ID,
			Name:       rb.Name,
			Status:     rb.Status,
			WorkflowID: rb.WorkflowID,
			TotalRuns:  rb.TotalRuns,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyItems: items, keyNextToken: next})
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
	q := c.Request().URL.Query()
	filter := &RunsInBatchFilter{
		RunID:            q.Get("runId"),
		RunSettingID:     q.Get("runSettingId"),
		SubmissionStatus: q.Get("submissionStatus"),
	}
	runs, next, err := h.Backend.ListRunsInBatch(batchID, filter, maxResults, nextToken)

	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"runs": runs, keyNextToken: next})
}
