package quicksight

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/body keys used only by asset-bundle and dashboard-snapshot job
// operations.
const (
	keyAssetBundleExportJobID = "AssetBundleExportJobId"
	keyAssetBundleImportJobID = "AssetBundleImportJobId"
	keyResourceArns           = "ResourceArns"
	keyExportFormat           = "ExportFormat"
	keyIncludeAllDeps         = "IncludeAllDependencies"
	keyJobStatus              = "JobStatus"
	keyDownloadURL            = "DownloadUrl"
	keyFailureAction          = "FailureAction"
	keyAssetBundleExportJobs  = "AssetBundleExportJobSummaryList"
	keyAssetBundleImportJobs  = "AssetBundleImportJobSummaryList"

	keySnapshotJobID  = "SnapshotJobId"
	keySnapshotConfig = "SnapshotConfiguration"
	keyResultField    = "Result"
	keyS3URI          = "S3Uri"
)

func isAssetBundleOp(op string) bool {
	switch op {
	case opStartAssetBundleExportJob, opDescribeAssetBundleExportJob, opListAssetBundleExportJobs,
		opStartAssetBundleImportJob, opDescribeAssetBundleImportJob, opListAssetBundleImportJobs,
		opStartDashboardSnapshotJob, opDescribeDashboardSnapshotJob, opDescribeDashboardSnapshotJobResult:
		return true
	}

	return false
}

func (h *Handler) dispatchAssetBundle(c *echo.Context, op string) error {
	switch op {
	case opStartAssetBundleExportJob:
		return h.handleStartAssetBundleExportJob(c)
	case opDescribeAssetBundleExportJob:
		return h.handleDescribeAssetBundleExportJob(c)
	case opListAssetBundleExportJobs:
		return h.handleListAssetBundleExportJobs(c)
	case opStartAssetBundleImportJob:
		return h.handleStartAssetBundleImportJob(c)
	case opDescribeAssetBundleImportJob:
		return h.handleDescribeAssetBundleImportJob(c)
	case opListAssetBundleImportJobs:
		return h.handleListAssetBundleImportJobs(c)
	}

	return h.dispatchDashboardSnapshot(c, op)
}

func (h *Handler) dispatchDashboardSnapshot(c *echo.Context, op string) error {
	switch op {
	case opStartDashboardSnapshotJob:
		return h.handleStartDashboardSnapshotJob(c)
	case opDescribeDashboardSnapshotJob:
		return h.handleDescribeDashboardSnapshotJob(c)
	case opDescribeDashboardSnapshotJobResult:
		return h.handleDescribeDashboardSnapshotJobResult(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// ---- Asset bundle export jobs ----

func exportJobToMap(job *AssetBundleExportJob) map[string]any {
	m := map[string]any{
		keyAssetBundleExportJobID: job.JobID,
		keyArn:                    job.Arn,
		keyJobStatus:              job.Status,
		keyExportFormat:           job.ExportFormat,
		keyResourceArns:           job.ResourceArns,
		keyIncludeAllDeps:         job.IncludeAllDependencies,
		keyCreatedTime:            job.CreatedTime.Unix(),
	}
	if job.DownloadURL != "" {
		m[keyDownloadURL] = job.DownloadURL
	}

	return m
}

func (h *Handler) handleStartAssetBundleExportJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	includeAllDeps, _ := body[keyIncludeAllDeps].(bool)

	job, err := h.Backend.StartAssetBundleExportJob(
		accountID,
		strField(body, keyAssetBundleExportJobID),
		strField(body, keyExportFormat),
		stringsFromBody(body, keyResourceArns),
		includeAllDeps,
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                    job.Arn,
		keyAssetBundleExportJobID: job.JobID,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	})
}

func (h *Handler) handleDescribeAssetBundleExportJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	jobID := seg(segs, segResID)

	job, err := h.Backend.DescribeAssetBundleExportJob(accountID, jobID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := exportJobToMap(job)
	resp[keyRequestID] = reqIDPlaceholder
	resp[keyStatus] = http.StatusOK

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListAssetBundleExportJobs(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	jobs, next, err := h.Backend.ListAssetBundleExportJobs(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, exportJobToMap(job))
	}

	resp := map[string]any{
		keyAssetBundleExportJobs: items,
		keyRequestID:             reqIDPlaceholder,
		keyStatus:                http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Asset bundle import jobs ----

func importJobToMap(job *AssetBundleImportJob) map[string]any {
	return map[string]any{
		keyAssetBundleImportJobID: job.JobID,
		keyArn:                    job.Arn,
		keyJobStatus:              job.Status,
		keyFailureAction:          job.FailureAction,
		keyCreatedTime:            job.CreatedTime.Unix(),
	}
}

func (h *Handler) handleStartAssetBundleImportJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	job, err := h.Backend.StartAssetBundleImportJob(
		accountID,
		strField(body, keyAssetBundleImportJobID),
		strField(body, keyFailureAction),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                    job.Arn,
		keyAssetBundleImportJobID: job.JobID,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	})
}

func (h *Handler) handleDescribeAssetBundleImportJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	jobID := seg(segs, segResID)

	job, err := h.Backend.DescribeAssetBundleImportJob(accountID, jobID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := importJobToMap(job)
	resp[keyRequestID] = reqIDPlaceholder
	resp[keyStatus] = http.StatusOK

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleListAssetBundleImportJobs(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	jobs, next, err := h.Backend.ListAssetBundleImportJobs(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, importJobToMap(job))
	}

	resp := map[string]any{
		keyAssetBundleImportJobs: items,
		keyRequestID:             reqIDPlaceholder,
		keyStatus:                http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- Dashboard snapshot jobs ----

func (h *Handler) handleStartDashboardSnapshotJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	job, err := h.Backend.StartDashboardSnapshotJob(
		accountID, dashboardID, strField(body, keySnapshotJobID), mapField(body, keySnapshotConfig),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:           job.Arn,
		keySnapshotJobID: job.JobID,
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleDescribeDashboardSnapshotJob(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)
	jobID := seg(segs, segSubResID)

	job, err := h.Backend.DescribeDashboardSnapshotJob(accountID, dashboardID, jobID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:             job.Arn,
		keyDashboardID:     job.DashboardID,
		keySnapshotJobID:   job.JobID,
		keyJobStatus:       job.Status,
		keyCreatedTime:     job.CreatedTime.Unix(),
		keyLastUpdatedTime: job.LastUpdatedTime.Unix(),
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleDescribeDashboardSnapshotJobResult(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	dashboardID := seg(segs, segResID)
	jobID := seg(segs, segSubResID)

	job, err := h.Backend.DescribeDashboardSnapshotJobResult(accountID, dashboardID, jobID)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		keyArn:         job.Arn,
		keyJobStatus:   job.Status,
		keyCreatedTime: job.CreatedTime.Unix(),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	}
	if job.S3URI != "" {
		resp[keyResultField] = map[string]any{keyS3URI: job.S3URI}
	}

	return writeJSON(c, http.StatusOK, resp)
}
