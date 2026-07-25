package glacier

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleInitiateJob(c *echo.Context, vaultName string, body []byte) error {
	var req initiateJobRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"invalid request body: "+err.Error(),
		)
	}

	j, err := h.Backend.InitiateJob(h.AccountID, h.DefaultRegion, vaultName, &req)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	location := "/" + h.AccountID + "/vaults/" + vaultName + "/jobs/" + j.JobID

	c.Response().Header().Set("X-Amz-Job-Id", j.JobID)
	c.Response().Header().Set("Location", location)

	if j.JobOutputPath != "" {
		c.Response().Header().Set("X-Amz-Job-Output-Path", j.JobOutputPath)
	}

	return c.JSON(http.StatusAccepted, initiateJobResponse{
		JobID:         j.JobID,
		Location:      location,
		JobOutputPath: j.JobOutputPath,
	})
}

func (h *Handler) handleDescribeJob(c *echo.Context, vaultName, jobID string) error {
	j, err := h.Backend.DescribeJob(h.AccountID, h.DefaultRegion, vaultName, jobID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, toDescribeJobResponse(j))
}

func (h *Handler) handleListJobs(c *echo.Context, vaultName string) error {
	jobs, err := h.Backend.ListJobs(h.AccountID, h.DefaultRegion, vaultName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	// Optional query filters: ?completed=true|false and ?statuscode=InProgress|Succeeded|Failed
	completedFilter := c.QueryParam("completed")
	if completedFilter != "" && completedFilter != "true" && completedFilter != "false" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"completed must be \"true\" or \"false\"")
	}

	statuscodeFilter := c.QueryParam("statuscode")

	items := make([]describeJobResponse, 0, len(jobs))

	for _, j := range jobs {
		if completedFilter != "" {
			want := completedFilter == "true"
			if j.Completed != want {
				continue
			}
		}

		if statuscodeFilter != "" && j.StatusCode != statuscodeFilter {
			continue
		}

		items = append(items, toDescribeJobResponse(j))
	}

	items, nextMarker, pErr := paginateJobList(c, items)
	if pErr != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			pErr.Error(),
		)
	}

	return c.JSON(http.StatusOK, listJobsResponse{
		Marker:  nextMarker,
		JobList: items,
	})
}

// paginateJobList applies marker+limit pagination to a slice of job responses.
func paginateJobList( //nolint:dupl // three typed paginate funcs share identical structure
	c *echo.Context,
	items []describeJobResponse,
) ([]describeJobResponse, *string, error) {
	marker := c.QueryParam("marker")
	if marker != "" {
		marker = decodeMarker(marker)
	}

	if marker != "" {
		start := 0

		for start < len(items) && items[start].JobID != marker {
			start++
		}

		if start < len(items) {
			items = items[start+1:]
		} else {
			items = items[:0]
		}
	}

	limitStr := c.QueryParam("limit")
	if limitStr == "" {
		return items, nil, nil
	}

	n, err := strconv.Atoi(limitStr)
	if err != nil || n < minListLimit || n > maxListJobsLimit {
		return nil, nil, fmt.Errorf(
			"%w: must be between %d and %d",
			ErrLimitOutOfRange,
			minListLimit,
			maxListJobsLimit,
		)
	}

	if n >= len(items) {
		return items, nil, nil
	}

	last := encodeMarker(items[n-1].JobID)

	return items[:n], &last, nil
}

func (h *Handler) handleGetJobOutput(c *echo.Context, vaultName, jobID string) error {
	j, err := h.Backend.DescribeJob(h.AccountID, h.DefaultRegion, vaultName, jobID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	if !j.Completed {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			ErrJobNotComplete.Error())
	}

	if j.SHA256TreeHash != "" {
		c.Response().Header().Set("X-Amz-Sha256-Tree-Hash", j.SHA256TreeHash)
	}

	c.Response().Header().Set("Accept-Ranges", "bytes")

	switch j.Action {
	case jobTypeInventoryRetrieval:
		return h.handleInventoryJobOutput(c, j, vaultName)
	case jobTypeSelect:
		return h.handleSelectJobOutput(c, j)
	default:
		return h.handleArchiveJobOutput(c, j)
	}
}

// handleInventoryJobOutput returns the vault inventory as JSON or CSV, applying the
// job's (optional) range-inventory-retrieval StartDate/EndDate/Marker/Limit filters.
func (h *Handler) handleInventoryJobOutput(c *echo.Context, j *Job, vaultName string) error {
	archives, listErr := h.Backend.ListArchives(h.AccountID, h.DefaultRegion, vaultName)
	if listErr != nil {
		archives = []*Archive{} // degrade gracefully
	}

	archives = filterArchivesForInventory(j, archives)

	if j.InventoryFormat != "" && j.InventoryFormat != defaultInventoryFormat {
		return h.writeInventoryCSV(c, j, vaultName, archives)
	}

	return h.writeInventoryJSON(c, j, vaultName, archives)
}

// handleSelectJobOutput executes the select job's SQL expression against the
// retrieved archive and serves the (real, not stubbed -- see select.go's package doc)
// result. Real AWS never serves select results via GetJobOutput (they go to S3); this
// is a documented gopherstack-specific delivery path in lieu of cross-service S3
// write-back.
func (h *Handler) handleSelectJobOutput(c *echo.Context, j *Job) error {
	data, hasData := h.Backend.GetArchiveData(j.ArchiveID)
	if !hasData {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "Archive not found")
	}

	result, err := executeSelect(data, j.SelectParameters)
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	c.Response().Header().Set("Content-Type", "text/csv")

	if len(result) == 0 {
		c.Response().Header().Set("Content-Range", "bytes 0-0/0")
	} else {
		c.Response().
			Header().
			Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(result)-1, len(result)))
	}

	return h.serveWithRange(c, result)
}

func (h *Handler) writeInventoryJSON(c *echo.Context, j *Job, vaultName string, archives []*Archive) error {
	items := make([]inventoryArchiveItem, 0, len(archives))

	for _, a := range archives {
		items = append(items, inventoryArchiveItem{
			ArchiveID:          a.ArchiveID,
			ArchiveDescription: a.Description,
			CreationDate:       a.CreationDate,
			Size:               a.Size,
			SHA256TreeHash:     a.SHA256TreeHash,
		})
	}

	payload, err := json.Marshal(map[string]any{
		"VaultARN":      j.VaultARN,
		"InventoryDate": j.CompletionDate,
		"ArchiveList":   items,
	})
	if err != nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceUnavailableException",
			"failed to encode inventory",
		)
	}

	// Populate InventorySizeInBytes on the job so DescribeJob returns it.
	if j.InventorySizeInBytes == 0 {
		h.Backend.SetJobInventorySize(h.AccountID, h.DefaultRegion, vaultName, j.JobID, int64(len(payload)))
	}

	c.Response().Header().Set("Content-Type", "application/json")
	c.Response().
		Header().
		Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))

	return h.serveWithRange(c, payload)
}

// csvField returns s encoded as an RFC 4180 CSV field: quotes are added only when s
// contains a comma, double-quote, or newline; internal double-quotes are doubled.
func csvField(s string) string {
	needsQuote := strings.ContainsAny(s, ",\"\n\r")
	if !needsQuote {
		return s
	}

	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func (h *Handler) writeInventoryCSV(c *echo.Context, j *Job, vaultName string, archives []*Archive) error {
	var buf bytes.Buffer

	buf.WriteString("ArchiveId,ArchiveDescription,CreationDate,Size,SHA256TreeHash\n")

	for _, a := range archives {
		fmt.Fprintf(
			&buf,
			"%s,%s,%s,%d,%s\n",
			csvField(a.ArchiveID),
			csvField(a.Description),
			csvField(a.CreationDate),
			a.Size,
			csvField(a.SHA256TreeHash),
		)
	}

	payload := buf.Bytes()

	// Populate InventorySizeInBytes on the job so DescribeJob returns it.
	if j.InventorySizeInBytes == 0 {
		h.Backend.SetJobInventorySize(h.AccountID, h.DefaultRegion, vaultName, j.JobID, int64(len(payload)))
	}

	c.Response().Header().Set("Content-Type", "text/csv")
	c.Response().
		Header().
		Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(payload)-1, len(payload)))

	return h.serveWithRange(c, payload)
}

// handleArchiveJobOutput streams stored archive bytes with Range support.
// If the job was initiated with a RetrievalByteRange, only that byte slice is served.
func (h *Handler) handleArchiveJobOutput(c *echo.Context, j *Job) error {
	c.Response().Header().Set("Content-Type", "application/octet-stream")

	if j.ArchiveDescription != "" {
		c.Response().Header().Set("X-Amz-Archive-Description", j.ArchiveDescription)
	}

	data, hasData := h.Backend.GetArchiveData(j.ArchiveID)

	if !hasData {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "Archive not found")
	}

	// Honour RetrievalByteRange set at job initiation time (e.g. "0-1048575").
	if j.RetrievalByteRange != "" {
		data = sliceRetrievalRange(data, j.RetrievalByteRange)
	}

	c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes 0-%d/%d", len(data)-1, len(data)))

	return h.serveWithRange(c, data)
}

// sliceRetrievalRange slices data to the byte range specified in rangeStr ("START-END").
// Returns data unchanged if rangeStr is malformed or out of bounds.
func sliceRetrievalRange(data []byte, rangeStr string) []byte {
	dash := strings.IndexByte(rangeStr, '-')
	if dash <= 0 || dash == len(rangeStr)-1 {
		return data
	}

	start, err1 := strconv.ParseInt(rangeStr[:dash], 10, 64)
	end, err2 := strconv.ParseInt(rangeStr[dash+1:], 10, 64)

	if err1 != nil || err2 != nil || start < 0 || end < start {
		return data
	}

	total := int64(len(data))

	if start >= total {
		return data[:0]
	}

	if end >= total {
		end = total - 1
	}

	return data[start : end+1]
}

// serveWithRange serves payload with optional HTTP Range support.
func (h *Handler) serveWithRange(c *echo.Context, payload []byte) error {
	rangeHeader := c.Request().Header.Get("Range")

	if rangeHeader == "" {
		c.Response().WriteHeader(http.StatusOK)
		_, err := io.Copy(c.Response(), bytes.NewReader(payload))

		return err
	}

	// Parse "bytes=start-end" range header.
	const rangePrefix = "bytes="
	if !strings.HasPrefix(rangeHeader, rangePrefix) {
		return h.writeError(
			c,
			http.StatusRequestedRangeNotSatisfiable,
			"InvalidRange",
			"invalid Range header",
		)
	}

	const rangeParts = 2 // start and end
	parts := strings.SplitN(rangeHeader[len(rangePrefix):], "-", rangeParts)
	if len(parts) != rangeParts {
		return h.writeError(
			c,
			http.StatusRequestedRangeNotSatisfiable,
			"InvalidRange",
			"invalid Range header",
		)
	}

	start, err1 := strconv.ParseInt(parts[0], 10, 64)
	end, err2 := strconv.ParseInt(parts[1], 10, 64)

	total := int64(len(payload))

	if err1 != nil || err2 != nil || start < 0 || end < start || end >= total {
		return h.writeError(c, http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
			fmt.Sprintf("Range %s not satisfiable for %d-byte resource", rangeHeader, total))
	}

	chunk := payload[start : end+1]
	c.Response().Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	c.Response().WriteHeader(http.StatusPartialContent)

	_, err := io.Copy(c.Response(), bytes.NewReader(chunk))

	return err
}

// toDescribeJobResponse converts a job to a describe job response.
func toDescribeJobResponse(j *Job) describeJobResponse {
	resp := describeJobResponse{
		JobID:              j.JobID,
		JobDescription:     j.JobDescription,
		Action:             j.Action,
		ArchiveID:          j.ArchiveID,
		VaultARN:           j.VaultARN,
		CreationDate:       j.CreationDate,
		Completed:          j.Completed,
		StatusCode:         j.StatusCode,
		StatusMessage:      j.StatusMessage,
		Tier:               j.Tier,
		SNSTopic:           j.SNSTopic,
		RetrievalByteRange: j.RetrievalByteRange,
		JobOutputPath:      j.JobOutputPath,
		OutputLocation:     j.OutputLocation,
		SelectParameters:   j.SelectParameters,
	}

	if j.ArchiveSizeInBytes > 0 {
		size := j.ArchiveSizeInBytes
		resp.ArchiveSizeInBytes = &size
	}

	if j.InventorySizeInBytes > 0 {
		size := j.InventorySizeInBytes
		resp.InventorySizeInBytes = &size
	}

	if j.SHA256TreeHash != "" {
		resp.SHA256TreeHash = j.SHA256TreeHash
	}

	if j.ArchiveSHA256TreeHash != "" {
		resp.ArchiveSHA256TreeHash = j.ArchiveSHA256TreeHash
	}

	if j.Completed {
		resp.CompletionDate = j.CompletionDate
	}

	// InventoryRetrievalParameters is only ever non-null for InventoryRetrieval jobs
	// on the real wire; this replaces the invented top-level "Format" field the
	// response DTO used to carry (see PARITY.md).
	if j.Action == jobTypeInventoryRetrieval {
		resp.InventoryRetrievalParameters = &inventoryRetrievalJobDescriptionResponse{
			StartDate: j.InventoryRetrievalStartDate,
			EndDate:   j.InventoryRetrievalEndDate,
			Format:    j.InventoryFormat,
			Limit:     j.InventoryRetrievalLimit,
			Marker:    j.InventoryRetrievalMarker,
		}
	}

	return resp
}
