package s3control

import (
	"encoding/xml"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathJobs      = "/v20180820/jobs"
	pathJobPrefix = "/v20180820/jobs/"
)

// extractJobOps handles S3 Batch Operations job operations.
func extractJobOps(path, method string) string {
	if path == pathJobs {
		switch method {
		case http.MethodPost:
			return "CreateJob"
		case http.MethodGet:
			return "ListJobs"
		}

		return ""
	}

	if isSimplePath(pathJobPrefix, path) && method == http.MethodGet {
		return "DescribeJob"
	}

	return extractJobSubResourceOp(path, method)
}

// extractJobSubResourceOp handles job tagging, priority, and status operations.
func extractJobSubResourceOp(path, method string) string {
	if isPrefixSuffix(pathJobPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return "GetJobTagging"
		case http.MethodPut:
			return "PutJobTagging"
		case http.MethodDelete:
			return "DeleteJobTagging"
		}

		return ""
	}

	// UpdateJobPriority and UpdateJobStatus are POST in the real SDK (not PUT):
	// see aws-sdk-go-v2/service/s3control serializers.go.
	if isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPost {
		return "UpdateJobPriority"
	}

	if isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPost {
		return "UpdateJobStatus"
	}

	return ""
}

// dispatchJobOps handles S3 Batch Operations job dispatch.
func (h *Handler) dispatchJobOps(c *echo.Context, path, method string) (bool, error) {
	if path == pathJobs {
		switch method {
		case http.MethodPost:
			return true, h.handleCreateJob(c)
		case http.MethodGet:
			return true, h.handleListJobs(c)
		}

		return false, nil
	}

	if isSimplePath(pathJobPrefix, path) && method == http.MethodGet {
		return true, h.handleDescribeJob(c)
	}

	return h.dispatchJobSubResourceOps(c, path, method)
}

// dispatchJobSubResourceOps handles job tagging, priority, and status dispatch.
func (h *Handler) dispatchJobSubResourceOps(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathJobPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetJobTagging(c)
		case http.MethodPut:
			return true, h.handlePutJobTagging(c)
		case http.MethodDelete:
			return true, h.handleDeleteJobTagging(c)
		}

		return false, nil
	}

	if isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPost {
		return true, h.handleUpdateJobPriority(c)
	}

	if isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPost {
		return true, h.handleUpdateJobStatus(c)
	}

	return false, nil
}

// --- CreateJob handler ---

// createJobXMLCapture captures the raw inner XML of an element without
// interpreting its structure. Shared by CreateJob (Manifest/Operation/Report)
// and CreateStorageLensGroup (Filter).
type createJobXMLCapture struct {
	Raw string `xml:",innerxml"`
}

type createJobRequestXML struct {
	XMLName              xml.Name            `xml:"CreateJobRequest"`
	ClientRequestToken   string              `xml:"ClientRequestToken"`
	Description          string              `xml:"Description"`
	RoleArn              string              `xml:"RoleArn"`
	Manifest             createJobXMLCapture `xml:"Manifest"`
	Operation            createJobXMLCapture `xml:"Operation"`
	Report               createJobXMLCapture `xml:"Report"`
	Priority             int32               `xml:"Priority"`
	ConfirmationRequired bool                `xml:"ConfirmationRequired"`
}

type createJobResponseXML struct {
	XMLName xml.Name `xml:"CreateJobResult"`
	JobID   string   `xml:"JobId"`
}

func (h *Handler) handleCreateJob(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createJobRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	job, err := h.Backend.CreateJob(accountID, body.RoleArn, body.Priority)
	if err != nil {
		return handleBackendError(c, err)
	}

	// Persist extended fields if present.
	if body.Description != "" || body.Manifest.Raw != "" || body.Operation.Raw != "" ||
		body.Report.Raw != "" || body.ConfirmationRequired {
		_ = h.Backend.UpdateJobDetails(
			accountID, job.JobID,
			body.Description,
			body.Manifest.Raw,
			body.Operation.Raw,
			body.Report.Raw,
			body.ConfirmationRequired,
		)
	}

	return writeXML(c, createJobResponseXML{
		JobID: job.JobID,
	})
}

// --- Batch job read/update handlers ---

type describeJobInnerXML struct {
	XMLName xml.Name
	Raw     string `xml:",innerxml"`
}

type describeJobDescriptorXML struct {
	Manifest             *describeJobInnerXML `xml:"Manifest,omitempty"`
	Report               *describeJobInnerXML `xml:"Report,omitempty"`
	Operation            *describeJobInnerXML `xml:"Operation,omitempty"`
	StatusUpdateReason   string               `xml:"StatusUpdateReason,omitempty"`
	Description          string               `xml:"Description,omitempty"`
	CreationTime         string               `xml:"CreationTime,omitempty"`
	TerminationDate      string               `xml:"TerminationDate,omitempty"`
	JobArn               string               `xml:"JobArn"`
	Status               string               `xml:"Status"`
	RoleArn              string               `xml:"RoleArn"`
	JobID                string               `xml:"JobId"`
	Priority             int32                `xml:"Priority"`
	ConfirmationRequired bool                 `xml:"ConfirmationRequired,omitempty"`
}

type describeJobResponseXML struct {
	XMLName xml.Name                 `xml:"DescribeJobResult"`
	Job     describeJobDescriptorXML `xml:"Job"`
}

func (h *Handler) handleDescribeJob(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix)

	job, err := h.Backend.GetJob(accountID, jobID)
	if err != nil {
		return handleBackendError(c, err)
	}

	desc := describeJobDescriptorXML{
		JobID:                job.JobID,
		JobArn:               job.JobArn,
		Status:               job.Status,
		Priority:             job.Priority,
		RoleArn:              job.RoleArn,
		Description:          job.Description,
		ConfirmationRequired: job.ConfirmationRequired,
		CreationTime:         job.CreationTime,
		TerminationDate:      job.TerminationDate,
		StatusUpdateReason:   job.StatusUpdateReason,
	}

	if job.Manifest != "" {
		desc.Manifest = &describeJobInnerXML{Raw: job.Manifest}
	}

	if job.Operation != "" {
		desc.Operation = &describeJobInnerXML{Raw: job.Operation}
	}

	if job.Report != "" {
		desc.Report = &describeJobInnerXML{Raw: job.Report}
	}

	return writeXML(c, describeJobResponseXML{Job: desc})
}

type listJobsJobXML struct {
	JobID    string `xml:"JobId"`
	Status   string `xml:"Status"`
	Priority int32  `xml:"Priority"`
}

type listJobsResponseXML struct {
	XMLName   xml.Name         `xml:"ListJobsResult"`
	NextToken string           `xml:"NextToken,omitempty"`
	Jobs      []listJobsJobXML `xml:"Jobs>member"`
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))
	jobStatuses := q["jobStatuses"]

	jobs := h.Backend.ListJobs(accountID)

	items := make([]listJobsJobXML, 0, len(jobs))
	for _, j := range jobs {
		if len(jobStatuses) > 0 && !slices.Contains(jobStatuses, j.Status) {
			continue
		}

		items = append(items, listJobsJobXML{
			JobID:    j.JobID,
			Status:   j.Status,
			Priority: j.Priority,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listJobsResponseXML{Jobs: page, NextToken: tok})
}

type updateJobPriorityRequestXML struct {
	XMLName  xml.Name `xml:"UpdateJobPriorityRequest"`
	Priority int32    `xml:"Priority"`
}

type updateJobPriorityResponseXML struct {
	XMLName  xml.Name `xml:"UpdateJobPriorityResult"`
	JobID    string   `xml:"JobId"`
	Priority int32    `xml:"Priority"`
}

func (h *Handler) handleUpdateJobPriority(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	jobID := strings.TrimSuffix(strings.TrimPrefix(path, pathJobPrefix), "/priority")

	var body updateJobPriorityRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	job, err := h.Backend.UpdateJobPriority(accountID, jobID, body.Priority)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, updateJobPriorityResponseXML{
		JobID:    job.JobID,
		Priority: job.Priority,
	})
}

type updateJobStatusRequestXML struct {
	XMLName            xml.Name `xml:"UpdateJobStatusRequest"`
	RequestedJobStatus string   `xml:"RequestedJobStatus"`
	StatusUpdateReason string   `xml:"StatusUpdateReason"`
}

type updateJobStatusResponseXML struct {
	XMLName xml.Name `xml:"UpdateJobStatusResult"`
	JobID   string   `xml:"JobId"`
	Status  string   `xml:"Status"`
}

func (h *Handler) handleUpdateJobStatus(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	jobID := strings.TrimSuffix(strings.TrimPrefix(path, pathJobPrefix), "/status")

	var body updateJobStatusRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	job, err := h.Backend.UpdateJobStatusValidated(accountID, jobID, body.RequestedJobStatus, body.StatusUpdateReason)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, updateJobStatusResponseXML{
		JobID:  job.JobID,
		Status: job.Status,
	})
}

// ---- Job Tagging ----

type jobTagSetXML struct {
	Tags []jobTagXML `xml:"Tag"`
}

type jobTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type putJobTaggingRequestXML struct {
	XMLName xml.Name     `xml:"PutJobTaggingRequest"`
	Tags    jobTagSetXML `xml:"Tags"`
}

type getJobTaggingResponseXML struct {
	XMLName xml.Name     `xml:"GetJobTaggingResult"`
	Tags    jobTagSetXML `xml:"Tags"`
}

func (h *Handler) handleGetJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	tags, err := h.Backend.GetJobTagging(accountID, jobID)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getJobTaggingResponseXML{}
	for k, v := range tags {
		resp.Tags.Tags = append(resp.Tags.Tags, jobTagXML{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

func (h *Handler) handlePutJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	var body putJobTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	tags := make(TagSet, len(body.Tags.Tags))
	for _, t := range body.Tags.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutJobTagging(accountID, jobID, tags); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutJobTaggingResult"`
	}{})
}

func (h *Handler) handleDeleteJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	if err := h.Backend.DeleteJobTagging(accountID, jobID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}
