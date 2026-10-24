package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathExportJobsV2    = "/exportjobsv2"
	maxExportJobsResult = 100

	keyExportJobID = "ExportJobId"
	keyStatus      = "Status"
	keyFindings    = "Findings"
)

func classifyExportJobsV2Path(method, path string) (string, string) {
	rest := strings.TrimPrefix(strings.TrimPrefix(path, pathExportJobsV2), "/")

	switch {
	case rest == "" && method == http.MethodPost:
		return opStartExportJobV2, ""
	case rest == "" && method == http.MethodGet:
		return opListExportJobsV2, ""
	case method == http.MethodGet && !strings.Contains(rest, "/"):
		return opGetExportJobV2, rest
	case method == http.MethodPost && strings.HasSuffix(rest, "/cancel"):
		return opCancelExportJobV2, strings.TrimSuffix(rest, "/cancel")
	}

	return opUnknown, ""
}

func exportErrorResponse(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrInvalidInput):
		return typedErrorResponse(c, http.StatusBadRequest, "ValidationException", err.Error())
	case errors.Is(err, ErrNotFound):
		return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "Export job not found")
	case errors.Is(err, ErrExportJobRunning):
		return typedErrorResponse(c, http.StatusPaymentRequired, "ServiceQuotaExceededException", err.Error())
	case errors.Is(err, ErrExportJobFinished):
		return typedErrorResponse(c, http.StatusConflict, "ConflictException", err.Error())
	default:
		return createErrorResponse(c, err)
	}
}

func exportInputFromBody(body map[string]any) *ExportJobV2Input {
	in := &ExportJobV2Input{}
	in.Name, _ = body[keyName].(string)

	dest, _ := body["Destination"].(map[string]any)
	s3, _ := dest["S3"].(map[string]any)
	in.BucketArn, _ = s3["BucketArn"].(string)
	in.KmsKeyArn, _ = s3["KmsKeyArn"].(string)
	in.ObjectPrefix, _ = s3["ObjectPrefix"].(string)

	out, _ := body["OutputConfiguration"].(map[string]any)
	findings, _ := out[keyFindings].(map[string]any)
	in.Format, _ = findings["Format"].(string)
	in.Filters, _ = findings["Filters"].(map[string]any)
	in.SelectedFields = stringListFromBody(findings, "SelectedFields")

	scopes, _ := body["Scopes"].(map[string]any)
	if orgs, ok := scopes["AwsOrganizations"].([]any); ok {
		for _, o := range orgs {
			m, _ := o.(map[string]any)
			entry := map[string]string{}
			entry["OrganizationId"], _ = m["OrganizationId"].(string)

			if ou, has := m["OrganizationalUnitId"].(string); has {
				entry["OrganizationalUnitId"] = ou
			}

			in.Scopes = append(in.Scopes, entry)
		}
	}

	return in
}

func exportSummaryResponse(j *ExportJobV2, full bool) map[string]any {
	s3 := map[string]any{"BucketArn": j.BucketArn, "KmsKeyArn": j.KmsKeyArn}
	if j.ObjectPrefix != "" {
		s3["ObjectPrefix"] = j.ObjectPrefix
	}

	findings := map[string]any{"Format": j.Format}

	if full {
		if j.Filters != nil {
			findings["Filters"] = j.Filters
		}

		if len(j.SelectedFields) > 0 {
			findings["SelectedFields"] = j.SelectedFields
		}
	}

	resp := map[string]any{
		keyExportJobID:        j.ExportJobID,
		"DataType":            j.DataType,
		keyStatus:             j.Status,
		"StartedAt":           j.StartedAt,
		"Destination":         map[string]any{"S3": s3},
		"OutputConfiguration": map[string]any{keyFindings: findings},
	}

	for k, v := range map[string]string{
		keyName: j.Name, "EndedAt": j.EndedAt, "FailureCode": j.FailureCode, "FailureMessage": j.FailureMessage,
	} {
		if v != "" {
			resp[k] = v
		}
	}

	if len(j.Scopes) > 0 {
		orgs := make([]map[string]string, len(j.Scopes))
		copy(orgs, j.Scopes)
		resp["Scopes"] = map[string]any{"AwsOrganizations": orgs}
	}

	return resp
}

func (h *Handler) handleStartExportJobV2(c *echo.Context, body map[string]any) error {
	in := exportInputFromBody(body)

	job, err := idemCreate(h, opStartExportJobV2, body,
		func(j *ExportJobV2) string { return j.ExportJobID }, h.Backend.GetExportJobV2,
		func() (*ExportJobV2, error) { return h.Backend.StartExportJobV2(in) })
	if err != nil {
		return exportErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyExportJobID: job.ExportJobID})
}

func (h *Handler) handleGetExportJobV2(c *echo.Context, id string) error {
	job, err := h.Backend.GetExportJobV2(id)
	if err != nil {
		return exportErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, exportSummaryResponse(job, true))
}

func (h *Handler) handleListExportJobsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam(keyMaxResults); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	if !validPagingMax(maxResults, maxExportJobsResult, nextToken) {
		return typedErrorResponse(c, http.StatusBadRequest, "ValidationException", "invalid MaxResults or NextToken")
	}

	jobs, next := h.Backend.ListExportJobsV2(c.QueryParam("DataType"), c.QueryParam("Status"), nextToken, maxResults)

	items := make([]map[string]any, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, exportSummaryResponse(j, false))
	}

	resp := map[string]any{"Items": items}
	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleCancelExportJobV2(c *echo.Context, id string) error {
	job, err := h.Backend.CancelExportJobV2(id)
	if err != nil {
		return exportErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyExportJobID: job.ExportJobID, keyStatus: job.Status})
}

func (h *Handler) exportJobsV2OpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opStartExportJobV2:  func() error { return h.handleStartExportJobV2(c, body) },
		opGetExportJobV2:    func() error { return h.handleGetExportJobV2(c, resource) },
		opListExportJobsV2:  func() error { return h.handleListExportJobsV2(c) },
		opCancelExportJobV2: func() error { return h.handleCancelExportJobV2(c, resource) },
	}
}
