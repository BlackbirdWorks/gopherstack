package iot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func readBody(c *echo.Context, dst any) error {
	if err := json.NewDecoder(c.Request().Body).Decode(dst); err != nil && !errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
	}

	return nil
}

const (
	twoparts = 2 // used for SplitN to get at most 2 parts

	keyJobID               = "jobId"
	keyJobARN              = "jobArn"
	keyDescription         = "description"
	keyCreatedAt           = "createdAt"
	keyLastUpdatedAt       = "lastUpdatedAt"
	keyDomainConfigName    = "domainConfigurationName"
	keyDomainConfigARN     = "domainConfigurationArn"
	keyTemplateName        = "templateName"
	keyAuthorizerName      = "authorizerName"
	keyAuthorizerARN       = "authorizerArn"
	keyScheduledAuditARN   = "scheduledAuditArn"
	keyActionARN           = "actionArn"
	keySecurityProfileName = "securityProfileName"
	keySecurityProfileARN  = "securityProfileArn"
)

// ---------------------------------------------------------------------------
// Route resolution helpers for new ops
// ---------------------------------------------------------------------------

//nolint:cyclop // mechanical switch over URL path patterns
func resolveJobOps(path, method string) string {
	switch {
	// GET /jobs/{jobId}/document → GetJobDocument
	case strings.HasPrefix(path, "/jobs/") && strings.HasSuffix(path, "/document") && method == http.MethodGet:
		return opGetJobDocument
	// More-specific thing-scoped ops before job-scoped.
	// PUT /jobs/{jobId}/things/{thingName}/cancel → CancelJobExecution
	case strings.HasPrefix(path, "/jobs/") &&
		strings.Contains(path, "/things/") &&
		strings.HasSuffix(path, "/cancel") &&
		method == http.MethodPut:
		return opCancelJobExecution
	// PUT /jobs/{jobId}/cancel → CancelJob
	case strings.HasPrefix(path, "/jobs/") && strings.HasSuffix(path, "/cancel") && method == http.MethodPut:
		return opCancelJob
	// DELETE /jobs/{jobId}/things/{thingName} → DeleteJobExecution
	case strings.HasPrefix(path, "/jobs/") && strings.Contains(path, "/things/") && method == http.MethodDelete:
		return opDeleteJobExecution
	// GET /jobs/{jobId}/things/{thingName} → DescribeJobExecution
	case strings.HasPrefix(path, "/jobs/") && strings.Contains(path, "/things/") && method == http.MethodGet:
		return opDescribeJobExecution
	// GET /jobs → ListJobs
	case path == "/jobs" && method == http.MethodGet:
		return opListJobs
	// GET /jobs/{jobId}/things → ListJobExecutionsForJob (no thingName segment)
	case strings.HasPrefix(path, "/jobs/") && strings.HasSuffix(path, "/things") && method == http.MethodGet:
		return opListJobExecutionsForJob
	// POST /jobs/{jobId} → CreateJob
	case strings.HasPrefix(path, "/jobs/") && method == http.MethodPost:
		return opCreateJob
	// GET /jobs/{jobId} → DescribeJob
	case strings.HasPrefix(path, "/jobs/") && method == http.MethodGet:
		return opDescribeJob
	// PATCH /jobs/{jobId} → UpdateJob
	case strings.HasPrefix(path, "/jobs/") && method == http.MethodPatch:
		return opUpdateJob
	// DELETE /jobs/{jobId} → DeleteJob
	case strings.HasPrefix(path, "/jobs/") && method == http.MethodDelete:
		return opDeleteJob
	}

	return unknownOperation
}

func resolveJobTemplateOps(path, method string) string {
	switch {
	case path == "/job-templates" && method == http.MethodGet:
		return opListJobTemplates
	case strings.HasPrefix(path, "/job-templates/") && method == http.MethodPost:
		return opCreateJobTemplate
	case strings.HasPrefix(path, "/job-templates/") && method == http.MethodGet:
		return opDescribeJobTemplate
	case strings.HasPrefix(path, "/job-templates/") && method == http.MethodDelete:
		return opDeleteJobTemplate
	}

	return unknownOperation
}

func resolveRoleAliasOps(path, method string) string {
	switch {
	case path == "/role-aliases" && method == http.MethodGet:
		return opListRoleAliases
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodPost:
		return opCreateRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodGet:
		return opDescribeRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodPut:
		return opUpdateRoleAlias
	case strings.HasPrefix(path, "/role-aliases/") && method == http.MethodDelete:
		return opDeleteRoleAlias
	}

	return unknownOperation
}

func resolveDomainConfigOps(path, method string) string {
	switch {
	case path == "/domainConfigurations" && method == http.MethodGet:
		return opListDomainConfigurations
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodPost:
		return opCreateDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodGet:
		return opDescribeDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodPut:
		return opUpdateDomainConfiguration
	case strings.HasPrefix(path, "/domainConfigurations/") && method == http.MethodDelete:
		return opDeleteDomainConfiguration
	}

	return unknownOperation
}

//nolint:cyclop // mechanical switch over URL path patterns
func resolveProvisioningTemplateOps(path, method string) string {
	switch {
	// POST /provisioning-templates → CreateProvisioningTemplate
	case path == "/provisioning-templates" && method == http.MethodPost:
		return opCreateProvisioningTemplate
	// GET /provisioning-templates → ListProvisioningTemplates
	case path == "/provisioning-templates" && method == http.MethodGet:
		return opListProvisioningTemplates
	// POST /provisioning-templates/{templateName}/versions → CreateProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/versions") &&
		method == http.MethodPost:
		return opCreateProvisioningTemplateVersion
	// GET /provisioning-templates/{templateName}/versions → ListProvisioningTemplateVersions
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.HasSuffix(path, "/versions") &&
		method == http.MethodGet:
		return opListProvisioningTemplateVersions
	// DELETE /provisioning-templates/{templateName}/versions/{versionId} → DeleteProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.Contains(path, "/versions/") &&
		method == http.MethodDelete:
		return opDeleteProvisioningTemplateVersion
	// GET /provisioning-templates/{templateName}/versions/{versionId} → DescribeProvisioningTemplateVersion
	case strings.HasPrefix(path, "/provisioning-templates/") &&
		strings.Contains(path, "/versions/") &&
		!strings.HasSuffix(path, "/versions") &&
		method == http.MethodGet:
		return opDescribeProvisioningTemplateVersion
	// GET /provisioning-templates/{templateName} → DescribeProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodGet:
		return opDescribeProvisioningTemplate
	// PATCH /provisioning-templates/{templateName} → UpdateProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodPatch:
		return opUpdateProvisioningTemplate
	// DELETE /provisioning-templates/{templateName} → DeleteProvisioningTemplate
	case strings.HasPrefix(path, "/provisioning-templates/") && method == http.MethodDelete:
		return opDeleteProvisioningTemplate
	}

	return unknownOperation
}

func resolveAuthorizerOps(path, method string) string {
	switch {
	case path == "/authorizers" && method == http.MethodGet:
		return opListAuthorizers
	// POST /authorizer/{authorizerName}/test → TestInvokeAuthorizer (checked before
	// the generic POST case below, which is CreateAuthorizer).
	case strings.HasPrefix(path, "/authorizer/") &&
		strings.HasSuffix(path, "/test") && method == http.MethodPost:
		return opTestInvokeAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodPost:
		return opCreateAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodGet:
		return opDescribeAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodPut:
		return opUpdateAuthorizer
	case strings.HasPrefix(path, "/authorizer/") && method == http.MethodDelete:
		return opDeleteAuthorizer
	}

	return unknownOperation
}

//nolint:cyclop // mechanical routing switch
func resolveBillingGroupOps(path, method string) string {
	switch {
	case path == "/billing-groups" && method == http.MethodGet:
		return opListBillingGroups
	case path == "/billing-groups/removeThingFromBillingGroup" && method == http.MethodPut:
		return opRemoveThingFromBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") &&
		strings.HasSuffix(path, "/things") &&
		method == http.MethodGet:
		return opListThingsInBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") && method == http.MethodPost:
		return opCreateBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") && method == http.MethodGet:
		return opDescribeBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") && method == http.MethodPatch:
		return opUpdateBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") && method == http.MethodDelete:
		return opDeleteBillingGroup
	}

	return unknownOperation
}

func resolveScheduledAuditOps(path, method string) string {
	switch {
	case path == "/audit/scheduledaudits" && method == http.MethodGet:
		return opListScheduledAudits
	case strings.HasPrefix(path, "/audit/scheduledaudits/") && method == http.MethodPost:
		return opCreateScheduledAudit
	case strings.HasPrefix(path, "/audit/scheduledaudits/") && method == http.MethodGet:
		return opDescribeScheduledAudit
	case strings.HasPrefix(path, "/audit/scheduledaudits/") && method == http.MethodPatch:
		return opUpdateScheduledAudit
	case strings.HasPrefix(path, "/audit/scheduledaudits/") && method == http.MethodDelete:
		return opDeleteScheduledAudit
	}

	return unknownOperation
}

func resolveMitigationActionOps(path, method string) string {
	switch {
	case path == "/mitigationactions/actions" && method == http.MethodGet:
		return opListMitigationActions
	case strings.HasPrefix(path, "/mitigationactions/actions/") && method == http.MethodPost:
		return opCreateMitigationAction
	case strings.HasPrefix(path, "/mitigationactions/actions/") && method == http.MethodGet:
		return opDescribeMitigationAction
	case strings.HasPrefix(path, "/mitigationactions/actions/") && method == http.MethodPatch:
		return opUpdateMitigationAction
	case strings.HasPrefix(path, "/mitigationactions/actions/") && method == http.MethodDelete:
		return opDeleteMitigationAction
	}

	return unknownOperation
}

// resolveSecurityProfileTargetOps handles /security-profiles/{name}/targets operations.
func resolveSecurityProfileTargetOps(path, method string) string {
	if !strings.HasPrefix(path, "/security-profiles/") || !strings.HasSuffix(path, "/targets") {
		return unknownOperation
	}
	switch method {
	case http.MethodDelete:
		return opDetachSecurityProfile
	case http.MethodGet:
		return opListTargetsForSecurityProfile
	case http.MethodPut:
		return opAttachSecurityProfile
	}

	return unknownOperation
}

func resolveSecurityProfileOps(path, method string) string {
	switch {
	case path == "/security-profiles" && method == http.MethodGet:
		return opListSecurityProfiles
	case path == "/security-profiles-for-target" && method == http.MethodGet:
		return opListSecurityProfilesForTarget
	}
	// /security-profiles/{name}/targets — must be before generic prefix cases
	if op := resolveSecurityProfileTargetOps(path, method); op != unknownOperation {
		return op
	}
	switch {
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodPost:
		return opCreateSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodGet:
		return opDescribeSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodPatch:
		return opUpdateSecurityProfile
	case strings.HasPrefix(path, "/security-profiles/") && method == http.MethodDelete:
		return opDeleteSecurityProfile
	}

	return unknownOperation
}

// ---------------------------------------------------------------------------
// HTTP handler implementations
// ---------------------------------------------------------------------------

type awsErrBody struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func respondNotFound(c *echo.Context, msg string) error {
	return c.JSON(http.StatusNotFound, awsErrBody{"ResourceNotFoundException", msg})
}

func respondConflict(c *echo.Context, msg string) error {
	return c.JSON(http.StatusConflict, awsErrBody{"ResourceAlreadyExistsException", msg})
}

func respondErr(c *echo.Context, err error) error {
	if errors.Is(err, ErrResourceNotFound) {
		return respondNotFound(c, err.Error())
	}
	if errors.Is(err, ErrAlreadyExists) {
		return respondConflict(c, err.Error())
	}

	return c.JSON(http.StatusBadRequest, awsErrBody{"InvalidRequestException", err.Error()})
}

// --- Jobs ---

func (h *Handler) handleCreateJob(c *echo.Context) error {
	// jobId is in the path: POST /jobs/{jobId}
	jobID := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	var input CreateJobInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.JobID = jobID
	job, err := h.Backend.CreateJob(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyJobID:       job.JobID,
		keyJobARN:      job.JobARN,
		keyDescription: job.Description,
	})
}

func (h *Handler) handleDescribeJob(c *echo.Context) error {
	jobID := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	job, err := h.Backend.DescribeJob(jobID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"job": job})
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	jobs := h.Backend.ListJobs()
	summaries := make([]map[string]any, len(jobs))
	for i, j := range jobs {
		summaries[i] = map[string]any{
			keyJobID:          j.JobID,
			keyJobARN:         j.JobARN,
			"status":          j.Status,
			"targetSelection": j.TargetSelection,
			keyCreatedAt:      j.CreatedAt,
			keyLastUpdatedAt:  j.LastUpdatedAt,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"jobs": summaries})
}

func (h *Handler) handleUpdateJob(c *echo.Context) error {
	jobID := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	var req struct {
		Description string `json:"description"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateJob(jobID, req.Description); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteJob(c *echo.Context) error {
	jobID := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	if err := h.Backend.DeleteJob(jobID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCancelJob(c *echo.Context) error {
	// PUT /jobs/{jobId}/cancel
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	jobID := strings.TrimSuffix(trimmed, "/cancel")
	var req struct {
		Comment string `json:"comment"`
	}
	_ = readBody(c, &req)
	job, err := h.Backend.CancelJob(jobID, req.Comment)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyJobID:  job.JobID,
		keyJobARN: job.JobARN,
	})
}

func (h *Handler) handleGetJobDocument(c *echo.Context) error {
	// GET /jobs/{jobId}/document
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	jobID := strings.TrimSuffix(trimmed, "/document")
	doc, err := h.Backend.GetJobDocument(jobID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"document": doc})
}

func (h *Handler) handleDescribeJobExecution(c *echo.Context) error {
	// GET /jobs/{jobId}/things/{thingName}
	jobID, thingName := parseJobThingPath(c.Request().URL.Path)
	exec, err := h.Backend.DescribeJobExecution(jobID, thingName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"execution": exec})
}

func (h *Handler) handleCancelJobExecution(c *echo.Context) error {
	// PUT /jobs/{jobId}/things/{thingName}/cancel
	path := c.Request().URL.Path
	path = strings.TrimSuffix(path, "/cancel")
	jobID, thingName := parseJobThingPath(path)
	if err := h.Backend.CancelJobExecution(jobID, thingName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteJobExecution(c *echo.Context) error {
	// DELETE /jobs/{jobId}/things/{thingName}
	jobID, thingName := parseJobThingPath(c.Request().URL.Path)
	if err := h.Backend.DeleteJobExecution(jobID, thingName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// parseJobThingPath extracts jobId and thingName from /jobs/{jobId}/things/{thingName}[/...].
func parseJobThingPath(path string) (string, string) {
	trimmed := strings.TrimPrefix(path, "/jobs/")
	parts := strings.SplitN(trimmed, "/things/", twoparts)
	if len(parts) == twoparts {
		return parts[0], strings.SplitN(parts[1], "/", twoparts)[0]
	}

	return "", ""
}

// --- JobTemplates ---

func (h *Handler) handleCreateJobTemplate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/job-templates/")
	var input CreateJobTemplateInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.JobTemplateID = id
	jt, err := h.Backend.CreateJobTemplate(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"jobTemplateId":  jt.JobTemplateID,
		"jobTemplateArn": jt.JobTemplateARN,
	})
}

func (h *Handler) handleDescribeJobTemplate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/job-templates/")
	jt, err := h.Backend.DescribeJobTemplate(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, jt)
}

func (h *Handler) handleListJobTemplates(c *echo.Context) error {
	templates := h.Backend.ListJobTemplates()
	summaries := make([]map[string]any, len(templates))
	for i, jt := range templates {
		summaries[i] = map[string]any{
			"jobTemplateId":  jt.JobTemplateID,
			"jobTemplateArn": jt.JobTemplateARN,
			keyDescription:   jt.Description,
			keyCreatedAt:     jt.CreatedAt,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"jobTemplates": summaries})
}

func (h *Handler) handleDeleteJobTemplate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/job-templates/")
	if err := h.Backend.DeleteJobTemplate(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- RoleAliases ---

func (h *Handler) handleCreateRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	var input CreateRoleAliasInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.RoleAlias = alias
	ra, err := h.Backend.CreateRoleAlias(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"roleAlias":    ra.RoleAlias,
		"roleAliasArn": ra.RoleAliasARN,
	})
}

func (h *Handler) handleDescribeRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	ra, err := h.Backend.DescribeRoleAlias(alias)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"roleAliasDescription": ra})
}

func (h *Handler) handleListRoleAliases(c *echo.Context) error {
	aliases := h.Backend.ListRoleAliases()
	names := make([]string, len(aliases))
	for i, ra := range aliases {
		names[i] = ra.RoleAlias
	}

	return c.JSON(http.StatusOK, map[string]any{"roleAliases": names})
}

func (h *Handler) handleUpdateRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	var req struct {
		RoleARN                   string `json:"roleArn"`
		CredentialDurationSeconds int    `json:"credentialDurationSeconds"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	ra, err := h.Backend.UpdateRoleAlias(alias, req.RoleARN, req.CredentialDurationSeconds)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"roleAlias":    ra.RoleAlias,
		"roleAliasArn": ra.RoleAliasARN,
	})
}

func (h *Handler) handleDeleteRoleAlias(c *echo.Context) error {
	alias := strings.TrimPrefix(c.Request().URL.Path, "/role-aliases/")
	if err := h.Backend.DeleteRoleAlias(alias); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- DomainConfigurations ---

func (h *Handler) handleCreateDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	var input CreateDomainConfigurationInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.DomainConfigurationName = name
	dc, err := h.Backend.CreateDomainConfiguration(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDomainConfigName: dc.DomainConfigurationName,
		keyDomainConfigARN:  dc.DomainConfigurationARN,
	})
}

func (h *Handler) handleDescribeDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	dc, err := h.Backend.DescribeDomainConfiguration(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, dc)
}

func (h *Handler) handleListDomainConfigurations(c *echo.Context) error {
	configs := h.Backend.ListDomainConfigurations()
	summaries := make([]map[string]any, len(configs))
	for i, dc := range configs {
		summaries[i] = map[string]any{
			keyDomainConfigName:         dc.DomainConfigurationName,
			keyDomainConfigARN:          dc.DomainConfigurationARN,
			"domainConfigurationStatus": dc.DomainConfigurationStatus,
			"serviceType":               dc.ServiceType,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"domainConfigurations": summaries})
}

func (h *Handler) handleUpdateDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	var req struct {
		DomainConfigurationStatus string `json:"domainConfigurationStatus"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	dc, err := h.Backend.UpdateDomainConfiguration(name, req.DomainConfigurationStatus)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDomainConfigName: dc.DomainConfigurationName,
		keyDomainConfigARN:  dc.DomainConfigurationARN,
	})
}

func (h *Handler) handleDeleteDomainConfiguration(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/domainConfigurations/")
	if err := h.Backend.DeleteDomainConfiguration(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ProvisioningTemplates ---

func (h *Handler) handleCreateProvisioningTemplate(c *echo.Context) error {
	var input CreateProvisioningTemplateInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	pt, err := h.Backend.CreateProvisioningTemplate(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"templateArn":   pt.TemplateARN,
		keyTemplateName: pt.TemplateName,
	})
}

func (h *Handler) handleDescribeProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	pt, err := h.Backend.DescribeProvisioningTemplate(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, pt)
}

func (h *Handler) handleListProvisioningTemplates(c *echo.Context) error {
	templates := h.Backend.ListProvisioningTemplates()
	summaries := make([]map[string]any, len(templates))
	for i, pt := range templates {
		summaries[i] = map[string]any{
			"templateArn":      pt.TemplateARN,
			keyTemplateName:    pt.TemplateName,
			keyDescription:     pt.Description,
			"enabled":          pt.Enabled,
			"creationDate":     pt.CreationDate,
			"lastModifiedDate": pt.LastModifiedDate,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"templates": summaries})
}

func (h *Handler) handleUpdateProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	var req struct {
		Enabled             *bool  `json:"enabled"`
		Description         string `json:"description"`
		ProvisioningRoleARN string `json:"provisioningRoleArn"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateProvisioningTemplate(
		name, req.Description, req.Enabled, req.ProvisioningRoleARN,
	); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteProvisioningTemplate(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	if err := h.Backend.DeleteProvisioningTemplate(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreateProvisioningTemplateVersion(c *echo.Context) error {
	// POST /provisioning-templates/{templateName}/versions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	name := strings.TrimSuffix(trimmed, "/versions")
	var req struct {
		TemplateBody string `json:"templateBody"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	v, err := h.Backend.CreateProvisioningTemplateVersion(name, req.TemplateBody)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTemplateName: name,
		"versionId":     v.VersionID,
	})
}

func (h *Handler) handleListProvisioningTemplateVersions(c *echo.Context) error {
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	name := strings.TrimSuffix(trimmed, "/versions")
	versions, err := h.Backend.ListProvisioningTemplateVersions(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{pathSegmentVersions: versions})
}

func (h *Handler) handleDeleteProvisioningTemplateVersion(c *echo.Context) error {
	// DELETE /provisioning-templates/{templateName}/versions/{versionId}
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/provisioning-templates/")
	parts := strings.SplitN(trimmed, "/versions/", twoparts)
	if len(parts) != twoparts {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid path"})
	}
	name := parts[0]
	var versionID int32
	if err := parseInt32(parts[1], &versionID); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: "invalid versionId"})
	}
	if err := h.Backend.DeleteProvisioningTemplateVersion(name, versionID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Authorizers ---

func (h *Handler) handleCreateAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	var input CreateAuthorizerInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.AuthorizerName = name
	a, err := h.Backend.CreateAuthorizer(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAuthorizerName: a.AuthorizerName,
		keyAuthorizerARN:  a.AuthorizerARN,
	})
}

func (h *Handler) handleDescribeAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	a, err := h.Backend.DescribeAuthorizer(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerDescription": a})
}

func (h *Handler) handleListAuthorizers(c *echo.Context) error {
	authorizers := h.Backend.ListAuthorizers()
	summaries := make([]map[string]any, len(authorizers))
	for i, a := range authorizers {
		summaries[i] = map[string]any{
			keyAuthorizerName: a.AuthorizerName,
			keyAuthorizerARN:  a.AuthorizerARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizers": summaries})
}

func (h *Handler) handleUpdateAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	var req struct {
		AuthorizerFunctionARN string `json:"authorizerFunctionArn"`
		Status                string `json:"status"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	a, err := h.Backend.UpdateAuthorizer(name, req.AuthorizerFunctionARN, req.Status)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyAuthorizerName: a.AuthorizerName,
		keyAuthorizerARN:  a.AuthorizerARN,
	})
}

func (h *Handler) handleDeleteAuthorizer(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/authorizer/")
	if err := h.Backend.DeleteAuthorizer(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- BillingGroups ---

func (h *Handler) handleCreateBillingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/billing-groups/")
	var input CreateBillingGroupInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.BillingGroupName = name
	bg, err := h.Backend.CreateBillingGroup(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"billingGroupName": bg.BillingGroupName,
		"billingGroupArn":  bg.BillingGroupARN,
		"billingGroupId":   bg.BillingGroupID,
	})
}

func (h *Handler) handleDescribeBillingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/billing-groups/")
	bg, err := h.Backend.DescribeBillingGroup(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, bg)
}

func (h *Handler) handleListBillingGroups(c *echo.Context) error {
	groups := h.Backend.ListBillingGroups()
	summaries := make([]map[string]any, len(groups))
	for i, bg := range groups {
		summaries[i] = map[string]any{
			"groupName": bg.BillingGroupName,
			"groupArn":  bg.BillingGroupARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"billingGroups": summaries})
}

func (h *Handler) handleUpdateBillingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/billing-groups/")
	var req struct {
		BillingGroupProperties BillingGroupProperties `json:"billingGroupProperties"`
		ExpectedVersion        int64                  `json:"expectedVersion"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	version, err := h.Backend.UpdateBillingGroup(name, req.BillingGroupProperties)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyVersion: version})
}

func (h *Handler) handleDeleteBillingGroup(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/billing-groups/")
	if err := h.Backend.DeleteBillingGroup(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- ScheduledAudits ---

func (h *Handler) handleCreateScheduledAudit(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/audit/scheduledaudits/")
	var input CreateScheduledAuditInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.ScheduledAuditName = name
	sa, err := h.Backend.CreateScheduledAudit(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScheduledAuditARN: sa.ScheduledAuditARN})
}

func (h *Handler) handleDescribeScheduledAudit(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/audit/scheduledaudits/")
	sa, err := h.Backend.DescribeScheduledAudit(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, sa)
}

func (h *Handler) handleListScheduledAudits(c *echo.Context) error {
	audits := h.Backend.ListScheduledAudits()
	summaries := make([]map[string]any, len(audits))
	for i, sa := range audits {
		summaries[i] = map[string]any{
			"scheduledAuditName": sa.ScheduledAuditName,
			keyScheduledAuditARN: sa.ScheduledAuditARN,
			"frequency":          sa.Frequency,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"scheduledAudits": summaries})
}

func (h *Handler) handleUpdateScheduledAudit(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/audit/scheduledaudits/")
	var req struct {
		Frequency        string   `json:"frequency"`
		DayOfMonth       string   `json:"dayOfMonth"`
		DayOfWeek        string   `json:"dayOfWeek"`
		TargetCheckNames []string `json:"targetCheckNames"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	sa, err := h.Backend.UpdateScheduledAudit(
		name,
		req.Frequency,
		req.DayOfMonth,
		req.DayOfWeek,
		req.TargetCheckNames,
	)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyScheduledAuditARN: sa.ScheduledAuditARN})
}

func (h *Handler) handleDeleteScheduledAudit(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/audit/scheduledaudits/")
	if err := h.Backend.DeleteScheduledAudit(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- MitigationActions ---

func (h *Handler) handleCreateMitigationAction(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/mitigationactions/actions/")
	var input CreateMitigationActionInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.ActionName = name
	ma, err := h.Backend.CreateMitigationAction(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyActionARN: ma.ActionARN,
		"actionId":   ma.ActionID,
	})
}

func (h *Handler) handleDescribeMitigationAction(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/mitigationactions/actions/")
	ma, err := h.Backend.DescribeMitigationAction(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, ma)
}

func (h *Handler) handleListMitigationActions(c *echo.Context) error {
	actions := h.Backend.ListMitigationActions()
	summaries := make([]map[string]any, len(actions))
	for i, ma := range actions {
		summaries[i] = map[string]any{
			"actionName": ma.ActionName,
			keyActionARN: ma.ActionARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"actionIdentifiers": summaries})
}

func (h *Handler) handleUpdateMitigationAction(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/mitigationactions/actions/")
	var req struct {
		ActionParams map[string]any `json:"actionParams"`
		RoleARN      string         `json:"roleArn"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	ma, err := h.Backend.UpdateMitigationAction(name, req.RoleARN, req.ActionParams)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyActionARN: ma.ActionARN,
		"actionId":   ma.ActionID,
	})
}

func (h *Handler) handleDeleteMitigationAction(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/mitigationactions/actions/")
	if err := h.Backend.DeleteMitigationAction(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- SecurityProfiles ---

func (h *Handler) handleCreateSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	var input CreateSecurityProfileInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.SecurityProfileName = name
	sp, err := h.Backend.CreateSecurityProfile(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySecurityProfileName: sp.SecurityProfileName,
		keySecurityProfileARN:  sp.SecurityProfileARN,
	})
}

func (h *Handler) handleDescribeSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	sp, err := h.Backend.DescribeSecurityProfile(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, sp)
}

func (h *Handler) handleListSecurityProfiles(c *echo.Context) error {
	profiles := h.Backend.ListSecurityProfiles()
	summaries := make([]map[string]any, len(profiles))
	for i, sp := range profiles {
		summaries[i] = map[string]any{
			keySecurityProfileName: sp.SecurityProfileName,
			keySecurityProfileARN:  sp.SecurityProfileARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"securityProfileIdentifiers": summaries})
}

func (h *Handler) handleUpdateSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	var req struct {
		SecurityProfileDescription string `json:"securityProfileDescription"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	sp, err := h.Backend.UpdateSecurityProfile(name, req.SecurityProfileDescription)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keySecurityProfileName: sp.SecurityProfileName,
		keySecurityProfileARN:  sp.SecurityProfileARN,
		keyVersion:             sp.Version,
	})
}

func (h *Handler) handleDeleteSecurityProfile(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/security-profiles/")
	if err := h.Backend.DeleteSecurityProfile(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func parseInt32(s string, out *int32) error {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	if err != nil {
		return err
	}
	*out = int32(n) //nolint:gosec // safe: versionId values are small positive integers

	return nil
}
