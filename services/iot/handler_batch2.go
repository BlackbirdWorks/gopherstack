package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	keyStreamID           = "streamId"
	keyStreamARN          = "streamArn"
	keyMetricName         = "metricName"
	keyMetricARN          = "metricArn"
	pathTags              = "/tags"
	pathDefaultAuthorizer = "/default-authorizer"
	keyCertificates       = "certificates"
	keyPolicies           = "policies"
	keyThings             = "things"
)

// ---------------------------------------------------------------------------
// Route resolvers for batch 2 ops
// ---------------------------------------------------------------------------

func resolveCACertOps(path, method string) string {
	switch {
	case path == "/cacertificates" && method == http.MethodGet:
		return opListCACertificates
	case path == "/cacertificate/register" && method == http.MethodPost:
		return opRegisterCACertificate
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodGet:
		return opDescribeCACertificate
	case strings.HasPrefix(path, "/cacertificates/") &&
		strings.HasSuffix(path, "/certificates") &&
		method == http.MethodGet:
		return opListCertificatesByCA
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodPut:
		return opUpdateCACertificate
	case strings.HasPrefix(path, "/cacertificates/") && method == http.MethodDelete:
		return opDeleteCACertificate
	}

	return unknownOperation
}

func resolveStreamOps(path, method string) string {
	switch {
	case path == "/streams" && method == http.MethodGet:
		return opListStreams
	case strings.HasPrefix(path, "/streams/") && method == http.MethodPost:
		return opCreateStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodGet:
		return opDescribeStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodPut:
		return opUpdateStream
	case strings.HasPrefix(path, "/streams/") && method == http.MethodDelete:
		return opDeleteStream
	}

	return unknownOperation
}

func resolveFleetMetricOps(path, method string) string {
	switch {
	case path == "/fleet-metrics" && method == http.MethodGet:
		return opListFleetMetrics
	case strings.HasPrefix(path, "/fleet-metric/") && method == http.MethodPut:
		return opCreateFleetMetric
	case strings.HasPrefix(path, "/fleet-metric/") && method == http.MethodGet:
		return opDescribeFleetMetric
	case strings.HasPrefix(path, "/fleet-metric/") && method == http.MethodPatch:
		return opUpdateFleetMetric
	case strings.HasPrefix(path, "/fleet-metric/") && method == http.MethodDelete:
		return opDeleteFleetMetric
	}

	return unknownOperation
}

func resolveCustomMetricOps(path, method string) string {
	switch {
	case path == "/custom-metric" && method == http.MethodGet:
		return opListCustomMetrics
	case strings.HasPrefix(path, "/custom-metric/") && method == http.MethodPost:
		return opCreateCustomMetric
	case strings.HasPrefix(path, "/custom-metric/") && method == http.MethodGet:
		return opDescribeCustomMetric
	case strings.HasPrefix(path, "/custom-metric/") && method == http.MethodPatch:
		return opUpdateCustomMetric
	case strings.HasPrefix(path, "/custom-metric/") && method == http.MethodDelete:
		return opDeleteCustomMetric
	}

	return unknownOperation
}

func resolveDimensionOps(path, method string) string {
	switch {
	case path == "/dimensions" && method == http.MethodGet:
		return opListDimensions
	case strings.HasPrefix(path, "/dimensions/") && method == http.MethodPost:
		return opCreateDimension
	case strings.HasPrefix(path, "/dimensions/") && method == http.MethodGet:
		return opDescribeDimension
	case strings.HasPrefix(path, "/dimensions/") && method == http.MethodPatch:
		return opUpdateDimension
	case strings.HasPrefix(path, "/dimensions/") && method == http.MethodDelete:
		return opDeleteDimension
	}

	return unknownOperation
}

func resolveTagOps(path, method string) string {
	switch {
	case path == pathTags && method == http.MethodPost:
		return opTagResource
	case path == pathTags && method == http.MethodDelete:
		return opUntagResource
	case path == pathTags && method == http.MethodGet:
		return opListTagsForResource
	}

	return unknownOperation
}

func resolveAuditConfigOps(path, method string) string {
	switch {
	case path == "/audit/configuration" && method == http.MethodGet:
		return opDescribeAccountAuditConfiguration
	case path == "/audit/configuration" && method == http.MethodPatch:
		return opUpdateAccountAuditConfiguration
	case path == "/audit/tasks" && method == http.MethodGet:
		return opListAuditTasks
	case path == "/audit/tasks" && method == http.MethodPost:
		return opStartOnDemandAuditTask
	case strings.HasPrefix(path, "/audit/tasks/") && method == http.MethodGet:
		return opDescribeAuditTask
	}

	return unknownOperation
}

//nolint:gocyclo,cyclop // mechanical routing switch
func resolveMiscBatch2Ops(path, method string) string {
	switch {
	// DetachThingPrincipal: DELETE /things/{thingName}/principals
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/principals") &&
		method == http.MethodDelete:
		return opDetachThingPrincipal
	// CancelCertificateTransfer
	case strings.HasPrefix(path, "/cancel-certificate-transfer/") && method == http.MethodPatch:
		return opCancelCertificateTransfer
	// RegistrationCode
	case path == "/registrationcode" && method == http.MethodGet:
		return opGetRegistrationCode
	case path == "/registrationcode" && method == http.MethodDelete:
		return opDeleteRegistrationCode
	// Billing group things
	case path == "/billing-groups/removeThingFromBillingGroup" && method == http.MethodPut:
		return opRemoveThingFromBillingGroup
	case strings.HasPrefix(path, "/billing-groups/") &&
		strings.HasSuffix(path, "/things") &&
		method == http.MethodGet:
		return opListThingsInBillingGroup
	// ThingGroups for thing
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/thing-groups") &&
		method == http.MethodGet:
		return opListThingGroupsForThing
	// ListPrincipalPolicies
	case path == "/principal-policies" && method == http.MethodGet:
		return opListPrincipalPolicies
	// ListPolicyPrincipals
	case path == "/policy-principals" && method == http.MethodGet:
		return opListPolicyPrincipals
	// ListTargetsForPolicy
	case strings.HasPrefix(path, "/policy-targets/") && method == http.MethodGet:
		return opListTargetsForPolicy
	// ListPrincipalThings
	case path == "/principal-things" && method == http.MethodGet:
		return opListPrincipalThings
	// ListPrincipalThingsV2
	case path == "/principal-things-v2" && method == http.MethodGet:
		return opListPrincipalThingsV2
	// GetEffectivePolicies
	case path == "/effective-policies" && method == http.MethodPost:
		return opGetEffectivePolicies
	// Default authorizer
	case path == pathDefaultAuthorizer && method == http.MethodPost:
		return opSetDefaultAuthorizer
	case path == pathDefaultAuthorizer && method == http.MethodDelete:
		return opClearDefaultAuthorizer
	case path == pathDefaultAuthorizer && method == http.MethodGet:
		return opDescribeDefaultAuthorizer
	// Job executions
	case strings.HasPrefix(path, "/jobs/") &&
		strings.HasSuffix(path, "/things") &&
		method == http.MethodGet:
		return opListJobExecutionsForJob
	case strings.HasPrefix(path, "/things/") &&
		strings.HasSuffix(path, "/jobs") &&
		method == http.MethodGet:
		return opListJobExecutionsForThing
	}

	return unknownOperation
}

// ---------------------------------------------------------------------------
// Handler implementations
// ---------------------------------------------------------------------------

// --- CACertificate ---

func (h *Handler) handleRegisterCACertificate(c *echo.Context) error {
	var req struct {
		CACertificate           string `json:"caCertificate"`
		VerificationCertificate string `json:"verificationCertificate,omitempty"`
		Status                  string `json:"registrationConfig,omitempty"`
		Tags                    []any  `json:"tags,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	ca, err := h.Backend.RegisterCACertificate(req.CACertificate, "ACTIVE")
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyCertificateArn: ca.CertificateARN,
		keyCertificateID:  ca.CertificateID,
	})
}

func (h *Handler) handleDescribeCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	ca, err := h.Backend.DescribeCACertificate(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"certificateDescription": ca})
}

func (h *Handler) handleListCACertificates(c *echo.Context) error {
	certs := h.Backend.ListCACertificates()
	summaries := make([]map[string]any, len(certs))
	for i, ca := range certs {
		summaries[i] = map[string]any{
			keyCertificateID:  ca.CertificateID,
			keyCertificateArn: ca.CertificateARN,
			keyStatus:         ca.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyCertificates: summaries})
}

func (h *Handler) handleUpdateCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	var req struct {
		NewStatus string `json:"newStatus"`
	}
	_ = readBody(c, &req)
	if err := h.Backend.UpdateCACertificate(id, req.NewStatus); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteCACertificate(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	if err := h.Backend.DeleteCACertificate(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListCertificatesByCA(c *echo.Context) error {
	// /cacertificates/{caCertificateId}/certificates
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/cacertificates/")
	caID := strings.TrimSuffix(trimmed, "/certificates")
	certs := h.Backend.ListCertificatesByCA(caID)
	summaries := make([]map[string]any, len(certs))
	for i, cert := range certs {
		summaries[i] = map[string]any{
			keyCertificateID:  cert.CertificateID,
			keyCertificateArn: cert.ARN,
			keyStatus:         cert.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyCertificates: summaries})
}

// --- Stream ---

func (h *Handler) handleCreateStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	var input CreateStreamInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.StreamID = id
	s, err := h.Backend.CreateStream(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStreamID:     s.StreamID,
		keyStreamARN:    s.StreamARN,
		"description":   s.Description,
		"streamVersion": s.StreamVersion,
	})
}

func (h *Handler) handleDescribeStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	s, err := h.Backend.DescribeStream(id)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"streamInfo": s})
}

func (h *Handler) handleListStreams(c *echo.Context) error {
	streams := h.Backend.ListStreams()
	summaries := make([]map[string]any, len(streams))
	for i, s := range streams {
		summaries[i] = map[string]any{
			keyStreamID:  s.StreamID,
			keyStreamARN: s.StreamARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"streams": summaries})
}

func (h *Handler) handleUpdateStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	var req struct { //nolint:govet // JSON field order matters
		Files       []StreamFile `json:"files,omitempty"`
		Description string       `json:"description,omitempty"`
		RoleARN     string       `json:"roleArn,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	s, err := h.Backend.UpdateStream(id, req.Description, req.RoleARN, req.Files)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyStreamID:     s.StreamID,
		keyStreamARN:    s.StreamARN,
		"streamVersion": s.StreamVersion,
	})
}

func (h *Handler) handleDeleteStream(c *echo.Context) error {
	id := strings.TrimPrefix(c.Request().URL.Path, "/streams/")
	if err := h.Backend.DeleteStream(id); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- FleetMetric ---

func (h *Handler) handleCreateFleetMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/fleet-metric/")
	var input CreateFleetMetricInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.MetricName = name
	fm, err := h.Backend.CreateFleetMetric(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyMetricName: fm.MetricName,
		keyMetricARN:  fm.MetricARN,
	})
}

func (h *Handler) handleDescribeFleetMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/fleet-metric/")
	fm, err := h.Backend.DescribeFleetMetric(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, fm)
}

func (h *Handler) handleListFleetMetrics(c *echo.Context) error {
	metrics := h.Backend.ListFleetMetrics()
	summaries := make([]map[string]any, len(metrics))
	for i, fm := range metrics {
		summaries[i] = map[string]any{
			keyMetricName: fm.MetricName,
			keyMetricARN:  fm.MetricARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"fleetMetrics": summaries})
}

func (h *Handler) handleUpdateFleetMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/fleet-metric/")
	var req struct {
		QueryString string `json:"queryString,omitempty"`
		Description string `json:"description,omitempty"`
		Period      int32  `json:"period,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateFleetMetric(name, req.QueryString, req.Description, req.Period); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteFleetMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/fleet-metric/")
	if err := h.Backend.DeleteFleetMetric(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- CustomMetric ---

func (h *Handler) handleCreateCustomMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/custom-metric/")
	var input CreateCustomMetricInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.MetricName = name
	cm, err := h.Backend.CreateCustomMetric(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyMetricName: cm.MetricName,
		keyMetricARN:  cm.MetricARN,
	})
}

func (h *Handler) handleDescribeCustomMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/custom-metric/")
	cm, err := h.Backend.DescribeCustomMetric(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, cm)
}

func (h *Handler) handleListCustomMetrics(c *echo.Context) error {
	metrics := h.Backend.ListCustomMetrics()
	names := make([]string, len(metrics))
	for i, cm := range metrics {
		names[i] = cm.MetricName
	}

	return c.JSON(http.StatusOK, map[string]any{"metricNames": names})
}

func (h *Handler) handleUpdateCustomMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/custom-metric/")
	var req struct {
		DisplayName string `json:"displayName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	cm, err := h.Backend.UpdateCustomMetric(name, req.DisplayName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyMetricName: cm.MetricName,
		keyMetricARN:  cm.MetricARN,
	})
}

func (h *Handler) handleDeleteCustomMetric(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/custom-metric/")
	if err := h.Backend.DeleteCustomMetric(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Dimension ---

func (h *Handler) handleCreateDimension(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dimensions/")
	var input CreateDimensionInput
	if err := readBody(c, &input); err != nil {
		return err
	}
	input.Name = name
	d, err := h.Backend.CreateDimension(&input)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"name": d.Name,
		"arn":  d.ARN,
	})
}

func (h *Handler) handleDescribeDimension(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dimensions/")
	d, err := h.Backend.DescribeDimension(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, d)
}

func (h *Handler) handleListDimensions(c *echo.Context) error {
	dims := h.Backend.ListDimensions()
	names := make([]string, len(dims))
	for i, d := range dims {
		names[i] = d.Name
	}

	return c.JSON(http.StatusOK, map[string]any{"dimensionNames": names})
}

func (h *Handler) handleUpdateDimension(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dimensions/")
	var req struct {
		StringValues []string `json:"stringValues"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	d, err := h.Backend.UpdateDimension(name, req.StringValues)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"name": d.Name,
		"arn":  d.ARN,
	})
}

func (h *Handler) handleDeleteDimension(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/dimensions/")
	if err := h.Backend.DeleteDimension(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// --- Tags ---

func (h *Handler) handleTagResource(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		ResourceArn string            `json:"resourceArn"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.TagResourceGeneric(req.ResourceArn, req.Tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	tags := h.Backend.ListTagsForResource(resourceARN)
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagList})
}

// --- Audit Configuration ---

func (h *Handler) handleDescribeAccountAuditConfiguration(c *echo.Context) error {
	cfg := h.Backend.DescribeAccountAuditConfiguration()

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleUpdateAccountAuditConfiguration(c *echo.Context) error {
	var req struct {
		AuditCheckConfigurations map[string]*AuditCheckConfig `json:"auditCheckConfigurations,omitempty"`
		RoleARN                  string                       `json:"roleArn,omitempty"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateAccountAuditConfiguration(req.RoleARN, req.AuditCheckConfigurations); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleStartOnDemandAuditTask(c *echo.Context) error {
	var req struct {
		TargetCheckNames []string `json:"targetCheckNames"`
	}
	_ = readBody(c, &req)
	taskID, err := h.Backend.StartOnDemandAuditTask(req.TargetCheckNames)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"taskId": taskID})
}

func (h *Handler) handleDescribeAuditTask(c *echo.Context) error {
	taskID := strings.TrimPrefix(c.Request().URL.Path, "/audit/tasks/")
	task, err := h.Backend.DescribeAuditTask(taskID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, task)
}

func (h *Handler) handleListAuditTasks(c *echo.Context) error {
	taskType := c.Request().URL.Query().Get("taskType")
	tasks := h.Backend.ListAuditTasks(taskType)
	summaries := make([]map[string]any, len(tasks))
	for i, t := range tasks {
		summaries[i] = map[string]any{
			"taskId":     t.TaskID,
			"taskStatus": t.TaskStatus,
			"taskType":   t.TaskType,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"tasks": summaries})
}

// --- Misc ---

func (h *Handler) handleDetachThingPrincipal(c *echo.Context) error {
	// DELETE /things/{thingName}/principals
	thingName := extractThingName(c.Request().URL.Path)
	principal := c.Request().Header.Get(headerIoTPrincipal)
	if err := h.Backend.DetachThingPrincipal(thingName, principal); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func extractThingName(path string) string {
	trimmed := strings.TrimPrefix(path, "/things/")

	return strings.SplitN(trimmed, "/", 2)[0] //nolint:mnd // split into at most 2 parts
}

func (h *Handler) handleCancelCertificateTransfer(c *echo.Context) error {
	certID := strings.TrimPrefix(c.Request().URL.Path, "/cancel-certificate-transfer/")
	if err := h.Backend.CancelCertificateTransfer(certID); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetRegistrationCode(c *echo.Context) error {
	code := h.Backend.GetRegistrationCode()

	return c.JSON(http.StatusOK, map[string]any{"registrationCode": code})
}

func (h *Handler) handleDeleteRegistrationCode(c *echo.Context) error {
	if err := h.Backend.DeleteRegistrationCode(); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListThingGroupsForThing(c *echo.Context) error {
	thingName := extractThingName(c.Request().URL.Path)
	groups := h.Backend.ListThingGroupsForThing(thingName)

	return c.JSON(http.StatusOK, map[string]any{"thingGroups": groups})
}

func (h *Handler) handleListThingsInBillingGroup(c *echo.Context) error {
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/billing-groups/")
	groupName := strings.TrimSuffix(trimmed, "/things")
	things := h.Backend.ListThingsInBillingGroup(groupName)

	return c.JSON(http.StatusOK, map[string]any{keyThings: things})
}

func (h *Handler) handleRemoveThingFromBillingGroup(c *echo.Context) error {
	var req struct {
		BillingGroupName string `json:"billingGroupName"`
		ThingName        string `json:"thingName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.RemoveThingFromBillingGroup(req.ThingName, req.BillingGroupName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListPrincipalPolicies(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	policies := h.Backend.ListPrincipalPolicies(principal)
	out := make([]map[string]any, len(policies))
	for i, p := range policies {
		out[i] = map[string]any{
			"policyName": p.PolicyName,
			"policyArn":  p.ARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyPolicies: out})
}

func (h *Handler) handleListPolicyPrincipals(c *echo.Context) error {
	policyName := c.Request().Header.Get("X-Amzn-Policy-Name")
	principals := h.Backend.ListPolicyPrincipals(policyName)
	if principals == nil {
		principals = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"principals": principals})
}

func (h *Handler) handleListTargetsForPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policy-targets/")
	targets := h.Backend.ListTargetsForPolicy(policyName)

	return c.JSON(http.StatusOK, map[string]any{"targets": targets})
}

func (h *Handler) handleListPrincipalThings(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	things := h.Backend.ListPrincipalThings(principal)

	return c.JSON(http.StatusOK, map[string]any{keyThings: things})
}

func (h *Handler) handleListPrincipalThingsV2(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	things := h.Backend.ListPrincipalThings(principal)
	summaries := make([]map[string]any, len(things))
	for i, t := range things {
		summaries[i] = map[string]any{"thingName": t}
	}

	return c.JSON(http.StatusOK, map[string]any{"principalThingObjects": summaries})
}

func (h *Handler) handleGetEffectivePolicies(c *echo.Context) error {
	var req struct {
		ThingName string `json:"thingName"`
		Principal string `json:"principal"`
	}
	_ = readBody(c, &req)
	policies := h.Backend.GetEffectivePolicies(req.ThingName, req.Principal)
	out := make([]map[string]any, len(policies))
	for i, p := range policies {
		out[i] = map[string]any{
			"policyName":     p.PolicyName,
			"policyArn":      p.ARN,
			"policyDocument": p.PolicyDocument,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"effectivePolicies": out})
}

func (h *Handler) handleSetDefaultAuthorizer(c *echo.Context) error {
	var req struct {
		AuthorizerName string `json:"authorizerName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.SetDefaultAuthorizer(req.AuthorizerName); err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerName": req.AuthorizerName})
}

func (h *Handler) handleClearDefaultAuthorizer(c *echo.Context) error {
	if err := h.Backend.ClearDefaultAuthorizer(); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDescribeDefaultAuthorizer(c *echo.Context) error {
	name, err := h.Backend.DescribeDefaultAuthorizer()
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authorizerDescription": map[string]any{"authorizerName": name}})
}

func (h *Handler) handleListJobExecutionsForJob(c *echo.Context) error {
	// GET /jobs/{jobId}/things
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/jobs/")
	jobID := strings.TrimSuffix(trimmed, "/things")
	execs := h.Backend.ListJobExecutionsForJob(jobID)
	summaries := make([]map[string]any, len(execs))
	for i, e := range execs {
		summaries[i] = map[string]any{
			"jobId":     e.JobID,
			"thingName": e.ThingName,
			keyStatus:   e.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"executionSummaries": summaries})
}

func (h *Handler) handleListJobExecutionsForThing(c *echo.Context) error {
	// GET /things/{thingName}/jobs
	thingName := extractThingName(c.Request().URL.Path)
	execs := h.Backend.ListJobExecutionsForThing(thingName)
	summaries := make([]map[string]any, len(execs))
	for i, e := range execs {
		summaries[i] = map[string]any{
			"jobId":   e.JobID,
			keyStatus: e.Status,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"executionSummaries": summaries})
}
