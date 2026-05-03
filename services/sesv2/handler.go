package sesv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opBatchGetMetricData                     = "BatchGetMetricData"
	opCancelExportJob                        = "CancelExportJob"
	opCreateConfigurationSet                 = "CreateConfigurationSet"
	opCreateConfigurationSetEventDestination = "CreateConfigurationSetEventDestination"
	opCreateContact                          = "CreateContact"
	opCreateContactList                      = "CreateContactList"
	opCreateCustomVerificationEmailTemplate  = "CreateCustomVerificationEmailTemplate"
	opCreateDedicatedIPPool                  = "CreateDedicatedIpPool"
	opCreateDeliverabilityTestReport         = "CreateDeliverabilityTestReport"
	opCreateEmailIdentity                    = "CreateEmailIdentity"
	opCreateEmailIdentityPolicy              = "CreateEmailIdentityPolicy"
	opCreateEmailTemplate                    = "CreateEmailTemplate"
	opDeleteConfigurationSet                 = "DeleteConfigurationSet"
	opDeleteEmailIdentity                    = "DeleteEmailIdentity"
	opGetConfigurationSet                    = "GetConfigurationSet"
	opGetEmailIdentity                       = "GetEmailIdentity"
	opListConfigurationSets                  = "ListConfigurationSets"
	opListEmailIdentities                    = "ListEmailIdentities"
	opListTagsForResource                    = "ListTagsForResource"
	opSendEmail                              = "SendEmail"
	opTagResource                            = "TagResource"
	opUntagResource                          = "UntagResource"
)

const (
	sesv2PathPrefix = "/v2/email/"
	unknownAction   = "Unknown"
)

// Handler is the Echo HTTP handler for SES v2 operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new SES v2 handler with the given backend.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset resets the backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SESv2"
}

// GetSupportedOperations returns the list of supported SES v2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opBatchGetMetricData,
		opCancelExportJob,
		opCreateConfigurationSet,
		opCreateConfigurationSetEventDestination,
		opCreateContact,
		opCreateContactList,
		opCreateCustomVerificationEmailTemplate,
		opCreateDedicatedIPPool,
		opCreateDeliverabilityTestReport,
		opCreateEmailIdentity,
		opCreateEmailIdentityPolicy,
		opCreateEmailTemplate,
		opDeleteConfigurationSet,
		opDeleteEmailIdentity,
		opGetConfigurationSet,
		opGetEmailIdentity,
		opListConfigurationSets,
		opListEmailIdentities,
		opListTagsForResource,
		opSendEmail,
		opTagResource,
		opUntagResource,
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "sesv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SES v2 instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches SES v2 REST requests.
// SES v2 requests use the /v2/email/ path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		return strings.HasPrefix(r.URL.Path, sesv2PathPrefix)
	}
}

// MatchPriority returns the routing priority for the SES v2 handler.
// Uses PriorityPathVersioned (85) since it matches a versioned path prefix.
func (h *Handler) MatchPriority() int {
	return service.PriorityPathVersioned
}

// ExtractOperation extracts the SES v2 operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the identity or config set name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

	if decoded, err := url.PathUnescape(resource); err == nil {
		return decoded
	}

	return resource
}

// parseSESv2Path maps a method + path to a SES v2 operation and resource name.
// Returns (unknownAction, "") when no pattern matches.
//

func parseSESv2Path(method, path string) (string, string) {
	// Strip /v2/email/ prefix and split remaining path into segments.
	tail := strings.TrimPrefix(path, sesv2PathPrefix)
	tail = strings.TrimSuffix(tail, "/")
	segments := strings.Split(tail, "/")

	if len(segments) == 0 || segments[0] == "" {
		return unknownAction, ""
	}

	switch segments[0] {
	case "identities":
		return parseIdentityPath(method, segments)
	case "outbound-emails":
		if method == http.MethodPost {
			return opSendEmail, ""
		}
	case "configuration-sets":
		return parseConfigSetPath(method, segments)
	case "tags":
		return parseTagsPath(method)
	default:
		return parseMiscPaths(method, segments)
	}

	return unknownAction, ""
}

// parseMiscPaths handles the newer SES v2 path prefixes.
func parseMiscPaths(method string, segments []string) (string, string) {
	switch segments[0] {
	case "metrics":
		return parseMetricsPath(method, segments)
	case "export-jobs":
		return parseExportJobsPath(method, segments)
	case "contact-lists":
		return parseContactListPath(method, segments)
	case "custom-verification-email-templates":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateCustomVerificationEmailTemplate, ""
		}
	case "dedicated-ip-pools":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateDedicatedIPPool, ""
		}
	case "deliverability-dashboard":
		return parseDeliverabilityDashboardPath(method, segments)
	case "templates":
		if method == http.MethodPost && len(segments) == 1 {
			return opCreateEmailTemplate, ""
		}
	}

	return unknownAction, ""
}

const metricsPathSegments = 2
const exportJobPathSegments = 3

func parseMetricsPath(method string, segments []string) (string, string) {
	if len(segments) == metricsPathSegments && segments[1] == "batch" && method == http.MethodPost {
		return opBatchGetMetricData, ""
	}

	return unknownAction, ""
}

func parseExportJobsPath(method string, segments []string) (string, string) {
	if len(segments) == exportJobPathSegments && segments[2] == "cancel" && method == http.MethodPut {
		return opCancelExportJob, segments[1]
	}

	return unknownAction, ""
}

func parseDeliverabilityDashboardPath(method string, segments []string) (string, string) {
	if len(segments) == metricsPathSegments && segments[1] == "test" && method == http.MethodPost {
		return opCreateDeliverabilityTestReport, ""
	}

	return unknownAction, ""
}

func parseIdentityPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return opListEmailIdentities, ""
	case method == http.MethodPost && len(segments) == 1:
		return opCreateEmailIdentity, ""
	case method == http.MethodGet && len(segments) == 2:
		return opGetEmailIdentity, segments[1]
	case method == http.MethodDelete && len(segments) == 2:
		return opDeleteEmailIdentity, segments[1]
	case method == http.MethodPost && len(segments) == 4 && segments[2] == "policies":
		// POST /v2/email/identities/{identity}/policies/{policyName}
		return opCreateEmailIdentityPolicy, segments[1]
	}

	return unknownAction, ""
}

func parseContactListPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodPost && len(segments) == 1:
		return opCreateContactList, ""
	case method == http.MethodPost && len(segments) == 3 && segments[2] == "contacts":
		return opCreateContact, segments[1]
	}

	return unknownAction, ""
}

func parseConfigSetPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return opListConfigurationSets, ""
	case method == http.MethodPost && len(segments) == 1:
		return opCreateConfigurationSet, ""
	case method == http.MethodGet && len(segments) == 2:
		return opGetConfigurationSet, segments[1]
	case method == http.MethodDelete && len(segments) == 2:
		return opDeleteConfigurationSet, segments[1]
	case method == http.MethodPost && len(segments) == 3 && segments[2] == "event-destinations":
		return opCreateConfigurationSetEventDestination, segments[1]
	}

	return unknownAction, ""
}

func parseTagsPath(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListTagsForResource, ""
	case http.MethodPost:
		return opTagResource, ""
	case http.MethodDelete:
		return opUntagResource, ""
	}

	return unknownAction, ""
}

// Handler returns the Echo handler function for SES v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, rawResource := parseSESv2Path(c.Request().Method, c.Request().URL.Path)

		// URL-decode the resource so identities with '@' survive SDK/Terraform percent-encoding.
		// Fall back to the raw value if the path segment is malformed.
		resource := rawResource
		if decoded, err := url.PathUnescape(rawResource); err == nil {
			resource = decoded
		}

		log.DebugContext(ctx, "SESv2 request", "operation", op, "resource", resource)

		if op == unknownAction {
			return h.writeError(c, http.StatusNotFound, "NotFoundException",
				fmt.Sprintf("no route for %s %s", c.Request().Method, c.Request().URL.Path))
		}

		resp, opErr := h.dispatchOp(c, op, resource)

		if opErr != nil {
			return h.handleOpError(c, op, opErr)
		}

		if resp == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// errOpNotHandled is returned by sub-dispatchers when they don't recognise an operation.
var errOpNotHandled = errors.New("sesv2: operation not handled by this dispatcher")

// dispatchOp routes an already-identified operation to its handler.
func (h *Handler) dispatchOp(c *echo.Context, op, resource string) (any, error) {
	resp, err := h.dispatchCoreOps(c, op, resource)
	if !errors.Is(err, errOpNotHandled) {
		return resp, err
	}

	resp, err = h.dispatchNewOps(c, op, resource)
	if !errors.Is(err, errOpNotHandled) {
		return resp, err
	}

	return nil, fmt.Errorf("%w: %s is not a valid SES v2 operation", ErrInvalidInput, op)
}

// dispatchCoreOps handles the original 12 SES v2 operations.
func (h *Handler) dispatchCoreOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opCreateEmailIdentity:
		return h.handleCreateEmailIdentity(c)
	case opGetEmailIdentity:
		return h.handleGetEmailIdentity(resource)
	case opListEmailIdentities:
		return h.handleListEmailIdentities(c), nil
	case opDeleteEmailIdentity:
		return h.handleDeleteEmailIdentity(resource)
	case opSendEmail:
		return h.handleSendEmail(c)
	case opCreateConfigurationSet:
		return h.handleCreateConfigurationSet(c)
	case opGetConfigurationSet:
		return h.handleGetConfigurationSet(resource)
	case opListConfigurationSets:
		return h.handleListConfigurationSets(c), nil
	case opDeleteConfigurationSet:
		return h.handleDeleteConfigurationSet(resource)
	case opListTagsForResource:
		return h.handleListTagsForResource(), nil
	case opTagResource, opUntagResource:
		return &emptyDeleteOutput{}, nil
	default:
		return nil, errOpNotHandled
	}
}

// dispatchNewOps handles the 10 newly added SES v2 operations.
func (h *Handler) dispatchNewOps(c *echo.Context, op, resource string) (any, error) {
	switch op {
	case opBatchGetMetricData:
		return h.handleBatchGetMetricData(c)
	case opCancelExportJob:
		return h.handleCancelExportJob(resource)
	case opCreateConfigurationSetEventDestination:
		return h.handleCreateConfigurationSetEventDestination(c, resource)
	case opCreateContact:
		return h.handleCreateContact(c, resource)
	case opCreateContactList:
		return h.handleCreateContactList(c)
	case opCreateCustomVerificationEmailTemplate:
		return h.handleCreateCustomVerificationEmailTemplate(c)
	case opCreateDedicatedIPPool:
		return h.handleCreateDedicatedIPPool(c)
	case opCreateDeliverabilityTestReport:
		return h.handleCreateDeliverabilityTestReport(c)
	case opCreateEmailIdentityPolicy:
		return h.handleCreateEmailIdentityPolicy(c, resource)
	case opCreateEmailTemplate:
		return h.handleCreateEmailTemplate(c)
	default:
		return nil, errOpNotHandled
	}
}

// ---- request types ----

type createEmailIdentityInput struct {
	EmailIdentity string `json:"EmailIdentity"`
}

type sendEmailInput struct {
	Content          emailContent     `json:"Content"`
	FromEmailAddress string           `json:"FromEmailAddress"`
	Destination      emailDestination `json:"Destination"`
}

type emailDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

type emailContent struct {
	Simple *simpleEmailContent `json:"Simple"`
	Raw    *rawEmailContent    `json:"Raw"`
}

type simpleEmailContent struct {
	Body    emailBody `json:"Body"`
	Subject emailData `json:"Subject"`
}

type emailBody struct {
	Text *emailData `json:"Text"`
	HTML *emailData `json:"Html"`
}

type emailData struct {
	Data    string `json:"Data"`
	Charset string `json:"Charset"`
}

type rawEmailContent struct {
	Data []byte `json:"Data"`
}

type createConfigurationSetInput struct {
	ConfigurationSetName string `json:"ConfigurationSetName"`
}

// ---- response types ----

type createEmailIdentityOutput struct {
	IdentityType       string `json:"IdentityType"`
	VerifiedForSending bool   `json:"VerifiedForSending"`
}

type getEmailIdentityOutput struct {
	EmailIdentity      string `json:"EmailIdentity"`
	IdentityType       string `json:"IdentityType"`
	VerifiedForSending bool   `json:"VerifiedForSending"`
}

type emailIdentitySummary struct {
	IdentityName   string `json:"IdentityName"`
	IdentityType   string `json:"IdentityType"`
	SendingEnabled bool   `json:"SendingEnabled"`
}

type listEmailIdentitiesOutput struct {
	NextToken       string                 `json:"NextToken,omitempty"`
	EmailIdentities []emailIdentitySummary `json:"EmailIdentities"`
}

type sendEmailOutput struct {
	MessageID string `json:"MessageId"`
}

type createConfigurationSetOutput struct{}

type getConfigurationSetOutput struct {
	ConfigurationSetName string `json:"ConfigurationSetName"`
}

type configurationSetSummary struct {
	Name string `json:"Name"`
}

type listConfigurationSetsOutput struct {
	NextToken         string                    `json:"NextToken,omitempty"`
	ConfigurationSets []configurationSetSummary `json:"ConfigurationSets"`
}

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type listTagsOutput struct {
	Tags []tagEntry `json:"Tags"`
}

// ---- action handlers ----

func (h *Handler) handleCreateEmailIdentity(c *echo.Context) (any, error) {
	var in createEmailIdentityInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	ei, err := h.Backend.CreateEmailIdentity(in.EmailIdentity)
	if err != nil {
		return nil, err
	}

	return &createEmailIdentityOutput{
		IdentityType:       ei.IdentityType,
		VerifiedForSending: ei.VerifiedForSending,
	}, nil
}

func (h *Handler) handleGetEmailIdentity(identity string) (any, error) {
	ei, err := h.Backend.GetEmailIdentity(identity)
	if err != nil {
		return nil, err
	}

	return &getEmailIdentityOutput{
		EmailIdentity:      ei.Identity,
		IdentityType:       ei.IdentityType,
		VerifiedForSending: ei.VerifiedForSending,
	}, nil
}

func (h *Handler) handleListEmailIdentities(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListEmailIdentities(nextToken, 0)

	items := make([]emailIdentitySummary, 0, len(pg.Data))

	for _, ei := range pg.Data {
		items = append(items, emailIdentitySummary{
			IdentityName:   ei.Identity,
			IdentityType:   ei.IdentityType,
			SendingEnabled: ei.VerifiedForSending,
		})
	}

	return &listEmailIdentitiesOutput{
		EmailIdentities: items,
		NextToken:       pg.Next,
	}
}

type emptyDeleteOutput struct{}

func (h *Handler) handleDeleteEmailIdentity(identity string) (any, error) {
	if err := h.Backend.DeleteEmailIdentity(identity); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleSendEmail(c *echo.Context) (any, error) {
	var in sendEmailInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	from := in.FromEmailAddress
	to := in.Destination.ToAddresses

	var subject, bodyHTML, bodyText string

	if in.Content.Simple != nil {
		subject = in.Content.Simple.Subject.Data
		if in.Content.Simple.Body.HTML != nil {
			bodyHTML = in.Content.Simple.Body.HTML.Data
		}

		if in.Content.Simple.Body.Text != nil {
			bodyText = in.Content.Simple.Body.Text.Data
		}
	}

	msgID, err := h.Backend.SendEmail(from, to, subject, bodyHTML, bodyText)
	if err != nil {
		return nil, err
	}

	return &sendEmailOutput{MessageID: msgID}, nil
}

func (h *Handler) handleCreateConfigurationSet(c *echo.Context) (any, error) {
	var in createConfigurationSetInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidParameter, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSet(in.ConfigurationSetName); err != nil {
		return nil, err
	}

	return &createConfigurationSetOutput{}, nil
}

func (h *Handler) handleGetConfigurationSet(name string) (any, error) {
	cs, err := h.Backend.GetConfigurationSet(name)
	if err != nil {
		return nil, err
	}

	return &getConfigurationSetOutput{
		ConfigurationSetName: cs.Name,
	}, nil
}

func (h *Handler) handleListConfigurationSets(c *echo.Context) any {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListConfigurationSets(nextToken, 0)

	items := make([]configurationSetSummary, 0, len(pg.Data))

	for _, cs := range pg.Data {
		items = append(items, configurationSetSummary{Name: cs.Name})
	}

	return &listConfigurationSetsOutput{
		ConfigurationSets: items,
		NextToken:         pg.Next,
	}
}

func (h *Handler) handleDeleteConfigurationSet(name string) (any, error) {
	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// handleListTagsForResource returns an empty tag list.
// The SES v2 Terraform provider calls this after every create to refresh tag state.
// Tags are not persisted in this implementation.
func (h *Handler) handleListTagsForResource() any {
	return &listTagsOutput{Tags: []tagEntry{}}
}

// ---- new operation request/response types ----

type batchGetMetricDataInput struct {
	Queries []struct {
		ID        string `json:"Id"`
		Namespace string `json:"Namespace"`
		Metric    string `json:"Metric"`
	} `json:"Queries"`
}

type batchGetMetricDataOutput struct {
	Results []struct {
		ID         string    `json:"Id"`
		Timestamps []any     `json:"Timestamps"`
		Values     []float64 `json:"Values"`
	} `json:"Results"`
}

type createConfigurationSetEventDestinationInput struct {
	EventDestinationName string `json:"EventDestinationName"`
	EventDestination     struct {
		MatchingEventTypes []string `json:"MatchingEventTypes"`
		Enabled            bool     `json:"Enabled"`
	} `json:"EventDestination"`
}

type createContactInput struct {
	EmailAddress     string            `json:"EmailAddress"`
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
}

type createContactListInput struct {
	ContactListName string `json:"ContactListName"`
	Description     string `json:"Description"`
}

type createCustomVerificationEmailTemplateInput struct {
	TemplateName          string `json:"TemplateName"`
	FromEmailAddress      string `json:"FromEmailAddress"`
	TemplateSubject       string `json:"TemplateSubject"`
	TemplateContent       string `json:"TemplateContent"`
	SuccessRedirectionURL string `json:"SuccessRedirectionURL"`
	FailureRedirectionURL string `json:"FailureRedirectionURL"`
}

type createDedicatedIPPoolInput struct {
	PoolName    string `json:"PoolName"`
	ScalingMode string `json:"ScalingMode"`
}

type createDeliverabilityTestReportInput struct {
	ReportName       string `json:"ReportName"`
	FromEmailAddress string `json:"FromEmailAddress"`
}

type createDeliverabilityTestReportOutput struct {
	ReportID                 string `json:"ReportId"`
	DeliverabilityTestStatus string `json:"DeliverabilityTestStatus"`
}

type createEmailIdentityPolicyInput struct {
	Policy string `json:"Policy"`
}

type createEmailTemplateInput struct {
	TemplateName    string               `json:"TemplateName"`
	TemplateContent EmailTemplateContent `json:"TemplateContent"`
}

// ---- new operation handlers ----

func (h *Handler) handleBatchGetMetricData(c *echo.Context) (any, error) {
	var in batchGetMetricDataInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	queries := make([]MetricDataQuery, 0, len(in.Queries))
	for _, q := range in.Queries {
		queries = append(queries, MetricDataQuery{ID: q.ID, Namespace: q.Namespace, Metric: q.Metric})
	}

	results, err := h.Backend.BatchGetMetricData(queries)
	if err != nil {
		return nil, err
	}

	out := batchGetMetricDataOutput{}
	for _, r := range results {
		out.Results = append(out.Results, struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{
			ID:         r.ID,
			Timestamps: []any{},
			Values:     r.Values,
		})
	}

	if out.Results == nil {
		out.Results = []struct {
			ID         string    `json:"Id"`
			Timestamps []any     `json:"Timestamps"`
			Values     []float64 `json:"Values"`
		}{}
	}

	return &out, nil
}

func (h *Handler) handleCancelExportJob(jobID string) (any, error) {
	if err := h.Backend.CancelExportJob(jobID); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateConfigurationSetEventDestination(c *echo.Context, configSetName string) (any, error) {
	var in createConfigurationSetEventDestinationInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateConfigurationSetEventDestination(
		configSetName,
		in.EventDestinationName,
		in.EventDestination.Enabled,
		in.EventDestination.MatchingEventTypes,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateContact(c *echo.Context, contactListName string) (any, error) {
	var in createContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContact(contactListName, in.EmailAddress, in.TopicPreferences); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateContactList(c *echo.Context) (any, error) {
	var in createContactListInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContactList(in.ContactListName, in.Description); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateCustomVerificationEmailTemplate(c *echo.Context) (any, error) {
	var in createCustomVerificationEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	tmpl := &CustomVerificationEmailTemplate{
		TemplateName:          in.TemplateName,
		FromEmailAddress:      in.FromEmailAddress,
		TemplateSubject:       in.TemplateSubject,
		TemplateContent:       in.TemplateContent,
		SuccessRedirectionURL: in.SuccessRedirectionURL,
		FailureRedirectionURL: in.FailureRedirectionURL,
	}

	if _, err := h.Backend.CreateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateDedicatedIPPool(c *echo.Context) (any, error) {
	var in createDedicatedIPPoolInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateDedicatedIPPool(in.PoolName, in.ScalingMode); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateDeliverabilityTestReport(c *echo.Context) (any, error) {
	var in createDeliverabilityTestReportInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	report, err := h.Backend.CreateDeliverabilityTestReport(in.ReportName, in.FromEmailAddress)
	if err != nil {
		return nil, err
	}

	return &createDeliverabilityTestReportOutput{
		ReportID:                 report.ReportID,
		DeliverabilityTestStatus: report.DeliverabilityTestStatus,
	}, nil
}

const policyPathMinSegments = 4

func (h *Handler) handleCreateEmailIdentityPolicy(c *echo.Context, identity string) (any, error) {
	// Extract policyName from the URL: .../identities/{identity}/policies/{policyName}
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < policyPathMinSegments {
		return nil, fmt.Errorf("%w: invalid policy path", ErrInvalidInput)
	}

	policyName := segments[3]
	if decoded, decErr := url.PathUnescape(policyName); decErr == nil {
		policyName = decoded
	}

	var in createEmailIdentityPolicyInput

	if decErr := json.NewDecoder(c.Request().Body).Decode(&in); decErr != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, decErr.Error())
	}

	if backErr := h.Backend.CreateEmailIdentityPolicy(identity, policyName, in.Policy); backErr != nil {
		return nil, backErr
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleCreateEmailTemplate(c *echo.Context) (any, error) {
	var in createEmailTemplateInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateEmailTemplate(in.TemplateName, &in.TemplateContent); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, op string, opErr error) error {
	switch {
	case errors.Is(opErr, ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFoundException", opErr.Error())
	case errors.Is(opErr, ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "AlreadyExistsException", opErr.Error())
	case errors.Is(opErr, ErrInvalidInput):
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", opErr.Error())
	default:
		logger.Load(c.Request().Context()).Error("SESv2 internal error", "error", opErr, "op", op)

		return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
	}
}

type sesv2ErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, sesv2ErrorResponse{Type: code, Message: message})
}
