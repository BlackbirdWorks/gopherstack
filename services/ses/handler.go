package ses

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	sesVersion    = "2010-12-01"
	sesXMLNS      = "http://ses.amazonaws.com/doc/2010-12-01/"
	unknownAction = "Unknown"
)

// Handler is the Echo HTTP handler for SES operations.
type Handler struct {
	Backend StorageBackend
	janitor *Janitor
}

// NewHandler creates a new SES handler with the given backend and logger.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts emails older than the backend TTL.
// interval=0 uses the default interval.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	ib, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h
	}

	j := NewJanitor(ib, interval)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Reset clears all in-memory state. Used by the POST /_gopherstack/reset endpoint.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SES"
}

// GetSupportedOperations returns the list of supported SES operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CloneReceiptRuleSet",
		"CreateConfigurationSet",
		"CreateConfigurationSetEventDestination",
		"CreateConfigurationSetTrackingOptions",
		"CreateCustomVerificationEmailTemplate",
		"CreateReceiptFilter",
		"CreateReceiptRule",
		"CreateReceiptRuleSet",
		"CreateTemplate",
		"DeleteConfigurationSet",
		"DeleteConfigurationSetEventDestination",
		"DeleteConfigurationSetTrackingOptions",
		"DeleteCustomVerificationEmailTemplate",
		"DeleteIdentity",
		"DeleteReceiptFilter",
		"DeleteReceiptRule",
		"DeleteReceiptRuleSet",
		"DeleteTemplate",
		"DescribeActiveReceiptRuleSet",
		"DescribeReceiptRuleSet",
		"GetAccountSendingEnabled",
		"GetCustomVerificationEmailTemplate",
		"GetIdentityVerificationAttributes",
		"GetSendQuota",
		"GetSendStatistics",
		"GetTemplate",
		"ListConfigurationSets",
		"ListCustomVerificationEmailTemplates",
		"ListIdentities",
		"ListReceiptFilters",
		"ListReceiptRuleSets",
		"ListTemplates",
		"SendEmail",
		"SendRawEmail",
		"SendTemplatedEmail",
		"SetActiveReceiptRuleSet",
		"UpdateTemplate",
		"VerifyEmailIdentity",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ses" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this SES instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches SES requests.
// SES requests are form-encoded POSTs containing Version=2010-12-01 and an
// action from the SES supported operations list. We check both the version and
// action to avoid routing conflicts with Elastic Beanstalk, which also uses
// Version=2010-12-01 but with a disjoint set of action names.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}

		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}

		ct := r.Header.Get("Content-Type")
		if !strings.Contains(ct, "application/x-www-form-urlencoded") {
			return false
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return false
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == sesVersion && slices.Contains(h.GetSupportedOperations(), vals.Get("Action"))
	}
}

// MatchPriority returns the routing priority for the SES handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityFormStandard
}

// ExtractOperation extracts the SES action from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return unknownAction
	}

	action := r.Form.Get("Action")
	if action == "" {
		return unknownAction
	}

	return action
}

// ExtractResource returns the source email address or identity from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	for _, key := range []string{"Source", "EmailAddress", "Identity", "RuleSetName", "FilterName"} {
		if v := r.Form.Get(key); v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for SES requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		reqID := newRequestID()

		r := c.Request()
		if err := r.ParseForm(); err != nil {
			log.ErrorContext(ctx, "failed to parse SES request form", "error", err)

			return h.writeError(
				c,
				reqID,
				http.StatusInternalServerError,
				"InternalFailure",
				"failed to read request body",
			)
		}

		vals := r.Form

		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, reqID, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log.DebugContext(ctx, "SES request", "action", action)

		resp, opErr := h.dispatch(vals, reqID, action)

		switch {
		case errors.Is(opErr, errUnknownSESAction):
			return h.writeError(c, reqID, http.StatusBadRequest, "InvalidAction",
				action+" is not a valid SES action")
		case opErr != nil:
			return h.handleOpError(c, reqID, action, opErr)
		}

		xmlBytes, marshalErr := marshalXML(resp)
		if marshalErr != nil {
			log.ErrorContext(ctx, "failed to marshal SES response", "action", action, "error", marshalErr)

			return h.writeError(c, reqID, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

// errUnknownSESAction is returned by dispatch when the action is not recognised.
var errUnknownSESAction = errors.New("unknown SES action")

// dispatch routes a parsed SES action to the appropriate handler.
func (h *Handler) dispatch(vals url.Values, reqID, action string) (any, error) {
	switch action {
	case "VerifyEmailIdentity":
		return h.handleVerifyEmailIdentity(vals, reqID)
	case "DeleteIdentity":
		return h.handleDeleteIdentity(vals, reqID), nil
	case "ListIdentities":
		return h.handleListIdentities(vals, reqID), nil
	case "GetIdentityVerificationAttributes":
		return h.handleGetIdentityVerificationAttributes(vals, reqID), nil
	case "GetAccountSendingEnabled":
		return h.handleGetAccountSendingEnabled(reqID), nil
	case "SendEmail":
		return h.handleSendEmail(vals, reqID)
	case "SendRawEmail":
		return h.handleSendRawEmail(vals, reqID)
	case "SendTemplatedEmail":
		return h.handleSendTemplatedEmail(vals, reqID)
	default:
		return h.dispatchExtended(vals, reqID, action)
	}
}

// dispatchExtended handles the template/config-set/stats/receipt operations.
func (h *Handler) dispatchExtended(vals url.Values, reqID, action string) (any, error) {
	switch action {
	case "CreateTemplate":
		return h.handleCreateTemplate(vals, reqID)
	case "UpdateTemplate":
		return h.handleUpdateTemplate(vals, reqID)
	case "GetTemplate":
		return h.handleGetTemplate(vals, reqID)
	case "ListTemplates":
		return h.handleListTemplates(vals, reqID), nil
	case "DeleteTemplate":
		return h.handleDeleteTemplate(vals, reqID), nil
	case "CreateConfigurationSet":
		return h.handleCreateConfigurationSet(vals, reqID)
	case "DeleteConfigurationSet":
		return h.handleDeleteConfigurationSet(vals, reqID)
	case "ListConfigurationSets":
		return h.handleListConfigurationSets(vals, reqID), nil
	case "GetSendQuota":
		return h.handleGetSendQuota(reqID), nil
	case "GetSendStatistics":
		return h.handleGetSendStatistics(reqID), nil
	default:
		return h.dispatchNewOps(vals, reqID, action)
	}
}

// dispatchNewOps handles receipt rule sets, filters, event destinations, tracking options,
// and custom verification email template operations.
func (h *Handler) dispatchNewOps(vals url.Values, reqID, action string) (any, error) {
	switch action {
	case "CreateReceiptRuleSet":
		return h.handleCreateReceiptRuleSet(vals, reqID)
	case "CloneReceiptRuleSet":
		return h.handleCloneReceiptRuleSet(vals, reqID)
	case "CreateReceiptRule":
		return h.handleCreateReceiptRule(vals, reqID)
	case "CreateReceiptFilter":
		return h.handleCreateReceiptFilter(vals, reqID)
	case "CreateConfigurationSetEventDestination":
		return h.handleCreateConfigurationSetEventDestination(vals, reqID)
	case "DeleteConfigurationSetEventDestination":
		return h.handleDeleteConfigurationSetEventDestination(vals, reqID)
	case "CreateConfigurationSetTrackingOptions":
		return h.handleCreateConfigurationSetTrackingOptions(vals, reqID)
	case "DeleteConfigurationSetTrackingOptions":
		return h.handleDeleteConfigurationSetTrackingOptions(vals, reqID)
	case "CreateCustomVerificationEmailTemplate":
		return h.handleCreateCustomVerificationEmailTemplate(vals, reqID)
	case "DeleteCustomVerificationEmailTemplate":
		return h.handleDeleteCustomVerificationEmailTemplate(vals, reqID)
	default:
		return h.dispatchRefinedOps(vals, reqID, action)
	}
}

// dispatchRefinedOps handles the newer receipt filter/rule set query and active rule set operations.
func (h *Handler) dispatchRefinedOps(vals url.Values, reqID, action string) (any, error) {
	switch action {
	case "ListReceiptFilters":
		return h.handleListReceiptFilters(reqID), nil
	case "ListReceiptRuleSets":
		return h.handleListReceiptRuleSets(reqID), nil
	case "DeleteReceiptFilter":
		return h.handleDeleteReceiptFilter(vals, reqID)
	case "DeleteReceiptRule":
		return h.handleDeleteReceiptRule(vals, reqID)
	case "DeleteReceiptRuleSet":
		return h.handleDeleteReceiptRuleSet(vals, reqID)
	case "GetCustomVerificationEmailTemplate":
		return h.handleGetCustomVerificationEmailTemplate(vals, reqID)
	case "ListCustomVerificationEmailTemplates":
		return h.handleListCustomVerificationEmailTemplates(reqID), nil
	case "DescribeReceiptRuleSet":
		return h.handleDescribeReceiptRuleSet(vals, reqID)
	case "SetActiveReceiptRuleSet":
		return h.handleSetActiveReceiptRuleSet(vals, reqID)
	case "DescribeActiveReceiptRuleSet":
		return h.handleDescribeActiveReceiptRuleSet(reqID)
	default:
		return nil, errUnknownSESAction
	}
}

// ---- action handlers ----

func (h *Handler) handleVerifyEmailIdentity(vals url.Values, reqID string) (any, error) {
	identity := vals.Get("EmailAddress")
	if identity == "" {
		identity = vals.Get("Identity")
	}

	if err := h.Backend.VerifyEmailIdentity(identity); err != nil {
		return nil, err
	}

	return &verifyEmailIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteIdentity(vals url.Values, reqID string) any {
	identity := vals.Get("Identity")

	h.Backend.DeleteIdentity(identity)

	return &deleteIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}
}

func (h *Handler) handleListIdentities(vals url.Values, reqID string) any {
	nextToken := vals.Get("NextToken")
	maxItems := 0
	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	p := h.Backend.ListIdentities(nextToken, maxItems)
	members := make([]xmlMember, 0, len(p.Data))

	for _, id := range p.Data {
		members = append(members, xmlMember{Value: id})
	}

	return &listIdentitiesResponse{
		Xmlns: sesXMLNS,
		Result: listIdentitiesResult{
			Identities: xmlMemberList{Members: members},
			NextToken:  p.Next,
		},
		RequestID: reqID,
	}
}

func (h *Handler) handleGetIdentityVerificationAttributes(vals url.Values, reqID string) any {
	identities := parseSESMemberList(vals, "Identities")

	attrs := h.Backend.GetIdentityVerificationAttributes(identities)
	entries := make([]xmlVerificationEntry, 0, len(attrs))

	for id, status := range attrs {
		entries = append(entries, xmlVerificationEntry{
			Key: id,
			Value: xmlVerificationAttributes{
				VerificationStatus: status,
			},
		})
	}

	return &getIdentityVerificationAttributesResponse{
		Xmlns: sesXMLNS,
		Result: getIdentityVerificationAttributesResult{
			VerificationAttributes: xmlVerificationMap{Entries: entries},
		},
		RequestID: reqID,
	}
}

func (h *Handler) handleSendEmail(vals url.Values, reqID string) (any, error) {
	source := vals.Get("Source")
	subject := vals.Get("Message.Subject.Data")
	bodyHTML := vals.Get("Message.Body.Html.Data")
	bodyText := vals.Get("Message.Body.Text.Data")
	toAddrs := parseSESMemberList(vals, "Destination.ToAddresses")

	msgID, err := h.Backend.SendEmail(source, toAddrs, subject, bodyHTML, bodyText)
	if err != nil {
		return nil, err
	}

	return &sendEmailResponse{
		Xmlns: sesXMLNS,
		Result: sendEmailResult{
			MessageID: msgID,
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleSendRawEmail(vals url.Values, reqID string) (any, error) {
	source := vals.Get("Source")
	rawData := vals.Get("RawMessage.Data")

	msgID, err := h.Backend.SendEmail(source, nil, "raw", "", rawData)
	if err != nil {
		return nil, err
	}

	return &sendRawEmailResponse{
		Xmlns: sesXMLNS,
		Result: sendEmailResult{
			MessageID: msgID,
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleSendTemplatedEmail(vals url.Values, reqID string) (any, error) {
	source := vals.Get("Source")
	templateName := vals.Get("Template")
	toAddrs := parseSESMemberList(vals, "Destination.ToAddresses")

	msgID, err := h.Backend.SendTemplatedEmail(source, toAddrs, templateName)
	if err != nil {
		return nil, err
	}

	return &sendTemplatedEmailResponse{
		Xmlns: sesXMLNS,
		Result: sendEmailResult{
			MessageID: msgID,
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleGetAccountSendingEnabled(reqID string) any {
	return &getAccountSendingEnabledResponse{
		Xmlns:     sesXMLNS,
		Result:    getAccountSendingEnabledResult{Enabled: true},
		RequestID: reqID,
	}
}

// ---- template action handlers ----

func (h *Handler) handleCreateTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := EmailTemplate{
		TemplateName: vals.Get("Template.TemplateName"),
		SubjectPart:  vals.Get("Template.SubjectPart"),
		TextPart:     vals.Get("Template.TextPart"),
		HTMLPart:     vals.Get("Template.HTMLPart"),
	}

	if err := h.Backend.CreateTemplate(tmpl); err != nil {
		return nil, err
	}

	return &createTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleUpdateTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := EmailTemplate{
		TemplateName: vals.Get("Template.TemplateName"),
		SubjectPart:  vals.Get("Template.SubjectPart"),
		TextPart:     vals.Get("Template.TextPart"),
		HTMLPart:     vals.Get("Template.HTMLPart"),
	}

	if err := h.Backend.UpdateTemplate(tmpl); err != nil {
		return nil, err
	}

	return &updateTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleGetTemplate(vals url.Values, reqID string) (any, error) {
	name := vals.Get("TemplateName")

	tmpl, err := h.Backend.GetTemplate(name)
	if err != nil {
		return nil, err
	}

	return &getTemplateResponse{
		Xmlns: sesXMLNS,
		Result: getTemplateResult{
			Template: xmlTemplate(tmpl),
		},
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleListTemplates(vals url.Values, reqID string) any {
	nextToken := vals.Get("NextToken")
	maxItems := 0

	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	p := h.Backend.ListTemplates(nextToken, maxItems)
	members := make([]xmlMember, 0, len(p.Data))

	for _, name := range p.Data {
		members = append(members, xmlMember{Value: name})
	}

	return &listTemplatesResponse{
		Xmlns: sesXMLNS,
		Result: listTemplatesResult{
			TemplatesMetadata: xmlMemberList{Members: members},
			NextToken:         p.Next,
		},
		RequestID: reqID,
	}
}

func (h *Handler) handleDeleteTemplate(vals url.Values, reqID string) any {
	name := vals.Get("TemplateName")

	h.Backend.DeleteTemplate(name)

	return &deleteTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}
}

// ---- configuration set action handlers ----

func (h *Handler) handleCreateConfigurationSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("ConfigurationSet.Name")

	if err := h.Backend.CreateConfigurationSet(name); err != nil {
		return nil, err
	}

	return &createConfigurationSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("ConfigurationSetName")

	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleListConfigurationSets(vals url.Values, reqID string) any {
	nextToken := vals.Get("NextToken")
	maxItems := 0

	if s := vals.Get("MaxItems"); s != "" {
		if n, err := strconv.Atoi(s); err == nil {
			maxItems = n
		}
	}

	p := h.Backend.ListConfigurationSets(nextToken, maxItems)
	members := make([]xmlMember, 0, len(p.Data))

	for _, name := range p.Data {
		members = append(members, xmlMember{Value: name})
	}

	return &listConfigurationSetsResponse{
		Xmlns: sesXMLNS,
		Result: listConfigurationSetsResult{
			ConfigurationSets: xmlMemberList{Members: members},
			NextToken:         p.Next,
		},
		RequestID: reqID,
	}
}

// ---- send quota / statistics action handlers ----

func (h *Handler) handleGetSendQuota(reqID string) any {
	q := h.Backend.GetSendQuota()

	return &getSendQuotaResponse{
		Xmlns:     sesXMLNS,
		Result:    getSendQuotaResult(q),
		RequestID: reqID,
	}
}

func (h *Handler) handleGetSendStatistics(reqID string) any {
	points := h.Backend.GetSendStatistics()

	members := make([]xmlSendDataPoint, 0, len(points))

	for _, p := range points {
		members = append(members, xmlSendDataPoint{
			Timestamp:        p.Timestamp.UTC().Format(time.RFC3339),
			DeliveryAttempts: p.DeliveryAttempts,
			Bounces:          p.Bounces,
			Complaints:       p.Complaints,
			Rejects:          p.Rejects,
		})
	}

	return &getSendStatisticsResponse{
		Xmlns: sesXMLNS,
		Result: getSendStatisticsResult{
			SendDataPoints: xmlSendDataPointList{Members: members},
		},
		RequestID: reqID,
	}
}

// ---- error handling ----

const errCodeAlreadyExists = "AlreadyExists"

// sesErrorCode maps an operation error to the SES XML error code and HTTP status.
// Returns empty string if the error is unrecognised (caller should use InternalFailure).
func sesErrorCode(opErr error) (string, int) {
	status := http.StatusBadRequest

	switch {
	case errors.Is(opErr, ErrIdentityNotFound):
		return "NoSuchEntity", status
	case errors.Is(opErr, ErrInvalidParameter):
		return "InvalidParameterValue", status
	case errors.Is(opErr, ErrMessageRejected):
		return "MessageRejected", status
	case errors.Is(opErr, ErrTemplateNotFound):
		return "TemplateDoesNotExist", status
	case errors.Is(opErr, ErrTemplateExists):
		return errCodeAlreadyExists, status
	case errors.Is(opErr, ErrConfigSetNotFound):
		return "ConfigurationSetDoesNotExist", status
	case errors.Is(opErr, ErrConfigSetExists):
		return "ConfigurationSetAlreadyExists", status
	}

	return sesNewOpsErrorCode(opErr, status)
}

// sesNewOpsErrorCode maps errors introduced by the new operations (receipt rules,
// filters, event destinations, tracking options, custom verification templates).
func sesNewOpsErrorCode(opErr error, status int) (string, int) {
	switch {
	case errors.Is(opErr, ErrReceiptRuleSetNotFound):
		return "RuleSetDoesNotExist", status
	case errors.Is(opErr, ErrReceiptRuleSetExists):
		return errCodeAlreadyExists, status
	case errors.Is(opErr, ErrReceiptRuleNotFound):
		return "RuleDoesNotExist", status
	case errors.Is(opErr, ErrReceiptRuleExists):
		return errCodeAlreadyExists, status
	case errors.Is(opErr, ErrReceiptFilterNotFound):
		return "FilterDoesNotExist", status
	case errors.Is(opErr, ErrReceiptFilterExists):
		return errCodeAlreadyExists, status
	case errors.Is(opErr, ErrEventDestinationNotFound):
		return "EventDestinationDoesNotExist", status
	case errors.Is(opErr, ErrEventDestinationExists):
		return "EventDestinationAlreadyExists", status
	case errors.Is(opErr, ErrTrackingOptionsNotFound):
		return "TrackingOptionsDoesNotExist", status
	case errors.Is(opErr, ErrTrackingOptionsExists):
		return "TrackingOptionsAlreadyExists", status
	case errors.Is(opErr, ErrCustomVerifTemplateNotFound):
		return "CustomVerificationEmailTemplateDoesNotExist", status
	case errors.Is(opErr, ErrCustomVerifTemplateExists):
		return "CustomVerificationEmailTemplateAlreadyExists", status
	default:
		return "", http.StatusInternalServerError
	}
}

func (h *Handler) handleOpError(c *echo.Context, reqID, action string, opErr error) error {
	code, statusCode := sesErrorCode(opErr)
	if code == "" {
		code = "InternalFailure"
		logger.Load(c.Request().Context()).Error("SES internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, reqID, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, reqID string, statusCode int, code, message string) error {
	errResp := &sesErrorResponse{
		Xmlns:     sesXMLNS,
		Error:     sesError{Code: code, Message: message, Type: "Sender"},
		RequestID: reqID,
	}

	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

// marshalXML encodes the payload with the XML declaration header.
func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// newRequestID generates a unique request ID for SES responses.
func newRequestID() string {
	return "gopherstack-" + uuid.New().String()
}

// ---- XML response types ----

type sesError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type sesErrorResponse struct {
	XMLName   xml.Name `xml:"ErrorResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	Error     sesError `xml:"Error"`
	RequestID string   `xml:"RequestId"`
}

type verifyEmailIdentityResponse struct {
	XMLName   xml.Name                  `xml:"VerifyEmailIdentityResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	Result    verifyEmailIdentityResult `xml:"VerifyEmailIdentityResult"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
}

type verifyEmailIdentityResult struct{}

type deleteIdentityResponse struct {
	XMLName   xml.Name             `xml:"DeleteIdentityResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	Result    deleteIdentityResult `xml:"DeleteIdentityResult"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
}

type deleteIdentityResult struct{}

type xmlMember struct {
	Value string `xml:",chardata"`
}

type xmlMemberList struct {
	Members []xmlMember `xml:"member"`
}

type listIdentitiesResult struct {
	NextToken  string        `xml:"NextToken,omitempty"`
	Identities xmlMemberList `xml:"Identities"`
}

type listIdentitiesResponse struct {
	XMLName   xml.Name             `xml:"ListIdentitiesResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	RequestID string               `xml:"ResponseMetadata>RequestId"`
	Result    listIdentitiesResult `xml:"ListIdentitiesResult"`
}

type xmlVerificationAttributes struct {
	VerificationStatus string `xml:"VerificationStatus"`
}

type xmlVerificationEntry struct {
	Key   string                    `xml:"key"`
	Value xmlVerificationAttributes `xml:"value"`
}

type xmlVerificationMap struct {
	Entries []xmlVerificationEntry `xml:"entry"`
}

type getIdentityVerificationAttributesResult struct {
	VerificationAttributes xmlVerificationMap `xml:"VerificationAttributes"`
}

type getIdentityVerificationAttributesResponse struct {
	XMLName   xml.Name                                `xml:"GetIdentityVerificationAttributesResponse"`
	Xmlns     string                                  `xml:"xmlns,attr"`
	RequestID string                                  `xml:"ResponseMetadata>RequestId"`
	Result    getIdentityVerificationAttributesResult `xml:"GetIdentityVerificationAttributesResult"`
}

type sendEmailResult struct {
	MessageID string `xml:"MessageId"`
}

type sendEmailResponse struct {
	XMLName   xml.Name        `xml:"SendEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

type sendRawEmailResponse struct {
	XMLName   xml.Name        `xml:"SendRawEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendRawEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

type sendTemplatedEmailResponse struct {
	XMLName   xml.Name        `xml:"SendTemplatedEmailResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	Result    sendEmailResult `xml:"SendTemplatedEmailResult"`
	RequestID string          `xml:"ResponseMetadata>RequestId"`
}

type getAccountSendingEnabledResult struct {
	Enabled bool `xml:"Enabled"`
}

type getAccountSendingEnabledResponse struct {
	XMLName   xml.Name                       `xml:"GetAccountSendingEnabledResponse"`
	Xmlns     string                         `xml:"xmlns,attr"`
	RequestID string                         `xml:"ResponseMetadata>RequestId"`
	Result    getAccountSendingEnabledResult `xml:"GetAccountSendingEnabledResult"`
}

// parseSESMemberList parses form values like "Prefix.member.1", "Prefix.member.2".
func parseSESMemberList(vals url.Values, prefix string) []string {
	var result []string
	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("%s.member.%d", prefix, i))
		if v == "" {
			return result
		}
		result = append(result, v)
	}
}

// ---- template XML types ----

type xmlTemplate struct {
	TemplateName string `xml:"TemplateName"`
	SubjectPart  string `xml:"SubjectPart,omitempty"`
	TextPart     string `xml:"TextPart,omitempty"`
	HTMLPart     string `xml:"HTMLPart,omitempty"`
}

type createTemplateResponse struct {
	XMLName   xml.Name `xml:"CreateTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type updateTemplateResponse struct {
	XMLName   xml.Name `xml:"UpdateTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type getTemplateResult struct {
	Template xmlTemplate `xml:"Template"`
}

type getTemplateResponse struct {
	XMLName   xml.Name          `xml:"GetTemplateResponse"`
	Xmlns     string            `xml:"xmlns,attr"`
	Result    getTemplateResult `xml:"GetTemplateResult"`
	RequestID string            `xml:"ResponseMetadata>RequestId"`
}

type listTemplatesResult struct {
	NextToken         string        `xml:"NextToken,omitempty"`
	TemplatesMetadata xmlMemberList `xml:"TemplatesMetadata"`
}

type listTemplatesResponse struct {
	XMLName   xml.Name            `xml:"ListTemplatesResponse"`
	Xmlns     string              `xml:"xmlns,attr"`
	RequestID string              `xml:"ResponseMetadata>RequestId"`
	Result    listTemplatesResult `xml:"ListTemplatesResult"`
}

type deleteTemplateResponse struct {
	XMLName   xml.Name `xml:"DeleteTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

// ---- configuration set XML types ----

type createConfigurationSetResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type listConfigurationSetsResult struct {
	NextToken         string        `xml:"NextToken,omitempty"`
	ConfigurationSets xmlMemberList `xml:"ConfigurationSets"`
}

type listConfigurationSetsResponse struct {
	XMLName   xml.Name                    `xml:"ListConfigurationSetsResponse"`
	Xmlns     string                      `xml:"xmlns,attr"`
	RequestID string                      `xml:"ResponseMetadata>RequestId"`
	Result    listConfigurationSetsResult `xml:"ListConfigurationSetsResult"`
}

// ---- send quota / statistics XML types ----

type getSendQuotaResult struct {
	Max24HourSend   float64 `xml:"Max24HourSend"`
	MaxSendRate     float64 `xml:"MaxSendRate"`
	SentLast24Hours float64 `xml:"SentLast24Hours"`
}

type getSendQuotaResponse struct {
	XMLName   xml.Name           `xml:"GetSendQuotaResponse"`
	Xmlns     string             `xml:"xmlns,attr"`
	RequestID string             `xml:"ResponseMetadata>RequestId"`
	Result    getSendQuotaResult `xml:"GetSendQuotaResult"`
}

type xmlSendDataPoint struct {
	Timestamp        string  `xml:"Timestamp"`
	DeliveryAttempts float64 `xml:"DeliveryAttempts"`
	Bounces          float64 `xml:"Bounces"`
	Complaints       float64 `xml:"Complaints"`
	Rejects          float64 `xml:"Rejects"`
}

type xmlSendDataPointList struct {
	Members []xmlSendDataPoint `xml:"member"`
}

type getSendStatisticsResult struct {
	SendDataPoints xmlSendDataPointList `xml:"SendDataPoints"`
}

type getSendStatisticsResponse struct {
	XMLName   xml.Name                `xml:"GetSendStatisticsResponse"`
	Xmlns     string                  `xml:"xmlns,attr"`
	RequestID string                  `xml:"ResponseMetadata>RequestId"`
	Result    getSendStatisticsResult `xml:"GetSendStatisticsResult"`
}

// ---- receipt rule set action handlers ----

func (h *Handler) handleCreateReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")

	if err := h.Backend.CreateReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &createReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleCloneReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	originalName := vals.Get("OriginalRuleSetName")
	newName := vals.Get("RuleSetName")

	if err := h.Backend.CloneReceiptRuleSet(originalName, newName); err != nil {
		return nil, err
	}

	return &cloneReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleCreateReceiptRule(vals url.Values, reqID string) (any, error) {
	ruleSetName := vals.Get("RuleSetName")
	after := vals.Get("After")

	enabled := vals.Get("Rule.Enabled") != "false"
	scanEnabled := vals.Get("Rule.ScanEnabled") != "false"

	rule := ReceiptRule{
		Name:        vals.Get("Rule.Name"),
		Enabled:     enabled,
		TLSPolicy:   vals.Get("Rule.TlsPolicy"),
		ScanEnabled: scanEnabled,
		Recipients:  parseSESMemberList(vals, "Rule.Recipients"),
	}

	if err := h.Backend.CreateReceiptRule(ruleSetName, rule, after); err != nil {
		return nil, err
	}

	return &createReceiptRuleResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

// ---- receipt filter action handlers ----

func (h *Handler) handleCreateReceiptFilter(vals url.Values, reqID string) (any, error) {
	filter := ReceiptFilter{
		Name:   vals.Get("Filter.Name"),
		Policy: vals.Get("Filter.IpFilter.Policy"),
		CIDR:   vals.Get("Filter.IpFilter.Cidr"),
	}

	if err := h.Backend.CreateReceiptFilter(filter); err != nil {
		return nil, err
	}

	return &createReceiptFilterResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

// ---- configuration set event destination action handlers ----

func (h *Handler) handleCreateConfigurationSetEventDestination(vals url.Values, reqID string) (any, error) {
	dest := EventDestination{
		Name:               vals.Get("EventDestination.Name"),
		Enabled:            vals.Get("EventDestination.Enabled") == "true",
		MatchingEventTypes: parseSESMemberList(vals, "EventDestination.MatchingEventTypes"),
		SNSTopicARN:        vals.Get("EventDestination.SNSDestination.TopicARN"),
	}

	configSetName := vals.Get("ConfigurationSetName")

	if err := h.Backend.CreateConfigurationSetEventDestination(configSetName, dest); err != nil {
		return nil, err
	}

	return &createConfigurationSetEventDestinationResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSetEventDestination(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")
	destName := vals.Get("EventDestinationName")

	if err := h.Backend.DeleteConfigurationSetEventDestination(configSetName, destName); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetEventDestinationResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

// ---- configuration set tracking options action handlers ----

func (h *Handler) handleCreateConfigurationSetTrackingOptions(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")
	customRedirectDomain := vals.Get("TrackingOptions.CustomRedirectDomain")

	if err := h.Backend.CreateConfigurationSetTrackingOptions(configSetName, customRedirectDomain); err != nil {
		return nil, err
	}

	return &createConfigurationSetTrackingOptionsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSetTrackingOptions(vals url.Values, reqID string) (any, error) {
	configSetName := vals.Get("ConfigurationSetName")

	if err := h.Backend.DeleteConfigurationSetTrackingOptions(configSetName); err != nil {
		return nil, err
	}

	return &deleteConfigurationSetTrackingOptionsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

// ---- custom verification email template action handlers ----

func (h *Handler) handleCreateCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	tmpl := CustomVerificationEmailTemplate{
		TemplateName:          vals.Get("TemplateName"),
		FromEmailAddress:      vals.Get("FromEmailAddress"),
		TemplateSubject:       vals.Get("TemplateSubject"),
		TemplateContent:       vals.Get("TemplateContent"),
		SuccessRedirectionURL: vals.Get("SuccessRedirectionURL"),
		FailureRedirectionURL: vals.Get("FailureRedirectionURL"),
	}

	if err := h.Backend.CreateCustomVerificationEmailTemplate(tmpl); err != nil {
		return nil, err
	}

	return &createCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleDeleteCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	templateName := vals.Get("TemplateName")

	if err := h.Backend.DeleteCustomVerificationEmailTemplate(templateName); err != nil {
		return nil, err
	}

	return &deleteCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

// ---- new operation XML response types ----

type createReceiptRuleSetResponse struct {
	XMLName   xml.Name `xml:"CreateReceiptRuleSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type cloneReceiptRuleSetResponse struct {
	XMLName   xml.Name `xml:"CloneReceiptRuleSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type createReceiptRuleResponse struct {
	XMLName   xml.Name `xml:"CreateReceiptRuleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type createReceiptFilterResponse struct {
	XMLName   xml.Name `xml:"CreateReceiptFilterResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type createConfigurationSetEventDestinationResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetEventDestinationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetEventDestinationResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetEventDestinationResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type createConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name `xml:"CreateConfigurationSetTrackingOptionsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteConfigurationSetTrackingOptionsResponse struct {
	XMLName   xml.Name `xml:"DeleteConfigurationSetTrackingOptionsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type createCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name `xml:"CreateCustomVerificationEmailTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name `xml:"DeleteCustomVerificationEmailTemplateResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

func (h *Handler) handleListReceiptFilters(reqID string) any {
	filters := h.Backend.ListReceiptFilters()
	members := make([]xmlReceiptFilter, 0, len(filters))
	for _, f := range filters {
		members = append(members, xmlReceiptFilter(f))
	}

	return &listReceiptFiltersResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: listReceiptFiltersResult{
			Filters: xmlReceiptFilterList{Members: members},
		},
	}
}

func (h *Handler) handleDeleteReceiptFilter(vals url.Values, reqID string) (any, error) {
	name := vals.Get("FilterName")
	if err := h.Backend.DeleteReceiptFilter(name); err != nil {
		return nil, err
	}

	return &deleteReceiptFilterResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleListReceiptRuleSets(reqID string) any {
	ruleSets := h.Backend.ListReceiptRuleSets()
	members := make([]xmlRuleSetMetadata, 0, len(ruleSets))
	for _, rs := range ruleSets {
		members = append(members, xmlRuleSetMetadata{
			Name:      rs.Name,
			CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	return &listReceiptRuleSetsResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: listReceiptRuleSetsResult{
			RuleSets: xmlRuleSetMetadataList{Members: members},
		},
	}
}

func (h *Handler) handleDescribeReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	rs, err := h.Backend.DescribeReceiptRuleSet(name)
	if err != nil {
		return nil, err
	}
	rules := make([]xmlReceiptRule, 0, len(rs.Rules))
	for _, r := range rs.Rules {
		rules = append(rules, toXMLReceiptRule(r))
	}

	return &describeReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: describeReceiptRuleSetResult{
			Metadata: xmlRuleSetMetadata{
				Name:      rs.Name,
				CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
			},
			Rules: xmlReceiptRuleList{Members: rules},
		},
	}, nil
}

func (h *Handler) handleDeleteReceiptRule(vals url.Values, reqID string) (any, error) {
	ruleSetName := vals.Get("RuleSetName")
	ruleName := vals.Get("RuleName")
	if err := h.Backend.DeleteReceiptRule(ruleSetName, ruleName); err != nil {
		return nil, err
	}

	return &deleteReceiptRuleResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleDeleteReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	if err := h.Backend.DeleteReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &deleteReceiptRuleSetResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleSetActiveReceiptRuleSet(vals url.Values, reqID string) (any, error) {
	name := vals.Get("RuleSetName")
	if err := h.Backend.SetActiveReceiptRuleSet(name); err != nil {
		return nil, err
	}

	return &setActiveReceiptRuleSetResponse{Xmlns: sesXMLNS, RequestID: reqID}, nil
}

func (h *Handler) handleDescribeActiveReceiptRuleSet(reqID string) (any, error) {
	rs, active, err := h.Backend.DescribeActiveReceiptRuleSet()
	if err != nil {
		return nil, err
	}
	result := describeActiveReceiptRuleSetResult{}
	if active {
		rules := make([]xmlReceiptRule, 0, len(rs.Rules))
		for _, r := range rs.Rules {
			rules = append(rules, toXMLReceiptRule(r))
		}
		result.Metadata = &xmlRuleSetMetadata{
			Name:      rs.Name,
			CreatedAt: rs.CreatedAt.UTC().Format(time.RFC3339),
		}
		result.Rules = xmlReceiptRuleList{Members: rules}
	}

	return &describeActiveReceiptRuleSetResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result:    result,
	}, nil
}

func (h *Handler) handleGetCustomVerificationEmailTemplate(vals url.Values, reqID string) (any, error) {
	name := vals.Get("TemplateName")
	tmpl, err := h.Backend.GetCustomVerificationEmailTemplate(name)
	if err != nil {
		return nil, err
	}

	return &getCustomVerificationEmailTemplateResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: getCustomVerificationEmailTemplateResult{
			Template: xmlCustomVerifTemplate(tmpl),
		},
	}, nil
}

func (h *Handler) handleListCustomVerificationEmailTemplates(reqID string) any {
	tmpls := h.Backend.ListCustomVerificationEmailTemplates()
	members := make([]xmlCustomVerifTemplate, 0, len(tmpls))
	for _, t := range tmpls {
		members = append(members, xmlCustomVerifTemplate(t))
	}

	return &listCustomVerificationEmailTemplatesResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
		Result: listCustomVerificationEmailTemplatesResult{
			CustomVerificationEmailTemplates: xmlCustomVerifTemplateList{Members: members},
		},
	}
}

func toXMLReceiptRule(r ReceiptRule) xmlReceiptRule {
	recipients := make([]xmlMember, 0, len(r.Recipients))
	for _, rec := range r.Recipients {
		recipients = append(recipients, xmlMember{Value: rec})
	}

	return xmlReceiptRule{
		Name:        r.Name,
		Enabled:     r.Enabled,
		TLSPolicy:   r.TLSPolicy,
		ScanEnabled: r.ScanEnabled,
		Recipients:  xmlMemberList{Members: recipients},
	}
}

type xmlReceiptFilter struct {
	Name   string `xml:"Name"`
	Policy string `xml:"IpFilter>Policy"`
	CIDR   string `xml:"IpFilter>Cidr"`
}

type xmlReceiptFilterList struct {
	Members []xmlReceiptFilter `xml:"member"`
}

type listReceiptFiltersResult struct {
	Filters xmlReceiptFilterList `xml:"Filters"`
}

type listReceiptFiltersResponse struct {
	XMLName   xml.Name                 `xml:"ListReceiptFiltersResponse"`
	Xmlns     string                   `xml:"xmlns,attr"`
	RequestID string                   `xml:"ResponseMetadata>RequestId"`
	Result    listReceiptFiltersResult `xml:"ListReceiptFiltersResult"`
}

type deleteReceiptFilterResponse struct {
	XMLName   xml.Name `xml:"DeleteReceiptFilterResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type xmlRuleSetMetadata struct {
	Name      string `xml:"Name"`
	CreatedAt string `xml:"CreatedTimestamp"`
}

type xmlRuleSetMetadataList struct {
	Members []xmlRuleSetMetadata `xml:"member"`
}

type listReceiptRuleSetsResult struct {
	RuleSets xmlRuleSetMetadataList `xml:"RuleSets"`
}

type listReceiptRuleSetsResponse struct {
	XMLName   xml.Name                  `xml:"ListReceiptRuleSetsResponse"`
	Xmlns     string                    `xml:"xmlns,attr"`
	RequestID string                    `xml:"ResponseMetadata>RequestId"`
	Result    listReceiptRuleSetsResult `xml:"ListReceiptRuleSetsResult"`
}

type xmlReceiptRule struct {
	Name        string        `xml:"Name"`
	TLSPolicy   string        `xml:"TlsPolicy,omitempty"`
	Recipients  xmlMemberList `xml:"Recipients"`
	Enabled     bool          `xml:"Enabled"`
	ScanEnabled bool          `xml:"ScanEnabled"`
}

type xmlReceiptRuleList struct {
	Members []xmlReceiptRule `xml:"member"`
}

type describeReceiptRuleSetResult struct {
	Metadata xmlRuleSetMetadata `xml:"Metadata"`
	Rules    xmlReceiptRuleList `xml:"Rules"`
}

type describeReceiptRuleSetResponse struct {
	XMLName   xml.Name                     `xml:"DescribeReceiptRuleSetResponse"`
	Xmlns     string                       `xml:"xmlns,attr"`
	RequestID string                       `xml:"ResponseMetadata>RequestId"`
	Result    describeReceiptRuleSetResult `xml:"DescribeReceiptRuleSetResult"`
}

type deleteReceiptRuleResponse struct {
	XMLName   xml.Name `xml:"DeleteReceiptRuleResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteReceiptRuleSetResponse struct {
	XMLName   xml.Name `xml:"DeleteReceiptRuleSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type setActiveReceiptRuleSetResponse struct {
	XMLName   xml.Name `xml:"SetActiveReceiptRuleSetResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type describeActiveReceiptRuleSetResult struct {
	Metadata *xmlRuleSetMetadata `xml:"Metadata,omitempty"`
	Rules    xmlReceiptRuleList  `xml:"Rules"`
}

type describeActiveReceiptRuleSetResponse struct {
	XMLName   xml.Name                           `xml:"DescribeActiveReceiptRuleSetResponse"`
	Xmlns     string                             `xml:"xmlns,attr"`
	RequestID string                             `xml:"ResponseMetadata>RequestId"`
	Result    describeActiveReceiptRuleSetResult `xml:"DescribeActiveReceiptRuleSetResult"`
}

type xmlCustomVerifTemplate struct {
	TemplateName          string `xml:"TemplateName"`
	FromEmailAddress      string `xml:"FromEmailAddress"`
	TemplateSubject       string `xml:"TemplateSubject"`
	TemplateContent       string `xml:"TemplateContent,omitempty"`
	SuccessRedirectionURL string `xml:"SuccessRedirectionURL,omitempty"`
	FailureRedirectionURL string `xml:"FailureRedirectionURL,omitempty"`
}

type getCustomVerificationEmailTemplateResult struct {
	Template xmlCustomVerifTemplate `xml:"CustomVerificationEmailTemplate"`
}

type getCustomVerificationEmailTemplateResponse struct {
	XMLName   xml.Name                                 `xml:"GetCustomVerificationEmailTemplateResponse"`
	Xmlns     string                                   `xml:"xmlns,attr"`
	RequestID string                                   `xml:"ResponseMetadata>RequestId"`
	Result    getCustomVerificationEmailTemplateResult `xml:"GetCustomVerificationEmailTemplateResult"`
}

type xmlCustomVerifTemplateList struct {
	Members []xmlCustomVerifTemplate `xml:"member"`
}

type listCustomVerificationEmailTemplatesResult struct {
	CustomVerificationEmailTemplates xmlCustomVerifTemplateList `xml:"CustomVerificationEmailTemplates"`
}

type listCustomVerificationEmailTemplatesResponse struct {
	XMLName   xml.Name                                   `xml:"ListCustomVerificationEmailTemplatesResponse"`
	Xmlns     string                                     `xml:"xmlns,attr"`
	RequestID string                                     `xml:"ResponseMetadata>RequestId"`
	Result    listCustomVerificationEmailTemplatesResult `xml:"ListCustomVerificationEmailTemplatesResult"`
}
