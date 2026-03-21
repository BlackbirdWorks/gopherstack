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
	Backend *InMemoryBackend
	janitor *Janitor
}

// NewHandler creates a new SES handler with the given backend and logger.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// WithJanitor attaches a background janitor to the handler.
// The janitor periodically evicts emails older than the backend TTL.
// interval=0 uses the default interval.
// The optional taskTimeout bounds each sweep; 0 means no per-task timeout.
func (h *Handler) WithJanitor(interval time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval)
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
		"SendEmail",
		"SendRawEmail",
		"SendTemplatedEmail",
		"VerifyEmailIdentity",
		"ListIdentities",
		"GetIdentityVerificationAttributes",
		"DeleteIdentity",
		"GetAccountSendingEnabled",
		"CreateTemplate",
		"UpdateTemplate",
		"GetTemplate",
		"ListTemplates",
		"DeleteTemplate",
		"CreateConfigurationSet",
		"DeleteConfigurationSet",
		"ListConfigurationSets",
		"GetSendQuota",
		"GetSendStatistics",
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

	for _, key := range []string{"Source", "EmailAddress", "Identity"} {
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
				fmt.Sprintf("%s is not a valid SES action", action))
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

// dispatchExtended handles the template/config-set/stats operations.
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

func (h *Handler) handleOpError(c *echo.Context, reqID, action string, opErr error) error {
	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(opErr, ErrIdentityNotFound):
		code = "NoSuchEntity"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "InvalidParameterValue"
	case errors.Is(opErr, ErrMessageRejected):
		code = "MessageRejected"
	case errors.Is(opErr, ErrTemplateNotFound):
		code = "TemplateDoesNotExist"
	case errors.Is(opErr, ErrTemplateExists):
		code = "AlreadyExists"
	case errors.Is(opErr, ErrConfigSetNotFound):
		code = "ConfigurationSetDoesNotExist"
	case errors.Is(opErr, ErrConfigSetExists):
		code = "ConfigurationSetAlreadyExists"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
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
	return fmt.Sprintf("gopherstack-%s", uuid.New().String())
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
