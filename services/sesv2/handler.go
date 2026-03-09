package sesv2

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	sesv2PathPrefix = "/v2/email/"
	unknownAction   = "Unknown"
)

// Handler is the Echo HTTP handler for SES v2 operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new SES v2 handler with the given backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SESv2"
}

// GetSupportedOperations returns the list of supported SES v2 operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateEmailIdentity",
		"GetEmailIdentity",
		"ListEmailIdentities",
		"DeleteEmailIdentity",
		"SendEmail",
		"CreateConfigurationSet",
		"GetConfigurationSet",
		"ListConfigurationSets",
		"DeleteConfigurationSet",
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

	return resource
}

// parseSESv2Path maps a method + path to a SES v2 operation and resource name.
// Returns (unknownAction, "") when no pattern matches.
//
//nolint:cyclop // path routing table has necessary branches for each HTTP method + resource combination
func parseSESv2Path(method, path string) (string, string) {
	// Strip /v2/email/ prefix and split remaining path into segments.
	tail := strings.TrimPrefix(path, sesv2PathPrefix)
	tail = strings.TrimSuffix(tail, "/")
	segments := strings.SplitN(tail, "/", segmentSplitN)

	if len(segments) == 0 || segments[0] == "" {
		return unknownAction, ""
	}

	switch segments[0] {
	case "identities":
		return parseIdentityPath(method, segments)
	case "outbound-emails":
		if method == http.MethodPost {
			return "SendEmail", ""
		}
	case "configuration-sets":
		return parseConfigSetPath(method, segments)
	}

	return unknownAction, ""
}

const segmentSplitN = 2

func parseIdentityPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return "ListEmailIdentities", ""
	case method == http.MethodPost && len(segments) == 1:
		return "CreateEmailIdentity", ""
	case method == http.MethodGet && len(segments) == segmentSplitN:
		return "GetEmailIdentity", segments[1]
	case method == http.MethodDelete && len(segments) == segmentSplitN:
		return "DeleteEmailIdentity", segments[1]
	}

	return unknownAction, ""
}

func parseConfigSetPath(method string, segments []string) (string, string) {
	switch {
	case method == http.MethodGet && len(segments) == 1:
		return "ListConfigurationSets", ""
	case method == http.MethodPost && len(segments) == 1:
		return "CreateConfigurationSet", ""
	case method == http.MethodGet && len(segments) == segmentSplitN:
		return "GetConfigurationSet", segments[1]
	case method == http.MethodDelete && len(segments) == segmentSplitN:
		return "DeleteConfigurationSet", segments[1]
	}

	return unknownAction, ""
}

// Handler returns the Echo handler function for SES v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op, resource := parseSESv2Path(c.Request().Method, c.Request().URL.Path)
		log.DebugContext(ctx, "SESv2 request", "operation", op, "resource", resource)

		if op == unknownAction {
			return h.writeError(c, http.StatusNotFound, "NotFoundException",
				fmt.Sprintf("no route for %s %s", c.Request().Method, c.Request().URL.Path))
		}

		var (
			resp  any
			opErr error
		)

		switch op {
		case "CreateEmailIdentity":
			resp, opErr = h.handleCreateEmailIdentity(c)
		case "GetEmailIdentity":
			resp, opErr = h.handleGetEmailIdentity(resource)
		case "ListEmailIdentities":
			resp, opErr = h.handleListEmailIdentities(c)
		case "DeleteEmailIdentity":
			resp, opErr = h.handleDeleteEmailIdentity(resource)
		case "SendEmail":
			resp, opErr = h.handleSendEmail(c)
		case "CreateConfigurationSet":
			resp, opErr = h.handleCreateConfigurationSet(c)
		case "GetConfigurationSet":
			resp, opErr = h.handleGetConfigurationSet(resource)
		case "ListConfigurationSets":
			resp, opErr = h.handleListConfigurationSets(c)
		case "DeleteConfigurationSet":
			resp, opErr = h.handleDeleteConfigurationSet(resource)
		default:
			return h.writeError(c, http.StatusBadRequest, "BadRequestException",
				fmt.Sprintf("%s is not a valid SES v2 operation", op))
		}

		if opErr != nil {
			return h.handleOpError(c, op, opErr)
		}

		if resp == nil {
			return c.NoContent(http.StatusOK)
		}

		return c.JSON(http.StatusOK, resp)
	}
}

// ---- request types ----

type createEmailIdentityInput struct {
	EmailIdentity string `json:"EmailIdentity"`
}

type sendEmailInput struct {
	FromEmailAddress string           `json:"FromEmailAddress"`
	Destination      emailDestination `json:"Destination"`
	Content          emailContent     `json:"Content"`
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
	Subject emailData   `json:"Subject"`
	Body    emailBody   `json:"Body"`
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
	IdentityName       string `json:"IdentityName"`
	IdentityType       string `json:"IdentityType"`
	SendingEnabled     bool   `json:"SendingEnabled"`
}

type listEmailIdentitiesOutput struct {
	EmailIdentities []emailIdentitySummary `json:"EmailIdentities"`
	NextToken       string                 `json:"NextToken,omitempty"`
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
	ConfigurationSets []configurationSetSummary `json:"ConfigurationSets"`
	NextToken         string                    `json:"NextToken,omitempty"`
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

func (h *Handler) handleListEmailIdentities(c *echo.Context) (any, error) {
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
	}, nil
}

func (h *Handler) handleDeleteEmailIdentity(identity string) (any, error) {
	if err := h.Backend.DeleteEmailIdentity(identity); err != nil {
		return nil, err
	}

	return nil, nil
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

func (h *Handler) handleListConfigurationSets(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListConfigurationSets(nextToken, 0)

	items := make([]configurationSetSummary, 0, len(pg.Data))

	for _, cs := range pg.Data {
		items = append(items, configurationSetSummary{Name: cs.Name})
	}

	return &listConfigurationSetsOutput{
		ConfigurationSets: items,
		NextToken:         pg.Next,
	}, nil
}

func (h *Handler) handleDeleteConfigurationSet(name string) (any, error) {
	if err := h.Backend.DeleteConfigurationSet(name); err != nil {
		return nil, err
	}

	return nil, nil
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, op string, opErr error) error {
	switch {
	case errors.Is(opErr, ErrIdentityNotFound), errors.Is(opErr, ErrConfigSetNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFoundException", opErr.Error())
	case errors.Is(opErr, ErrIdentityAlreadyExists), errors.Is(opErr, ErrConfigSetAlreadyExists):
		return h.writeError(c, http.StatusConflict, "AlreadyExistsException", opErr.Error())
	case errors.Is(opErr, ErrInvalidParameter):
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", opErr.Error())
	default:
		logger.Load(c.Request().Context()).Error("SESv2 internal error", "error", opErr, "op", op)

		return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
	}
}

type sesv2ErrorResponse struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(c *echo.Context, statusCode int, _ string, message string) error {
	return c.JSON(statusCode, sesv2ErrorResponse{Message: message})
}
