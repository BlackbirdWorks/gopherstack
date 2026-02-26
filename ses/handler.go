package ses

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	sesAPIVersion    = "Version=2010-12-01"
	sesXMLNS         = "http://ses.amazonaws.com/doc/2010-12-01/"
	sesMatchPriority = 80
	unknownAction    = "Unknown"
)

// Handler is the Echo HTTP handler for SES operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
}

// NewHandler creates a new SES handler with the given backend and logger.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
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
		"VerifyEmailIdentity",
		"ListIdentities",
		"GetIdentityVerificationAttributes",
		"DeleteIdentity",
	}
}

// RouteMatcher returns a function that matches SES requests.
// SES requests are form-encoded POSTs containing the SES API version.
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

		body, err := httputil.ReadBody(r)
		if err != nil {
			return false
		}

		return strings.Contains(string(body), sesAPIVersion)
	}
}

// MatchPriority returns the routing priority for the SES handler.
func (h *Handler) MatchPriority() int {
	return sesMatchPriority
}

// ExtractOperation extracts the SES action from the request body.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return unknownAction
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return unknownAction
	}

	action := vals.Get("Action")
	if action == "" {
		return unknownAction
	}

	return action
}

// ExtractResource returns the source email address or identity from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	vals, err := url.ParseQuery(string(body))
	if err != nil {
		return ""
	}

	for _, key := range []string{"Source", "EmailAddress", "Identity"} {
		if v := vals.Get(key); v != "" {
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

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read SES request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValue", "invalid request body")
		}

		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		log.DebugContext(ctx, "SES request", "action", action)

		reqID := newRequestID()

		var (
			resp  any
			opErr error
		)

		switch action {
		case "VerifyEmailIdentity":
			resp, opErr = h.handleVerifyEmailIdentity(vals, reqID)
		case "DeleteIdentity":
			resp, opErr = h.handleDeleteIdentity(vals, reqID)
		case "ListIdentities":
			resp = h.handleListIdentities(vals, reqID)
		case "GetIdentityVerificationAttributes":
			resp = h.handleGetIdentityVerificationAttributes(vals, reqID)
		case "SendEmail":
			resp, opErr = h.handleSendEmail(vals, reqID)
		case "SendRawEmail":
			resp, opErr = h.handleSendRawEmail(vals, reqID)
		default:
			return h.writeError(c, http.StatusBadRequest, "InvalidAction",
				fmt.Sprintf("%s is not a valid SES action", action))
		}

		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, marshalErr := marshalXML(resp)
		if marshalErr != nil {
			log.ErrorContext(ctx, "failed to marshal SES response", "action", action, "error", marshalErr)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
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

func (h *Handler) handleDeleteIdentity(vals url.Values, reqID string) (any, error) {
	identity := vals.Get("Identity")

	if err := h.Backend.DeleteIdentity(identity); err != nil {
		return nil, err
	}

	return &deleteIdentityResponse{
		Xmlns:     sesXMLNS,
		RequestID: reqID,
	}, nil
}

func (h *Handler) handleListIdentities(vals url.Values, reqID string) any {
	_ = vals // no filter params needed for stub

	identities := h.Backend.ListIdentities()
	members := make([]xmlMember, 0, len(identities))

	for _, id := range identities {
		members = append(members, xmlMember{Value: id})
	}

	return &listIdentitiesResponse{
		Xmlns: sesXMLNS,
		Result: listIdentitiesResult{
			Identities: xmlMemberList{Members: members},
		},
		RequestID: reqID,
	}
}

func (h *Handler) handleGetIdentityVerificationAttributes(vals url.Values, reqID string) any {
	var identities []string

	// AWS SDK sends Identities.member.1, Identities.member.2, ...
	for i := 1; ; i++ {
		key := fmt.Sprintf("Identities.member.%d", i)
		v := vals.Get(key)

		if v == "" {
			break
		}

		identities = append(identities, v)
	}

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

	var toAddrs []string

	for i := 1; ; i++ {
		v := vals.Get(fmt.Sprintf("Destination.ToAddresses.member.%d", i))
		if v == "" {
			break
		}

		toAddrs = append(toAddrs, v)
	}

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

	return &sendEmailResponse{
		Xmlns: sesXMLNS,
		Result: sendEmailResult{
			MessageID: msgID,
		},
		RequestID: reqID,
	}, nil
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest

	var code string

	switch {
	case errors.Is(opErr, ErrIdentityNotFound):
		code = "NoSuchEntity"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		h.Logger.Error("SES internal error", "error", opErr, "action", action)
	}

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &sesErrorResponse{
		Xmlns:     sesXMLNS,
		Error:     sesError{Code: code, Message: message, Type: "Sender"},
		RequestID: newRequestID(),
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
	XMLName   xml.Name `xml:"VerifyEmailIdentityResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type deleteIdentityResponse struct {
	XMLName   xml.Name `xml:"DeleteIdentityResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"ResponseMetadata>RequestId"`
}

type xmlMember struct {
	Value string `xml:",chardata"`
}

type xmlMemberList struct {
	Members []xmlMember `xml:"member"`
}

type listIdentitiesResult struct {
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
