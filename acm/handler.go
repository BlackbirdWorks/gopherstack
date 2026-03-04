package acm

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	acmMatchPriority = 81
	acmTargetPrefix  = "CertificateManager."
)

type requestCertificateInput struct {
	DomainName              string `json:"DomainName"`
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type requestCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type domainValidationOption struct {
	DomainName       string `json:"DomainName"`
	ValidationDomain string `json:"ValidationDomain"`
	ValidationStatus string `json:"ValidationStatus"`
	ValidationMethod string `json:"ValidationMethod"`
}

type certificateDetail struct {
	CertificateArn          string                   `json:"CertificateArn"`
	DomainName              string                   `json:"DomainName"`
	Status                  string                   `json:"Status"`
	Type                    string                   `json:"Type"`
	DomainValidationOptions []domainValidationOption `json:"DomainValidationOptions"`
	CreatedAt               int64                    `json:"CreatedAt"`
}

type describeCertificateOutput struct {
	Certificate certificateDetail `json:"Certificate"`
}

type certificateSummary struct {
	CertificateArn string `json:"CertificateArn"`
	DomainName     string `json:"DomainName"`
}

type listCertificatesOutput struct {
	CertificateSummaryList []certificateSummary `json:"CertificateSummaryList"`
}

type deleteCertificateOutput struct{}

type listTagsForCertificateOutput struct {
	Tags []map[string]string `json:"Tags"`
}

type addTagsToCertificateOutput struct{}

type removeTagsFromCertificateOutput struct{}

type describeCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
}

type deleteCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
}

type listTagsForCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
}

type addTagsToCertificateInput struct {
	CertificateArn string       `json:"CertificateArn"`
	Tags           []svcTags.KV `json:"Tags"`
}

type acmTagKey struct {
	Key string `json:"Key"`
}

type removeTagsFromCertificateInput struct {
	CertificateArn string      `json:"CertificateArn"`
	Tags           []acmTagKey `json:"Tags"`
}

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new ACM handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  log,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("acm.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("acm." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.RLock("removeTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) getTags(resourceID string) []map[string]string {
	h.tagsMu.RLock("getTags")
	t := h.tags[resourceID]
	h.tagsMu.RUnlock()
	if t == nil {
		return []map[string]string{}
	}
	tagMap := t.Clone()
	result := make([]map[string]string, 0, len(tagMap))
	for k, v := range tagMap {
		result = append(result, map[string]string{"Key": k, "Value": v})
	}

	return result
}

// Name returns the service name.
func (h *Handler) Name() string { return "ACM" }

// GetSupportedOperations returns supported ACM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RequestCertificate",
		"DescribeCertificate",
		"ListCertificates",
		"DeleteCertificate",
		"ListTagsForCertificate",
		"AddTagsToCertificate",
		"RemoveTagsFromCertificate",
	}
}

// RouteMatcher returns a function that matches ACM JSON-protocol requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if r.Method != http.MethodPost {
			return false
		}
		if strings.HasPrefix(r.URL.Path, "/dashboard/") {
			return false
		}
		target := r.Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, acmTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return acmMatchPriority }

// ExtractOperation extracts the ACM action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, acmTargetPrefix)
}

// ExtractResource returns the certificate ARN from the JSON body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}
	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}
	var arn string
	if raw, ok := m["CertificateArn"]; ok {
		_ = json.Unmarshal(raw, &arn)
	}

	return arn
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ACM", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

// dispatch routes the operation to the appropriate handler and marshals the response.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	resp, err := h.dispatchJSON(action, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(resp)
}

// handleError writes a standardized error response back to the client.
func (h *Handler) handleError(_ context.Context, c *echo.Context, action string, reqErr error) error {
	if errors.Is(reqErr, errUnknownACMAction) {
		return h.writeJSONError(c, http.StatusBadRequest, "InvalidAction",
			action+" is not a valid ACM action")
	}

	return h.handleOpError(c, action, reqErr)
}

// errUnknownACMAction is returned by dispatchJSON for unrecognised action names.
var errUnknownACMAction = errors.New("unknown ACM action")

// dispatchJSON routes a JSON-protocol ACM action to the appropriate handler.
func (h *Handler) dispatchJSON(action string, body []byte) (any, error) {
	switch action {
	case "RequestCertificate":
		return h.jsonRequestCertificate(body)
	case "DescribeCertificate":
		return h.jsonDescribeCertificate(body)
	case "ListCertificates":
		return h.jsonListCertificates()
	case "DeleteCertificate":
		return h.jsonDeleteCertificate(body)
	case "ListTagsForCertificate":
		return h.jsonListTagsForCertificate(body)
	case "AddTagsToCertificate":
		return h.jsonAddTagsToCertificate(body)
	case "RemoveTagsFromCertificate":
		return h.jsonRemoveTagsFromCertificate(body)
	default:
		return nil, errUnknownACMAction
	}
}

func (h *Handler) jsonRequestCertificate(body []byte) (any, error) {
	var input requestCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	certType := ""
	if input.CertificateAuthorityArn != "" {
		certType = "PRIVATE"
	}
	cert, err := h.Backend.RequestCertificate(input.DomainName, certType)
	if err != nil {
		return nil, err
	}

	return &requestCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonDescribeCertificate(body []byte) (any, error) {
	var input describeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.DescribeCertificate(input.CertificateArn)
	if err != nil {
		return nil, err
	}

	return &describeCertificateOutput{
		Certificate: certificateDetail{
			CertificateArn: cert.ARN,
			DomainName:     cert.DomainName,
			Status:         cert.Status,
			Type:           cert.Type,
			CreatedAt:      cert.CreatedAt.Unix(),
			DomainValidationOptions: []domainValidationOption{
				{
					DomainName:       cert.DomainName,
					ValidationDomain: cert.DomainName,
					ValidationStatus: "SUCCESS",
					ValidationMethod: "DNS",
				},
			},
		},
	}, nil
}

func (h *Handler) jsonListCertificates() (any, error) {
	certs := h.Backend.ListCertificates()
	summaries := make([]certificateSummary, 0, len(certs))
	for _, c := range certs {
		summaries = append(summaries, certificateSummary{
			CertificateArn: c.ARN,
			DomainName:     c.DomainName,
		})
	}

	return &listCertificatesOutput{CertificateSummaryList: summaries}, nil
}

func (h *Handler) jsonDeleteCertificate(body []byte) (any, error) {
	var input deleteCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	if err := h.Backend.DeleteCertificate(input.CertificateArn); err != nil {
		return nil, err
	}

	return &deleteCertificateOutput{}, nil
}

func (h *Handler) jsonListTagsForCertificate(body []byte) (any, error) {
	var input listTagsForCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	return &listTagsForCertificateOutput{Tags: h.getTags(input.CertificateArn)}, nil
}

func (h *Handler) jsonAddTagsToCertificate(body []byte) (any, error) {
	var input addTagsToCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}
	h.setTags(input.CertificateArn, kv)

	return &addTagsToCertificateOutput{}, nil
}

func (h *Handler) jsonRemoveTagsFromCertificate(body []byte) (any, error) {
	var input removeTagsFromCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	keys := make([]string, 0, len(input.Tags))
	for _, t := range input.Tags {
		keys = append(keys, t.Key)
	}
	h.removeTags(input.CertificateArn, keys)

	return &removeTagsFromCertificateOutput{}, nil
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	var code string
	switch {
	case errors.Is(opErr, ErrCertNotFound):
		code = "ResourceNotFoundException"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "ValidationException"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		h.Logger.Error("ACM internal error", "error", opErr, "action", action)
	}

	return h.writeJSONError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeJSONError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  code,
		"message": message,
	})
}
