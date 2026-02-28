package acm

import (
	"encoding/json"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	acmMatchPriority = 81
	acmTargetPrefix  = "CertificateManager."
)

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
	tags    map[string]map[string]string
	tagsMu  sync.RWMutex
}

// NewHandler creates a new ACM handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) []map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	tags := h.tags[resourceID]
	result := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
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
		action := h.ExtractOperation(c)
		if action == "" {
			return h.writeJSONError(c, http.StatusBadRequest, "MissingAction", "missing X-Amz-Target")
		}

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			return h.writeJSONError(c, http.StatusBadRequest, "InvalidParameterValue", "cannot read body")
		}

		resp, opErr := h.dispatchJSON(action, body)
		if errors.Is(opErr, errUnknownACMAction) {
			return h.writeJSONError(c, http.StatusBadRequest, "InvalidAction",
				action+" is not a valid ACM action")
		}

		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		return c.JSON(http.StatusOK, resp)
	}
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
	var input struct {
		DomainName              string `json:"DomainName"`
		CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	}
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

	return map[string]string{"CertificateArn": cert.ARN}, nil
}

func (h *Handler) jsonDescribeCertificate(body []byte) (any, error) {
	var input struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.DescribeCertificate(input.CertificateArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Certificate": map[string]any{
			"CertificateArn": cert.ARN,
			"DomainName":     cert.DomainName,
			"Status":         cert.Status,
			"Type":           cert.Type,
			"CreatedAt":      cert.CreatedAt.Unix(),
			"DomainValidationOptions": []map[string]any{
				{
					"DomainName":       cert.DomainName,
					"ValidationDomain": cert.DomainName,
					"ValidationStatus": "SUCCESS",
					"ValidationMethod": "DNS",
				},
			},
		},
	}, nil
}

func (h *Handler) jsonListCertificates() (any, error) {
	certs := h.Backend.ListCertificates()
	summaries := make([]map[string]string, 0, len(certs))
	for _, c := range certs {
		summaries = append(summaries, map[string]string{
			"CertificateArn": c.ARN,
			"DomainName":     c.DomainName,
		})
	}

	return map[string]any{"CertificateSummaryList": summaries}, nil
}

func (h *Handler) jsonDeleteCertificate(body []byte) (any, error) {
	var input struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	if err := h.Backend.DeleteCertificate(input.CertificateArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) jsonListTagsForCertificate(body []byte) (any, error) {
	var input struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	return map[string]any{"Tags": h.getTags(input.CertificateArn)}, nil
}

func (h *Handler) jsonAddTagsToCertificate(body []byte) (any, error) {
	var input struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}
	h.setTags(input.CertificateArn, kv)

	return map[string]any{}, nil
}

func (h *Handler) jsonRemoveTagsFromCertificate(body []byte) (any, error) {
	var input struct {
		CertificateArn string `json:"CertificateArn"`
		Tags           []struct {
			Key string `json:"Key"`
		} `json:"Tags"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	keys := make([]string, 0, len(input.Tags))
	for _, t := range input.Tags {
		keys = append(keys, t.Key)
	}
	h.removeTags(input.CertificateArn, keys)

	return map[string]any{}, nil
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
