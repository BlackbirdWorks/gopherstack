package opensearch

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	openSearchPathPrefix  = "/2021-01-01/opensearch/domain"
	openSearchMatchPriority = 82
)

// Handler is the HTTP handler for OpenSearch operations.
type Handler struct {
	Backend   *InMemoryBackend
	Logger    *slog.Logger
	AccountID string
	Region    string
}

// NewHandler creates a new OpenSearch Handler.
func NewHandler(backend *InMemoryBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "OpenSearch" }

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return openSearchMatchPriority }

// RouteMatcher returns a matcher that selects OpenSearch requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, openSearchPathPrefix)
	}
}

// GetSupportedOperations returns supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateDomain",
		"DescribeDomain",
		"DeleteDomain",
		"ListDomainNames",
	}
}

// ExtractOperation returns the operation name from a request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	method := c.Request().Method

	rest := strings.TrimPrefix(path, openSearchPathPrefix)

	switch {
	case rest == "" || rest == "/":
		if method == http.MethodPost {
			return "CreateDomain"
		}

		if method == http.MethodGet {
			return "ListDomainNames"
		}

		return "Unknown"
	case strings.HasPrefix(rest, "/") && method == http.MethodGet:
		return "DescribeDomain"
	case strings.HasPrefix(rest, "/") && method == http.MethodDelete:
		return "DeleteDomain"
	}

	return "Unknown"
}

// ExtractResource returns the domain name from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, openSearchPathPrefix+"/")

	if rest == path {
		return ""
	}

	return strings.TrimSuffix(rest, "/")
}

// domainJSON is the JSON request body for CreateDomain.
type domainJSON struct {
	ClusterConfig *struct {
		InstanceType  string `json:"InstanceType"`
		InstanceCount int    `json:"InstanceCount"`
	} `json:"ClusterConfig"`
	DomainName    string `json:"DomainName"`
	EngineVersion string `json:"EngineVersion"`
}

// domainStatusJSON is the JSON response for domain operations.
type domainStatusJSON struct {
	ClusterConfig clusterConfigJSON `json:"ClusterConfig"`
	DomainName    string            `json:"DomainName"`
	ARN           string            `json:"ARN"`
	EngineVersion string            `json:"EngineVersion"`
	Endpoint      string            `json:"Endpoint"`
	Processing    bool              `json:"Processing"`
}

// clusterConfigJSON is the JSON representation of cluster config.
type clusterConfigJSON struct {
	InstanceType  string `json:"InstanceType"`
	InstanceCount int    `json:"InstanceCount"`
}

// domainStatusWrapJSON wraps the domain status in a DomainStatus key.
type domainStatusWrapJSON struct {
	DomainStatus domainStatusJSON `json:"DomainStatus"`
}

// domainListJSON is the response for ListDomainNames.
type domainListJSON struct {
	DomainNames []domainNameEntry `json:"DomainNames"`
}

// domainNameEntry is an element of the ListDomainNames response.
type domainNameEntry struct {
	DomainName    string `json:"DomainName"`
	EngineVersion string `json:"EngineVersion"`
}

// ServeHTTP implements http.Handler for the OpenSearch service.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	rest := strings.TrimPrefix(path, openSearchPathPrefix)

	switch {
	case (rest == "" || rest == "/") && r.Method == http.MethodPost:
		h.handleCreateDomain(w, r)
	case (rest == "" || rest == "/") && r.Method == http.MethodGet:
		h.handleListDomainNames(w, r)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodGet:
		domainName := strings.TrimPrefix(rest, "/")
		domainName = strings.TrimSuffix(domainName, "/")
		h.handleDescribeDomain(w, r, domainName)
	case strings.HasPrefix(rest, "/") && r.Method == http.MethodDelete:
		domainName := strings.TrimPrefix(rest, "/")
		domainName = strings.TrimSuffix(domainName, "/")
		h.handleDeleteDomain(w, r, domainName)
	default:
		h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

// Handle satisfies the Echo handler interface.
func (h *Handler) Handle(c *echo.Context) error {
	h.ServeHTTP(c.Response(), c.Request())

	return nil
}

// Handler returns the Echo HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.Handle
}

func (h *Handler) handleCreateDomain(w http.ResponseWriter, r *http.Request) {
	body, err := httputil.ReadBody(r)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req domainJSON
	if err = json.Unmarshal(body, &req); err != nil {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

		return
	}

	if req.DomainName == "" {
		h.writeError(w, http.StatusBadRequest, "ValidationException", "DomainName is required")

		return
	}

	var cfg ClusterConfig
	if req.ClusterConfig != nil {
		cfg.InstanceType = req.ClusterConfig.InstanceType
		cfg.InstanceCount = req.ClusterConfig.InstanceCount
	}

	domain, err := h.Backend.CreateDomain(req.DomainName, req.EngineVersion, cfg)
	if err != nil {
		if errors.Is(err, ErrDomainAlreadyExists) {
			h.writeError(w, http.StatusConflict, "ResourceAlreadyExistsException", err.Error())
		} else {
			h.writeError(w, http.StatusBadRequest, "ValidationException", err.Error())
		}

		return
	}

	h.writeJSON(w, http.StatusOK, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDescribeDomain(w http.ResponseWriter, _ *http.Request, name string) {
	domain, err := h.Backend.DescribeDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(w, http.StatusOK, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleDeleteDomain(w http.ResponseWriter, _ *http.Request, name string) {
	domain, err := h.Backend.DeleteDomain(name)
	if err != nil {
		if errors.Is(err, ErrDomainNotFound) {
			h.writeError(w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		} else {
			h.writeError(w, http.StatusInternalServerError, "InternalException", err.Error())
		}

		return
	}

	h.writeJSON(w, http.StatusOK, domainStatusWrapJSON{
		DomainStatus: toDomainStatusJSON(domain),
	})
}

func (h *Handler) handleListDomainNames(w http.ResponseWriter, _ *http.Request) {
	names := h.Backend.ListDomainNames()
	entries := make([]domainNameEntry, 0, len(names))

	for _, name := range names {
		d, err := h.Backend.DescribeDomain(name)
		if err != nil {
			continue
		}

		entries = append(entries, domainNameEntry{
			DomainName:    name,
			EngineVersion: d.EngineVersion,
		})
	}

	h.writeJSON(w, http.StatusOK, domainListJSON{DomainNames: entries})
}

func toDomainStatusJSON(d *Domain) domainStatusJSON {
	return domainStatusJSON{
		DomainName:    d.Name,
		ARN:           d.ARN,
		EngineVersion: d.EngineVersion,
		Endpoint:      d.Endpoint,
		Processing:    false,
		ClusterConfig: clusterConfigJSON{
			InstanceType:  d.ClusterConfig.InstanceType,
			InstanceCount: d.ClusterConfig.InstanceCount,
		},
	}
}

type errorResponseJSON struct {
	Message string `json:"message"`
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	h.Logger.Error("opensearch error", "code", code, "message", message)
	w.Header().Set("x-amzn-ErrorType", code)
	httputil.WriteJSON(h.Logger, w, status, errorResponseJSON{Message: message})
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, v any) {
	httputil.WriteJSON(h.Logger, w, status, v)
}
