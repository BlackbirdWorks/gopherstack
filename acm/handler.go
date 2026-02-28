package acm

import (
	"encoding/xml"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	acmVersion       = "2015-12-08"
	acmXMLNS         = "http://acm.amazonaws.com/doc/2015-12-08/"
	acmMatchPriority = 81
)

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend *InMemoryBackend
	Logger  *slog.Logger
	tagsMu  sync.RWMutex
	tags    map[string]map[string]string
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
	for k, v := range kv {
		h.tags[resourceID][k] = v
	}
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	for k, v := range h.tags[resourceID] {
		result[k] = v
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

// RouteMatcher returns a function that matches ACM requests.
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
		vals, err := url.ParseQuery(string(body))
		if err != nil {
			return false
		}

		return vals.Get("Version") == acmVersion
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return acmMatchPriority }

// ExtractOperation extracts the ACM action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return "Unknown"
	}
	action := r.Form.Get("Action")
	if action == "" {
		return "Unknown"
	}

	return action
}

// ExtractResource returns the certificate ARN from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	r := c.Request()
	if err := r.ParseForm(); err != nil {
		return ""
	}

	return r.Form.Get("CertificateArn")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		if err := r.ParseForm(); err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		vals := r.Form
		action := vals.Get("Action")
		if action == "" {
			return h.writeError(c, http.StatusBadRequest, "MissingAction", "missing Action parameter")
		}

		var (
			resp  any
			opErr error
		)

		switch action {
		case "RequestCertificate":
			resp, opErr = h.handleRequestCertificate(vals)
		case "DescribeCertificate":
			resp, opErr = h.handleDescribeCertificate(vals)
		case "ListCertificates":
			resp = h.handleListCertificates()
		case "DeleteCertificate":
			resp, opErr = h.handleDeleteCertificate(vals)
		case "ListTagsForCertificate":
			arn := vals.Get("CertificateArn")
			tags := h.getTags(arn)
			type tagEntry struct {
				Key   string `xml:"Key"`
				Value string `xml:"Value"`
			}
			type listTagsResp struct {
				XMLName xml.Name   `xml:"ListTagsForCertificateResponse"`
				Xmlns   string     `xml:"xmlns,attr"`
				Result  struct {
					XMLName xml.Name   `xml:"ListTagsForCertificateResult"`
					Tags    []tagEntry `xml:"Tags>member"`
				} `xml:"ListTagsForCertificateResult"`
			}
			tagList := make([]tagEntry, 0, len(tags))
			for k, v := range tags {
				tagList = append(tagList, tagEntry{Key: k, Value: v})
			}
			r := listTagsResp{Xmlns: acmXMLNS}
			r.Result.Tags = tagList
			resp = r
		case "AddTagsToCertificate":
			arn := vals.Get("CertificateArn")
			newTags := make(map[string]string)
			for i := 1; ; i++ {
				k := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
				if k == "" {
					break
				}
				v := vals.Get(fmt.Sprintf("Tags.member.%d.Value", i))
				newTags[k] = v
			}
			h.setTags(arn, newTags)
			resp = struct {
				XMLName xml.Name `xml:"AddTagsToCertificateResponse"`
			}{}
		case "RemoveTagsFromCertificate":
			arn := vals.Get("CertificateArn")
			var keys []string
			for i := 1; ; i++ {
				k := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
				if k == "" {
					break
				}
				keys = append(keys, k)
			}
			h.removeTags(arn, keys)
			resp = struct {
				XMLName xml.Name `xml:"RemoveTagsFromCertificateResponse"`
			}{}
		default:
			return h.writeError(c, http.StatusBadRequest, "InvalidAction",
				fmt.Sprintf("%s is not a valid ACM action", action))
		}

		if opErr != nil {
			return h.handleOpError(c, action, opErr)
		}

		xmlBytes, err := marshalXML(resp)
		if err != nil {
			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
		}

		return c.Blob(http.StatusOK, "text/xml", xmlBytes)
	}
}

func (h *Handler) handleRequestCertificate(vals url.Values) (any, error) {
	domainName := vals.Get("DomainName")
	certType := ""
	if vals.Get("CertificateAuthorityArn") != "" {
		certType = "PRIVATE"
	}
	cert, err := h.Backend.RequestCertificate(domainName, certType)
	if err != nil {
		return nil, err
	}

	return &requestCertificateResponse{
		Xmlns:          acmXMLNS,
		CertificateArn: cert.ARN,
	}, nil
}

func (h *Handler) handleDescribeCertificate(vals url.Values) (any, error) {
	arn := vals.Get("CertificateArn")
	cert, err := h.Backend.DescribeCertificate(arn)
	if err != nil {
		return nil, err
	}

	return &describeCertificateResponse{
		Xmlns: acmXMLNS,
		Certificate: xmlCertificateDetail{
			CertificateArn: cert.ARN,
			DomainName:     cert.DomainName,
			Status:         cert.Status,
			Type:           cert.Type,
			CreatedAt:      cert.CreatedAt.Format("2006-01-02T15:04:05Z"),
		},
	}, nil
}

func (h *Handler) handleListCertificates() any {
	certs := h.Backend.ListCertificates()
	summaries := make([]xmlCertSummary, 0, len(certs))
	for _, c := range certs {
		summaries = append(summaries, xmlCertSummary{
			CertificateArn: c.ARN,
			DomainName:     c.DomainName,
		})
	}

	return &listCertificatesResponse{
		Xmlns: acmXMLNS,
		CertificateSummaryList: xmlCertSummaryList{
			Members: summaries,
		},
	}
}

func (h *Handler) handleDeleteCertificate(vals url.Values) (any, error) {
	arn := vals.Get("CertificateArn")
	if err := h.Backend.DeleteCertificate(arn); err != nil {
		return nil, err
	}

	return &deleteCertificateResponse{Xmlns: acmXMLNS}, nil
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

	return h.writeError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeError(c *echo.Context, statusCode int, code, message string) error {
	errResp := &acmErrorResponse{
		Xmlns: acmXMLNS,
		Error: acmError{Code: code, Message: message, Type: "Sender"},
	}
	xmlBytes, err := marshalXML(errResp)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	return c.Blob(statusCode, "text/xml", xmlBytes)
}

func marshalXML(v any) ([]byte, error) {
	raw, err := xml.Marshal(v)
	if err != nil {
		return nil, err
	}

	return append([]byte(xml.Header), raw...), nil
}

// ---- XML response types ----

type acmError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

type acmErrorResponse struct {
	XMLName xml.Name `xml:"ErrorResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Error   acmError `xml:"Error"`
}

type requestCertificateResponse struct {
	XMLName        xml.Name `xml:"RequestCertificateResponse"`
	Xmlns          string   `xml:"xmlns,attr"`
	CertificateArn string   `xml:"RequestCertificateResult>CertificateArn"`
}

type xmlCertificateDetail struct {
	CertificateArn string `xml:"CertificateArn"`
	DomainName     string `xml:"DomainName"`
	Status         string `xml:"Status"`
	Type           string `xml:"Type"`
	CreatedAt      string `xml:"CreatedAt"`
}

type describeCertificateResponse struct {
	XMLName     xml.Name             `xml:"DescribeCertificateResponse"`
	Xmlns       string               `xml:"xmlns,attr"`
	Certificate xmlCertificateDetail `xml:"DescribeCertificateResult>Certificate"`
}

type xmlCertSummary struct {
	CertificateArn string `xml:"CertificateArn"`
	DomainName     string `xml:"DomainName"`
}

type xmlCertSummaryList struct {
	Members []xmlCertSummary `xml:"member"`
}

type listCertificatesResponse struct {
	XMLName                xml.Name           `xml:"ListCertificatesResponse"`
	Xmlns                  string             `xml:"xmlns,attr"`
	CertificateSummaryList xmlCertSummaryList `xml:"ListCertificatesResult>CertificateSummaryList"`
}

type deleteCertificateResponse struct {
	XMLName xml.Name `xml:"DeleteCertificateResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}
