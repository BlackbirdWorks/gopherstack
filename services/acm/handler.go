package acm

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	DomainName              string   `json:"DomainName"`
	ValidationMethod        string   `json:"ValidationMethod"`
	CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
	SubjectAlternativeNames []string `json:"SubjectAlternativeNames"`
}

type requestCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type domainValidationOption struct {
	ResourceRecord   *resourceRecord `json:"ResourceRecord,omitempty"`
	DomainName       string          `json:"DomainName"`
	ValidationDomain string          `json:"ValidationDomain"`
	ValidationStatus string          `json:"ValidationStatus"`
	ValidationMethod string          `json:"ValidationMethod"`
}

type resourceRecord struct {
	Name  string `json:"Name"`
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

type certificateDetail struct {
	CertificateArn          string                   `json:"CertificateArn"`
	DomainName              string                   `json:"DomainName"`
	Status                  string                   `json:"Status"`
	Type                    string                   `json:"Type"`
	SubjectAlternativeNames []string                 `json:"SubjectAlternativeNames,omitempty"`
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

type listCertificatesInput struct {
	NextToken string `json:"NextToken"`
	MaxItems  int    `json:"MaxItems"`
}

type listCertificatesOutput struct {
	NextToken              string               `json:"NextToken,omitempty"`
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

type importCertificateInput struct {
	CertificateArn   string `json:"CertificateArn"`
	Certificate      string `json:"Certificate"`
	PrivateKey       string `json:"PrivateKey"`
	CertificateChain string `json:"CertificateChain"`
}

type importCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type renewCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
}

type renewCertificateOutput struct{}

type exportCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
	Passphrase     string `json:"Passphrase"`
}

type exportCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
	PrivateKey       string `json:"PrivateKey"`
}

type getCertificateInput struct {
	CertificateArn string `json:"CertificateArn"`
}

type getCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend *InMemoryBackend
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new ACM handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
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
		"ImportCertificate",
		"RenewCertificate",
		"ExportCertificate",
		"GetCertificate",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "acm" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ACM instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

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
	body, err := httputils.ReadBody(c.Request())
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
		return h.jsonListCertificates(body)
	case "DeleteCertificate":
		return h.jsonDeleteCertificate(body)
	case "ListTagsForCertificate":
		return h.jsonListTagsForCertificate(body)
	case "AddTagsToCertificate":
		return h.jsonAddTagsToCertificate(body)
	case "RemoveTagsFromCertificate":
		return h.jsonRemoveTagsFromCertificate(body)
	case "ImportCertificate":
		return h.jsonImportCertificate(body)
	case "RenewCertificate":
		return h.jsonRenewCertificate(body)
	case "ExportCertificate":
		return h.jsonExportCertificate(body)
	case "GetCertificate":
		return h.jsonGetCertificate(body)
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
	cert, err := h.Backend.RequestCertificate(
		input.DomainName,
		certType,
		input.ValidationMethod,
		input.SubjectAlternativeNames,
	)
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

	dvoList := make([]domainValidationOption, 0, len(cert.DomainValidationOptions))
	for _, dvo := range cert.DomainValidationOptions {
		opt := domainValidationOption{
			DomainName:       dvo.DomainName,
			ValidationDomain: dvo.ValidationDomain,
			ValidationStatus: dvo.ValidationStatus,
			ValidationMethod: dvo.ValidationMethod,
		}
		if dvo.ResourceRecord != nil {
			opt.ResourceRecord = &resourceRecord{
				Name:  dvo.ResourceRecord.Name,
				Type:  dvo.ResourceRecord.Type,
				Value: dvo.ResourceRecord.Value,
			}
		}
		dvoList = append(dvoList, opt)
	}

	return &describeCertificateOutput{
		Certificate: certificateDetail{
			CertificateArn:          cert.ARN,
			DomainName:              cert.DomainName,
			Status:                  cert.Status,
			Type:                    cert.Type,
			CreatedAt:               cert.CreatedAt.Unix(),
			SubjectAlternativeNames: cert.SubjectAlternativeNames,
			DomainValidationOptions: dvoList,
		},
	}, nil
}

func (h *Handler) jsonListCertificates(body []byte) (any, error) {
	var input listCertificatesInput
	_ = json.Unmarshal(body, &input)

	p := h.Backend.ListCertificates(input.NextToken, input.MaxItems)
	summaries := make([]certificateSummary, 0, len(p.Data))
	for _, c := range p.Data {
		summaries = append(summaries, certificateSummary{
			CertificateArn: c.ARN,
			DomainName:     c.DomainName,
		})
	}

	return &listCertificatesOutput{CertificateSummaryList: summaries, NextToken: p.Next}, nil
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

func (h *Handler) jsonImportCertificate(body []byte) (any, error) {
	var input importCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.ImportCertificate(input.Certificate, input.PrivateKey, input.CertificateChain)
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonRenewCertificate(body []byte) (any, error) {
	var input renewCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	if err := h.Backend.RenewCertificate(input.CertificateArn); err != nil {
		return nil, err
	}

	return &renewCertificateOutput{}, nil
}

func (h *Handler) jsonExportCertificate(body []byte) (any, error) {
	var input exportCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.ExportCertificate(input.CertificateArn)
	if err != nil {
		return nil, err
	}

	return &exportCertificateOutput{
		Certificate:      cert.CertificateBody,
		CertificateChain: cert.CertificateChain,
		PrivateKey:       cert.PrivateKey,
	}, nil
}

func (h *Handler) jsonGetCertificate(body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	certBody, certChain, err := h.Backend.GetCertificate(input.CertificateArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateOutput{
		Certificate:      certBody,
		CertificateChain: certChain,
	}, nil
}

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	var code string
	switch {
	case errors.Is(opErr, ErrCertNotFound):
		code = "ResourceNotFoundException"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "ValidationException"
	case errors.Is(opErr, ErrNotEligible):
		code = "RequestError"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("ACM internal error", "error", opErr, "action", action)
	}

	return h.writeJSONError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeJSONError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  code,
		"message": message,
	})
}
