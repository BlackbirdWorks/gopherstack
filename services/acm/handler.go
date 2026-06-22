package acm

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

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
	Options                 *certificateOptionsInput `json:"Options,omitempty"`
	DomainName              string                   `json:"DomainName"`
	ValidationMethod        string                   `json:"ValidationMethod"`
	CertificateAuthorityArn string                   `json:"CertificateAuthorityArn"`
	IdempotencyToken        string                   `json:"IdempotencyToken"`
	KeyAlgorithm            string                   `json:"KeyAlgorithm"`
	SubjectAlternativeNames []string                 `json:"SubjectAlternativeNames"`
	Tags                    []map[string]string      `json:"Tags"`
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
	ValidationEmails []string        `json:"ValidationEmails,omitempty"`
}

type resourceRecord struct {
	Name  string `json:"Name"`
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

// renewalSummaryDetail is the wire format for the RenewalSummary field in DescribeCertificate.
type renewalSummaryDetail struct {
	RenewalStatus           string                   `json:"RenewalStatus"`
	DomainValidationOptions []domainValidationOption `json:"DomainValidationOptions,omitempty"`
}

type certificateDetail struct {
	RevokedAt               *int64                `json:"RevokedAt,omitempty"`
	IssuedAt                *int64                `json:"IssuedAt,omitempty"`
	ImportedAt              *int64                `json:"ImportedAt,omitempty"`
	RenewalSummary          *renewalSummaryDetail `json:"RenewalSummary,omitempty"`
	CertificateArn          string                `json:"CertificateArn"`
	DomainName              string                `json:"DomainName"`
	Serial                  string                `json:"Serial,omitempty"`
	Subject                 string                `json:"Subject,omitempty"`
	Issuer                  string                `json:"Issuer,omitempty"`
	KeyAlgorithm            string                `json:"KeyAlgorithm,omitempty"`
	SignatureAlgorithm      string                `json:"SignatureAlgorithm,omitempty"`
	Status                  string                `json:"Status"`
	Type                    string                `json:"Type"`
	RevocationReason        string                `json:"RevocationReason,omitempty"`
	RenewalEligibility      string                `json:"RenewalEligibility,omitempty"`
	FailureReason           string                `json:"FailureReason,omitempty"`
	CertificateAuthorityArn string                `json:"CertificateAuthorityArn,omitempty"`
	KeyID                   string                `json:"KeyId,omitempty"`
	Options                 *certificateOptions   `json:"Options,omitempty"`
	SubjectAlternativeNames []string              `json:"SubjectAlternativeNames,omitempty"`
	// DomainValidationOptions uses a concrete slice type here to satisfy the JSON
	// marshaller; the field name matches the AWS wire format.
	DomainValidationOptions []domainValidationOption `json:"DomainValidationOptions"`
	InUseBy                 []string                 `json:"InUseBy,omitempty"`
	KeyUsage                []keyUsageDetail         `json:"KeyUsage,omitempty"`
	ExtendedKeyUsage        []extKeyUsageDetail      `json:"ExtendedKeyUsage,omitempty"`
	CreatedAt               int64                    `json:"CreatedAt"`
	NotBefore               int64                    `json:"NotBefore,omitempty"`
	NotAfter                int64                    `json:"NotAfter,omitempty"`
}

// keyUsageDetail wraps a single AWS key usage string.
type keyUsageDetail struct {
	Name string `json:"Name"`
}

// extKeyUsageDetail wraps a single AWS extended key usage string.
type extKeyUsageDetail struct {
	Name string `json:"Name"`
}

type certificateOptions struct {
	CertificateTransparencyLoggingPreference string `json:"CertificateTransparencyLoggingPreference,omitempty"`
}

type describeCertificateOutput struct {
	Certificate certificateDetail `json:"Certificate"`
}

type certificateSummary struct {
	IssuedAt                        *int64   `json:"IssuedAt,omitempty"`
	ImportedAt                      *int64   `json:"ImportedAt,omitempty"`
	NotBefore                       *int64   `json:"NotBefore,omitempty"`
	NotAfter                        *int64   `json:"NotAfter,omitempty"`
	CertificateArn                  string   `json:"CertificateArn"`
	DomainName                      string   `json:"DomainName"`
	Status                          string   `json:"Status,omitempty"`
	KeyAlgorithm                    string   `json:"KeyAlgorithm,omitempty"`
	RenewalEligibility              string   `json:"RenewalEligibility,omitempty"`
	Type                            string   `json:"Type,omitempty"`
	SubjectAlternativeNameSummaries []string `json:"SubjectAlternativeNameSummaries,omitempty"`
}

// listCertificatesIncludes mirrors the AWS Filters shape for ListCertificates.
type listCertificatesIncludes struct {
	KeyTypes         []string `json:"KeyTypes,omitempty"`
	ExtendedKeyUsage []string `json:"ExtendedKeyUsage,omitempty"`
	KeyUsage         []string `json:"KeyUsage,omitempty"`
}

type listCertificatesInput struct {
	Includes            *listCertificatesIncludes `json:"Includes,omitempty"`
	NextToken           string                    `json:"NextToken"`
	SortBy              string                    `json:"SortBy,omitempty"`
	SortOrder           string                    `json:"SortOrder,omitempty"`
	CertificateStatuses []string                  `json:"CertificateStatuses,omitempty"`
	MaxItems            int                       `json:"MaxItems"`
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

type expiryEventsConfiguration struct {
	DaysBeforeExpiry *int32 `json:"DaysBeforeExpiry,omitempty"`
}

type getAccountConfigurationOutput struct {
	ExpiryEvents *expiryEventsConfiguration `json:"ExpiryEvents,omitempty"`
}

type putAccountConfigurationInput struct {
	ExpiryEvents     *expiryEventsConfiguration `json:"ExpiryEvents,omitempty"`
	IdempotencyToken string                     `json:"IdempotencyToken"`
}

type putAccountConfigurationOutput struct{}

type resendValidationEmailInput struct {
	CertificateArn   string `json:"CertificateArn"`
	Domain           string `json:"Domain"`
	ValidationDomain string `json:"ValidationDomain"`
}

type resendValidationEmailOutput struct{}

type revokeCertificateInput struct {
	CertificateArn   string `json:"CertificateArn"`
	RevocationReason string `json:"RevocationReason"`
}

type revokeCertificateOutput struct{}

type certificateOptionsInput struct {
	CertificateTransparencyLoggingPreference string `json:"CertificateTransparencyLoggingPreference"`
}

type updateCertificateOptionsInput struct {
	CertificateArn string                  `json:"CertificateArn"`
	Options        certificateOptionsInput `json:"Options"`
}

type updateCertificateOptionsOutput struct{}

// Handler is the Echo HTTP handler for ACM operations.
type Handler struct {
	Backend       *InMemoryBackend
	tags          map[string]*svcTags.Tags
	tagsMu        *lockmetrics.RWMutex
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new ACM handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("acm.tags"),
	}
}

// StartWorker starts the background janitor for idempotency tokens.
func (h *Handler) StartWorker(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	h.janitorCancel = cancel
	h.janitorDone = done

	go func() {
		defer close(done)
		// Run janitor every hour.
		h.Backend.RunJanitor(runCtx, time.Hour)
	}()

	return nil
}

// Shutdown stops the background janitor and all in-flight certificate
// auto-validation timers so no goroutine outlives the service.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.janitorCancel != nil {
		h.janitorCancel()
	}

	if h.janitorDone != nil {
		select {
		case <-h.janitorDone:
		case <-ctx.Done():
		}
	}

	h.Backend.Close()
}

var (
	_ service.BackgroundWorker = (*Handler)(nil)
	_ service.Shutdowner       = (*Handler)(nil)
)

func (h *Handler) setTags(resourceID string, kv map[string]string) error {
	const maxTagKeyLength = 128
	const maxTagValueLength = 256
	for k, v := range kv {
		if len(k) > maxTagKeyLength {
			return fmt.Errorf("%w: tag key exceeds 128 characters", ErrInvalidParameter)
		}
		if len(v) > maxTagValueLength {
			return fmt.Errorf("%w: tag value exceeds 256 characters", ErrInvalidParameter)
		}
	}

	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()

	const maxTagsPerCertificate = 50
	newKeys := 0
	if h.tags[resourceID] != nil {
		for k := range kv {
			if !h.tags[resourceID].HasTag(k) {
				newKeys++
			}
		}
		if h.tags[resourceID].Len()+newKeys > maxTagsPerCertificate {
			return fmt.Errorf("%w: maximum of 50 tags allowed", ErrInvalidParameter)
		}
	} else if len(kv) > maxTagsPerCertificate {
		return fmt.Errorf("%w: maximum of 50 tags allowed", ErrInvalidParameter)
	}

	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("acm." + resourceID + ".tags")
	}
	h.tags[resourceID].Merge(kv)

	return nil
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock("removeTags")
	defer h.tagsMu.Unlock()
	if t := h.tags[resourceID]; t != nil {
		t.DeleteKeys(keys)
	}
}

func (h *Handler) cleanupTags(resourceID string) {
	h.tagsMu.Lock("cleanupTags")
	defer h.tagsMu.Unlock()
	if t, ok := h.tags[resourceID]; ok {
		t.Close()
		delete(h.tags, resourceID)
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
		"GetAccountConfiguration",
		"PutAccountConfiguration",
		"ResendValidationEmail",
		"RevokeCertificate",
		"UpdateCertificateOptions",
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
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ACM", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(_ context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(ctx, action, body)
			},
			h.handleError,
		)
	}
}

// dispatch routes the operation to the appropriate handler and marshals the response.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	resp, err := h.dispatchJSON(ctx, action, body)
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

// acmDispatchTable maps ACM action names to their JSON handler functions.
//
//nolint:gochecknoglobals // read-only dispatch table initialized once at startup
var acmDispatchTable = map[string]func(*Handler, context.Context, []byte) (any, error){
	"RequestCertificate":        (*Handler).jsonRequestCertificate,
	"DescribeCertificate":       (*Handler).jsonDescribeCertificate,
	"ListCertificates":          (*Handler).jsonListCertificates,
	"DeleteCertificate":         (*Handler).jsonDeleteCertificate,
	"ListTagsForCertificate":    (*Handler).jsonListTagsForCertificate,
	"AddTagsToCertificate":      (*Handler).jsonAddTagsToCertificate,
	"RemoveTagsFromCertificate": (*Handler).jsonRemoveTagsFromCertificate,
	"ImportCertificate":         (*Handler).jsonImportCertificate,
	"RenewCertificate":          (*Handler).jsonRenewCertificate,
	"ExportCertificate":         (*Handler).jsonExportCertificate,
	"GetCertificate":            (*Handler).jsonGetCertificate,
	"GetAccountConfiguration":   (*Handler).jsonGetAccountConfiguration,
	"PutAccountConfiguration":   (*Handler).jsonPutAccountConfiguration,
	"ResendValidationEmail":     (*Handler).jsonResendValidationEmail,
	"RevokeCertificate":         (*Handler).jsonRevokeCertificate,
	"UpdateCertificateOptions":  (*Handler).jsonUpdateCertificateOptions,
}

// dispatchJSON routes a JSON-protocol ACM action to the appropriate handler.
func (h *Handler) dispatchJSON(ctx context.Context, action string, body []byte) (any, error) {
	if fn, ok := acmDispatchTable[action]; ok {
		return fn(h, ctx, body)
	}

	return nil, errUnknownACMAction
}

func (h *Handler) jsonRequestCertificate(ctx context.Context, body []byte) (any, error) {
	var input requestCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	certType := ""
	if input.CertificateAuthorityArn != "" {
		certType = "PRIVATE"
	}
	opts := ""
	if input.Options != nil {
		opts = input.Options.CertificateTransparencyLoggingPreference
	}

	cert, err := h.Backend.RequestCertificate(
		ctx,
		input.DomainName,
		certType,
		input.ValidationMethod,
		input.IdempotencyToken,
		input.KeyAlgorithm,
		input.CertificateAuthorityArn,
		opts,
		input.SubjectAlternativeNames,
	)
	if err != nil {
		return nil, err
	}

	// If tags were provided, apply them immediately after creating the certificate.
	if len(input.Tags) > 0 {
		kvMap := make(map[string]string, len(input.Tags))

		for _, tag := range input.Tags {
			if k, ok := tag["Key"]; ok {
				kvMap[k] = tag["Value"]
			}
		}
		if setErr := h.setTags(cert.ARN, kvMap); setErr != nil {
			return nil, setErr
		}
	}

	return &requestCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonDescribeCertificate(ctx context.Context, body []byte) (any, error) {
	var input describeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.DescribeCertificate(ctx, input.CertificateArn)
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

		if len(dvo.ValidationEmails) > 0 {
			opt.ValidationEmails = dvo.ValidationEmails
		}

		dvoList = append(dvoList, opt)
	}

	keyUsages := make([]keyUsageDetail, 0, len(cert.KeyUsage))
	for _, ku := range cert.KeyUsage {
		keyUsages = append(keyUsages, keyUsageDetail{Name: ku})
	}

	extKeyUsages := make([]extKeyUsageDetail, 0, len(cert.ExtendedKeyUsage))
	for _, eku := range cert.ExtendedKeyUsage {
		extKeyUsages = append(extKeyUsages, extKeyUsageDetail{Name: eku})
	}

	detail := certificateDetail{
		CertificateArn:          cert.ARN,
		DomainName:              cert.DomainName,
		Serial:                  cert.Serial,
		Subject:                 cert.Subject,
		Issuer:                  cert.Issuer,
		KeyAlgorithm:            cert.KeyAlgorithm,
		SignatureAlgorithm:      cert.SignatureAlgorithm,
		Status:                  cert.Status,
		Type:                    cert.Type,
		RenewalEligibility:      cert.RenewalEligibility,
		RevocationReason:        cert.RevocationReason,
		FailureReason:           cert.FailureReason,
		CertificateAuthorityArn: cert.CertificateAuthorityArn,
		KeyID:                   cert.KeyID,
		CreatedAt:               cert.CreatedAt.Unix(),
		NotBefore:               cert.NotBefore.Unix(),
		NotAfter:                cert.NotAfter.Unix(),
		SubjectAlternativeNames: cert.SubjectAlternativeNames,
		DomainValidationOptions: dvoList,
		Options:                 describeCertOptions(cert),
		RevokedAt:               certTimeUnix(cert.RevokedAt),
		IssuedAt:                certTimeUnix(cert.IssuedAt),
		ImportedAt:              certTimeUnix(cert.ImportedAt),
		InUseBy:                 cert.InUseBy,
		KeyUsage:                keyUsages,
		ExtendedKeyUsage:        extKeyUsages,
	}

	if cert.RenewalSummary != nil {
		rsDVOs := make([]domainValidationOption, 0, len(cert.RenewalSummary.DomainValidationOptions))
		for _, dvo := range cert.RenewalSummary.DomainValidationOptions {
			opt := domainValidationOption{
				DomainName:       dvo.DomainName,
				ValidationDomain: dvo.ValidationDomain,
				ValidationStatus: dvo.ValidationStatus,
				ValidationMethod: dvo.ValidationMethod,
			}
			if dvo.ResourceRecord != nil {
				rr := *dvo.ResourceRecord
				opt.ResourceRecord = &resourceRecord{Name: rr.Name, Type: rr.Type, Value: rr.Value}
			}

			rsDVOs = append(rsDVOs, opt)
		}

		detail.RenewalSummary = &renewalSummaryDetail{
			RenewalStatus:           cert.RenewalSummary.RenewalStatus,
			DomainValidationOptions: rsDVOs,
		}
	}

	return &describeCertificateOutput{Certificate: detail}, nil
}

// jsonListCertificates handles the ListCertificates operation.
func (h *Handler) jsonListCertificates(ctx context.Context, body []byte) (any, error) {
	var input listCertificatesInput
	_ = json.Unmarshal(body, &input)

	params := ListCertificatesParams{
		NextToken:    input.NextToken,
		MaxItems:     input.MaxItems,
		StatusFilter: input.CertificateStatuses,
		SortBy:       input.SortBy,
		SortOrder:    input.SortOrder,
	}

	if input.Includes != nil {
		params.KeyTypes = input.Includes.KeyTypes
		params.KeyUsage = input.Includes.KeyUsage
		params.ExtendedKeyUsage = input.Includes.ExtendedKeyUsage
	}

	p, err := h.Backend.ListCertificates(ctx, params)
	if err != nil {
		return nil, err
	}

	summaries := make([]certificateSummary, 0, len(p.Data))

	for _, c := range p.Data {
		summary := certificateSummary{
			CertificateArn:                  c.ARN,
			DomainName:                      c.DomainName,
			Status:                          c.Status,
			KeyAlgorithm:                    c.KeyAlgorithm,
			RenewalEligibility:              c.RenewalEligibility,
			Type:                            c.Type,
			IssuedAt:                        certTimeUnix(c.IssuedAt),
			ImportedAt:                      certTimeUnix(c.ImportedAt),
			SubjectAlternativeNameSummaries: c.SubjectAlternativeNames,
		}

		if !c.NotBefore.IsZero() {
			ts := c.NotBefore.Unix()
			summary.NotBefore = &ts
		}

		if !c.NotAfter.IsZero() {
			ts := c.NotAfter.Unix()
			summary.NotAfter = &ts
		}

		summaries = append(summaries, summary)
	}

	return &listCertificatesOutput{CertificateSummaryList: summaries, NextToken: p.Next}, nil
}

func (h *Handler) jsonDeleteCertificate(ctx context.Context, body []byte) (any, error) {
	var input deleteCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	if err := h.Backend.DeleteCertificate(ctx, input.CertificateArn); err != nil {
		return nil, err
	}
	h.cleanupTags(input.CertificateArn)

	return &deleteCertificateOutput{}, nil
}

func (h *Handler) jsonListTagsForCertificate(ctx context.Context, body []byte) (any, error) {
	var input listTagsForCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if !h.Backend.CertExists(ctx, input.CertificateArn) {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, input.CertificateArn)
	}

	return &listTagsForCertificateOutput{Tags: h.getTags(input.CertificateArn)}, nil
}

func (h *Handler) jsonAddTagsToCertificate(ctx context.Context, body []byte) (any, error) {
	var input addTagsToCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if !h.Backend.CertExists(ctx, input.CertificateArn) {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, input.CertificateArn)
	}

	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}
	if err := h.setTags(input.CertificateArn, kv); err != nil {
		return nil, err
	}

	return &addTagsToCertificateOutput{}, nil
}

func (h *Handler) jsonRemoveTagsFromCertificate(ctx context.Context, body []byte) (any, error) {
	var input removeTagsFromCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if !h.Backend.CertExists(ctx, input.CertificateArn) {
		return nil, fmt.Errorf("%w: certificate %s not found", ErrCertNotFound, input.CertificateArn)
	}

	keys := make([]string, 0, len(input.Tags))
	for _, t := range input.Tags {
		keys = append(keys, t.Key)
	}
	h.removeTags(input.CertificateArn, keys)

	return &removeTagsFromCertificateOutput{}, nil
}

func (h *Handler) jsonImportCertificate(ctx context.Context, body []byte) (any, error) {
	var input importCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	cert, err := h.Backend.ImportCertificate(
		ctx,
		input.Certificate,
		input.PrivateKey,
		input.CertificateChain,
		input.CertificateArn,
	)
	if err != nil {
		return nil, err
	}

	return &importCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonRenewCertificate(ctx context.Context, body []byte) (any, error) {
	var input renewCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	if err := h.Backend.RenewCertificate(ctx, input.CertificateArn); err != nil {
		return nil, err
	}

	return &renewCertificateOutput{}, nil
}

func (h *Handler) jsonExportCertificate(ctx context.Context, body []byte) (any, error) {
	var input exportCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if input.Passphrase == "" {
		return nil, fmt.Errorf("%w: Passphrase is required for ExportCertificate", ErrInvalidParameter)
	}

	passphrase, decErr := base64.StdEncoding.DecodeString(input.Passphrase)
	if decErr != nil {
		// Try raw (unpadded) base64.
		passphrase, decErr = base64.RawStdEncoding.DecodeString(input.Passphrase)
		if decErr != nil {
			return nil, fmt.Errorf("%w: Passphrase must be base64-encoded", ErrInvalidParameter)
		}
	}

	cert, err := h.Backend.ExportCertificate(ctx, input.CertificateArn, passphrase)
	if err != nil {
		return nil, err
	}

	return &exportCertificateOutput{
		Certificate:      cert.CertificateBody,
		CertificateChain: cert.CertificateChain,
		PrivateKey:       cert.PrivateKey,
	}, nil
}

func (h *Handler) jsonGetCertificate(ctx context.Context, body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}
	certBody, certChain, err := h.Backend.GetCertificate(ctx, input.CertificateArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateOutput{
		Certificate:      certBody,
		CertificateChain: certChain,
	}, nil
}

// jsonGetAccountConfiguration handles the GetAccountConfiguration operation.
func (h *Handler) jsonGetAccountConfiguration(ctx context.Context, _ []byte) (any, error) {
	cfg := h.Backend.GetAccountConfiguration(ctx)
	days := cfg.DaysBeforeExpiry

	return &getAccountConfigurationOutput{
		ExpiryEvents: &expiryEventsConfiguration{DaysBeforeExpiry: &days},
	}, nil
}

func (h *Handler) jsonPutAccountConfiguration(ctx context.Context, body []byte) (any, error) {
	var input putAccountConfigurationInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	var days *int32
	if input.ExpiryEvents != nil {
		days = input.ExpiryEvents.DaysBeforeExpiry
	}

	if err := h.Backend.PutAccountConfiguration(ctx, input.IdempotencyToken, days); err != nil {
		return nil, err
	}

	return &putAccountConfigurationOutput{}, nil
}

func (h *Handler) jsonResendValidationEmail(ctx context.Context, body []byte) (any, error) {
	var input resendValidationEmailInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	err := h.Backend.ResendValidationEmail(ctx, input.CertificateArn, input.Domain, input.ValidationDomain)
	if err != nil {
		return nil, err
	}

	return &resendValidationEmailOutput{}, nil
}

func (h *Handler) jsonRevokeCertificate(ctx context.Context, body []byte) (any, error) {
	var input revokeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RevokeCertificate(ctx, input.CertificateArn, input.RevocationReason); err != nil {
		return nil, err
	}

	return &revokeCertificateOutput{}, nil
}

func (h *Handler) jsonUpdateCertificateOptions(ctx context.Context, body []byte) (any, error) {
	var input updateCertificateOptionsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.UpdateCertificateOptions(
		ctx,
		input.CertificateArn,
		input.Options.CertificateTransparencyLoggingPreference,
	); err != nil {
		return nil, err
	}

	return &updateCertificateOptionsOutput{}, nil
}

// describeCertOptions builds the Options response field if the cert has a transparency preference set.
func describeCertOptions(cert *Certificate) *certificateOptions {
	if cert.CertificateTransparencyLoggingPref == "" {
		return nil
	}

	return &certificateOptions{
		CertificateTransparencyLoggingPreference: cert.CertificateTransparencyLoggingPref,
	}
}

// certTimeUnix returns the Unix timestamp of a [time.Time] pointer, or nil if nil.
func certTimeUnix(t *time.Time) *int64 {
	if t == nil {
		return nil
	}

	ts := t.Unix()

	return &ts
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
		code = "RequestInProgressException"
	case errors.Is(opErr, ErrRequestInProgress):
		code = "RequestInProgressException"
	case errors.Is(opErr, ErrInvalidState):
		code = "InvalidStateException"
	case errors.Is(opErr, ErrResourceInUse):
		code = "ResourceInUseException"
	case errors.Is(opErr, ErrAlreadyRevoked):
		code = "InvalidStateException"
	case errors.Is(opErr, ErrConflict):
		code = "ConflictException"
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

// Reset clears all handler tag state and delegates to the backend Reset.
func (h *Handler) Reset() {
	h.tagsMu.Lock("Reset")
	for _, t := range h.tags {
		t.Close()
	}
	h.tags = make(map[string]*svcTags.Tags)
	h.tagsMu.Unlock()

	h.Backend.Reset()
}
