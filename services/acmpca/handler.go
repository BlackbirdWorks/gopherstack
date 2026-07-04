package acmpca

import (
	"context"
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
	acmpcaMatchPriority = 82
	acmpcaTargetPrefix  = "ACMPrivateCA."
	daysPerYear         = 365
	daysPerMonth        = 30
	hoursPerDay         = 24
)

// Handler is the Echo HTTP handler for ACM PCA operations.
type Handler struct {
	Backend *InMemoryBackend
	tags    map[string]*svcTags.Tags
	tagsMu  *lockmetrics.RWMutex
}

// NewHandler creates a new ACM PCA handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{
		Backend: backend,
		tags:    make(map[string]*svcTags.Tags),
		tagsMu:  lockmetrics.New("acmpca.tags"),
	}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock("setTags")
	defer h.tagsMu.Unlock()

	if h.tags[resourceID] == nil {
		h.tags[resourceID] = svcTags.New("acmpca." + resourceID + ".tags")
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

// SetTagsForTest is a test helper that sets tags for a resource by ARN.
func (h *Handler) SetTagsForTest(resourceID string, kv map[string]string) {
	h.setTags(resourceID, kv)
}

// GetTagsForTest is a test helper that returns all tags for a resource by ARN.
func (h *Handler) GetTagsForTest(resourceID string) []map[string]string {
	return h.getTags(resourceID)
}

// Name returns the service name.
func (h *Handler) Name() string { return "ACMPCA" }

// GetSupportedOperations returns the list of supported ACM PCA operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCertificateAuthority",
		"CreateCertificateAuthorityAuditReport",
		"CreatePermission",
		"DeleteCertificateAuthority",
		"DeletePermission",
		"DeletePolicy",
		"DescribeCertificateAuthority",
		"DescribeCertificateAuthorityAuditReport",
		"GetCertificate",
		"GetCertificateAuthorityCertificate",
		"GetCertificateAuthorityCsr",
		"GetPolicy",
		"ImportCertificateAuthorityCertificate",
		"IssueCertificate",
		"ListCertificateAuthorities",
		"ListPermissions",
		"ListTags",
		"ListTagsForCertificateAuthority",
		"PutPolicy",
		"RevokeCertificate",
		"RestoreCertificateAuthority",
		"TagCertificateAuthority",
		"UntagCertificateAuthority",
		"UpdateCertificateAuthority",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "acm-pca" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ACM PCA instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches ACM PCA JSON-protocol requests.
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

		return strings.HasPrefix(target, acmpcaTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return acmpcaMatchPriority }

// ExtractOperation extracts the ACM PCA action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, acmpcaTargetPrefix)
}

// ExtractResource returns the primary ARN from the JSON body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}

	return extractFirstResourceByKeys(m, "CertificateAuthorityArn", "CertificateArn", "ResourceArn")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())
		ctx := context.WithValue(c.Request().Context(), regionContextKey{}, region)

		return service.HandleTarget(
			c, logger.Load(ctx),
			"ACMPCA", "application/x-amz-json-1.1",
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
	if errors.Is(reqErr, errUnknownACMPCAAction) {
		return h.writeJSONError(c, http.StatusBadRequest, "InvalidAction",
			action+" is not a valid ACM PCA action")
	}

	return h.handleOpError(c, action, reqErr)
}

var errUnknownACMPCAAction = errors.New("unknown ACM PCA action")

// ---- request / response types ----

type caConfigSubjectInput struct {
	CommonName         string `json:"CommonName"`
	Country            string `json:"Country"`
	Organization       string `json:"Organization"`
	OrganizationalUnit string `json:"OrganizationalUnit"`
	State              string `json:"State"`
	Locality           string `json:"Locality"`
}

type caConfigInput struct {
	Subject          caConfigSubjectInput `json:"Subject"`
	KeyAlgorithm     string               `json:"KeyAlgorithm"`
	SigningAlgorithm string               `json:"SigningAlgorithm"`
}

type createCertificateAuthorityInput struct {
	CertificateAuthorityConfiguration caConfigInput `json:"CertificateAuthorityConfiguration"`
	CertificateAuthorityType          string        `json:"CertificateAuthorityType"`
	Tags                              []svcTags.KV  `json:"Tags"`
}

type createCertificateAuthorityOutput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type describeCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type caConfigSubjectOutput struct {
	CommonName         string `json:"CommonName,omitempty"`
	Country            string `json:"Country,omitempty"`
	Organization       string `json:"Organization,omitempty"`
	OrganizationalUnit string `json:"OrganizationalUnit,omitempty"`
	State              string `json:"State,omitempty"`
	Locality           string `json:"Locality,omitempty"`
}

type caConfigOutput struct {
	Subject          caConfigSubjectOutput `json:"Subject"`
	KeyAlgorithm     string                `json:"KeyAlgorithm"`
	SigningAlgorithm string                `json:"SigningAlgorithm"`
}

type revocationConfigOutput struct{}

type certAuthorityOutput struct {
	CertificateAuthorityConfiguration caConfigOutput         `json:"CertificateAuthorityConfiguration"`
	RevocationConfiguration           revocationConfigOutput `json:"RevocationConfiguration"`
	Arn                               string                 `json:"Arn"`
	OwnerAccount                      string                 `json:"OwnerAccount,omitempty"`
	Type                              string                 `json:"Type"`
	Status                            string                 `json:"Status"`
	Serial                            string                 `json:"Serial,omitempty"`
	CreatedAt                         int64                  `json:"CreatedAt"`
	NotBefore                         int64                  `json:"NotBefore,omitempty"`
	NotAfter                          int64                  `json:"NotAfter,omitempty"`
}

type describeCertificateAuthorityOutput struct {
	CertificateAuthority certAuthorityOutput `json:"CertificateAuthority"`
}

type listCertificateAuthoritiesInput struct {
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listCertificateAuthoritiesOutput struct {
	NextToken              string                `json:"NextToken,omitempty"`
	CertificateAuthorities []certAuthorityOutput `json:"CertificateAuthorities"`
}

type deleteCertificateAuthorityInput struct {
	CertificateAuthorityArn     string `json:"CertificateAuthorityArn"`
	PermanentDeletionTimeInDays int32  `json:"PermanentDeletionTimeInDays"`
}

type deleteCertificateAuthorityOutput struct{}

type updateCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Status                  string `json:"Status"`
}

type updateCertificateAuthorityOutput struct{}

type getCertificateAuthorityCsrInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type getCertificateAuthorityCsrOutput struct {
	Csr string `json:"Csr"`
}

type importCertificateAuthorityCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Certificate             string `json:"Certificate"`
	CertificateChain        string `json:"CertificateChain"`
}

type importCertificateAuthorityCertificateOutput struct{}

type getCertificateAuthorityCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type getCertificateAuthorityCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

type validityInput struct {
	Type  string `json:"Type"`
	Value int64  `json:"Value"`
}

type issueCertificateInput struct {
	CertificateAuthorityArn string        `json:"CertificateAuthorityArn"`
	Csr                     string        `json:"Csr"`
	SigningAlgorithm        string        `json:"SigningAlgorithm"`
	Validity                validityInput `json:"Validity"`
}

type issueCertificateOutput struct {
	CertificateArn string `json:"CertificateArn"`
}

type getCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateArn          string `json:"CertificateArn"`
}

type getCertificateOutput struct {
	Certificate      string `json:"Certificate"`
	CertificateChain string `json:"CertificateChain,omitempty"`
}

type revokeCertificateInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	CertificateSerial       string `json:"CertificateSerial"`
	RevocationReason        string `json:"RevocationReason"`
}

type revokeCertificateOutput struct{}

type listPermissionsInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	NextToken               string `json:"NextToken"`
	MaxResults              int    `json:"MaxResults"`
}

type permissionOutput struct {
	CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
	Policy                  string   `json:"Policy,omitempty"`
	Principal               string   `json:"Principal"`
	SourceAccount           string   `json:"SourceAccount,omitempty"`
	Actions                 []string `json:"Actions"`
	CreatedAt               int64    `json:"CreatedAt,omitempty"`
}

type listPermissionsOutput struct {
	NextToken   string             `json:"NextToken,omitempty"`
	Permissions []permissionOutput `json:"Permissions"`
}

type createPermissionInput struct {
	CertificateAuthorityArn string   `json:"CertificateAuthorityArn"`
	Principal               string   `json:"Principal"`
	SourceAccount           string   `json:"SourceAccount"`
	Actions                 []string `json:"Actions"`
}

type createPermissionOutput struct{}

type deletePermissionInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
	Principal               string `json:"Principal"`
	SourceAccount           string `json:"SourceAccount"`
}

type deletePermissionOutput struct{}

type createCertificateAuthorityAuditReportInput struct {
	AuditReportResponseFormat string `json:"AuditReportResponseFormat"`
	CertificateAuthorityArn   string `json:"CertificateAuthorityArn"`
	S3BucketName              string `json:"S3BucketName"`
}

type createCertificateAuthorityAuditReportOutput struct {
	AuditReportID string `json:"AuditReportId,omitempty"`
	S3Key         string `json:"S3Key,omitempty"`
}

type describeCertificateAuthorityAuditReportInput struct {
	AuditReportID           string `json:"AuditReportId"`
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type describeCertificateAuthorityAuditReportOutput struct {
	AuditReportStatus string `json:"AuditReportStatus,omitempty"`
	S3BucketName      string `json:"S3BucketName,omitempty"`
	S3Key             string `json:"S3Key,omitempty"`
	CreatedAt         int64  `json:"CreatedAt,omitempty"`
}

type getPolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type getPolicyOutput struct {
	Policy string `json:"Policy,omitempty"`
}

type putPolicyInput struct {
	Policy      string `json:"Policy"`
	ResourceArn string `json:"ResourceArn"`
}

type putPolicyOutput struct{}

type deletePolicyInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type deletePolicyOutput struct{}

type restoreCertificateAuthorityInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type restoreCertificateAuthorityOutput struct{}

type tagCertificateAuthorityInput struct {
	CertificateAuthorityArn string       `json:"CertificateAuthorityArn"`
	Tags                    []svcTags.KV `json:"Tags"`
}

type tagCertificateAuthorityOutput struct{}

type acmpcaTagKey struct {
	Key string `json:"Key"`
}

type untagCertificateAuthorityInput struct {
	CertificateAuthorityArn string         `json:"CertificateAuthorityArn"`
	Tags                    []acmpcaTagKey `json:"Tags"`
}

type untagCertificateAuthorityOutput struct{}

type listTagsInput struct {
	CertificateAuthorityArn string `json:"CertificateAuthorityArn"`
}

type listTagsOutput struct {
	Tags []map[string]string `json:"Tags"`
}

// ---- dispatch ----

func (h *Handler) dispatchJSON(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateCertificateAuthority":
		return h.jsonCreateCA(ctx, body)
	case "DescribeCertificateAuthority":
		return h.jsonDescribeCA(ctx, body)
	case "ListCertificateAuthorities":
		return h.jsonListCAs(ctx, body)
	case "DeleteCertificateAuthority":
		return h.jsonDeleteCA(ctx, body)
	case "UpdateCertificateAuthority":
		return h.jsonUpdateCA(ctx, body)
	case "GetCertificateAuthorityCsr":
		return h.jsonGetCsr(ctx, body)
	case "ImportCertificateAuthorityCertificate":
		return h.jsonImportCACert(ctx, body)
	case "GetCertificateAuthorityCertificate":
		return h.jsonGetCACert(ctx, body)
	default:
		return h.dispatchCertAndTagOps(ctx, action, body)
	}
}

func (h *Handler) dispatchCertAndTagOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "IssueCertificate":
		return h.jsonIssueCert(ctx, body)
	case "GetCertificate":
		return h.jsonGetCert(ctx, body)
	case "RevokeCertificate":
		return h.jsonRevokeCert(ctx, body)
	case "ListPermissions":
		return h.jsonListPermissions(ctx, body)
	case "TagCertificateAuthority":
		return h.jsonTagCA(ctx, body)
	case "UntagCertificateAuthority":
		return h.jsonUntagCA(ctx, body)
	case "ListTagsForCertificateAuthority", "ListTags":
		return h.jsonListTags(ctx, body)
	default:
		return h.dispatchPermissionAndAuditOps(ctx, action, body)
	}
}

func (h *Handler) dispatchPermissionAndAuditOps(ctx context.Context, action string, body []byte) (any, error) {
	switch action {
	case "CreateCertificateAuthorityAuditReport":
		return h.jsonCreateAuditReport(ctx, body)
	case "CreatePermission":
		return h.jsonCreatePermission(ctx, body)
	case "DeletePermission":
		return h.jsonDeletePermission(ctx, body)
	case "DeletePolicy":
		return h.jsonDeletePolicy(ctx, body)
	case "DescribeCertificateAuthorityAuditReport":
		return h.jsonDescribeAuditReport(ctx, body)
	case "GetPolicy":
		return h.jsonGetPolicy(ctx, body)
	case "PutPolicy":
		return h.jsonPutPolicy(ctx, body)
	case "RestoreCertificateAuthority":
		return h.jsonRestoreCA(ctx, body)
	default:
		return nil, errUnknownACMPCAAction
	}
}

func (h *Handler) jsonCreateCA(ctx context.Context, body []byte) (any, error) {
	var input createCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	cfg := CertificateAuthorityConfiguration{
		Subject: CertificateAuthoritySubject{
			CommonName:         input.CertificateAuthorityConfiguration.Subject.CommonName,
			Country:            input.CertificateAuthorityConfiguration.Subject.Country,
			Organization:       input.CertificateAuthorityConfiguration.Subject.Organization,
			OrganizationalUnit: input.CertificateAuthorityConfiguration.Subject.OrganizationalUnit,
			State:              input.CertificateAuthorityConfiguration.Subject.State,
			Locality:           input.CertificateAuthorityConfiguration.Subject.Locality,
		},
		KeyAlgorithm:     input.CertificateAuthorityConfiguration.KeyAlgorithm,
		SigningAlgorithm: input.CertificateAuthorityConfiguration.SigningAlgorithm,
	}

	ca, err := h.Backend.CreateCertificateAuthority(ctx, input.CertificateAuthorityType, cfg)
	if err != nil {
		return nil, err
	}

	if len(input.Tags) > 0 {
		kv := make(map[string]string, len(input.Tags))
		for _, t := range input.Tags {
			kv[t.Key] = t.Value
		}

		h.setTags(ca.ARN, kv)
	}

	return &createCertificateAuthorityOutput{CertificateAuthorityArn: ca.ARN}, nil
}

func (h *Handler) jsonDescribeCA(ctx context.Context, body []byte) (any, error) {
	var input describeCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	ca, err := h.Backend.DescribeCertificateAuthority(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &describeCertificateAuthorityOutput{CertificateAuthority: toCAOutput(ca)}, nil
}

func (h *Handler) jsonListCAs(ctx context.Context, body []byte) (any, error) {
	var input listCertificateAuthoritiesInput
	_ = json.Unmarshal(body, &input)

	p := h.Backend.ListCertificateAuthorities(ctx, input.NextToken, input.MaxResults)
	cas := make([]certAuthorityOutput, 0, len(p.Data))

	for _, ca := range p.Data {
		cas = append(cas, toCAOutput(&ca))
	}

	return &listCertificateAuthoritiesOutput{
		CertificateAuthorities: cas,
		NextToken:              p.Next,
	}, nil
}

func (h *Handler) jsonDeleteCA(ctx context.Context, body []byte) (any, error) {
	var input deleteCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeleteCertificateAuthority(
		ctx,
		input.CertificateAuthorityArn,
		input.PermanentDeletionTimeInDays,
	); err != nil {
		return nil, err
	}

	h.cleanupTags(input.CertificateAuthorityArn)

	return &deleteCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonUpdateCA(ctx context.Context, body []byte) (any, error) {
	var input updateCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.UpdateCertificateAuthority(ctx, input.CertificateAuthorityArn, input.Status); err != nil {
		return nil, err
	}

	return &updateCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonGetCsr(ctx context.Context, body []byte) (any, error) {
	var input getCertificateAuthorityCsrInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	csr, err := h.Backend.GetCertificateAuthorityCsr(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateAuthorityCsrOutput{Csr: csr}, nil
}

func (h *Handler) jsonImportCACert(ctx context.Context, body []byte) (any, error) {
	var input importCertificateAuthorityCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.ImportCertificateAuthorityCertificate(
		ctx,
		input.CertificateAuthorityArn,
		input.Certificate,
		input.CertificateChain,
	); err != nil {
		return nil, err
	}

	return &importCertificateAuthorityCertificateOutput{}, nil
}

func (h *Handler) jsonGetCACert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateAuthorityCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	certPEM, chainPEM, err := h.Backend.GetCertificateAuthorityCertificate(ctx, input.CertificateAuthorityArn)
	if err != nil {
		return nil, err
	}

	return &getCertificateAuthorityCertificateOutput{Certificate: certPEM, CertificateChain: chainPEM}, nil
}

func (h *Handler) jsonIssueCert(ctx context.Context, body []byte) (any, error) {
	var input issueCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	var days int
	switch input.Validity.Type {
	case "YEARS":
		days = int(input.Validity.Value) * daysPerYear
	case "MONTHS":
		days = int(input.Validity.Value) * daysPerMonth
	case "DAYS", "":
		days = int(input.Validity.Value)
	case "END_DATE", "ABSOLUTE":
		endDate := time.Unix(input.Validity.Value, 0)
		days = int(time.Until(endDate).Hours() / hoursPerDay)
		if days <= 0 {
			days = 1
		}
	default:
		return nil, fmt.Errorf("%w: unsupported Validity.Type %q (must be DAYS, MONTHS, YEARS, or END_DATE)",
			ErrInvalidParameter, input.Validity.Type)
	}

	cert, err := h.Backend.IssueCertificate(ctx, input.CertificateAuthorityArn, input.Csr, days)
	if err != nil {
		return nil, err
	}

	return &issueCertificateOutput{CertificateArn: cert.ARN}, nil
}

func (h *Handler) jsonGetCert(ctx context.Context, body []byte) (any, error) {
	var input getCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	cert, err := h.Backend.GetCertificate(ctx, input.CertificateAuthorityArn, input.CertificateArn)
	if err != nil {
		return nil, err
	}

	caChain := ""
	if certPEM, chainPEM, chainErr := h.Backend.GetCertificateAuthorityCertificate(
		ctx,
		input.CertificateAuthorityArn,
	); chainErr == nil && certPEM != "" {
		caChain = certPEM
		if chainPEM != "" {
			caChain = certPEM + chainPEM
		}
	}

	return &getCertificateOutput{Certificate: cert.CertBody, CertificateChain: caChain}, nil
}

func (h *Handler) jsonRevokeCert(ctx context.Context, body []byte) (any, error) {
	var input revokeCertificateInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RevokeCertificate(
		ctx,
		input.CertificateAuthorityArn,
		input.CertificateSerial,
		input.RevocationReason,
	); err != nil {
		return nil, err
	}

	return &revokeCertificateOutput{}, nil
}

func (h *Handler) jsonListPermissions(ctx context.Context, body []byte) (any, error) {
	var input listPermissionsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	p, err := h.Backend.ListPermissions(ctx, input.CertificateAuthorityArn, input.NextToken, input.MaxResults)
	if err != nil {
		return nil, err
	}

	permissions := make([]permissionOutput, 0, len(p.Data))
	for _, permission := range p.Data {
		out := permissionOutput{
			Actions:                 copyStringSlice(permission.Actions),
			CertificateAuthorityArn: permission.CertificateAuthorityArn,
			Policy:                  permission.Policy,
			Principal:               permission.Principal,
			SourceAccount:           permission.SourceAccount,
		}
		if !permission.CreatedAt.IsZero() {
			out.CreatedAt = permission.CreatedAt.Unix()
		}
		permissions = append(permissions, out)
	}

	return &listPermissionsOutput{
		NextToken:   p.Next,
		Permissions: permissions,
	}, nil
}

func (h *Handler) jsonCreatePermission(ctx context.Context, body []byte) (any, error) {
	var input createPermissionInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if _, err := h.Backend.CreatePermission(
		ctx,
		input.CertificateAuthorityArn,
		input.Principal,
		input.SourceAccount,
		input.Actions,
	); err != nil {
		return nil, err
	}

	return &createPermissionOutput{}, nil
}

func (h *Handler) jsonDeletePermission(ctx context.Context, body []byte) (any, error) {
	var input deletePermissionInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeletePermission(
		ctx,
		input.CertificateAuthorityArn,
		input.Principal,
		input.SourceAccount,
	); err != nil {
		return nil, err
	}

	return &deletePermissionOutput{}, nil
}

func (h *Handler) jsonCreateAuditReport(ctx context.Context, body []byte) (any, error) {
	var input createCertificateAuthorityAuditReportInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	report, err := h.Backend.CreateCertificateAuthorityAuditReport(
		ctx,
		input.CertificateAuthorityArn,
		input.S3BucketName,
		input.AuditReportResponseFormat,
	)
	if err != nil {
		return nil, err
	}

	return &createCertificateAuthorityAuditReportOutput{
		AuditReportID: report.AuditReportID,
		S3Key:         report.S3Key,
	}, nil
}

func (h *Handler) jsonDescribeAuditReport(ctx context.Context, body []byte) (any, error) {
	var input describeCertificateAuthorityAuditReportInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	report, err := h.Backend.DescribeCertificateAuthorityAuditReport(
		ctx,
		input.CertificateAuthorityArn,
		input.AuditReportID,
	)
	if err != nil {
		return nil, err
	}

	out := &describeCertificateAuthorityAuditReportOutput{
		AuditReportStatus: report.Status,
		S3BucketName:      report.S3BucketName,
		S3Key:             report.S3Key,
	}
	if !report.CreatedAt.IsZero() {
		out.CreatedAt = report.CreatedAt.Unix()
	}

	return out, nil
}

func (h *Handler) jsonGetPolicy(ctx context.Context, body []byte) (any, error) {
	var input getPolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	policy, err := h.Backend.GetPolicy(ctx, input.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getPolicyOutput{Policy: policy}, nil
}

func (h *Handler) jsonPutPolicy(ctx context.Context, body []byte) (any, error) {
	var input putPolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.PutPolicy(ctx, input.ResourceArn, input.Policy); err != nil {
		return nil, err
	}

	return &putPolicyOutput{}, nil
}

func (h *Handler) jsonDeletePolicy(ctx context.Context, body []byte) (any, error) {
	var input deletePolicyInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.DeletePolicy(ctx, input.ResourceArn); err != nil {
		return nil, err
	}

	return &deletePolicyOutput{}, nil
}

func (h *Handler) jsonRestoreCA(ctx context.Context, body []byte) (any, error) {
	var input restoreCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.RestoreCertificateAuthority(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	return &restoreCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonTagCA(ctx context.Context, body []byte) (any, error) {
	var input tagCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	kv := make(map[string]string, len(input.Tags))
	for _, t := range input.Tags {
		kv[t.Key] = t.Value
	}

	h.setTags(input.CertificateAuthorityArn, kv)

	return &tagCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonUntagCA(ctx context.Context, body []byte) (any, error) {
	var input untagCertificateAuthorityInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	keys := make([]string, 0, len(input.Tags))
	for _, t := range input.Tags {
		keys = append(keys, t.Key)
	}

	h.removeTags(input.CertificateAuthorityArn, keys)

	return &untagCertificateAuthorityOutput{}, nil
}

func (h *Handler) jsonListTags(ctx context.Context, body []byte) (any, error) {
	var input listTagsInput
	if err := json.Unmarshal(body, &input); err != nil {
		return nil, ErrInvalidParameter
	}

	if err := h.Backend.verifyCertificateAuthorityActive(ctx, input.CertificateAuthorityArn); err != nil {
		return nil, err
	}

	return &listTagsOutput{Tags: h.getTags(input.CertificateAuthorityArn)}, nil
}

// ---- error handling ----

func (h *Handler) handleOpError(c *echo.Context, action string, opErr error) error {
	statusCode := http.StatusBadRequest
	var code string

	switch {
	case errors.Is(opErr, ErrCANotFound), errors.Is(opErr, ErrCertNotFound),
		errors.Is(opErr, ErrPermissionNotFound), errors.Is(opErr, ErrPolicyNotFound),
		errors.Is(opErr, ErrAuditReportNotFound):
		code = "ResourceNotFoundException"
	case errors.Is(opErr, ErrInvalidParameter):
		code = "InvalidParameterException"
	case errors.Is(opErr, ErrInvalidState):
		code = "InvalidStateException"
	default:
		code = "InternalFailure"
		statusCode = http.StatusInternalServerError
		logger.Load(c.Request().Context()).Error("ACM PCA internal error", "error", opErr, "action", action)
	}

	return h.writeJSONError(c, statusCode, code, opErr.Error())
}

func (h *Handler) writeJSONError(c *echo.Context, statusCode int, code, message string) error {
	return c.JSON(statusCode, map[string]string{
		"__type":  code,
		"message": message,
	})
}

// ---- helpers ----

func toCAOutput(ca *CertificateAuthority) certAuthorityOutput {
	out := certAuthorityOutput{
		Arn:          ca.ARN,
		OwnerAccount: ca.OwnerAccount,
		Type:         ca.Type,
		Status:       ca.Status,
		Serial:       ca.Serial,
		CreatedAt:    ca.CreatedAt.Unix(),
		CertificateAuthorityConfiguration: caConfigOutput{
			Subject: caConfigSubjectOutput{
				CommonName:         ca.CertificateAuthorityConfiguration.Subject.CommonName,
				Country:            ca.CertificateAuthorityConfiguration.Subject.Country,
				Organization:       ca.CertificateAuthorityConfiguration.Subject.Organization,
				OrganizationalUnit: ca.CertificateAuthorityConfiguration.Subject.OrganizationalUnit,
				State:              ca.CertificateAuthorityConfiguration.Subject.State,
				Locality:           ca.CertificateAuthorityConfiguration.Subject.Locality,
			},
			KeyAlgorithm:     ca.CertificateAuthorityConfiguration.KeyAlgorithm,
			SigningAlgorithm: ca.CertificateAuthorityConfiguration.SigningAlgorithm,
		},
	}

	if !ca.NotBefore.IsZero() {
		out.NotBefore = ca.NotBefore.Unix()
	}

	if !ca.NotAfter.IsZero() {
		out.NotAfter = ca.NotAfter.Unix()
	}

	return out
}

func copyStringSlice(values []string) []string {
	return append([]string(nil), values...)
}

// extractFirstResourceByKeys returns the first string resource found for the provided JSON field names.
func extractFirstResourceByKeys(body map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if raw, ok := body[key]; ok {
			var resourceID string
			if err := json.Unmarshal(raw, &resourceID); err == nil {
				return resourceID
			}
		}
	}

	return ""
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
