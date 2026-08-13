package lightsail

import "context"

// distributionCertOps returns the dispatch table for family T+V (13 ops).
func (h *Handler) distributionCertOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateDistribution":                h.handleCreateDistribution,
		"UpdateDistribution":                h.handleUpdateDistribution,
		"DeleteDistribution":                h.handleDeleteDistribution,
		"GetDistributions":                  h.handleGetDistributions,
		"UpdateDistributionBundle":          h.handleUpdateDistributionBundle,
		"ResetDistributionCache":            h.handleResetDistributionCache,
		"GetDistributionLatestCacheReset":   h.handleGetDistributionLatestCacheReset,
		"GetDistributionMetricData":         h.handleGetDistributionMetricData,
		"AttachCertificateToDistribution":   h.handleAttachCertificateToDistribution,
		"DetachCertificateFromDistribution": h.handleDetachCertificateFromDistribution,
		"CreateCertificate":                 h.handleCreateCertificate,
		"DeleteCertificate":                 h.handleDeleteCertificate,
		"GetCertificates":                   h.handleGetCertificates,
	}
}

type originWire struct {
	Name           string `json:"name,omitempty"`
	RegionName     string `json:"regionName,omitempty"`
	ProtocolPolicy string `json:"protocolPolicy,omitempty"`
}

// cacheBehaviorWire mirrors types.CacheBehavior.
type cacheBehaviorWire struct {
	Behavior string `json:"behavior,omitempty"`
}

// cacheBehaviorPerPathWire mirrors types.CacheBehaviorPerPath.
type cacheBehaviorPerPathWire struct {
	Behavior string `json:"behavior,omitempty"`
	Path     string `json:"path,omitempty"`
}

// cookieObjectWire mirrors types.CookieObject.
type cookieObjectWire struct {
	Option           string   `json:"option,omitempty"`
	CookiesAllowList []string `json:"cookiesAllowList,omitempty"`
}

// headerObjectWire mirrors types.HeaderObject.
type headerObjectWire struct {
	Option           string   `json:"option,omitempty"`
	HeadersAllowList []string `json:"headersAllowList,omitempty"`
}

// queryStringObjectWire mirrors types.QueryStringObject. Option is *bool: a
// real client can omit it (default forwarding), send false, or send true.
type queryStringObjectWire struct {
	Option                *bool    `json:"option,omitempty"`
	QueryStringsAllowList []string `json:"queryStringsAllowList,omitempty"`
}

// cacheSettingsWire mirrors types.CacheSettings.
type cacheSettingsWire struct {
	ForwardedCookies      *cookieObjectWire      `json:"forwardedCookies,omitempty"`
	ForwardedHeaders      *headerObjectWire      `json:"forwardedHeaders,omitempty"`
	ForwardedQueryStrings *queryStringObjectWire `json:"forwardedQueryStrings,omitempty"`
	AllowedHTTPMethods    string                 `json:"allowedHTTPMethods,omitempty"`
	CachedHTTPMethods     string                 `json:"cachedHTTPMethods,omitempty"`
	DefaultTTL            int64                  `json:"defaultTTL,omitempty"`
	MaximumTTL            int64                  `json:"maximumTTL,omitempty"`
	MinimumTTL            int64                  `json:"minimumTTL,omitempty"`
}

func cacheSettingsFromWire(w *cacheSettingsWire) *CacheSettings {
	if w == nil {
		return nil
	}

	cs := &CacheSettings{
		AllowedHTTPMethods: w.AllowedHTTPMethods,
		CachedHTTPMethods:  w.CachedHTTPMethods,
		DefaultTTL:         w.DefaultTTL,
		MaximumTTL:         w.MaximumTTL,
		MinimumTTL:         w.MinimumTTL,
	}

	if w.ForwardedCookies != nil {
		cs.ForwardedCookies = &CookieObject{
			Option: w.ForwardedCookies.Option, CookiesAllowList: w.ForwardedCookies.CookiesAllowList,
		}
	}

	if w.ForwardedHeaders != nil {
		cs.ForwardedHeaders = &HeaderObject{
			Option: w.ForwardedHeaders.Option, HeadersAllowList: w.ForwardedHeaders.HeadersAllowList,
		}
	}

	if w.ForwardedQueryStrings != nil {
		cs.ForwardedQueryStrings = &QueryStringObject{
			Option:                w.ForwardedQueryStrings.Option,
			QueryStringsAllowList: w.ForwardedQueryStrings.QueryStringsAllowList,
		}
	}

	return cs
}

func cacheSettingsToWire(cs *CacheSettings) *cacheSettingsWire {
	if cs == nil {
		return nil
	}

	w := &cacheSettingsWire{
		AllowedHTTPMethods: cs.AllowedHTTPMethods,
		CachedHTTPMethods:  cs.CachedHTTPMethods,
		DefaultTTL:         cs.DefaultTTL,
		MaximumTTL:         cs.MaximumTTL,
		MinimumTTL:         cs.MinimumTTL,
	}

	if cs.ForwardedCookies != nil {
		w.ForwardedCookies = &cookieObjectWire{
			Option: cs.ForwardedCookies.Option, CookiesAllowList: cs.ForwardedCookies.CookiesAllowList,
		}
	}

	if cs.ForwardedHeaders != nil {
		w.ForwardedHeaders = &headerObjectWire{
			Option: cs.ForwardedHeaders.Option, HeadersAllowList: cs.ForwardedHeaders.HeadersAllowList,
		}
	}

	if cs.ForwardedQueryStrings != nil {
		w.ForwardedQueryStrings = &queryStringObjectWire{
			Option:                cs.ForwardedQueryStrings.Option,
			QueryStringsAllowList: cs.ForwardedQueryStrings.QueryStringsAllowList,
		}
	}

	return w
}

func cacheBehaviorsPerPathFromWire(in []cacheBehaviorPerPathWire) []CacheBehaviorPerPath {
	if in == nil {
		return nil
	}

	out := make([]CacheBehaviorPerPath, len(in))
	for i, w := range in {
		out[i] = CacheBehaviorPerPath(w)
	}

	return out
}

func cacheBehaviorsPerPathToWire(in []CacheBehaviorPerPath) []cacheBehaviorPerPathWire {
	if in == nil {
		return nil
	}

	out := make([]cacheBehaviorPerPathWire, len(in))
	for i, c := range in {
		out[i] = cacheBehaviorPerPathWire(c)
	}

	return out
}

type distributionWire struct {
	CreatedAt              *float64                   `json:"createdAt,omitempty"`
	Origin                 *originWire                `json:"origin,omitempty"`
	Location               *resourceLocationWire      `json:"location,omitempty"`
	DefaultCacheBehavior   *cacheBehaviorWire         `json:"defaultCacheBehavior,omitempty"`
	CacheBehaviorSettings  *cacheSettingsWire         `json:"cacheBehaviorSettings,omitempty"`
	OriginPublicDNS        string                     `json:"originPublicDNS,omitempty"`
	Name                   string                     `json:"name,omitempty"`
	BundleID               string                     `json:"bundleId,omitempty"`
	DomainName             string                     `json:"domainName,omitempty"`
	IPAddressType          string                     `json:"ipAddressType,omitempty"`
	SupportCode            string                     `json:"supportCode,omitempty"`
	Arn                    string                     `json:"arn,omitempty"`
	CertificateName        string                     `json:"certificateName,omitempty"`
	Status                 string                     `json:"status,omitempty"`
	ResourceType           string                     `json:"resourceType,omitempty"`
	ViewerMinTLSVersion    string                     `json:"viewerMinimumTlsProtocolVersion,omitempty"`
	AlternativeDomainNames []string                   `json:"alternativeDomainNames,omitempty"`
	CacheBehaviors         []cacheBehaviorPerPathWire `json:"cacheBehaviors,omitempty"`
	Tags                   []tagWire                  `json:"tags,omitempty"`
	AbleToUpdateBundle     bool                       `json:"ableToUpdateBundle,omitempty"`
	IsEnabled              bool                       `json:"isEnabled,omitempty"`
}

func distributionToWire(d *Distribution) distributionWire {
	var defaultCacheBehavior *cacheBehaviorWire
	if d.DefaultCacheBehavior.Behavior != "" {
		defaultCacheBehavior = &cacheBehaviorWire{Behavior: d.DefaultCacheBehavior.Behavior}
	}

	return distributionWire{
		AbleToUpdateBundle:     d.AbleToUpdateBundle,
		AlternativeDomainNames: d.AlternativeDomainNames,
		Arn:                    d.Arn,
		BundleID:               d.BundleID,
		CacheBehaviors:         cacheBehaviorsPerPathToWire(d.CacheBehaviors),
		CacheBehaviorSettings:  cacheSettingsToWire(d.CacheBehaviorSettings),
		CertificateName:        d.CertificateName,
		CreatedAt:              epochPtr(d.CreatedAt),
		DefaultCacheBehavior:   defaultCacheBehavior,
		DomainName:             d.DomainName,
		IPAddressType:          d.IPAddressType,
		IsEnabled:              d.IsEnabled,
		Location:               locationToWire(d.Location),
		Name:                   d.Name,
		Origin: &originWire{
			Name:           d.Origin.Name,
			RegionName:     d.Origin.RegionName,
			ProtocolPolicy: d.Origin.ProtocolPolicy,
		},
		OriginPublicDNS:     d.OriginPublicDNS,
		ResourceType:        ResourceTypeDistribution,
		Status:              d.Status,
		SupportCode:         d.SupportCode,
		Tags:                mapFromTags(d.Tags),
		ViewerMinTLSVersion: d.ViewerMinTLSVersion,
	}
}

type createDistributionRequest struct {
	CacheBehaviorSettings           *cacheSettingsWire `json:"cacheBehaviorSettings,omitempty"`
	BundleID                        string             `json:"bundleId"`
	CertificateName                 string             `json:"certificateName,omitempty"`
	DistributionName                string             `json:"distributionName"`
	IPAddressType                   string             `json:"ipAddressType,omitempty"`
	ViewerMinimumTLSProtocolVersion string             `json:"viewerMinimumTlsProtocolVersion,omitempty"`
	Origin                          struct {
		Name string `json:"name"`
	} `json:"origin"`
	DefaultCacheBehavior cacheBehaviorWire          `json:"defaultCacheBehavior"`
	CacheBehaviors       []cacheBehaviorPerPathWire `json:"cacheBehaviors,omitempty"`
	Tags                 []tagWire                  `json:"tags,omitempty"`
}

type distributionAndOpsResponse struct {
	Distribution *distributionWire `json:"distribution,omitempty"`
	Operation    *operationWire    `json:"operation,omitempty"`
}

func (h *Handler) handleCreateDistribution(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createDistributionRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateDistribution(CreateDistributionRequest{
		Name:                  req.DistributionName,
		BundleID:              req.BundleID,
		OriginName:            req.Origin.Name,
		IPAddressType:         req.IPAddressType,
		CertificateName:       req.CertificateName,
		DefaultCacheBehavior:  CacheBehavior{Behavior: req.DefaultCacheBehavior.Behavior},
		CacheBehaviorSettings: cacheSettingsFromWire(req.CacheBehaviorSettings),
		CacheBehaviors:        cacheBehaviorsPerPathFromWire(req.CacheBehaviors),
		ViewerMinTLSVersion:   req.ViewerMinimumTLSProtocolVersion,
		Tags:                  tagsFromWire(req.Tags),
	})
	if createErr != nil {
		return nil, createErr
	}

	var opw *operationWire

	if len(ops) > 0 {
		w := operationToWire(&ops[0])
		opw = &w
	}

	return marshalResponse(distributionAndOpsResponse{Operation: opw})
}

type distributionNameRequest struct {
	DistributionName string `json:"distributionName"`
}

type updateDistributionRequest struct {
	CacheBehaviorSettings           *cacheSettingsWire         `json:"cacheBehaviorSettings,omitempty"`
	DefaultCacheBehavior            *cacheBehaviorWire         `json:"defaultCacheBehavior,omitempty"`
	IsEnabled                       *bool                      `json:"isEnabled,omitempty"`
	CertificateName                 string                     `json:"certificateName,omitempty"`
	DistributionName                string                     `json:"distributionName"`
	ViewerMinimumTLSProtocolVersion string                     `json:"viewerMinimumTlsProtocolVersion,omitempty"`
	CacheBehaviors                  []cacheBehaviorPerPathWire `json:"cacheBehaviors,omitempty"`
}

func (h *Handler) handleUpdateDistribution(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateDistributionRequest](body)
	if err != nil {
		return nil, err
	}

	var defaultCacheBehavior *CacheBehavior
	if req.DefaultCacheBehavior != nil {
		defaultCacheBehavior = &CacheBehavior{Behavior: req.DefaultCacheBehavior.Behavior}
	}

	op, updateErr := h.Backend.UpdateDistribution(UpdateDistributionRequest{
		Name:                  req.DistributionName,
		CertificateName:       req.CertificateName,
		IsEnabled:             req.IsEnabled,
		DefaultCacheBehavior:  defaultCacheBehavior,
		CacheBehaviorSettings: cacheSettingsFromWire(req.CacheBehaviorSettings),
		CacheBehaviors:        cacheBehaviorsPerPathFromWire(req.CacheBehaviors),
		ViewerMinTLSVersion:   req.ViewerMinimumTLSProtocolVersion,
	})
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleDeleteDistribution(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[distributionNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, delErr := h.Backend.DeleteDistribution(req.DistributionName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opEnvelope(op))
}

type getDistributionsRequest struct {
	DistributionName string `json:"distributionName,omitempty"`
	PageToken        string `json:"pageToken,omitempty"`
}

type distributionsListResponse struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	Distributions []distributionWire `json:"distributions,omitempty"`
}

func (h *Handler) handleGetDistributions(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getDistributionsRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetDistributions(req.DistributionName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]distributionWire, len(pg.Data))
	for i, d := range pg.Data {
		out[i] = distributionToWire(d)
	}

	return marshalResponse(distributionsListResponse{Distributions: out, NextPageToken: pg.Next})
}

type updateDistributionBundleRequest struct {
	BundleID         string `json:"bundleId,omitempty"`
	DistributionName string `json:"distributionName,omitempty"`
}

func (h *Handler) handleUpdateDistributionBundle(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateDistributionBundleRequest](body)
	if err != nil {
		return nil, err
	}

	op, updateErr := h.Backend.UpdateDistributionBundle(req.DistributionName, req.BundleID)
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opEnvelope(op))
}

type resetDistributionCacheResponse struct {
	CreateTime *float64       `json:"createTime,omitempty"`
	Operation  *operationWire `json:"operation,omitempty"`
	Status     string         `json:"status,omitempty"`
}

func (h *Handler) handleResetDistributionCache(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[distributionNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, resetTime, resetErr := h.Backend.ResetDistributionCache(req.DistributionName)
	if resetErr != nil {
		return nil, resetErr
	}

	ow := operationToWire(op)

	return marshalResponse(
		resetDistributionCacheResponse{
			CreateTime: epochPtr(resetTime),
			Operation:  &ow,
			Status:     "Reset",
		},
	)
}

type getDistributionLatestCacheResetResponse struct {
	CreateTime *float64 `json:"createTime,omitempty"`
	Status     string   `json:"status,omitempty"`
}

func (h *Handler) handleGetDistributionLatestCacheReset(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	req, err := decodeBody[distributionNameRequest](body)
	if err != nil {
		return nil, err
	}

	t, getErr := h.Backend.GetDistributionLatestCacheReset(req.DistributionName)
	if getErr != nil {
		return nil, getErr
	}

	return marshalResponse(
		getDistributionLatestCacheResetResponse{CreateTime: epochPtr(t), Status: "Done"},
	)
}

type distributionMetricDataRequest struct {
	DistributionName string `json:"distributionName"`
	MetricName       string `json:"metricName,omitempty"`
}

type distributionMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

func (h *Handler) handleGetDistributionMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[distributionMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetDistributionMetricData(req.DistributionName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(
		distributionMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName},
	)
}

type attachCertToDistributionRequest struct {
	CertificateName  string `json:"certificateName"`
	DistributionName string `json:"distributionName"`
}

func (h *Handler) handleAttachCertificateToDistribution(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	req, err := decodeBody[attachCertToDistributionRequest](body)
	if err != nil {
		return nil, err
	}

	op, attachErr := h.Backend.AttachCertificateToDistribution(
		req.DistributionName,
		req.CertificateName,
	)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleDetachCertificateFromDistribution(
	_ context.Context,
	body []byte,
) ([]byte, error) {
	req, err := decodeBody[distributionNameRequest](body)
	if err != nil {
		return nil, err
	}

	op, detachErr := h.Backend.DetachCertificateFromDistribution(req.DistributionName)
	if detachErr != nil {
		return nil, detachErr
	}

	return marshalResponse(opEnvelope(op))
}

type certificateWire struct {
	CertificateDetail *certificateDetailWire `json:"certificateDetail,omitempty"`
	Arn               string                 `json:"certificateArn,omitempty"`
	DomainName        string                 `json:"domainName,omitempty"`
	CertificateName   string                 `json:"certificateName,omitempty"`
	Tags              []tagWire              `json:"tags,omitempty"`
}

type certificateDetailWire struct {
	CreatedAt               *float64  `json:"createdAt,omitempty"`
	DomainName              string    `json:"domainName,omitempty"`
	IssuedAt                *float64  `json:"issuedAt,omitempty"`
	Name                    string    `json:"name,omitempty"`
	NotAfter                *float64  `json:"notAfter,omitempty"`
	NotBefore               *float64  `json:"notBefore,omitempty"`
	Status                  string    `json:"status,omitempty"`
	SubjectAlternativeNames []string  `json:"subjectAlternativeNames,omitempty"`
	Tags                    []tagWire `json:"tags,omitempty"`
}

func certificateToWire(c *Certificate) certificateWire {
	return certificateWire{
		Arn: c.Arn, DomainName: c.DomainName, CertificateName: c.Name, Tags: mapFromTags(c.Tags),
		CertificateDetail: &certificateDetailWire{
			CreatedAt: epochPtr(
				c.CreatedAt,
			), DomainName: c.DomainName, IssuedAt: epochPtr(c.IssuedAt),
			Name: c.Name, NotAfter: epochPtr(c.NotAfter), NotBefore: epochPtr(c.NotBefore), Status: c.Status,
			SubjectAlternativeNames: c.SubjectAlternativeNames, Tags: mapFromTags(c.Tags),
		},
	}
}

type createCertificateRequest struct {
	CertificateName         string    `json:"certificateName"`
	DomainName              string    `json:"domainName"`
	SubjectAlternativeNames []string  `json:"subjectAlternativeNames,omitempty"`
	Tags                    []tagWire `json:"tags,omitempty"`
}

type certificateEnvelope struct {
	Certificate *certificateWire `json:"certificate,omitempty"`
	Operations  []operationWire  `json:"operations,omitempty"`
}

func (h *Handler) handleCreateCertificate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createCertificateRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateCertificate(
		req.CertificateName,
		req.DomainName,
		req.SubjectAlternativeNames,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	cert, getErr := h.Backend.GetCertificates(req.CertificateName, "")
	if getErr != nil {
		return nil, getErr
	}

	var certWire *certificateWire

	if len(cert.Data) > 0 {
		w := certificateToWire(cert.Data[0])
		certWire = &w
	}

	return marshalResponse(
		certificateEnvelope{Certificate: certWire, Operations: operationsToWire(ops)},
	)
}

type certificateNameRequest struct {
	CertificateName string `json:"certificateName"`
}

func (h *Handler) handleDeleteCertificate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[certificateNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteCertificate(req.CertificateName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getCertificatesRequest struct {
	CertificateName           string   `json:"certificateName,omitempty"`
	PageToken                 string   `json:"pageToken,omitempty"`
	CertificateStatuses       []string `json:"certificateStatuses,omitempty"`
	IncludeCertificateDetails bool     `json:"includeCertificateDetails,omitempty"`
}

type certificatesListResponse struct {
	NextPageToken string            `json:"nextPageToken,omitempty"`
	Certificates  []certificateWire `json:"certificates,omitempty"`
}

func (h *Handler) handleGetCertificates(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getCertificatesRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetCertificates(req.CertificateName, req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]certificateWire, len(pg.Data))
	for i, c := range pg.Data {
		out[i] = certificateToWire(c)
	}

	return marshalResponse(certificatesListResponse{Certificates: out, NextPageToken: pg.Next})
}
