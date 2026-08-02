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

type distributionWire struct {
	CreatedAt              *float64              `json:"createdAt,omitempty"`
	Origin                 *originWire           `json:"origin,omitempty"`
	Location               *resourceLocationWire `json:"location,omitempty"`
	OriginPublicDNS        string                `json:"originPublicDNS,omitempty"`
	Name                   string                `json:"name,omitempty"`
	BundleID               string                `json:"bundleId,omitempty"`
	DomainName             string                `json:"domainName,omitempty"`
	IPAddressType          string                `json:"ipAddressType,omitempty"`
	SupportCode            string                `json:"supportCode,omitempty"`
	Arn                    string                `json:"arn,omitempty"`
	CertificateName        string                `json:"certificateName,omitempty"`
	Status                 string                `json:"status,omitempty"`
	ResourceType           string                `json:"resourceType,omitempty"`
	AlternativeDomainNames []string              `json:"alternativeDomainNames,omitempty"`
	Tags                   []tagWire             `json:"tags,omitempty"`
	AbleToUpdateBundle     bool                  `json:"ableToUpdateBundle,omitempty"`
	IsEnabled              bool                  `json:"isEnabled,omitempty"`
}

func distributionToWire(d *Distribution) distributionWire {
	return distributionWire{
		AbleToUpdateBundle:     d.AbleToUpdateBundle,
		AlternativeDomainNames: d.AlternativeDomainNames,
		Arn:                    d.Arn,
		BundleID:               d.BundleID,
		CertificateName:        d.CertificateName,
		CreatedAt:              epochPtr(d.CreatedAt),
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
		OriginPublicDNS: d.OriginPublicDNS,
		ResourceType:    ResourceTypeDistribution,
		Status:          d.Status,
		SupportCode:     d.SupportCode,
		Tags:            mapFromTags(d.Tags),
	}
}

type createDistributionRequest struct {
	BundleID         string `json:"bundleId"`
	CertificateName  string `json:"certificateName,omitempty"`
	DistributionName string `json:"distributionName"`
	IPAddressType    string `json:"ipAddressType,omitempty"`
	Origin           struct {
		Name string `json:"name"`
	} `json:"origin"`
	Tags []tagWire `json:"tags,omitempty"`
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

	ops, createErr := h.Backend.CreateDistribution(
		req.DistributionName,
		req.BundleID,
		req.Origin.Name,
		req.IPAddressType,
		req.CertificateName,
		tagsFromWire(req.Tags),
	)
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
	IsEnabled        *bool  `json:"isEnabled,omitempty"`
	CertificateName  string `json:"certificateName,omitempty"`
	DistributionName string `json:"distributionName"`
}

func (h *Handler) handleUpdateDistribution(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateDistributionRequest](body)
	if err != nil {
		return nil, err
	}

	op, updateErr := h.Backend.UpdateDistribution(req.DistributionName, req.CertificateName, req.IsEnabled)
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
		resetDistributionCacheResponse{CreateTime: epochPtr(resetTime), Operation: &ow, Status: "Reset"},
	)
}

type getDistributionLatestCacheResetResponse struct {
	CreateTime *float64 `json:"createTime,omitempty"`
	Status     string   `json:"status,omitempty"`
}

func (h *Handler) handleGetDistributionLatestCacheReset(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[distributionNameRequest](body)
	if err != nil {
		return nil, err
	}

	t, getErr := h.Backend.GetDistributionLatestCacheReset(req.DistributionName)
	if getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getDistributionLatestCacheResetResponse{CreateTime: epochPtr(t), Status: "Done"})
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

	return marshalResponse(distributionMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName})
}

type attachCertToDistributionRequest struct {
	CertificateName  string `json:"certificateName"`
	DistributionName string `json:"distributionName"`
}

func (h *Handler) handleAttachCertificateToDistribution(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachCertToDistributionRequest](body)
	if err != nil {
		return nil, err
	}

	op, attachErr := h.Backend.AttachCertificateToDistribution(req.DistributionName, req.CertificateName)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opEnvelope(op))
}

func (h *Handler) handleDetachCertificateFromDistribution(_ context.Context, body []byte) ([]byte, error) {
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
			CreatedAt: epochPtr(c.CreatedAt), DomainName: c.DomainName, IssuedAt: epochPtr(c.IssuedAt),
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

	return marshalResponse(certificateEnvelope{Certificate: certWire, Operations: operationsToWire(ops)})
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
