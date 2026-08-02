package lightsail

import "context"

// loadBalancerOps returns the dispatch table for family L+M (14 ops).
func (h *Handler) loadBalancerOps() map[string]opFunc {
	return map[string]opFunc{
		"CreateLoadBalancer":               h.handleCreateLoadBalancer,
		"DeleteLoadBalancer":               h.handleDeleteLoadBalancer,
		"GetLoadBalancer":                  h.handleGetLoadBalancer,
		"GetLoadBalancers":                 h.handleGetLoadBalancers,
		"AttachInstancesToLoadBalancer":    h.handleAttachInstancesToLoadBalancer,
		"DetachInstancesFromLoadBalancer":  h.handleDetachInstancesFromLoadBalancer,
		"UpdateLoadBalancerAttribute":      h.handleUpdateLoadBalancerAttribute,
		"SetIpAddressType":                 h.handleSetIPAddressType,
		"GetLoadBalancerMetricData":        h.handleGetLoadBalancerMetricData,
		"CreateLoadBalancerTlsCertificate": h.handleCreateLoadBalancerTLSCertificate,
		"DeleteLoadBalancerTlsCertificate": h.handleDeleteLoadBalancerTLSCertificate,
		"AttachLoadBalancerTlsCertificate": h.handleAttachLoadBalancerTLSCertificate,
		"GetLoadBalancerTlsCertificates":   h.handleGetLoadBalancerTLSCertificates,
		"GetLoadBalancerTlsPolicies":       h.handleGetLoadBalancerTLSPolicies,
	}
}

type instanceHealthSummaryWire struct {
	InstanceHealth       string `json:"instanceHealth,omitempty"`
	InstanceHealthReason string `json:"instanceHealthReason,omitempty"`
	InstanceName         string `json:"instanceName,omitempty"`
}

type loadBalancerTLSCertSummaryWire struct {
	Name       string `json:"name,omitempty"`
	IsAttached bool   `json:"isAttached,omitempty"`
}

type loadBalancerWire struct {
	Location                *resourceLocationWire            `json:"location,omitempty"`
	CreatedAt               *float64                         `json:"createdAt,omitempty"`
	ResourceType            string                           `json:"resourceType,omitempty"`
	Arn                     string                           `json:"arn,omitempty"`
	TLSPolicyName           string                           `json:"tlsPolicyName,omitempty"`
	SupportCode             string                           `json:"supportCode,omitempty"`
	State                   string                           `json:"state,omitempty"`
	IPAddressType           string                           `json:"ipAddressType,omitempty"`
	DNSName                 string                           `json:"dnsName,omitempty"`
	Name                    string                           `json:"name,omitempty"`
	Protocol                string                           `json:"protocol,omitempty"`
	HealthCheckPath         string                           `json:"healthCheckPath,omitempty"`
	PublicPorts             []int32                          `json:"publicPorts,omitempty"`
	InstanceHealthSummary   []instanceHealthSummaryWire      `json:"instanceHealthSummary,omitempty"`
	Tags                    []tagWire                        `json:"tags,omitempty"`
	TLSCertificateSummaries []loadBalancerTLSCertSummaryWire `json:"tlsCertificateSummaries,omitempty"`
	InstancePort            int32                            `json:"instancePort,omitempty"`
	HTTPSRedirectionEnabled bool                             `json:"httpsRedirectionEnabled,omitempty"`
}

func loadBalancerToWire(l *LoadBalancer) loadBalancerWire {
	health := make([]instanceHealthSummaryWire, 0, len(l.InstanceHealth))

	for _, name := range l.AttachedInstances {
		st := l.InstanceHealth[name]
		health = append(
			health,
			instanceHealthSummaryWire{InstanceHealth: st.State, InstanceHealthReason: st.Reason, InstanceName: name},
		)
	}

	certs := make([]loadBalancerTLSCertSummaryWire, len(l.TLSCertificateNames))
	for i, n := range l.TLSCertificateNames {
		certs[i] = loadBalancerTLSCertSummaryWire{Name: n, IsAttached: true}
	}

	return loadBalancerWire{
		Arn: l.Arn, CreatedAt: epochPtr(l.CreatedAt), DNSName: l.DNSName, HealthCheckPath: l.HealthCheckPath,
		HTTPSRedirectionEnabled: l.HTTPSRedirectionEnabled, InstanceHealthSummary: health, InstancePort: l.InstancePort,
		IPAddressType: l.IPAddressType, Location: locationToWire(l.Location), Name: l.Name, Protocol: l.Protocol,
		PublicPorts: l.PublicPorts, ResourceType: ResourceTypeLoadBalancer, State: l.State, SupportCode: l.SupportCode,
		Tags: mapFromTags(l.Tags), TLSCertificateSummaries: certs, TLSPolicyName: l.TLSPolicyName,
	}
}

type createLoadBalancerRequest struct {
	CertificateDomainName       string    `json:"certificateDomainName,omitempty"`
	CertificateName             string    `json:"certificateName,omitempty"`
	HealthCheckPath             string    `json:"healthCheckPath,omitempty"`
	IPAddressType               string    `json:"ipAddressType,omitempty"`
	LoadBalancerName            string    `json:"loadBalancerName"`
	TLSPolicyName               string    `json:"tlsPolicyName,omitempty"`
	CertificateAlternativeNames []string  `json:"certificateAlternativeNames,omitempty"`
	Tags                        []tagWire `json:"tags,omitempty"`
	InstancePort                int32     `json:"instancePort"`
}

func (h *Handler) handleCreateLoadBalancer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createLoadBalancerRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateLoadBalancer(
		req.LoadBalancerName,
		req.InstancePort,
		req.HealthCheckPath,
		req.IPAddressType,
		req.TLSPolicyName,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type loadBalancerNameRequest struct {
	LoadBalancerName string `json:"loadBalancerName"`
}

func (h *Handler) handleDeleteLoadBalancer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[loadBalancerNameRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteLoadBalancer(req.LoadBalancerName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type loadBalancerEnvelope struct {
	LoadBalancer *loadBalancerWire `json:"loadBalancer,omitempty"`
}

func (h *Handler) handleGetLoadBalancer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[loadBalancerNameRequest](body)
	if err != nil {
		return nil, err
	}

	lb, getErr := h.Backend.GetLoadBalancer(req.LoadBalancerName)
	if getErr != nil {
		return nil, getErr
	}

	w := loadBalancerToWire(lb)

	return marshalResponse(loadBalancerEnvelope{LoadBalancer: &w})
}

type loadBalancersListResponse struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	LoadBalancers []loadBalancerWire `json:"loadBalancers,omitempty"`
}

func (h *Handler) handleGetLoadBalancers(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetLoadBalancers(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]loadBalancerWire, len(pg.Data))
	for i, l := range pg.Data {
		out[i] = loadBalancerToWire(l)
	}

	return marshalResponse(loadBalancersListResponse{LoadBalancers: out, NextPageToken: pg.Next})
}

type attachInstancesToLoadBalancerRequest struct {
	LoadBalancerName string   `json:"loadBalancerName"`
	InstanceNames    []string `json:"instanceNames"`
}

func (h *Handler) handleAttachInstancesToLoadBalancer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachInstancesToLoadBalancerRequest](body)
	if err != nil {
		return nil, err
	}

	ops, attachErr := h.Backend.AttachInstancesToLoadBalancer(req.LoadBalancerName, req.InstanceNames)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opsEnvelope(ops))
}

func (h *Handler) handleDetachInstancesFromLoadBalancer(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachInstancesToLoadBalancerRequest](body)
	if err != nil {
		return nil, err
	}

	ops, detachErr := h.Backend.DetachInstancesFromLoadBalancer(req.LoadBalancerName, req.InstanceNames)
	if detachErr != nil {
		return nil, detachErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type updateLoadBalancerAttributeRequest struct {
	AttributeName    string `json:"attributeName"`
	AttributeValue   string `json:"attributeValue"`
	LoadBalancerName string `json:"loadBalancerName"`
}

func (h *Handler) handleUpdateLoadBalancerAttribute(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[updateLoadBalancerAttributeRequest](body)
	if err != nil {
		return nil, err
	}

	ops, updateErr := h.Backend.UpdateLoadBalancerAttribute(req.LoadBalancerName, req.AttributeName, req.AttributeValue)
	if updateErr != nil {
		return nil, updateErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type setIPAddressTypeRequest struct {
	IPAddressType      string `json:"ipAddressType"`
	ResourceName       string `json:"resourceName"`
	ResourceType       string `json:"resourceType"`
	AcceptBundleUpdate bool   `json:"acceptBundleUpdate,omitempty"`
}

func (h *Handler) handleSetIPAddressType(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[setIPAddressTypeRequest](body)
	if err != nil {
		return nil, err
	}

	ops, setErr := h.Backend.SetIPAddressType(req.ResourceName, req.ResourceType, req.IPAddressType)
	if setErr != nil {
		return nil, setErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getLoadBalancerMetricDataResponse struct {
	MetricName string     `json:"metricName,omitempty"`
	MetricData []struct{} `json:"metricData"`
}

type loadBalancerMetricDataRequest struct {
	LoadBalancerName string `json:"loadBalancerName"`
	MetricName       string `json:"metricName,omitempty"`
}

func (h *Handler) handleGetLoadBalancerMetricData(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[loadBalancerMetricDataRequest](body)
	if err != nil {
		return nil, err
	}

	if getErr := h.Backend.GetLoadBalancerMetricData(req.LoadBalancerName); getErr != nil {
		return nil, getErr
	}

	return marshalResponse(getLoadBalancerMetricDataResponse{MetricData: []struct{}{}, MetricName: req.MetricName})
}

type lbTLSCertWire struct {
	NotAfter                *float64              `json:"notAfter,omitempty"`
	Location                *resourceLocationWire `json:"location,omitempty"`
	NotBefore               *float64              `json:"notBefore,omitempty"`
	CreatedAt               *float64              `json:"createdAt,omitempty"`
	IssuedAt                *float64              `json:"issuedAt,omitempty"`
	LoadBalancerName        string                `json:"loadBalancerName,omitempty"`
	ResourceType            string                `json:"resourceType,omitempty"`
	Name                    string                `json:"name,omitempty"`
	DomainName              string                `json:"domainName,omitempty"`
	Arn                     string                `json:"arn,omitempty"`
	Serial                  string                `json:"serial,omitempty"`
	Status                  string                `json:"status,omitempty"`
	Subject                 string                `json:"subject,omitempty"`
	SupportCode             string                `json:"supportCode,omitempty"`
	SubjectAlternativeNames []string              `json:"subjectAlternativeNames,omitempty"`
	Tags                    []tagWire             `json:"tags,omitempty"`
	IsAttached              bool                  `json:"isAttached,omitempty"`
}

func lbTLSCertToWire(c *LoadBalancerTLSCertificate) lbTLSCertWire {
	return lbTLSCertWire{
		Arn: c.Arn, CreatedAt: epochPtr(c.CreatedAt), DomainName: c.DomainName, IsAttached: c.IsAttached,
		IssuedAt: epochPtr(c.IssuedAt), LoadBalancerName: c.LoadBalancerName, Location: locationToWire(c.Location),
		Name: c.Name, NotAfter: epochPtr(c.NotAfter), NotBefore: epochPtr(c.NotBefore),
		ResourceType: ResourceTypeLoadBalancerTLSCertificate, Serial: c.SerialNumber, Status: c.Status,
		Subject: c.Subject, SubjectAlternativeNames: c.SubjectAlternativeNames, SupportCode: c.SupportCode,
		Tags: mapFromTags(c.Tags),
	}
}

type createLBTLSCertRequest struct {
	CertificateAlternativeNames []string  `json:"certificateAlternativeNames,omitempty"`
	CertificateDomainName       string    `json:"certificateDomainName"`
	CertificateName             string    `json:"certificateName"`
	LoadBalancerName            string    `json:"loadBalancerName"`
	Tags                        []tagWire `json:"tags,omitempty"`
}

func (h *Handler) handleCreateLoadBalancerTLSCertificate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[createLBTLSCertRequest](body)
	if err != nil {
		return nil, err
	}

	ops, createErr := h.Backend.CreateLoadBalancerTLSCertificate(
		req.LoadBalancerName,
		req.CertificateName,
		req.CertificateDomainName,
		req.CertificateAlternativeNames,
		tagsFromWire(req.Tags),
	)
	if createErr != nil {
		return nil, createErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type deleteLBTLSCertRequest struct {
	CertificateName  string `json:"certificateName"`
	LoadBalancerName string `json:"loadBalancerName"`
	Force            bool   `json:"force,omitempty"`
}

func (h *Handler) handleDeleteLoadBalancerTLSCertificate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[deleteLBTLSCertRequest](body)
	if err != nil {
		return nil, err
	}

	ops, delErr := h.Backend.DeleteLoadBalancerTLSCertificate(req.LoadBalancerName, req.CertificateName)
	if delErr != nil {
		return nil, delErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type attachLBTLSCertRequest struct {
	CertificateName  string `json:"certificateName"`
	LoadBalancerName string `json:"loadBalancerName"`
}

func (h *Handler) handleAttachLoadBalancerTLSCertificate(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[attachLBTLSCertRequest](body)
	if err != nil {
		return nil, err
	}

	ops, attachErr := h.Backend.AttachLoadBalancerTLSCertificate(req.LoadBalancerName, req.CertificateName)
	if attachErr != nil {
		return nil, attachErr
	}

	return marshalResponse(opsEnvelope(ops))
}

type getLBTLSCertsResponse struct {
	TLSCertificates []lbTLSCertWire `json:"tlsCertificates,omitempty"`
}

func (h *Handler) handleGetLoadBalancerTLSCertificates(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[loadBalancerNameRequest](body)
	if err != nil {
		return nil, err
	}

	certs, getErr := h.Backend.GetLoadBalancerTLSCertificates(req.LoadBalancerName)
	if getErr != nil {
		return nil, getErr
	}

	out := make([]lbTLSCertWire, len(certs))
	for i, c := range certs {
		out[i] = lbTLSCertToWire(c)
	}

	return marshalResponse(getLBTLSCertsResponse{TLSCertificates: out})
}

type lbTLSPolicyWire struct {
	Description string   `json:"description,omitempty"`
	Name        string   `json:"name,omitempty"`
	Ciphers     []string `json:"ciphers,omitempty"`
	Protocols   []string `json:"protocols,omitempty"`
	IsDefault   bool     `json:"isDefault,omitempty"`
}

type lbTLSPoliciesListResponse struct {
	NextPageToken string            `json:"nextPageToken,omitempty"`
	TLSPolicies   []lbTLSPolicyWire `json:"tlsPolicies,omitempty"`
}

func (h *Handler) handleGetLoadBalancerTLSPolicies(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[pageTokenRequest](body)
	if err != nil {
		return nil, err
	}

	pg, pgErr := h.Backend.GetLoadBalancerTLSPolicies(req.PageToken)
	if pgErr != nil {
		return nil, pgErr
	}

	out := make([]lbTLSPolicyWire, len(pg.Data))
	for i, p := range pg.Data {
		out[i] = lbTLSPolicyWire{
			Ciphers:     p.Ciphers,
			Description: p.Description,
			IsDefault:   p.IsDefault,
			Name:        p.Name,
			Protocols:   p.Protocols,
		}
	}

	return marshalResponse(lbTLSPoliciesListResponse{TLSPolicies: out, NextPageToken: pg.Next})
}
