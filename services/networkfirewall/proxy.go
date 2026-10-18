package networkfirewall

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	proxyAttaching  = "ATTACHING"
	proxyAttached   = "ATTACHED"
	proxyDetaching  = "DETACHING"
	modifyModifying = "MODIFYING"
	modifyCompleted = "COMPLETED"

	tlsEnabled  = "ENABLED"
	tlsDisabled = "DISABLED"
)

// TLSInterceptProperties configures TLS interception on a proxy.
type TLSInterceptProperties struct {
	PcaArn           string `json:"PcaArn,omitempty"`
	TLSInterceptMode string `json:"TlsInterceptMode,omitempty"`
}

// Proxy is the Proxy response shape.
type Proxy struct {
	CreateTime             *float64                `json:"CreateTime,omitempty"`
	DeleteTime             *float64                `json:"DeleteTime,omitempty"`
	UpdateTime             *float64                `json:"UpdateTime,omitempty"`
	TLSInterceptProperties *TLSInterceptProperties `json:"TlsInterceptProperties,omitempty"`
	NatGatewayID           string                  `json:"NatGatewayId"`
	ProxyArn               string                  `json:"ProxyArn"`
	ProxyConfigurationArn  string                  `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string                  `json:"ProxyConfigurationName"`
	ProxyModifyState       string                  `json:"ProxyModifyState"`
	ProxyName              string                  `json:"ProxyName"`
	ProxyState             string                  `json:"ProxyState"`
	ListenerProperties     []ListenerProperty      `json:"ListenerProperties"`
	Tags                   []Tag                   `json:"Tags,omitempty"`
}

// DescribeProxyResource is the DescribeProxy response shape.
type DescribeProxyResource struct {
	PrivateDNSName         string `json:"PrivateDNSName,omitempty"`
	VpcEndpointServiceName string `json:"VpcEndpointServiceName,omitempty"`
	Proxy
}

// CreateProxyInput is the CreateProxy request.
type CreateProxyInput struct {
	TLSInterceptProperties *TLSInterceptProperties `json:"TlsInterceptProperties"`
	ProxyName              string                  `json:"ProxyName"`
	NatGatewayID           string                  `json:"NatGatewayId"`
	ProxyConfigurationArn  string                  `json:"ProxyConfigurationArn"`
	ProxyConfigurationName string                  `json:"ProxyConfigurationName"`
	ListenerProperties     []ListenerProperty      `json:"ListenerProperties"`
	Tags                   []Tag                   `json:"Tags"`
}

// ProxyOutput is the Create/UpdateProxy response.
type ProxyOutput struct {
	UpdateToken string `json:"UpdateToken"`
	Proxy       Proxy  `json:"Proxy"`
}

// DescribeProxyInput is the DescribeProxy request.
type DescribeProxyInput struct {
	ProxyArn  string `json:"ProxyArn"`
	ProxyName string `json:"ProxyName"`
}

// DescribeProxyOutput is the DescribeProxy response.
type DescribeProxyOutput struct {
	UpdateToken string                `json:"UpdateToken"`
	Proxy       DescribeProxyResource `json:"Proxy"`
}

// UpdateProxyInput is the UpdateProxy request.
type UpdateProxyInput struct {
	TLSInterceptProperties     *TLSInterceptProperties `json:"TlsInterceptProperties"`
	ProxyArn                   string                  `json:"ProxyArn"`
	ProxyName                  string                  `json:"ProxyName"`
	NatGatewayID               string                  `json:"NatGatewayId"`
	UpdateToken                string                  `json:"UpdateToken"`
	ListenerPropertiesToAdd    []ListenerProperty      `json:"ListenerPropertiesToAdd"`
	ListenerPropertiesToRemove []ListenerProperty      `json:"ListenerPropertiesToRemove"`
}

// DeleteProxyInput is the DeleteProxy request.
type DeleteProxyInput struct {
	NatGatewayID string `json:"NatGatewayId"`
	ProxyArn     string `json:"ProxyArn"`
	ProxyName    string `json:"ProxyName"`
}

// DeleteProxyOutput is the DeleteProxy response.
type DeleteProxyOutput struct {
	NatGatewayID string `json:"NatGatewayId"`
	ProxyArn     string `json:"ProxyArn"`
	ProxyName    string `json:"ProxyName"`
}

// ListProxiesInput is the ListProxies request.
type ListProxiesInput struct {
	MaxResults *int32 `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
}

// ListProxiesOutput is the ListProxies response.
type ListProxiesOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Proxies   []ResourceMetadata `json:"Proxies"`
}

func validateListeners(l []ListenerProperty) error {
	return validateProxySettings(&ProxySettings{ListenerProperties: l})
}

func validateTLSIntercept(t *TLSInterceptProperties) error {
	switch {
	case t == nil:
		return invalid("TlsInterceptProperties is required")
	case t.TLSInterceptMode != tlsEnabled && t.TLSInterceptMode != tlsDisabled:
		return invalid("TlsInterceptMode %q is not valid", t.TLSInterceptMode)
	case t.TLSInterceptMode == tlsEnabled && t.PcaArn == "":
		return invalid("PcaArn is required when TlsInterceptMode is %s", tlsEnabled)
	}

	return nil
}

func (r *proxyRecord) expired(now time.Time) bool {
	return !r.DeleteUntil.IsZero() && !now.Before(r.DeleteUntil)
}

func (r *proxyRecord) states(now time.Time) (string, string) {
	state, modify := proxyAttached, modifyCompleted

	switch {
	case !r.DeleteUntil.IsZero():
		state = proxyDetaching
	case now.Before(r.AttachUntil):
		state = proxyAttaching
	}

	if now.Before(r.ModifyUntil) {
		modify = modifyModifying
	}

	return state, modify
}

func (r *proxyRecord) view(now time.Time) Proxy {
	state, modify := r.states(now)

	p := Proxy{
		CreateTime:             epoch(r.CreateTime),
		UpdateTime:             epoch(r.UpdateTime),
		TLSInterceptProperties: r.TLS,
		NatGatewayID:           r.NatGatewayID,
		ProxyArn:               r.ARN,
		ProxyConfigurationArn:  r.ConfigARN,
		ProxyConfigurationName: r.ConfigName,
		ProxyModifyState:       modify,
		ProxyName:              r.Name,
		ProxyState:             state,
		ListenerProperties:     slices.Clone(r.Listeners),
		Tags:                   tagList(r.Tags),
	}

	if !r.DeleteUntil.IsZero() {
		p.DeleteTime = epoch(r.DeleteUntil)
	}

	return p
}

func (b *InMemoryBackend) sweepProxiesLocked(now time.Time) {
	for _, r := range b.proxies.All() {
		if r.expired(now) {
			b.proxies.Delete(r.ARN)
		}
	}
}

func (b *InMemoryBackend) findProxyLocked(id arnOrName, now time.Time) (*proxyRecord, error) {
	r, err := lookup(b, b.proxies, kindProxy, "proxy", id)
	if err != nil {
		return nil, err
	}

	if r.expired(now) {
		return nil, notFound("proxy", r.ARN)
	}

	return r, nil
}

// CreateProxy creates a proxy attached to a NAT gateway under a proxy configuration.
func (b *InMemoryBackend) CreateProxy(_ context.Context, in *CreateProxyInput) (*ProxyOutput, error) {
	if err := validateName("ProxyName", in.ProxyName); err != nil {
		return nil, err
	}

	if in.NatGatewayID == "" {
		return nil, invalid("NatGatewayId is required")
	}

	if err := validateTLSIntercept(in.TLSInterceptProperties); err != nil {
		return nil, err
	}

	if err := validateListeners(in.ListenerProperties); err != nil {
		return nil, err
	}

	if err := validateTags(tagMap(in.Tags)); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateProxy")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepProxiesLocked(now)

	cfg, err := lookup(b, b.proxyConfigs, kindProxyConfig, "proxy configuration",
		arnOrName{in.ProxyConfigurationArn, in.ProxyConfigurationName})
	if err != nil {
		return nil, err
	}

	proxyARN := b.arnFor(kindProxy, in.ProxyName)
	if b.proxies.Has(proxyARN) {
		return nil, invalid("A proxy named %s already exists", in.ProxyName)
	}

	id := awsid.New("")
	r := &proxyRecord{
		Name:         in.ProxyName,
		ARN:          proxyARN,
		ConfigARN:    cfg.ARN,
		ConfigName:   cfg.Name,
		NatGatewayID: in.NatGatewayID,
		UpdateToken:  newToken(),
		DNSName:      fmt.Sprintf("%s.%s.proxy.network-firewall.%s.amazonaws.com", in.ProxyName, id, b.region),
		ServiceName:  fmt.Sprintf("com.amazonaws.vpce.%s.vpce-svc-%s", b.region, id),
		TLS:          in.TLSInterceptProperties,
		Listeners:    slices.Clone(in.ListenerProperties),
		Tags:         tagMap(in.Tags),
		CreateTime:   now,
		UpdateTime:   now,
		AttachUntil:  b.deadline(now),
	}

	b.proxies.Put(r)

	return &ProxyOutput{Proxy: r.view(now), UpdateToken: r.UpdateToken}, nil
}

// DescribeProxy returns a proxy and its update token.
func (b *InMemoryBackend) DescribeProxy(_ context.Context, in *DescribeProxyInput) (*DescribeProxyOutput, error) {
	b.mu.RLock("DescribeProxy")
	defer b.mu.RUnlock()

	now := time.Now()

	r, err := b.findProxyLocked(arnOrName{in.ProxyArn, in.ProxyName}, now)
	if err != nil {
		return nil, err
	}

	return &DescribeProxyOutput{
		Proxy: DescribeProxyResource{
			PrivateDNSName:         r.DNSName,
			VpcEndpointServiceName: r.ServiceName,
			Proxy:                  r.view(now),
		},
		UpdateToken: r.UpdateToken,
	}, nil
}

// ListProxies lists proxies.
func (b *InMemoryBackend) ListProxies(_ context.Context, in *ListProxiesInput) (*ListProxiesOutput, error) {
	b.mu.RLock("ListProxies")
	defer b.mu.RUnlock()

	now := time.Now()

	live := slices.DeleteFunc(b.proxies.All(), func(r *proxyRecord) bool { return r.expired(now) })

	data, next, err := listMetadata(live,
		func(r *proxyRecord) ResourceMetadata { return ResourceMetadata{Arn: r.ARN, Name: r.Name} },
		in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &ListProxiesOutput{Proxies: data, NextToken: next}, nil
}

func hasListener(l []ListenerProperty, want ListenerProperty) bool {
	return slices.ContainsFunc(l, func(x ListenerProperty) bool {
		return x.Type == want.Type && x.Port != nil && want.Port != nil && *x.Port == *want.Port
	})
}

func applyListenerChanges(r *proxyRecord, add, remove []ListenerProperty) error {
	kept := slices.Clone(r.Listeners)

	for _, l := range remove {
		if !hasListener(kept, l) {
			return invalid("Listener %s on port %d does not exist", l.Type, ptrInt(l.Port))
		}

		kept = slices.DeleteFunc(kept, func(x ListenerProperty) bool { return hasListener([]ListenerProperty{x}, l) })
	}

	kept = append(kept, add...)

	if err := validateListeners(kept); err != nil {
		return err
	}

	r.Listeners = kept

	return nil
}

func ptrInt(p *int32) int32 {
	if p == nil {
		return 0
	}

	return *p
}

// UpdateProxy changes listeners and TLS interception of a proxy.
func (b *InMemoryBackend) UpdateProxy(_ context.Context, in *UpdateProxyInput) (*ProxyOutput, error) {
	if in.NatGatewayID == "" {
		return nil, invalid("NatGatewayId is required")
	}

	if err := validateListeners(in.ListenerPropertiesToAdd); err != nil {
		return nil, err
	}

	if in.TLSInterceptProperties != nil {
		if err := validateTLSIntercept(in.TLSInterceptProperties); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("UpdateProxy")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepProxiesLocked(now)

	r, err := b.findProxyLocked(arnOrName{in.ProxyArn, in.ProxyName}, now)
	if err != nil {
		return nil, err
	}

	if !r.DeleteUntil.IsZero() {
		return nil, invalidOp("The proxy %s is being deleted", r.Name)
	}

	if err = requireToken(r.UpdateToken, in.UpdateToken); err != nil {
		return nil, err
	}

	if in.NatGatewayID != r.NatGatewayID {
		return nil, invalid("NatGatewayId does not match the proxy's NAT gateway")
	}

	if err = applyListenerChanges(r, in.ListenerPropertiesToAdd, in.ListenerPropertiesToRemove); err != nil {
		return nil, err
	}

	if in.TLSInterceptProperties != nil {
		r.TLS = in.TLSInterceptProperties
	}

	r.UpdateTime = now
	r.ModifyUntil = b.deadline(now)
	r.UpdateToken = newToken()

	return &ProxyOutput{Proxy: r.view(now), UpdateToken: r.UpdateToken}, nil
}

// DeleteProxy starts proxy deletion; it disappears once the DETACHING window passes.
func (b *InMemoryBackend) DeleteProxy(_ context.Context, in *DeleteProxyInput) (*DeleteProxyOutput, error) {
	b.mu.Lock("DeleteProxy")
	defer b.mu.Unlock()

	now := time.Now()
	b.sweepProxiesLocked(now)

	r, err := b.findProxyLocked(arnOrName{in.ProxyArn, in.ProxyName}, now)
	if err != nil {
		return nil, err
	}

	if in.NatGatewayID != r.NatGatewayID {
		return nil, invalid("NatGatewayId does not match the proxy's NAT gateway")
	}

	if r.DeleteUntil.IsZero() {
		r.DeleteUntil = b.deadline(now)
	}

	out := &DeleteProxyOutput{NatGatewayID: r.NatGatewayID, ProxyArn: r.ARN, ProxyName: r.Name}

	if b.delay <= 0 {
		b.proxies.Delete(r.ARN)
	}

	return out, nil
}
