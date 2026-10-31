package ociloadbalancer

import (
	"crypto/tls"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	minFlexMbps = 10
	maxFlexMbps = 8000
	maxSubnets  = 2
	minSharing  = 2
)

type createLBIn struct {
	Tags
	ShapeDetails    *ShapeDetails              `json:"shapeDetails"`
	IsPrivate       *bool                      `json:"isPrivate"`
	DeleteProtected *bool                      `json:"isDeleteProtectionEnabled"`
	RequestIDOn     *bool                      `json:"isRequestIdEnabled"`
	Listeners       map[string]*listenerIn     `json:"listeners"`
	Hostnames       map[string]*hostnameIn     `json:"hostnames"`
	BackendSets     map[string]*backendSetIn   `json:"backendSets"`
	Certificates    map[string]*certIn         `json:"certificates"`
	SSLCipherSuites map[string]*cipherSuiteIn  `json:"sslCipherSuites"`
	PathRouteSets   map[string]*pathRouteSetIn `json:"pathRouteSets"`
	RuleSets        map[string]*ruleSetIn      `json:"ruleSets"`
	CompartmentID   string                     `json:"compartmentId"`
	DisplayName     string                     `json:"displayName"`
	ShapeName       string                     `json:"shapeName"`
	IPMode          string                     `json:"ipMode"`
	Ipv6SubnetCidr  string                     `json:"ipv6SubnetCidr"`
	RequestIDHeader string                     `json:"requestIdHeader"`
	SubnetIDs       []string                   `json:"subnetIds"`
	NSGs            []string                   `json:"networkSecurityGroupIds"`
	ReservedIPs     []ReservedIP               `json:"reservedIps"`
}

type updateLBIn struct {
	Tags
	DisplayName     *string      `json:"displayName"`
	DeleteProtected *bool        `json:"isDeleteProtectionEnabled"`
	RequestIDOn     *bool        `json:"isRequestIdEnabled"`
	RequestIDHeader *string      `json:"requestIdHeader"`
	IPMode          string       `json:"ipMode"`
	Ipv6SubnetCidr  *string      `json:"ipv6SubnetCidr"`
	ReservedIPs     []ReservedIP `json:"reservedIps"`
}

type updateShapeIn struct {
	ShapeDetails *ShapeDetails `json:"shapeDetails"`
	ShapeName    string        `json:"shapeName"`
}

type nsgIn struct {
	NSGs []string `json:"networkSecurityGroupIds"`
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func checkShape(name string, d *ShapeDetails) *APIError {
	if name == "" {
		return errMissing("shapeName is required")
	}

	if !slices.Contains(shapeNames, name) {
		return errParam("shapeName must be one of " + strings.Join(shapeNames, ", "))
	}

	if name != shapeFlexible {
		if d != nil {
			return errParam("shapeDetails applies to the flexible shape only")
		}

		return nil
	}

	if d == nil {
		return errMissing("shapeDetails is required for the flexible shape")
	}

	if d.Minimum < minFlexMbps || d.Maximum > maxFlexMbps || d.Minimum > d.Maximum {
		return errParam(
			"flexible shape bandwidth must satisfy 10 <= minimumBandwidthInMbps <= maximumBandwidthInMbps <= 8000",
		)
	}

	return nil
}

func (b *InMemoryBackend) checkSubnets(ids []string, private bool) (string, *APIError) {
	if len(ids) == 0 {
		return "", errMissing("subnetIds is required")
	}

	if len(ids) > maxSubnets || (private && len(ids) > 1) {
		return "", errParam("a public load balancer takes one or two subnets and a private one takes one")
	}

	cidr := ""

	for _, id := range ids {
		if b.subnet == nil {
			continue
		}

		c, ok := b.subnet(id)
		if !ok {
			return "", errNotFound(id)
		}

		if cidr == "" {
			cidr = c
		}
	}

	return cidr, nil
}

func (b *InMemoryBackend) validateCreate(in *createLBIn) (string, *APIError) {
	if err := b.checkCompartment(in.CompartmentID); err != nil {
		return "", err
	}

	if in.DisplayName == "" {
		return "", errMissing("displayName is required")
	}

	if err := checkShape(in.ShapeName, in.ShapeDetails); err != nil {
		return "", err
	}

	if len(in.ReservedIPs) > 0 {
		return "", errUnsupported("reservedIps")
	}

	if in.IPMode != "" && in.IPMode != "IPV4" {
		return "", errUnsupported("ipMode " + in.IPMode)
	}

	if in.Ipv6SubnetCidr != "" {
		return "", errUnsupported("ipv6SubnetCidr")
	}

	return b.checkSubnets(in.SubnetIDs, boolOr(in.IsPrivate))
}

func (b *InMemoryBackend) buildNested(lb *LoadBalancer, in *createLBIn) *APIError {
	for name, c := range in.Certificates {
		c.Name = name

		cert, key, err := buildCertificate(c)
		if err != nil {
			return err
		}

		lb.Certificates[name] = cert

		if key != nil {
			b.keys[lb.ID+"/"+name] = key
		}
	}

	return buildNestedConfig(lb, in)
}

func buildNestedConfig(lb *LoadBalancer, in *createLBIn) *APIError {
	for name, s := range in.SSLCipherSuites {
		cs, err := buildCipherSuite(name, s)
		if err != nil {
			return err
		}

		lb.SSLCipherSuites[name] = cs
	}

	for name, h := range in.Hostnames {
		if err := checkName("hostname", name); err != nil {
			return err
		}

		if err := checkHostname(h.Hostname); err != nil {
			return err
		}

		lb.Hostnames[name] = &Hostname{Name: name, Hostname: h.Hostname}
	}

	for name, s := range in.BackendSets {
		bs, err := buildBackendSet(name, s)
		if err != nil {
			return err
		}

		lb.BackendSets[name] = bs
	}

	for name, s := range in.PathRouteSets {
		ps, err := buildPathRouteSet(name, s)
		if err != nil {
			return err
		}

		lb.PathRouteSets[name] = ps
	}

	for name, s := range in.RuleSets {
		rs, err := buildRuleSet(name, s)
		if err != nil {
			return err
		}

		lb.RuleSets[name] = rs
	}

	for name, l := range in.Listeners {
		li, err := buildListener(name, l)
		if err != nil {
			return err
		}

		lb.Listeners[name] = li
	}

	return nil
}

func newLB() *LoadBalancer {
	return &LoadBalancer{
		Listeners:       map[string]*Listener{},
		Hostnames:       map[string]*Hostname{},
		SSLCipherSuites: map[string]*SSLCipherSuite{},
		Certificates:    map[string]*Certificate{},
		BackendSets:     map[string]*BackendSet{},
		PathRouteSets:   map[string]*PathRouteSet{},
		RuleSets:        map[string]*RuleSet{},
		RoutingPolicies: map[string]*RoutingPolicy{},
	}
}

// CreateLoadBalancer creates a load balancer in CREATING state and returns its work request id.
func (b *InMemoryBackend) CreateLoadBalancer(in createLBIn) (string, error) {
	b.mu.Lock("CreateLoadBalancer")
	wr, err := b.createLB(&in)
	b.mu.Unlock()

	if err != nil {
		return "", err
	}

	b.dp.sync()

	return wr, nil
}

func (b *InMemoryBackend) createLB(in *createLBIn) (string, error) {
	cidr, err := b.validateCreate(in)
	if err != nil {
		return "", err
	}

	now := b.nowUTC()
	lb := newLB()
	lb.ID, lb.CompartmentID, lb.DisplayName = newOCID("loadbalancer"), in.CompartmentID, in.DisplayName
	lb.ShapeName, lb.ShapeDetails, lb.SubnetIDs, lb.NSGs = in.ShapeName, in.ShapeDetails, in.SubnetIDs, in.NSGs
	lb.IsPrivate, lb.DeleteProtected, lb.RequestIDOn = boolOr(
		in.IsPrivate,
	), boolOr(
		in.DeleteProtected,
	), boolOr(
		in.RequestIDOn,
	)
	lb.RequestIDHeader, lb.Tags, lb.State = in.RequestIDHeader, in.Tags, stateCreating
	lb.TimeCreated, lb.TransitionAt, lb.Etag = now, b.transitionAt(), newEtag()

	if lb.RequestIDOn && lb.RequestIDHeader == "" {
		lb.RequestIDHeader = defaultRequestIDHeader
	}

	if perr := b.buildNested(lb, in); perr != nil {
		b.dropKeys(lb.ID)

		return "", perr
	}

	if verr := b.validateLB(lb); verr != nil {
		b.dropKeys(lb.ID)

		return "", verr
	}

	lb.IPAddresses = []IPAddress{{IPAddress: b.allocIP(cidrFor(lb, cidr)), IsPublic: !lb.IsPrivate}}
	lb.BusyUntil = *lb.TransitionAt
	b.lbs[lb.ID] = lb

	return b.newWR(lb, "CreateLoadBalancer", *lb.TransitionAt), nil
}

func cidrFor(lb *LoadBalancer, cidr string) string {
	if lb.IsPrivate {
		return cidr
	}

	return ""
}

// Mutate applies fn to a copy of the load balancer, validates the result, publishes it and returns the
// id of the work request that tracks the change.
func (b *InMemoryBackend) Mutate(
	id, ifMatch, wrType string, refConflict bool, fn func(lb *LoadBalancer) *APIError,
) (string, error) {
	b.mu.Lock("Mutate")
	wr, err := b.mutate(id, ifMatch, wrType, refConflict, fn)
	b.reapKeys(id)
	b.mu.Unlock()

	if err != nil {
		return "", err
	}

	b.dp.sync()

	return wr, nil
}

func (b *InMemoryBackend) liveLB(id string) (*LoadBalancer, *APIError) {
	lb, ok := b.lbs[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return lb, nil
}

func (b *InMemoryBackend) mutate(
	id, ifMatch, wrType string, refConflict bool, fn func(lb *LoadBalancer) *APIError,
) (string, error) {
	lb, err := b.liveLB(id)
	if err != nil {
		return "", err
	}

	if lb.State == stateDeleted {
		return "", errNotFound(id)
	}

	if eerr := checkEtag(lb.Etag, ifMatch); eerr != nil {
		return "", eerr
	}

	if lb.State != stateActive {
		return "", errState("load balancer " + id + " is in state " + lb.State)
	}

	now := b.nowUTC()
	if lb.BusyUntil.After(now) {
		return "", errConflict("another operation is in progress on load balancer " + id)
	}

	next := lb.clone()
	if ferr := fn(next); ferr != nil {
		return "", ferr
	}

	if verr := b.validateLB(next); verr != nil {
		if refConflict {
			return "", errConflict(verr.Message)
		}

		return "", verr
	}

	finish := now.Add(b.transition)
	next.Etag, next.BusyUntil = newEtag(), finish
	b.lbs[id] = next

	return b.newWR(next, wrType, finish), nil
}

// validateLB checks every cross-reference of a load balancer.
func (b *InMemoryBackend) validateLB(lb *LoadBalancer) *APIError {
	if err := validateSets(lb); err != nil {
		return err
	}

	if err := validateRoutes(lb); err != nil {
		return err
	}

	for _, l := range lb.Listeners {
		if err := validateListenerRefs(lb, l); err != nil {
			return err
		}
	}

	return checkPortSharing(lb)
}

func validateSets(lb *LoadBalancer) *APIError {
	for name, bs := range lb.BackendSets {
		if bs.SSL != nil {
			if err := checkBackendSSL(lb, bs.SSL); err != nil {
				return err
			}
		}

		if bs.Name != name {
			return errParam("backend set name mismatch for " + name)
		}
	}

	return nil
}

func validateRoutes(lb *LoadBalancer) *APIError {
	for _, ps := range lb.PathRouteSets {
		for _, r := range ps.PathRoutes {
			if _, ok := lb.BackendSets[r.BackendSetName]; !ok {
				return errParam("path route set " + ps.Name + " references unknown backend set " + r.BackendSetName)
			}
		}
	}

	for _, rp := range lb.RoutingPolicies {
		for _, r := range rp.Rules {
			if _, ok := lb.BackendSets[r.Actions[0].BackendSetName]; !ok {
				return errParam(
					"routing policy " + rp.Name + " references unknown backend set " + r.Actions[0].BackendSetName,
				)
			}
		}
	}

	return nil
}

func checkBackendSSL(lb *LoadBalancer, s *SSLConfig) *APIError {
	if len(s.CertificateIDs) > 0 || len(s.TrustedCertificateAuthorityIDs) > 0 {
		return errUnsupported("certificateIds and trustedCertificateAuthorityIds (OCI Certificates service)")
	}

	if err := checkTLSVersions(s); err != nil {
		return err
	}

	if s.CertificateName != "" {
		if _, ok := lb.Certificates[s.CertificateName]; !ok {
			return errParam("backend set references unknown certificate " + s.CertificateName)
		}
	}

	if boolOr(s.VerifyPeerCertificate) && s.CertificateName == "" {
		return errParam("verifyPeerCertificate requires a certificateName bundle holding the CA to trust")
	}

	return checkCipherRef(lb, s.CipherSuiteName)
}

func checkCipherRef(lb *LoadBalancer, name string) *APIError {
	if name == "" || isBuiltinSuite(name) {
		return nil
	}

	if _, ok := lb.SSLCipherSuites[name]; !ok {
		return errParam("unknown cipher suite " + name)
	}

	return nil
}

func validateListenerRefs(lb *LoadBalancer, l *Listener) *APIError {
	if _, ok := lb.BackendSets[l.DefaultSet]; !ok {
		return errParam("listener " + l.Name + " references unknown backend set " + l.DefaultSet)
	}

	for _, h := range l.Hostnames {
		if _, ok := lb.Hostnames[h]; !ok {
			return errParam("listener " + l.Name + " references unknown hostname " + h)
		}
	}

	if l.PathRoutes != "" {
		if _, ok := lb.PathRouteSets[l.PathRoutes]; !ok {
			return errParam("listener " + l.Name + " references unknown path route set " + l.PathRoutes)
		}
	}

	if l.Policy != "" {
		if _, ok := lb.RoutingPolicies[l.Policy]; !ok {
			return errParam("listener " + l.Name + " references unknown routing policy " + l.Policy)
		}
	}

	for _, r := range l.RuleSets {
		if _, ok := lb.RuleSets[r]; !ok {
			return errParam("listener " + l.Name + " references unknown rule set " + r)
		}
	}

	if l.SSL != nil {
		if _, ok := lb.Certificates[l.SSL.CertificateName]; !ok {
			return errParam("listener " + l.Name + " references unknown certificate " + l.SSL.CertificateName)
		}

		return checkCipherRef(lb, l.SSL.CipherSuiteName)
	}

	return nil
}

// checkPortSharing lets several listeners share a port only when each is bound to disjoint virtual hostnames.
func checkPortSharing(lb *LoadBalancer) *APIError {
	byPort := map[int][]*Listener{}
	for _, l := range lb.Listeners {
		byPort[l.Port] = append(byPort[l.Port], l)
	}

	for port, ls := range byPort {
		if len(ls) < minSharing {
			continue
		}

		seen := map[string]bool{}

		for _, l := range ls {
			if len(l.Hostnames) == 0 || (l.Protocol == protoTCP) != (ls[0].Protocol == protoTCP) ||
				(l.SSL == nil) != (ls[0].SSL == nil) {
				return errConflict("port " + itoa(port) + " is already used by another listener")
			}

			for _, h := range l.Hostnames {
				name := strs.Fold(lb.Hostnames[h].Hostname)
				if seen[name] {
					return errConflict("hostname " + name + " is already bound to a listener on port " + itoa(port))
				}

				seen[name] = true
			}
		}
	}

	return nil
}

// UpdateLoadBalancer changes mutable load balancer properties.
func (b *InMemoryBackend) UpdateLoadBalancer(id, ifMatch string, in updateLBIn) (string, error) {
	return b.Mutate(id, ifMatch, "UpdateLoadBalancer", false, func(lb *LoadBalancer) *APIError {
		if in.IPMode != "" && in.IPMode != "IPV4" || in.Ipv6SubnetCidr != nil || len(in.ReservedIPs) > 0 {
			return errUnsupported("ipMode, ipv6SubnetCidr and reservedIps changes")
		}

		if in.DisplayName != nil {
			if *in.DisplayName == "" {
				return errParam("displayName cannot be empty")
			}

			lb.DisplayName = *in.DisplayName
		}

		if in.DeleteProtected != nil {
			lb.DeleteProtected = *in.DeleteProtected
		}

		if in.RequestIDOn != nil {
			lb.RequestIDOn = *in.RequestIDOn
		}

		if in.RequestIDHeader != nil {
			lb.RequestIDHeader = *in.RequestIDHeader
		}

		if lb.RequestIDOn && lb.RequestIDHeader == "" {
			lb.RequestIDHeader = defaultRequestIDHeader
		}

		lb.Tags.merge(in.Tags)

		return nil
	})
}

// UpdateShape changes the load balancer shape.
func (b *InMemoryBackend) UpdateShape(id, ifMatch string, in updateShapeIn) (string, error) {
	return b.Mutate(id, ifMatch, "UpdateLoadBalancerShape", false, func(lb *LoadBalancer) *APIError {
		if err := checkShape(in.ShapeName, in.ShapeDetails); err != nil {
			return err
		}

		lb.ShapeName, lb.ShapeDetails = in.ShapeName, in.ShapeDetails

		return nil
	})
}

// UpdateNSGs replaces the network security groups.
func (b *InMemoryBackend) UpdateNSGs(id, ifMatch string, in nsgIn) (string, error) {
	return b.Mutate(id, ifMatch, "UpdateNetworkSecurityGroups", false, func(lb *LoadBalancer) *APIError {
		lb.NSGs = in.NSGs

		return nil
	})
}

// ChangeCompartment moves a load balancer to another compartment.
func (b *InMemoryBackend) ChangeCompartment(id, ifMatch string, in changeCompartmentIn) (string, error) {
	return b.Mutate(id, ifMatch, "ChangeLoadBalancerCompartment", false, func(lb *LoadBalancer) *APIError {
		if err := b.checkCompartment(in.CompartmentID); err != nil {
			return err
		}

		lb.CompartmentID = in.CompartmentID

		return nil
	})
}

// DeleteLoadBalancer starts deleting a load balancer.
func (b *InMemoryBackend) DeleteLoadBalancer(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteLoadBalancer")

	lb, err := b.liveLB(id)
	if err == nil && lb.State == stateDeleted {
		err = errNotFound(id)
	}

	if err == nil {
		err = checkEtag(lb.Etag, ifMatch)
	}

	if err == nil && lb.DeleteProtected {
		err = errConflict("load balancer " + id + " has delete protection enabled")
	}

	if err == nil && lb.State != stateActive {
		err = errState("load balancer " + id + " is in state " + lb.State)
	}

	if err == nil && lb.BusyUntil.After(b.nowUTC()) {
		err = errConflict("another operation is in progress on load balancer " + id)
	}

	var wr string

	if err == nil {
		next := lb.clone()
		next.State, next.TransitionAt, next.Etag = stateDeleting, b.transitionAt(), newEtag()
		next.BusyUntil = *next.TransitionAt
		b.lbs[id] = next
		wr = b.newWR(next, "DeleteLoadBalancer", *next.TransitionAt)
	}

	b.mu.Unlock()

	if err != nil {
		return "", err
	}

	b.dp.sync()

	return wr, nil
}

// LoadBalancerWire is the wire form of a load balancer.
type LoadBalancerWire = LoadBalancer

func (b *InMemoryBackend) view(lb *LoadBalancer, full bool) *LoadBalancerWire {
	c := *lb
	c.Etag, c.BusyUntil, c.TransitionAt = "", time.Time{}, nil

	if !full {
		c.Listeners, c.Hostnames, c.SSLCipherSuites, c.Certificates = nil, nil, nil, nil
		c.BackendSets, c.PathRouteSets, c.RuleSets, c.RoutingPolicies = nil, nil, nil, nil

		return &c
	}

	c.Listeners = make(map[string]*Listener, len(lb.Listeners))
	for n, l := range lb.Listeners {
		lc := *l
		lc.Address = b.dp.address(lb.ID, l.Port)
		c.Listeners[n] = &lc
	}

	return &c
}

// GetLoadBalancer returns one load balancer and its etag.
func (b *InMemoryBackend) GetLoadBalancer(id string) (*LoadBalancerWire, string, error) {
	b.mu.RLock("GetLoadBalancer")
	defer b.mu.RUnlock()

	lb, err := b.liveLB(id)
	if err != nil {
		return nil, "", err
	}

	return b.view(lb, true), lb.Etag, nil
}

// ListLoadBalancers lists the load balancers of a compartment.
func (b *InMemoryBackend) ListLoadBalancers(
	compartment, name, state string, full bool, o ListOpts,
) (page.Page[*LoadBalancerWire], error) {
	if compartment == "" {
		return page.Page[*LoadBalancerWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListLoadBalancers")
	defer b.mu.RUnlock()

	var all []*LoadBalancer

	for _, lb := range b.lbs {
		if lb.CompartmentID != compartment || (name != "" && lb.DisplayName != name) {
			continue
		}

		if (state == "" && lb.State == stateDeleted) || (state != "" && !strings.EqualFold(lb.State, state)) {
			continue
		}

		all = append(all, lb)
	}

	pg := paginate(all, o, func(l *LoadBalancer) string { return l.DisplayName },
		func(l *LoadBalancer) (time.Time, int64) { return l.TimeCreated, 0 })

	out := make([]*LoadBalancerWire, 0, len(pg.Data))
	for _, lb := range pg.Data {
		out = append(out, b.view(lb, full))
	}

	return page.Page[*LoadBalancerWire]{Data: out, Next: pg.Next}, nil
}

// certKey returns the in-memory TLS key pair for a certificate bundle. Callers hold the lock.
func (b *InMemoryBackend) certKey(lbID, name string) *tls.Certificate { return b.keys[lbID+"/"+name] }

// reapKeys drops in-memory TLS keys whose certificate is not part of the stored load balancer.
func (b *InMemoryBackend) reapKeys(lbID string) {
	lb := b.lbs[lbID]

	for k := range b.keys {
		name, ok := strings.CutPrefix(k, lbID+"/")
		if !ok {
			continue
		}

		if lb == nil || lb.Certificates[name] == nil {
			delete(b.keys, k)
		}
	}
}
