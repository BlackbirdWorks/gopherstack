package ociobjectstorage

import (
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	peActive          = "ACTIVE"
	maxPrivateEnds    = 10
	maxAccessTargets  = 10
	maxSecurityAttrs  = 3
	maxPrefixLen      = 63
	wildcard          = "*"
	peEntityType      = "privateendpoint"
	peFqdnSuffix      = ".oci.customer-oci.com"
	opTypePECreate    = "PRIVATE_ENDPOINT_CREATE"
	opTypePEUpdate    = "PRIVATE_ENDPOINT_UPDATE"
	opTypePEDelete    = "PRIVATE_ENDPOINT_DELETE"
	actionCreated     = "CREATED"
	actionUpdated     = "UPDATED"
	actionDeleted     = "DELETED"
	codeAlreadyExists = "NotAuthorizedOrResourceAlreadyExists"
)

var (
	peNameRe   = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)
	prefixRe   = regexp.MustCompile(`^[A-Za-z0-9]+$`)
	ocidPartRe = regexp.MustCompile(`^[^\s*]+$`)
)

// SubnetInfo is the part of an OCI Core subnet a private endpoint needs.
type SubnetInfo struct {
	VcnID         string
	AD            string
	CompartmentID string
}

// NicRequest asks the network service for the VNIC that backs a private endpoint.
type NicRequest struct {
	CompartmentID string
	AD            string
	SubnetID      string
	DisplayName   string
	IPAddress     string
	NsgIDs        []string
}

// Nic is the VNIC and private IP created for a private endpoint.
type Nic struct {
	VnicID    string
	IPAddress string
}

// Network is the in-process view of OCI Core that private endpoints depend on.
type Network struct {
	Subnet    func(id string) (SubnetInfo, bool)
	CreateNic func(NicRequest) (Nic, error)
	DeleteNic func(vnicID string)
}

// SetNetwork wires private endpoints to an OCI Core VCN/subnet source.
func (b *InMemoryBackend) SetNetwork(n Network) {
	b.mu.Lock("SetNetwork")
	b.net = &n
	b.mu.Unlock()
}

// AccessTarget is one namespace/compartment/bucket triple a private endpoint may reach.
type AccessTarget struct {
	Namespace     string `json:"namespace"`
	CompartmentID string `json:"compartmentId"`
	Bucket        string `json:"bucket"`
}

// PrivateEndpoint is a stored private endpoint.
type PrivateEndpoint struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeModified       time.Time                 `json:"timeModified"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
	ID                 string                    `json:"id"`
	Name               string                    `json:"name"`
	CompartmentID      string                    `json:"compartmentId"`
	CreatedBy          string                    `json:"createdBy"`
	SubnetID           string                    `json:"subnetId"`
	VcnID              string                    `json:"vcnId"`
	IP                 string                    `json:"privateEndpointIp"`
	VnicID             string                    `json:"vnicId"`
	Prefix             string                    `json:"prefix"`
	Etag               string                    `json:"etag"`
	AdditionalPrefixes []string                  `json:"additionalPrefixes,omitempty"`
	NsgIDs             []string                  `json:"nsgIds,omitempty"`
	AccessTargets      []AccessTarget            `json:"accessTargets"`
}

// PrefixFqdnsWire is the PrefixFqdns body.
type PrefixFqdnsWire struct {
	ObjectStorageAPIFqdn   string `json:"objectStorageApiFqdn"`
	S3CompatibilityAPIFqdn string `json:"s3CompatibilityApiFqdn"`
	SwiftAPIFqdn           string `json:"swiftApiFqdn"`
}

// FqdnsWire is the Fqdns body.
type FqdnsWire struct {
	AdditionalPrefixesFqdns map[string]PrefixFqdnsWire `json:"additionalPrefixesFqdns"`
	PrefixFqdns             PrefixFqdnsWire            `json:"prefixFqdns"`
}

// PrivateEndpointWire is the PrivateEndpoint / PrivateEndpointSummary JSON body.
type PrivateEndpointWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeModified       time.Time                 `json:"timeModified"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
	Fqdns              FqdnsWire                 `json:"fqdns"`
	ID                 string                    `json:"id,omitempty"`
	Name               string                    `json:"name"`
	Namespace          string                    `json:"namespace"`
	CompartmentID      string                    `json:"compartmentId"`
	CreatedBy          string                    `json:"createdBy"`
	SubnetID           string                    `json:"subnetId,omitempty"`
	PrivateEndpointIP  string                    `json:"privateEndpointIp,omitempty"`
	Prefix             string                    `json:"prefix"`
	Etag               string                    `json:"etag"`
	LifecycleState     string                    `json:"lifecycleState"`
	AdditionalPrefixes []string                  `json:"additionalPrefixes,omitempty"`
	NsgIDs             []string                  `json:"nsgIds,omitempty"`
	AccessTargets      []AccessTarget            `json:"accessTargets,omitempty"`
}

func (b *InMemoryBackend) peFqdns(p *PrivateEndpoint) FqdnsWire {
	one := func(prefix string) PrefixFqdnsWire {
		host := strs.Fold(prefix) + "-" + b.namespace
		tail := ".objectstorage." + Region + peFqdnSuffix

		return PrefixFqdnsWire{
			ObjectStorageAPIFqdn:   host + ".private" + tail,
			S3CompatibilityAPIFqdn: host + ".private.compat" + tail,
			SwiftAPIFqdn:           host + ".private.swift" + tail,
		}
	}

	out := FqdnsWire{PrefixFqdns: one(p.Prefix), AdditionalPrefixesFqdns: map[string]PrefixFqdnsWire{}}
	for _, a := range p.AdditionalPrefixes {
		out.AdditionalPrefixesFqdns[a] = one(a)
	}

	return out
}

func (b *InMemoryBackend) peWire(p *PrivateEndpoint, summary bool) PrivateEndpointWire {
	w := PrivateEndpointWire{
		Name: p.Name, Namespace: b.namespace, CompartmentID: p.CompartmentID, CreatedBy: p.CreatedBy,
		TimeCreated: p.TimeCreated, TimeModified: p.TimeModified, Prefix: p.Prefix, Fqdns: b.peFqdns(p),
		Etag: p.Etag, LifecycleState: peActive,
	}
	if summary {
		return w
	}

	w.ID, w.SubnetID, w.PrivateEndpointIP = p.ID, p.SubnetID, p.IP
	w.AdditionalPrefixes, w.NsgIDs, w.AccessTargets = p.AdditionalPrefixes, p.NsgIDs, p.AccessTargets
	w.FreeformTags, w.DefinedTags, w.SecurityAttributes = p.FreeformTags, p.DefinedTags, p.SecurityAttributes

	return w
}

func errPEAlreadyExists(what string) *APIError {
	return newErr(http.StatusConflict, codeAlreadyExists,
		"A private endpoint with "+what+" already exists, or you are not authorized to create it")
}

func errPENotFound(name string) *APIError {
	return newErr(http.StatusNotFound, "NotAuthorizedOrNotFound",
		"Authorization failed or requested resource ("+name+") not found.")
}

func validAccessPart(v string) bool { return v == wildcard || ocidPartRe.MatchString(v) }

func validateAccessTargets(ts []AccessTarget) error {
	if len(ts) == 0 {
		return errMissing("accessTargets must contain at least one target")
	}

	if len(ts) > maxAccessTargets {
		return errParam("A private endpoint can have at most 10 access targets")
	}

	for _, t := range ts {
		if t.Namespace == "" || t.CompartmentID == "" || t.Bucket == "" {
			return errMissing("each access target needs namespace, compartmentId and bucket")
		}

		if !validAccessPart(t.Namespace) || !validAccessPart(t.CompartmentID) || !validAccessPart(t.Bucket) {
			return errParam("access target values must be a literal or the wildcard '*', not a pattern")
		}

		if t.Namespace == wildcard && (t.CompartmentID != wildcard || t.Bucket != wildcard) {
			return errParam("A wildcard namespace requires the compartment and the bucket to be wildcards too")
		}
	}

	return nil
}

func validPrefix(p string) bool { return len(p) <= maxPrefixLen && prefixRe.MatchString(p) }

func validateSecurityAttrs(sa map[string]map[string]any) error {
	n := 0
	for _, attrs := range sa {
		n += len(attrs)
	}

	if n > maxSecurityAttrs {
		return errParam("A private endpoint can have at most 3 security attributes")
	}

	return nil
}

// CreatePrivateEndpointInput is the CreatePrivateEndpointDetails payload.
type CreatePrivateEndpointInput struct {
	FreeformTags       map[string]string         `json:"freeformTags"`
	DefinedTags        map[string]map[string]any `json:"definedTags"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	Name               string                    `json:"name"`
	CompartmentID      string                    `json:"compartmentId"`
	SubnetID           string                    `json:"subnetId"`
	Prefix             string                    `json:"prefix"`
	PrivateEndpointIP  string                    `json:"privateEndpointIp"`
	AdditionalPrefixes []string                  `json:"additionalPrefixes"`
	NsgIDs             []string                  `json:"nsgIds"`
	AccessTargets      []AccessTarget            `json:"accessTargets"`
}

func (in *CreatePrivateEndpointInput) validate() error {
	switch {
	case in.Name == "":
		return errMissing("name is required")
	case in.CompartmentID == "":
		return errMissing("compartmentId is required")
	case in.SubnetID == "":
		return errMissing("subnetId is required")
	case in.Prefix == "":
		return errMissing("prefix is required")
	case len(in.Name) > maxBucketName || !peNameRe.MatchString(in.Name):
		return errParam("name may contain letters, numbers, hyphens, underscores and periods")
	}

	for _, p := range append([]string{in.Prefix}, in.AdditionalPrefixes...) {
		if !validPrefix(p) {
			return errParam("prefix '" + p + "' must be 1-63 letters or numbers")
		}
	}

	if err := validateSecurityAttrs(in.SecurityAttributes); err != nil {
		return err
	}

	return validateAccessTargets(in.AccessTargets)
}

// prefixTaken reports whether a prefix is already used by another endpoint in the same VCN.
func (b *InMemoryBackend) prefixTaken(vcn string, prefixes []string, except *PrivateEndpoint) bool {
	for _, p := range b.privateEnds {
		if p == except || p.VcnID != vcn {
			continue
		}

		for _, have := range append([]string{p.Prefix}, p.AdditionalPrefixes...) {
			if strs.ContainsFold(prefixes, have) {
				return true
			}
		}
	}

	return false
}

func (b *InMemoryBackend) peByName(name string) (*PrivateEndpoint, bool) {
	p, ok := b.privateEnds[strs.Fold(name)]

	return p, ok
}

func (b *InMemoryBackend) newPEWorkRequest(op, action string, p *PrivateEndpoint) string {
	now := b.nowUTC()
	wr := &WorkRequest{
		ID: newWorkRequestID(), OperationType: op, Status: wrCompleted, CompartmentID: p.CompartmentID,
		TimeAccepted: now, TimeFinished: now, EntityURI: "/n/" + b.namespace + "/pe/" + p.Name,
		EntityType: peEntityType, ActionType: action, EntityID: p.ID,
	}
	b.workRequests[wr.ID] = wr

	return wr.ID
}

func (b *InMemoryBackend) createNic(in CreatePrivateEndpointInput) (Nic, SubnetInfo, error) {
	if b.net == nil || b.net.Subnet == nil || b.net.CreateNic == nil {
		return Nic{}, SubnetInfo{}, errRelatedNotFound(in.SubnetID)
	}

	sub, ok := b.net.Subnet(in.SubnetID)
	if !ok {
		return Nic{}, SubnetInfo{}, errRelatedNotFound(in.SubnetID)
	}

	nic, err := b.net.CreateNic(NicRequest{
		CompartmentID: in.CompartmentID, AD: sub.AD, SubnetID: in.SubnetID, DisplayName: "pe-" + in.Name,
		IPAddress: in.PrivateEndpointIP, NsgIDs: in.NsgIDs,
	})
	if err != nil {
		if ae, isAPI := err.(*APIError); isAPI { //nolint:errorlint // own type, never wrapped
			return Nic{}, SubnetInfo{}, ae
		}

		return Nic{}, SubnetInfo{}, errParam(err.Error())
	}

	return nic, sub, nil
}

// CreatePrivateEndpoint creates a private endpoint backed by a VNIC in the subnet and returns its work request id.
func (b *InMemoryBackend) CreatePrivateEndpoint(ns string, in CreatePrivateEndpointInput, user string) (string, error) {
	if err := in.validate(); err != nil {
		return "", err
	}

	b.mu.Lock("CreatePrivateEndpoint")
	defer b.mu.Unlock()

	if err := b.checkNS(ns); err != nil {
		return "", err
	}

	if err := b.checkTags(in.FreeformTags, in.DefinedTags); err != nil {
		return "", err
	}

	if _, dup := b.peByName(in.Name); dup {
		return "", errPEAlreadyExists("the name '" + in.Name + "'")
	}

	if len(b.privateEnds) >= maxPrivateEnds {
		return "", newErr(http.StatusBadRequest, "LimitExceeded",
			"The tenancy has reached the limit of 10 private endpoints")
	}

	nic, sub, err := b.createNic(in)
	if err != nil {
		return "", err
	}

	if b.prefixTaken(sub.VcnID, append([]string{in.Prefix}, in.AdditionalPrefixes...), nil) {
		b.net.DeleteNic(nic.VnicID)

		return "", errPEAlreadyExists("the prefix '" + in.Prefix + "' in this VCN")
	}

	if user == "" {
		user = anonymousUser
	}

	now := b.nowUTC()
	p := &PrivateEndpoint{
		ID:   "ocid1.objectstorageprivateendpoint.oc1." + Region + "." + awsid.NewN("", ocidHexLen),
		Name: in.Name, CompartmentID: in.CompartmentID, CreatedBy: user, SubnetID: in.SubnetID, VcnID: sub.VcnID,
		IP: nic.IPAddress, VnicID: nic.VnicID, Prefix: in.Prefix, Etag: newEtag(), TimeCreated: now,
		TimeModified: now, AdditionalPrefixes: in.AdditionalPrefixes, NsgIDs: in.NsgIDs,
		AccessTargets: in.AccessTargets, FreeformTags: in.FreeformTags, DefinedTags: in.DefinedTags,
		SecurityAttributes: in.SecurityAttributes,
	}
	b.privateEnds[strs.Fold(p.Name)] = p

	return b.newPEWorkRequest(opTypePECreate, actionCreated, p), nil
}

// GetPrivateEndpoint returns one private endpoint.
func (b *InMemoryBackend) GetPrivateEndpoint(ns, name string, pre Preconds) (PrivateEndpointWire, error) {
	b.mu.RLock("GetPrivateEndpoint")
	defer b.mu.RUnlock()

	if err := b.checkNS(ns); err != nil {
		return PrivateEndpointWire{}, err
	}

	p, ok := b.peByName(name)
	if !ok {
		return PrivateEndpointWire{}, errPENotFound(name)
	}

	if err := pre.check(p.Etag, true, true); err != nil {
		return PrivateEndpointWire{}, err
	}

	return b.peWire(p, false), nil
}

// ListPrivateEndpoints lists the private endpoints of a compartment, optionally filtered by lifecycle state.
func (b *InMemoryBackend) ListPrivateEndpoints(
	ns, compartment, state, token string, limit int,
) (page.Page[PrivateEndpointWire], error) {
	if compartment == "" {
		return page.Page[PrivateEndpointWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListPrivateEndpoints")
	defer b.mu.RUnlock()

	if err := b.checkNS(ns); err != nil {
		return page.Page[PrivateEndpointWire]{}, err
	}

	var ps []*PrivateEndpoint

	for _, p := range b.privateEnds {
		if p.CompartmentID == compartment && (state == "" || strings.EqualFold(state, peActive)) {
			ps = append(ps, p)
		}
	}

	sort.Slice(ps, func(i, j int) bool { return ps[i].Name < ps[j].Name })

	all := make([]PrivateEndpointWire, len(ps))
	for i, p := range ps {
		all[i] = b.peWire(p, true)
	}

	return page.New(all, token, limit, defaultListLimit), nil
}

// UpdatePrivateEndpointInput is the UpdatePrivateEndpointDetails payload; absent fields are left unchanged.
type UpdatePrivateEndpointInput struct {
	FreeformTags       map[string]string         `json:"freeformTags"`
	DefinedTags        map[string]map[string]any `json:"definedTags"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes"`
	Name               *string                   `json:"name"`
	Namespace          *string                   `json:"namespace"`
	AccessTargets      []AccessTarget            `json:"accessTargets"`
}

func (b *InMemoryBackend) applyPEUpdate(p *PrivateEndpoint, in UpdatePrivateEndpointInput) error {
	if in.Namespace != nil && *in.Namespace != "" {
		if err := b.checkNS(*in.Namespace); err != nil {
			return err
		}
	}

	if err := b.checkTags(in.FreeformTags, in.DefinedTags); err != nil {
		return err
	}

	if err := validateSecurityAttrs(in.SecurityAttributes); err != nil {
		return err
	}

	if in.AccessTargets != nil {
		if err := validateAccessTargets(in.AccessTargets); err != nil {
			return err
		}
	}

	if in.Name != nil && *in.Name != p.Name {
		if len(*in.Name) > maxBucketName || !peNameRe.MatchString(*in.Name) {
			return errParam("name may contain letters, numbers, hyphens, underscores and periods")
		}

		if other, dup := b.peByName(*in.Name); dup && other != p {
			return errPEAlreadyExists("the name '" + *in.Name + "'")
		}
	}

	return nil
}

// UpdatePrivateEndpoint replaces the given fields of a private endpoint and returns its work request id.
func (b *InMemoryBackend) UpdatePrivateEndpoint(
	ns, name string, in UpdatePrivateEndpointInput, pre Preconds,
) (string, error) {
	b.mu.Lock("UpdatePrivateEndpoint")
	defer b.mu.Unlock()

	if err := b.checkNS(ns); err != nil {
		return "", err
	}

	p, ok := b.peByName(name)
	if !ok {
		return "", errPENotFound(name)
	}

	if err := pre.check(p.Etag, true, false); err != nil {
		return "", err
	}

	if err := b.applyPEUpdate(p, in); err != nil {
		return "", err
	}

	if in.Name != nil && *in.Name != p.Name {
		delete(b.privateEnds, strs.Fold(p.Name))
		p.Name = *in.Name
		b.privateEnds[strs.Fold(p.Name)] = p
	}

	if in.AccessTargets != nil {
		p.AccessTargets = slices.Clone(in.AccessTargets)
	}

	if in.FreeformTags != nil {
		p.FreeformTags = in.FreeformTags
	}

	if in.DefinedTags != nil {
		p.DefinedTags = in.DefinedTags
	}

	if in.SecurityAttributes != nil {
		p.SecurityAttributes = in.SecurityAttributes
	}

	p.Etag, p.TimeModified = newEtag(), b.nowUTC()

	return b.newPEWorkRequest(opTypePEUpdate, actionUpdated, p), nil
}

// DeletePrivateEndpoint removes a private endpoint and its VNIC and returns its work request id.
func (b *InMemoryBackend) DeletePrivateEndpoint(ns, name string, pre Preconds) (string, error) {
	b.mu.Lock("DeletePrivateEndpoint")
	defer b.mu.Unlock()

	if err := b.checkNS(ns); err != nil {
		return "", err
	}

	p, ok := b.peByName(name)
	if !ok {
		return "", errPENotFound(name)
	}

	if err := pre.check(p.Etag, true, false); err != nil {
		return "", err
	}

	if b.net != nil && b.net.DeleteNic != nil && p.VnicID != "" {
		b.net.DeleteNic(p.VnicID)
	}

	delete(b.privateEnds, strs.Fold(p.Name))

	return b.newPEWorkRequest(opTypePEDelete, actionDeleted, p), nil
}
