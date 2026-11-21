package ociidentity

import (
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	opCreateNetworkSource = "CreateNetworkSource"
	opGetNetworkSource    = "GetNetworkSource"
	opListNetworkSources  = "ListNetworkSources"
	opUpdateNetworkSource = "UpdateNetworkSource"
	opDeleteNetworkSource = "DeleteNetworkSource"
	opGetAuthPolicy       = "GetAuthenticationPolicy"
	opUpdateAuthPolicy    = "UpdateAuthenticationPolicy"
	opCreateRegionSub     = "CreateRegionSubscription"

	maxNetworkSources   = 200
	maxNetworkSourceLen = 100
	maxSourceEntries    = 500
	maxPasswordLength   = 100
)

// VirtualSource is a VCN and the IP ranges allowed from it.
type VirtualSource struct {
	VcnID    string   `json:"vcnId"`
	IPRanges []string `json:"ipRanges"`
}

// NetworkSource is a stored network source.
type NetworkSource struct {
	Meta
	PublicSourceList  []string        `json:"publicSourceList"`
	VirtualSourceList []VirtualSource `json:"virtualSourceList"`
	Services          []string        `json:"services"`
	Internal          Internal        `json:"internal"`
}

// NetworkSourceWire is the network source wire shape.
type NetworkSourceWire struct {
	Meta
	PublicSourceList  []string        `json:"publicSourceList"`
	VirtualSourceList []VirtualSource `json:"virtualSourceList"`
	Services          []string        `json:"services"`
}

// CreateNetworkSourceIn is the CreateNetworkSource request body.
type CreateNetworkSourceIn struct {
	RetryToken string `json:"-"`
	Tags
	CompartmentID     string          `json:"compartmentId"`
	Name              string          `json:"name"`
	Description       string          `json:"description"`
	PublicSourceList  []string        `json:"publicSourceList"`
	VirtualSourceList []VirtualSource `json:"virtualSourceList"`
	Services          []string        `json:"services"`
}

// UpdateNetworkSourceIn is the UpdateNetworkSource request body.
type UpdateNetworkSourceIn struct {
	Tags
	Description       *string         `json:"description"`
	PublicSourceList  []string        `json:"publicSourceList"`
	VirtualSourceList []VirtualSource `json:"virtualSourceList"`
	Services          []string        `json:"services"`
}

func nonNil[T any](s []T) []T {
	if s == nil {
		return []T{}
	}

	return s
}

func (n *NetworkSource) view() NetworkSourceWire {
	return NetworkSourceWire{
		Meta: n.Meta, PublicSourceList: nonNil(n.PublicSourceList), VirtualSourceList: nonNil(n.VirtualSourceList),
		Services: nonNil(n.Services),
	}
}

func validIPOrCIDR(s string) bool {
	if strings.Contains(s, "/") {
		_, err := netip.ParsePrefix(s)

		return err == nil
	}

	a, err := netip.ParseAddr(s)

	return err == nil && a.Zone() == ""
}

func validSources(public []string, virtual []VirtualSource, services []string) *APIError {
	total := len(public)

	if len(services) > maxSourceEntries {
		return errLimit("A network source holds at most 500 services")
	}

	for _, ip := range public {
		if !validIPOrCIDR(ip) {
			return errParam("publicSourceList entry " + ip + " is not an IP address or CIDR range")
		}
	}

	for _, v := range virtual {
		if !strings.HasPrefix(v.VcnID, "ocid1.vcn.") {
			return errParam("virtualSourceList vcnId must be a VCN OCID")
		}

		total += len(v.IPRanges)

		for _, ip := range v.IPRanges {
			if !validIPOrCIDR(ip) {
				return errParam("virtualSourceList ipRanges entry " + ip + " is not an IP address or CIDR range")
			}
		}
	}

	if total > maxSourceEntries || len(virtual) > maxSourceEntries {
		return errLimit("A network source holds at most 500 entries")
	}

	return nil
}

func (b *InMemoryBackend) networkSourceByID(id string) (*NetworkSource, *APIError) {
	n, ok := b.netSources[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return n, nil
}

// CreateNetworkSource creates a network source.
func (b *InMemoryBackend) CreateNetworkSource(in CreateNetworkSourceIn) (NetworkSourceWire, string, error) {
	if aerr := requireTenancy(in.CompartmentID); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if aerr := validateName(in.Name, maxNetworkSourceLen); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if aerr := validateDescription(in.Description); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if aerr := validSources(in.PublicSourceList, in.VirtualSourceList, in.Services); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	b.mu.Lock("CreateNetworkSource")
	defer b.mu.Unlock()

	r, hit, aerr := replayCreate(b, "CreateNetworkSource", in.RetryToken, b.netSources, nil)
	if aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if hit {
		return r.view(), r.Internal.Etag, nil
	}

	if len(b.netSources) >= maxNetworkSources {
		return NetworkSourceWire{}, "", errLimit("The tenancy network source limit (200) has been reached")
	}

	for _, n := range b.netSources {
		if strs.Equal(n.Name, in.Name) {
			return NetworkSourceWire{}, "", errConflict(
				"Conflict",
				"Network source "+in.Name+" already exists in the tenancy",
			)
		}
	}

	n := &NetworkSource{
		Meta:             b.newMeta("networksource", TenancyID, in.Name, in.Description, in.Tags),
		PublicSourceList: slices.Clone(in.PublicSourceList), VirtualSourceList: slices.Clone(in.VirtualSourceList),
		Services: slices.Clone(in.Services), Internal: b.newInternal(),
	}
	b.netSources[n.ID] = n
	b.retry.Remember("CreateNetworkSource", in.RetryToken, n.ID, b.nowUTC())

	return n.view(), n.Internal.Etag, nil
}

// GetNetworkSource returns one network source.
func (b *InMemoryBackend) GetNetworkSource(id string) (NetworkSourceWire, string, error) {
	b.mu.RLock("GetNetworkSource")
	defer b.mu.RUnlock()

	n, aerr := b.networkSourceByID(id)
	if aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	return n.view(), n.Internal.Etag, nil
}

// ListNetworkSources lists network sources.
func (b *InMemoryBackend) ListNetworkSources(f NameFilter, o ListOpts) (page.Page[NetworkSourceWire], error) {
	if aerr := requireTenancy(f.CompartmentID); aerr != nil {
		return page.Page[NetworkSourceWire]{}, aerr
	}

	if aerr := validListOpts(o, "TIMECREATED", "NAME"); aerr != nil {
		return page.Page[NetworkSourceWire]{}, aerr
	}

	if aerr := validState(f.State, stateCreating, stateActive, stateDeleting, stateDeleted, "INACTIVE"); aerr != nil {
		return page.Page[NetworkSourceWire]{}, aerr
	}

	b.mu.RLock("ListNetworkSources")
	defer b.mu.RUnlock()

	acc := listAccess[NetworkSource]{fields: func(v *NetworkSource) (string, string, time.Time, int64) {
		return v.Name, v.LifecycleState, v.TimeCreated, v.Internal.Seq
	}}

	return listNamed(b.netSources, f, o, acc, (*NetworkSource).view), nil
}

// UpdateNetworkSource changes the description, source lists, services and tags.
func (b *InMemoryBackend) UpdateNetworkSource(
	id, ifMatch string,
	in UpdateNetworkSourceIn,
) (NetworkSourceWire, string, error) {
	b.mu.Lock("UpdateNetworkSource")
	defer b.mu.Unlock()

	n, aerr := b.networkSourceByID(id)
	if aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if aerr = checkEtag(n.Internal.Etag, ifMatch); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	public, virtual := n.PublicSourceList, n.VirtualSourceList
	if in.PublicSourceList != nil {
		public = in.PublicSourceList
	}

	if in.VirtualSourceList != nil {
		virtual = in.VirtualSourceList
	}

	if aerr = validSources(public, virtual, in.Services); aerr != nil {
		return NetworkSourceWire{}, "", aerr
	}

	if in.Description != nil {
		if aerr = validateDescription(*in.Description); aerr != nil {
			return NetworkSourceWire{}, "", aerr
		}

		n.Description = *in.Description
	}

	n.PublicSourceList, n.VirtualSourceList = slices.Clone(public), slices.Clone(virtual)
	if in.Services != nil {
		n.Services = slices.Clone(in.Services)
	}

	applyTags(&n.Tags, in.Tags)
	n.Internal.Etag = newEtag()

	return n.view(), n.Internal.Etag, nil
}

// DeleteNetworkSource deletes a network source the authentication policy does not reference.
func (b *InMemoryBackend) DeleteNetworkSource(id, ifMatch string) error {
	b.mu.Lock("DeleteNetworkSource")
	defer b.mu.Unlock()

	n, aerr := b.networkSourceByID(id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(n.Internal.Etag, ifMatch); aerr != nil {
		return aerr
	}

	if b.authPolicy != nil && slices.Contains(b.authPolicy.NetworkSourceIDs, id) {
		return errConflict("Conflict", "Network source "+id+" is referenced by the authentication policy")
	}

	delete(b.netSources, id)

	return nil
}

// PasswordPolicy is the tenancy password policy.
type PasswordPolicy struct {
	MinimumPasswordLength         int  `json:"minimumPasswordLength"`
	IsUppercaseCharactersRequired bool `json:"isUppercaseCharactersRequired"`
	IsLowercaseCharactersRequired bool `json:"isLowercaseCharactersRequired"`
	IsNumericCharactersRequired   bool `json:"isNumericCharactersRequired"`
	IsSpecialCharactersRequired   bool `json:"isSpecialCharactersRequired"`
	IsUsernameContainmentAllowed  bool `json:"isUsernameContainmentAllowed"`
}

// AuthPolicy is the stored authentication policy.
type AuthPolicy struct {
	Etag             string         `json:"etag"`
	NetworkSourceIDs []string       `json:"networkSourceIds"`
	Password         PasswordPolicy `json:"passwordPolicy"`
}

// PasswordPolicyIn is a partial password policy update.
type PasswordPolicyIn struct {
	MinimumPasswordLength         *int  `json:"minimumPasswordLength"`
	IsUppercaseCharactersRequired *bool `json:"isUppercaseCharactersRequired"`
	IsLowercaseCharactersRequired *bool `json:"isLowercaseCharactersRequired"`
	IsNumericCharactersRequired   *bool `json:"isNumericCharactersRequired"`
	IsSpecialCharactersRequired   *bool `json:"isSpecialCharactersRequired"`
	IsUsernameContainmentAllowed  *bool `json:"isUsernameContainmentAllowed"`
}

// NetworkPolicyWire is the network policy wire shape.
type NetworkPolicyWire struct {
	NetworkSourceIDs []string `json:"networkSourceIds"`
}

// UpdateAuthPolicyIn is the UpdateAuthenticationPolicy request body.
type UpdateAuthPolicyIn struct {
	PasswordPolicy *PasswordPolicyIn  `json:"passwordPolicy"`
	NetworkPolicy  *NetworkPolicyWire `json:"networkPolicy"`
}

// AuthPolicyWire is the authentication policy wire shape.
type AuthPolicyWire struct {
	CompartmentID  string            `json:"compartmentId"`
	NetworkPolicy  NetworkPolicyWire `json:"networkPolicy"`
	PasswordPolicy PasswordPolicy    `json:"passwordPolicy"`
}

const defaultMinPasswordLength = 12

func defaultAuthPolicy() AuthPolicy {
	return AuthPolicy{
		Etag: newEtag(), NetworkSourceIDs: []string{},
		Password: PasswordPolicy{
			MinimumPasswordLength: defaultMinPasswordLength, IsUppercaseCharactersRequired: true,
			IsLowercaseCharactersRequired: true, IsNumericCharactersRequired: true,
		},
	}
}

func (p *AuthPolicy) view() AuthPolicyWire {
	return AuthPolicyWire{
		CompartmentID: TenancyID, PasswordPolicy: p.Password,
		NetworkPolicy: NetworkPolicyWire{NetworkSourceIDs: nonNil(slices.Clone(p.NetworkSourceIDs))},
	}
}

func (b *InMemoryBackend) authPolicyLocked() *AuthPolicy {
	if b.authPolicy == nil {
		d := defaultAuthPolicy()
		b.authPolicy = &d
	}

	return b.authPolicy
}

// GetAuthenticationPolicy returns the tenancy authentication policy.
func (b *InMemoryBackend) GetAuthenticationPolicy(compartment string) (AuthPolicyWire, string, error) {
	if compartment != TenancyID {
		return AuthPolicyWire{}, "", errNotFound(compartment)
	}

	b.mu.Lock("GetAuthenticationPolicy")
	defer b.mu.Unlock()

	p := b.authPolicyLocked()

	return p.view(), p.Etag, nil
}

func mergePassword(cur PasswordPolicy, in *PasswordPolicyIn) (PasswordPolicy, *APIError) {
	if in == nil {
		return cur, nil
	}

	if in.MinimumPasswordLength != nil {
		if *in.MinimumPasswordLength < 1 || *in.MinimumPasswordLength > maxPasswordLength {
			return cur, errParam("minimumPasswordLength must be between 1 and 100")
		}

		cur.MinimumPasswordLength = *in.MinimumPasswordLength
	}

	for _, f := range []struct {
		dst *bool
		src *bool
	}{
		{&cur.IsUppercaseCharactersRequired, in.IsUppercaseCharactersRequired},
		{&cur.IsLowercaseCharactersRequired, in.IsLowercaseCharactersRequired},
		{&cur.IsNumericCharactersRequired, in.IsNumericCharactersRequired},
		{&cur.IsSpecialCharactersRequired, in.IsSpecialCharactersRequired},
		{&cur.IsUsernameContainmentAllowed, in.IsUsernameContainmentAllowed},
	} {
		if f.src != nil {
			*f.dst = *f.src
		}
	}

	return cur, nil
}

// UpdateAuthenticationPolicy updates the password policy and the network sources of the network policy.
func (b *InMemoryBackend) UpdateAuthenticationPolicy(
	compartment, ifMatch string,
	in UpdateAuthPolicyIn,
) (AuthPolicyWire, string, error) {
	if compartment != TenancyID {
		return AuthPolicyWire{}, "", errNotFound(compartment)
	}

	b.mu.Lock("UpdateAuthenticationPolicy")
	defer b.mu.Unlock()

	p := b.authPolicyLocked()

	if aerr := checkEtag(p.Etag, ifMatch); aerr != nil {
		return AuthPolicyWire{}, "", aerr
	}

	pw, aerr := mergePassword(p.Password, in.PasswordPolicy)
	if aerr != nil {
		return AuthPolicyWire{}, "", aerr
	}

	ids := p.NetworkSourceIDs

	if in.NetworkPolicy != nil && in.NetworkPolicy.NetworkSourceIDs != nil {
		ids = slices.Compact(slices.Sorted(slices.Values(in.NetworkPolicy.NetworkSourceIDs)))

		for _, id := range ids {
			if _, ok := b.netSources[id]; !ok {
				return AuthPolicyWire{}, "", errParam(
					"networkSourceIds entry " + id + " is not an existing network source",
				)
			}
		}
	}

	p.Password, p.NetworkSourceIDs, p.Etag = pw, slices.Clone(ids), newEtag()

	return p.view(), p.Etag, nil
}

// RegionSubscription is a tenancy subscription to an extra region.
type RegionSubscription struct {
	SubscribedAt time.Time `json:"subscribedAt"`
	Key          string    `json:"key"`
}

// CreateRegionSubscription subscribes the tenancy to another region; it is IN_PROGRESS for the transition window.
func (b *InMemoryBackend) CreateRegionSubscription(tenancy, key, token string) (regionSubWire, error) {
	if tenancy != TenancyID {
		return regionSubWire{}, errNotFound(tenancy)
	}

	if key == "" {
		return regionSubWire{}, errMissing("regionKey is required")
	}

	region, ok := regionByKey(key)
	if !ok {
		return regionSubWire{}, errParam("regionKey " + key + " is not a known region")
	}

	b.mu.Lock("CreateRegionSubscription")
	defer b.mu.Unlock()

	if v, hit := b.retry.Lookup("CreateRegionSubscription", token, b.nowUTC()); hit {
		if s, found := b.regionSubs[v]; found {
			return b.regionSubView(s), nil
		}

		return regionSubWire{}, errInvalidatedToken()
	}

	if region.Key == homeRegionKey || b.regionSubs[region.Key] != nil {
		return regionSubWire{}, errConflict("Conflict", "The tenancy is already subscribed to region "+region.Key)
	}

	s := &RegionSubscription{Key: region.Key, SubscribedAt: b.nowUTC()}
	b.regionSubs[region.Key] = s
	b.retry.Remember("CreateRegionSubscription", token, region.Key, b.nowUTC())

	return b.regionSubView(s), nil
}

func regionByKey(key string) (regionWire, bool) {
	for _, r := range regionTable {
		if strs.Equal(r.Key, key) || strs.Equal(r.Name, key) {
			return r, true
		}
	}

	return regionWire{}, false
}

func (b *InMemoryBackend) regionSubView(s *RegionSubscription) regionSubWire {
	r, _ := regionByKey(s.Key)
	status := "READY"

	if b.nowUTC().Before(s.SubscribedAt.Add(transitionWindow)) {
		status = statusProgress
	}

	return regionSubWire{RegionKey: r.Key, RegionName: r.Name, Status: status}
}

func (h *Handler) createNetworkSource(q *req) error {
	var in CreateNetworkSourceIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)
	v, etag, err := h.Backend.CreateNetworkSource(in)

	return reply(q, v, etag, err)
}

func (h *Handler) getNetworkSource(q *req) error {
	v, etag, err := h.Backend.GetNetworkSource(q.param("networkSourceId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listNetworkSources(q *req) error {
	pg, err := h.Backend.ListNetworkSources(nameFilter(q), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateNetworkSource(q *req) error {
	var in UpdateNetworkSourceIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateNetworkSource(q.param("networkSourceId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) deleteNetworkSource(q *req) error {
	return done(q, h.Backend.DeleteNetworkSource(q.param("networkSourceId"), q.ifMatch()))
}

func (h *Handler) getAuthenticationPolicy(q *req) error {
	v, etag, err := h.Backend.GetAuthenticationPolicy(q.param("compartmentId"))

	return reply(q, v, etag, err)
}

func (h *Handler) updateAuthenticationPolicy(q *req) error {
	var in UpdateAuthPolicyIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, err := h.Backend.UpdateAuthenticationPolicy(q.param("compartmentId"), q.ifMatch(), in)

	return reply(q, v, etag, err)
}

func (h *Handler) createRegionSubscription(q *req) error {
	var in struct {
		RegionKey string `json:"regionKey"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, err := h.Backend.CreateRegionSubscription(q.param("tenancyId"), in.RegionKey, q.r.Header.Get(headerRetry))

	return reply(q, v, "", err)
}
