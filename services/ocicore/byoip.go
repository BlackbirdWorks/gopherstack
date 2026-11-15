package ocicore

import (
	"net/http"
	"net/netip"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	kindByoip      = "byoiprange"
	kindPublicPool = "publicippool"
	kindByoasn     = "byoasn"

	byoipCreating    = "CREATING"
	byoipValidating  = "VALIDATING"
	byoipProvisioned = "PROVISIONED"
	byoipActive      = "ACTIVE"
	byoipFailed      = "FAILED"
	byoipDeleting    = "DELETING"
	byoipAdvertising = "ADVERTISING"
	byoipWithdrawing = "WITHDRAWING"
	oracleAsn        = int64(31898)
	maxByoipRanges   = 20
	minByoipV4Bits   = 8
	maxByoipV4Bits   = 24
	maxByoipV6Bits   = 48
	minByoipV6Bits   = 16
	maxPrepend       = 10
	ipv6VcnMaxBits   = 64
	maxPublicASN     = int64(4294967295)
	challengeLen     = 32

	opValidateByoip  = "VALIDATE_BYOIP_RANGE"
	opAdvertiseByoip = "ADVERTISE_BYOIP_RANGE"
	opRetractByoip   = "WITHDRAW_BYOIP_RANGE"
	opDeleteByoip    = "DELETE_BYOIP_RANGE"
	opSetOriginByoip = "SET_ORIGIN_ASN"
	opValidateByoasn = "VALIDATE_BYOASN"
)

// OriginAsn is the BGP origin of a BYOIP range.
type OriginAsn struct {
	ByoasnID            string `json:"byoasnId,omitempty"`
	Asn                 int64  `json:"asn"`
	AsPathPrependLength int    `json:"asPathPrependLength,omitempty"`
}

// ByoipV6Alloc is an IPv6 BYOIP prefix handed to a VCN.
type ByoipV6Alloc struct {
	ByoipRangeID  string `json:"byoipRangeId"`
	CompartmentID string `json:"compartmentId"`
	Ipv6CidrBlock string `json:"ipv6CidrBlock"`
	VcnID         string `json:"vcnId"`
}

// ByoipRange is an imported public address block; ownership validation and BGP advertisement are recorded only.
type ByoipRange struct {
	TimeValidated  *time.Time `json:"timeValidated,omitempty"`
	TimeAdvertised *time.Time `json:"timeAdvertised,omitempty"`
	TimeWithdrawn  *time.Time `json:"timeWithdrawn,omitempty"`
	OriginAsn      *OriginAsn `json:"originAsn,omitempty"`
	Base
	CidrBlock       string         `json:"cidrBlock,omitempty"`
	Ipv6CidrBlock   string         `json:"ipv6CidrBlock,omitempty"`
	ValidationToken string         `json:"validationToken"`
	LifecycleDetail string         `json:"lifecycleDetails"`
	Ipv6Allocations []ByoipV6Alloc `json:"byoipRangeVcnIpv6Allocations,omitempty"`
}

// PoolCapacity is one BYOIP sub-range of a public IP pool.
type PoolCapacity struct {
	CidrBlock    string `json:"cidrBlock"`
	ByoipRangeID string `json:"byoipRangeId"`
}

// PublicIPPool is a pool of public addresses taken from BYOIP ranges.
type PublicIPPool struct {
	Base
	Capacity  []PoolCapacity `json:"capacity,omitempty"`
	CidrBlock []string       `json:"cidrBlocks"`
}

// Byoasn is an imported autonomous system number.
type Byoasn struct {
	TimeValidated *time.Time `json:"timeValidated,omitempty"`
	TimeUpdated   *time.Time `json:"timeUpdated,omitempty"`
	Base
	ValidationToken string             `json:"validationToken"`
	ByoipRanges     []ByoasnByoipRange `json:"byoipRanges,omitempty"`
	Asn             int64              `json:"asn"`
}

// ByoasnByoipRange is a BYOIP range that originates from a BYOASN.
type ByoasnByoipRange struct {
	ByoipRangeID        string `json:"byoipRangeId"`
	CidrBlock           string `json:"cidrBlock,omitempty"`
	Ipv6CidrBlock       string `json:"ipv6CidrBlock,omitempty"`
	AsPathPrependLength int    `json:"asPathPrependLength,omitempty"`
}

func (v *ByoipRange) vcnRef() string   { return "" }
func (v *PublicIPPool) vcnRef() string { return "" }
func (v *Byoasn) vcnRef() string       { return "" }

func (v *ByoipRange) view() Resource {
	c := *v
	c.Internal = nil
	c.Ipv6Allocations = slices.Clone(v.Ipv6Allocations)

	switch v.LifecycleState {
	case statDeleting:
		c.LifecycleDetail = byoipDeleting
	case statDeleted:
		c.LifecycleDetail = statDeleted
	}

	return &c
}

func (v *PublicIPPool) view() Resource {
	c := *v
	c.Internal = nil
	c.CidrBlock = slices.Clone(v.CidrBlock)

	if c.CidrBlock == nil {
		c.CidrBlock = []string{}
	}

	return &c
}

func (v *Byoasn) view() Resource {
	c := *v
	c.Internal = nil
	c.ByoipRanges = slices.Clone(v.ByoipRanges)

	return &c
}

// detailsTo moves a range through an interim lifecycleDetails value that settles into final on the clock.
func (b *InMemoryBackend) detailsTo(r *ByoipRange, interim, final string) {
	at := b.nowUTC().Add(transitionWindow)
	r.LifecycleDetail, r.Internal.TransitionAt, r.Internal.DetailsNext = interim, &at, final
	b.bump(r)
}

func (b *InMemoryBackend) settleDetails(r *ByoipRange, at time.Time) {
	r.LifecycleDetail, r.Internal.DetailsNext = r.Internal.DetailsNext, ""

	switch r.LifecycleDetail {
	case byoipProvisioned:
		if r.TimeValidated == nil {
			r.TimeValidated = &at
		}
	case byoipActive:
		r.TimeAdvertised = &at
	}
}

// CreateByoipRangeIn is the CreateByoipRange request body.
type CreateByoipRangeIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
	CidrBlock     string `json:"cidrBlock"`
	Ipv6CidrBlock string `json:"ipv6CidrBlock"`
}

func publicV4(p netip.Prefix) bool {
	a := p.Addr()

	return !a.IsPrivate() && !a.IsLoopback() && !a.IsLinkLocalUnicast() && !a.IsMulticast() && !a.IsUnspecified() &&
		a.As4()[0] < 224
}

func parseByoipBlock(v4, v6 string) (netip.Prefix, *APIError) {
	if (v4 == "") == (v6 == "") {
		return netip.Prefix{}, errParam("exactly one of cidrBlock and ipv6CidrBlock is required")
	}

	if v4 != "" {
		p, err := netip.ParsePrefix(v4)
		if err != nil || !p.Addr().Is4() || p.Masked() != p {
			return p, errParam("cidrBlock must be a valid IPv4 CIDR block")
		}

		if p.Bits() < minByoipV4Bits || p.Bits() > maxByoipV4Bits || !publicV4(p) {
			return p, errParam("cidrBlock must be a public IPv4 block between /8 and /24")
		}

		return p, nil
	}

	p, err := netip.ParsePrefix(v6)
	if err != nil || !p.Addr().Is6() || p.Masked() != p {
		return p, errParam("ipv6CidrBlock must be a valid IPv6 CIDR block")
	}

	if p.Bits() < minByoipV6Bits || p.Bits() > maxByoipV6Bits {
		return p, errParam("ipv6CidrBlock must be /48 or larger")
	}

	return p, nil
}

// CreateByoipRange records an import request; the range stays CREATING until it is validated.
func (b *InMemoryBackend) CreateByoipRange(in CreateByoipRangeIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	p, aerr := parseByoipBlock(in.CidrBlock, in.Ipv6CidrBlock)
	if aerr != nil {
		return nil, "", aerr
	}

	cur := liveOf(b, func(*ByoipRange) bool { return true })
	if len(cur) >= maxByoipRanges {
		return nil, "", errLimit("A tenancy can have at most 20 BYOIP ranges")
	}

	for _, o := range cur {
		other := o.CidrBlock + o.Ipv6CidrBlock
		if q, err := netip.ParsePrefix(other); err == nil && q.Overlaps(p) {
			return nil, "", errConflict("The block " + p.String() + " overlaps BYOIP range " + o.ID)
		}
	}

	r := &ByoipRange{
		Base:      b.newBase(kindByoip, in.CompartmentID, in.DisplayName, in.TagsIn),
		CidrBlock: in.CidrBlock, Ipv6CidrBlock: in.Ipv6CidrBlock, ValidationToken: awsid.NewN("", challengeLen),
		LifecycleDetail: byoipCreating, OriginAsn: &OriginAsn{Asn: oracleAsn},
	}
	r.LifecycleState, r.Internal.TransitionAt = byoipActive, nil
	b.add(r)

	return r.view(), r.Internal.Etag, nil
}

// UpdateByoipRange changes the name and tags of a range.
func (b *InMemoryBackend) UpdateByoipRange(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*ByoipRange) *APIError { return nil })
}

// DeleteByoipRange removes a range that is not validating or advertised and has no sub-ranges or allocations.
func (b *InMemoryBackend) DeleteByoipRange(id, ifMatch string) (string, *APIError) {
	b.mu.Lock("DeleteByoipRange")
	defer b.mu.Unlock()

	r, aerr := get[*ByoipRange](b, id)
	if aerr != nil {
		return "", aerr
	}

	if !live(r) || r.LifecycleState == statDeleted {
		return b.byoipWorkRequest(opDeleteByoip, r), nil
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return "", aerr
	}

	if !slices.Contains([]string{byoipCreating, byoipProvisioned, byoipActive, byoipFailed}, r.LifecycleDetail) {
		return "", errState("The BYOIP range " + id + " is " + r.LifecycleDetail)
	}

	if pool := firstID(liveOf(b, func(p *PublicIPPool) bool { return p.holds(id) })); pool != "" {
		return "", errConflict("The BYOIP range " + id + " still has a sub-range in public IP pool " + pool)
	}

	if len(r.Ipv6Allocations) > 0 {
		return "", errConflict("The BYOIP range " + id + " still has prefixes allocated to VCNs")
	}

	b.moveTo(r, statDeleting, statDeleted)

	return b.byoipWorkRequest(opDeleteByoip, r), nil
}

func (v *PublicIPPool) holds(rangeID string) bool {
	return slices.ContainsFunc(v.Capacity, func(c PoolCapacity) bool { return c.ByoipRangeID == rangeID })
}

func (b *InMemoryBackend) byoipWorkRequest(op string, r *ByoipRange) string {
	return b.newWorkRequest(op, r.CompartmentID, []WorkResource{
		{
			ActionType: wrActionUpdated,
			EntityType: "byoiprange",
			Identifier: r.ID,
			EntityURI:  apiBase + "/byoipRanges/" + r.ID,
		},
	})
}

// byoipStep runs a validate, advertise or withdraw transition when the range is in an allowed stage.
func (b *InMemoryBackend) byoipStep(id, ifMatch, op, interim, final string, from ...string) (string, *APIError) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	r, aerr := getLive[*ByoipRange](b, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return "", aerr
	}

	if !slices.Contains(from, r.LifecycleDetail) {
		return "", errState("The BYOIP range " + id + " is " + r.LifecycleDetail + "; " + op + " needs " + from[0])
	}

	b.detailsTo(r, interim, final)

	if final == byoipProvisioned && r.LifecycleDetail == byoipWithdrawing {
		at := b.nowUTC()
		r.TimeWithdrawn = &at
	}

	return b.byoipWorkRequest(op, r), nil
}

// ValidateByoipRange records the ownership check; the RIR is never contacted.
func (b *InMemoryBackend) ValidateByoipRange(id, ifMatch string) (string, *APIError) {
	return b.byoipStep(id, ifMatch, opValidateByoip, byoipValidating, byoipProvisioned, byoipCreating, byoipFailed)
}

// AdvertiseByoipRange records the BGP advertisement; no route is announced.
func (b *InMemoryBackend) AdvertiseByoipRange(id, ifMatch string) *APIError {
	_, aerr := b.byoipStep(id, ifMatch, opAdvertiseByoip, byoipAdvertising, byoipActive, byoipProvisioned)

	return aerr
}

// WithdrawByoipRange records the end of the advertisement.
func (b *InMemoryBackend) WithdrawByoipRange(id, ifMatch string) *APIError {
	_, aerr := b.byoipStep(id, ifMatch, opRetractByoip, byoipWithdrawing, byoipProvisioned, byoipActive)

	return aerr
}

// SetOriginAsnIn is the SetOriginAsn request body.
type SetOriginAsnIn struct {
	AsPathPrependLength *int   `json:"asPathPrependLength"`
	ByoasnID            string `json:"byoasnId"`
}

func (b *InMemoryBackend) dropOrigin(r *ByoipRange) {
	if a, ok := b.res[r.OriginAsn.ByoasnID].(*Byoasn); ok {
		a.ByoipRanges = slices.DeleteFunc(slices.Clone(a.ByoipRanges), func(x ByoasnByoipRange) bool {
			return x.ByoipRangeID == r.ID
		})
		b.bump(a)
	}
}

// SetOriginAsn makes a BYOASN the BGP origin of a range that is not being advertised.
func (b *InMemoryBackend) SetOriginAsn(id, ifMatch string, in SetOriginAsnIn) (string, *APIError) {
	b.mu.Lock("SetOriginAsn")
	defer b.mu.Unlock()

	r, aerr := getLive[*ByoipRange](b, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return "", aerr
	}

	a, aerr := getLive[*Byoasn](b, in.ByoasnID)
	if aerr != nil {
		return "", aerr
	}

	prepend := 0
	if in.AsPathPrependLength != nil {
		prepend = *in.AsPathPrependLength
	}

	switch {
	case a.LifecycleState != byoipActive:
		return "", errState("The BYOASN " + a.ID + " is " + a.LifecycleState)
	case prepend < 0 || prepend > maxPrepend:
		return "", errParam("asPathPrependLength must be between 0 and 10")
	case r.LifecycleDetail != byoipProvisioned:
		return "", errState("The BYOIP range " + id + " must be PROVISIONED to change its origin")
	}

	b.dropOrigin(r)
	r.OriginAsn = &OriginAsn{ByoasnID: a.ID, Asn: a.Asn, AsPathPrependLength: prepend}
	a.ByoipRanges = append(a.ByoipRanges, ByoasnByoipRange{
		ByoipRangeID: r.ID, CidrBlock: r.CidrBlock, Ipv6CidrBlock: r.Ipv6CidrBlock, AsPathPrependLength: prepend,
	})
	b.bump(a)
	b.bump(r)

	return b.byoipWorkRequest(opSetOriginByoip, r), nil
}

// SetOriginAsnToOracle returns the origin of a range to Oracle's ASN.
func (b *InMemoryBackend) SetOriginAsnToOracle(id, ifMatch string) *APIError {
	b.mu.Lock("SetOriginAsnToOracle")
	defer b.mu.Unlock()

	r, aerr := getLive[*ByoipRange](b, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(r, ifMatch); aerr != nil {
		return aerr
	}

	if r.LifecycleDetail != byoipProvisioned {
		return errState("The BYOIP range " + id + " must be PROVISIONED to change its origin")
	}

	b.dropOrigin(r)
	r.OriginAsn = &OriginAsn{Asn: oracleAsn}
	b.bump(r)

	return nil
}

// byoipAllocated lists the pool sub-ranges of a range.
func (b *InMemoryBackend) byoipAllocated(id string) ([]PoolCapacityOut, *APIError) {
	b.mu.RLock("ListByoipAllocatedRanges")
	defer b.mu.RUnlock()

	if _, aerr := get[*ByoipRange](b, id); aerr != nil {
		return nil, aerr
	}

	out := []PoolCapacityOut{}

	for _, p := range liveOf(b, func(p *PublicIPPool) bool { return p.holds(id) }) {
		for _, c := range p.Capacity {
			if c.ByoipRangeID == id {
				out = append(out, PoolCapacityOut{CidrBlock: c.CidrBlock, PublicIPPoolID: p.ID})
			}
		}
	}

	slices.SortFunc(out, func(a, c PoolCapacityOut) int { return compareStr(a.CidrBlock, c.CidrBlock) })

	return out, nil
}

// PoolCapacityOut is a ByoipAllocatedRangeSummary.
type PoolCapacityOut struct {
	CidrBlock      string `json:"cidrBlock"`
	PublicIPPoolID string `json:"publicIpPoolId"`
}

// CreatePublicIPPoolIn is the CreatePublicIpPool request body.
type CreatePublicIPPoolIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
}

// CreatePublicIPPool creates an empty pool.
func (b *InMemoryBackend) CreatePublicIPPool(in CreatePublicIPPoolIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	p := &PublicIPPool{Base: b.newBase(kindPublicPool, in.CompartmentID, in.DisplayName, in.TagsIn)}
	p.LifecycleState, p.Internal.TransitionAt = byoipActive, nil
	b.add(p)

	return p.view(), p.Internal.Etag, nil
}

// UpdatePublicIPPool changes the name and tags of a pool.
func (b *InMemoryBackend) UpdatePublicIPPool(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(*PublicIPPool) *APIError { return nil })
}

// DeletePublicIPPool deletes a pool that has no capacity.
func (b *InMemoryBackend) DeletePublicIPPool(id, ifMatch string) *APIError {
	b.mu.Lock("DeletePublicIPPool")
	defer b.mu.Unlock()

	p, aerr := get[*PublicIPPool](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(p) || p.LifecycleState == statDeleted {
		return nil
	}

	if aerr = checkEtag(p, ifMatch); aerr != nil {
		return aerr
	}

	if len(p.CidrBlock) > 0 {
		return errConflict("The public IP pool " + id + " still has capacity; remove it first")
	}

	b.moveTo(p, statDeleting, statDeleted)

	return nil
}

// PoolCapacityIn is the AddPublicIpPoolCapacity request body.
type PoolCapacityIn struct {
	ByoipRangeID string `json:"byoipRangeId"`
	CidrBlock    string `json:"cidrBlock"`
}

// AddPublicIPPoolCapacity adds a sub-range of a provisioned IPv4 BYOIP range to a pool.
func (b *InMemoryBackend) AddPublicIPPoolCapacity(id, ifMatch string, in PoolCapacityIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, UpdateIn{}, func(p *PublicIPPool) *APIError {
		r, aerr := getLive[*ByoipRange](b, in.ByoipRangeID)
		if aerr != nil {
			return aerr
		}

		c, err := netip.ParsePrefix(in.CidrBlock)
		switch {
		case err != nil || !c.Addr().Is4() || c.Masked() != c:
			return errParam("cidrBlock must be a valid IPv4 CIDR block")
		case r.CidrBlock == "":
			return errParam("Only an IPv4 BYOIP range can feed a public IP pool")
		case r.LifecycleDetail != byoipProvisioned && r.LifecycleDetail != byoipActive:
			return errState("The BYOIP range " + r.ID + " is " + r.LifecycleDetail)
		case !within(netip.MustParsePrefix(r.CidrBlock), c):
			return errParam("cidrBlock must lie inside BYOIP range " + r.CidrBlock)
		}

		for _, o := range liveOf(b, func(*PublicIPPool) bool { return true }) {
			for _, have := range o.CidrBlock {
				if netip.MustParsePrefix(have).Overlaps(c) {
					return errConflict("The block " + in.CidrBlock + " overlaps capacity of public IP pool " + o.ID)
				}
			}
		}

		p.CidrBlock = append(slices.Clone(p.CidrBlock), in.CidrBlock)
		p.Capacity = append(slices.Clone(p.Capacity), PoolCapacity{CidrBlock: in.CidrBlock, ByoipRangeID: r.ID})

		return nil
	})
}

// RemovePublicIPPoolCapacity removes a sub-range that has no reserved public IPs.
func (b *InMemoryBackend) RemovePublicIPPoolCapacity(id, ifMatch, cidr string) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, UpdateIn{}, func(p *PublicIPPool) *APIError {
		i := slices.Index(p.CidrBlock, cidr)
		if i < 0 {
			return errParam("cidrBlock " + cidr + " is not capacity of this pool")
		}

		c := netip.MustParsePrefix(cidr)

		for _, ip := range liveOf(b, func(x *PublicIP) bool { return x.PublicIPPoolID == p.ID }) {
			if a, err := netip.ParseAddr(ip.IPAddress); err == nil && c.Contains(a) {
				return errConflict("Public IP " + ip.ID + " still uses " + cidr + "; delete it first")
			}
		}

		p.CidrBlock = slices.Delete(slices.Clone(p.CidrBlock), i, i+1)
		p.Capacity = slices.DeleteFunc(
			slices.Clone(p.Capacity),
			func(x PoolCapacity) bool { return x.CidrBlock == cidr },
		)

		return nil
	})
}

// poolAddress picks the lowest free address of a pool for a new reserved public IP.
func (b *InMemoryBackend) poolAddress(poolID string) (string, *APIError) {
	p, aerr := getLive[*PublicIPPool](b, poolID)
	if aerr != nil {
		return "", aerr
	}

	used := map[string]bool{}
	for _, x := range liveOf(b, func(*PublicIP) bool { return true }) {
		used[x.IPAddress] = true
	}

	for _, cidr := range p.CidrBlock {
		c := netip.MustParsePrefix(cidr)
		for a := c.Addr(); c.Contains(a); a = a.Next() {
			if c.Bits() <= 30 && (a == c.Addr() || !c.Contains(a.Next())) {
				continue
			}

			if !used[a.String()] {
				return a.String(), nil
			}
		}
	}

	return "", errLimit("Public IP pool " + poolID + " has no free address")
}

// CreateByoasnIn is the CreateByoasn request body.
type CreateByoasnIn struct {
	TagsIn
	CompartmentID string `json:"compartmentId"`
	DisplayName   string `json:"displayName"`
	Asn           int64  `json:"asn"`
}

// CreateByoasn records an imported ASN; it stays CREATING until validated.
func (b *InMemoryBackend) CreateByoasn(in CreateByoasnIn) (Resource, string, *APIError) {
	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return nil, "", aerr
	}

	if aerr := validName(in.DisplayName); aerr != nil {
		return nil, "", aerr
	}

	private := (in.Asn >= 64512 && in.Asn <= 65534) || (in.Asn >= 4200000000 && in.Asn <= 4294967294)
	if in.Asn < 1 || in.Asn > maxPublicASN || private || in.Asn == oracleAsn || in.Asn == 14544 {
		return nil, "", errParam("asn must be a public autonomous system number other than Oracle's")
	}

	if slices.ContainsFunc(
		liveOf(b, func(*Byoasn) bool { return true }),
		func(a *Byoasn) bool { return a.Asn == in.Asn },
	) {
		return nil, "", errConflict("The ASN is already imported")
	}

	a := &Byoasn{
		Base: b.newBase(kindByoasn, in.CompartmentID, in.DisplayName, in.TagsIn), Asn: in.Asn,
		ValidationToken: awsid.NewN("", challengeLen),
	}
	a.LifecycleState, a.Internal.TransitionAt = byoipCreating, nil
	b.add(a)

	return a.view(), a.Internal.Etag, nil
}

// UpdateByoasn changes the name and tags of a BYOASN.
func (b *InMemoryBackend) UpdateByoasn(id, ifMatch string, in UpdateIn) (Resource, string, *APIError) {
	return updateWith(b, id, ifMatch, in, func(a *Byoasn) *APIError {
		at := b.nowUTC()
		a.TimeUpdated = &at

		return nil
	})
}

// ValidateByoasn records the ownership check and activates the ASN.
func (b *InMemoryBackend) ValidateByoasn(id, ifMatch string) (string, *APIError) {
	b.mu.Lock("ValidateByoasn")
	defer b.mu.Unlock()

	a, aerr := getLive[*Byoasn](b, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return "", aerr
	}

	if a.LifecycleState != byoipCreating && a.LifecycleState != byoipFailed {
		return "", errState("The BYOASN " + id + " is " + a.LifecycleState)
	}

	at := b.nowUTC()
	a.TimeValidated = &at
	b.moveTo(a, statUpdating, byoipActive)

	return b.newWorkRequest(opValidateByoasn, a.CompartmentID, []WorkResource{{
		ActionType: wrActionUpdated, EntityType: "byoasn", Identifier: a.ID, EntityURI: apiBase + "/byoasns/" + a.ID,
	}}), nil
}

// DeleteByoasn deletes an ASN that no range originates from.
func (b *InMemoryBackend) DeleteByoasn(id, ifMatch string) *APIError {
	b.mu.Lock("DeleteByoasn")
	defer b.mu.Unlock()

	a, aerr := get[*Byoasn](b, id)
	if aerr != nil {
		return aerr
	}

	if !live(a) || a.LifecycleState == statDeleted {
		return nil
	}

	if aerr = checkEtag(a, ifMatch); aerr != nil {
		return aerr
	}

	if len(a.ByoipRanges) > 0 {
		return errConflict("The BYOASN " + id + " is the origin of BYOIP range " + a.ByoipRanges[0].ByoipRangeID)
	}

	b.moveTo(a, statDeleting, statDeleted)

	return nil
}

func listCollection[T Resource](extra func(*req) func(Resource) bool) opFunc {
	return func(h *Handler, q *req) error {
		o, err := q.listOpts()
		if err != nil {
			return err
		}

		if extra != nil {
			o.Match = extra(q)
		}

		pg, aerr := list[T](h.Backend, o)
		if aerr != nil {
			return aerr
		}

		if pg.Data == nil {
			pg.Data = []Resource{}
		}

		q.nextPage(pg.Next)

		return q.json(http.StatusOK, map[string]any{"items": pg.Data}, "")
	}
}

func workAction(param string, run func(*InMemoryBackend, string, string) (string, *APIError)) opFunc {
	return func(h *Handler, q *req) error {
		wr, aerr := run(h.Backend, q.param(param), q.ifMatch())
		if aerr != nil {
			return aerr
		}

		return q.acceptWorkRequest(wr)
	}
}

func poolFilter(q *req) func(Resource) bool {
	return queryEq(q, "byoipRangeId", func(r Resource) string {
		if p, ok := r.(*PublicIPPool); ok && len(p.Capacity) > 0 {
			return p.Capacity[0].ByoipRangeID
		}

		return ""
	})
}

func byoipEntries() []entry {
	rng, pool, asn := idPath("/byoipRanges", "byoipRangeId"), idPath("/publicIpPools", "publicIpPoolId"),
		idPath("/byoasns", "byoasnId")

	return []entry{
		{createOp((*InMemoryBackend).CreateByoipRange), http.MethodPost, "/byoipRanges", "CreateByoipRange"},
		{listCollection[*ByoipRange](nil), http.MethodGet, "/byoipRanges", "ListByoipRanges"},
		{getOp[*ByoipRange]("byoipRangeId"), http.MethodGet, rng, "GetByoipRange"},
		{updateOp("byoipRangeId", (*InMemoryBackend).UpdateByoipRange), http.MethodPut, rng, "UpdateByoipRange"},
		{workAction("byoipRangeId", (*InMemoryBackend).DeleteByoipRange), http.MethodDelete, rng, "DeleteByoipRange"},
		{
			changeOp[*ByoipRange]("byoipRangeId"), http.MethodPost, rng + "/actions/changeCompartment",
			"ChangeByoipRangeCompartment",
		},
		{
			workAction(
				"byoipRangeId",
				(*InMemoryBackend).ValidateByoipRange,
			),
			http.MethodPost,
			rng + "/actions/validate",
			"ValidateByoipRange",
		},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.AdvertiseByoipRange(q.param("byoipRangeId"), q.ifMatch()))
		}, http.MethodPost, rng + "/actions/advertise", "AdvertiseByoipRange"},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.WithdrawByoipRange(q.param("byoipRangeId"), q.ifMatch()))
		}, http.MethodPost, rng + "/actions/withdraw", "WithdrawByoipRange"},
		{setOriginOp, http.MethodPost, rng + "/actions/setOrigin/byoasn", "SetOriginAsn"},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.SetOriginAsnToOracle(q.param("byoipRangeId"), q.ifMatch()))
		}, http.MethodPost, rng + "/actions/setOrigin/oracleAsn", "SetOriginAsnToOracle"},
		{allocatedOp, http.MethodGet, rng + "/byoipAllocatedRanges", "ListByoipAllocatedRanges"},
		{createOp((*InMemoryBackend).CreatePublicIPPool), http.MethodPost, "/publicIpPools", "CreatePublicIpPool"},
		{listCollection[*PublicIPPool](poolFilter), http.MethodGet, "/publicIpPools", "ListPublicIpPools"},
		{getOp[*PublicIPPool]("publicIpPoolId"), http.MethodGet, pool, "GetPublicIpPool"},
		{updateOp("publicIpPoolId", (*InMemoryBackend).UpdatePublicIPPool), http.MethodPut, pool, "UpdatePublicIpPool"},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.DeletePublicIPPool(q.param("publicIpPoolId"), q.ifMatch()))
		}, http.MethodDelete, pool, "DeletePublicIpPool"},
		{
			changeOp[*PublicIPPool]("publicIpPoolId"), http.MethodPost, pool + "/actions/changeCompartment",
			"ChangePublicIpPoolCompartment",
		},
		{
			updateOp("publicIpPoolId", (*InMemoryBackend).AddPublicIPPoolCapacity), http.MethodPost,
			pool + "/actions/addCapacity", "AddPublicIpPoolCapacity",
		},
		{removeCapacityOp, http.MethodPost, pool + "/actions/removeCapacity", "RemovePublicIpPoolCapacity"},
		{createOp((*InMemoryBackend).CreateByoasn), http.MethodPost, "/byoasns", "CreateByoasn"},
		{listCollection[*Byoasn](nil), http.MethodGet, "/byoasns", "ListByoasns"},
		{getOp[*Byoasn]("byoasnId"), http.MethodGet, asn, "GetByoasn"},
		{updateOp("byoasnId", (*InMemoryBackend).UpdateByoasn), http.MethodPut, asn, "UpdateByoasn"},
		{func(h *Handler, q *req) error {
			return noBody(q, h.Backend.DeleteByoasn(q.param("byoasnId"), q.ifMatch()))
		}, http.MethodDelete, asn, "DeleteByoasn"},
		{changeOp[*Byoasn]("byoasnId"), http.MethodPost, asn + "/actions/changeCompartment", "ChangeByoasnCompartment"},
		{
			workAction("byoasnId", (*InMemoryBackend).ValidateByoasn), http.MethodPost, asn + "/actions/validate",
			"ValidateByoasn",
		},
	}
}

func setOriginOp(h *Handler, q *req) error {
	var in SetOriginAsnIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, aerr := h.Backend.SetOriginAsn(q.param("byoipRangeId"), q.ifMatch(), in)
	if aerr != nil {
		return aerr
	}

	return q.acceptWorkRequest(wr)
}

func allocatedOp(h *Handler, q *req) error {
	items, aerr := h.Backend.byoipAllocated(q.param("byoipRangeId"))
	if aerr != nil {
		return aerr
	}

	return q.json(http.StatusOK, map[string]any{"items": items}, "")
}

func removeCapacityOp(h *Handler, q *req) error {
	var in struct {
		CidrBlock string `json:"cidrBlock"`
	}

	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, aerr := h.Backend.RemovePublicIPPoolCapacity(q.param("publicIpPoolId"), q.ifMatch(), in.CidrBlock)

	return send(q, v, etag, aerr)
}

func (b *InMemoryBackend) byoipCidrsOf(vcnID string) []string {
	var out []string

	for _, r := range liveOf(b, func(*ByoipRange) bool { return true }) {
		for _, a := range r.Ipv6Allocations {
			if a.VcnID == vcnID {
				out = append(out, a.Ipv6CidrBlock)
			}
		}
	}

	return out
}

func (b *InMemoryBackend) releaseByoipv6(vcnID, cidr string) {
	for _, r := range liveOf(b, func(*ByoipRange) bool { return true }) {
		n := len(r.Ipv6Allocations)
		r.Ipv6Allocations = slices.DeleteFunc(slices.Clone(r.Ipv6Allocations), func(a ByoipV6Alloc) bool {
			return a.VcnID == vcnID && a.Ipv6CidrBlock == cidr
		})

		if len(r.Ipv6Allocations) != n {
			b.bump(r)
		}
	}
}

// byoipv6Check validates handing cidr of an IPv6 BYOIP range to a VCN; pending are prefixes already claimed.
func (b *InMemoryBackend) byoipv6Check(rangeID, cidr string, pending []string) (*ByoipRange, *APIError) {
	r, aerr := getLive[*ByoipRange](b, rangeID)
	if aerr != nil {
		return nil, aerr
	}

	c, err := netip.ParsePrefix(cidr)

	switch {
	case err != nil || !c.Addr().Is6() || c.Masked() != c || c.Bits() > ipv6VcnMaxBits:
		return nil, errParam("ipv6CidrBlock must be a valid IPv6 prefix of /64 or larger")
	case r.Ipv6CidrBlock == "":
		return nil, errParam("BYOIP range " + rangeID + " is not an IPv6 range")
	case r.LifecycleDetail != byoipProvisioned && r.LifecycleDetail != byoipActive:
		return nil, errState("The BYOIP range " + rangeID + " is " + r.LifecycleDetail)
	case !within(netip.MustParsePrefix(r.Ipv6CidrBlock), c):
		return nil, errParam("ipv6CidrBlock must lie inside BYOIP range " + r.Ipv6CidrBlock)
	}

	taken := slices.Clone(pending)
	for _, o := range liveOf(b, func(*ByoipRange) bool { return true }) {
		for _, a := range o.Ipv6Allocations {
			taken = append(taken, a.Ipv6CidrBlock)
		}
	}

	for _, t := range taken {
		if netip.MustParsePrefix(t).Overlaps(c) {
			return nil, errConflict("The prefix " + cidr + " overlaps an allocated prefix " + t)
		}
	}

	return r, nil
}

func (b *InMemoryBackend) claimByoipv6(v *Vcn, r *ByoipRange, cidr string) {
	r.Ipv6Allocations = append(slices.Clone(r.Ipv6Allocations), ByoipV6Alloc{
		ByoipRangeID: r.ID, CompartmentID: v.CompartmentID, Ipv6CidrBlock: cidr, VcnID: v.ID,
	})
	b.bump(r)
	v.Ipv6CidrBlocks = append(slices.Clone(v.Ipv6CidrBlocks), cidr)
}

// addByoipv6 hands a /64 or larger piece of a provisioned IPv6 BYOIP range to a VCN.
func (b *InMemoryBackend) addByoipv6(v *Vcn, rangeID, cidr string) (string, *APIError) {
	r, aerr := b.byoipv6Check(rangeID, cidr, nil)
	if aerr != nil {
		return "", aerr
	}

	b.claimByoipv6(v, r, cidr)
	b.addIpv6DefaultRules(v)

	return b.beginVcnUpdate(v, opAddVcnIpv6), nil
}
