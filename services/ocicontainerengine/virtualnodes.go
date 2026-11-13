package ocicontainerengine

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxVirtualNodes = 1000
	faultDomainOne  = "FAULT-DOMAIN-1"
	maxEvictionMin  = 60
	digitsMax       = 3
	decimalBase     = 10
)

var evictionRe = regexp.MustCompile(`^PT(?:(\d{1,3})H)?(?:(\d{1,3})M)?(?:(\d{1,3})S)?$`)

// VPlacement is one virtual node pool placement configuration.
type VPlacement struct {
	AvailabilityDomain string   `json:"availabilityDomain"`
	SubnetID           string   `json:"subnetId"`
	FaultDomain        []string `json:"faultDomain,omitempty"`
}

// PodConfig is the pod configuration of a virtual node pool.
type PodConfig struct {
	SubnetID string   `json:"subnetId"`
	Shape    string   `json:"shape"`
	NsgIDs   []string `json:"nsgIds,omitempty"`
}

// Taint is a Kubernetes taint applied to virtual nodes.
type Taint struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Effect string `json:"effect"`
}

// VirtualNode is one virtual node of a virtual node pool.
type VirtualNode struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Tags         Tags       `json:"tags"`
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Version      string     `json:"version"`
	AD           string     `json:"availabilityDomain"`
	FaultDomain  string     `json:"faultDomain"`
	SubnetID     string     `json:"subnetId"`
	PrivateIP    string     `json:"privateIp,omitempty"`
	State        string     `json:"state"`
	NsgIDs       []string   `json:"nsgIds,omitempty"`
	Seq          int64      `json:"seq"`
}

// VirtualNodePool is a stored virtual node pool.
type VirtualNodePool struct {
	TimeCreated   time.Time       `json:"timeCreated"`
	TimeUpdated   *time.Time      `json:"timeUpdated,omitempty"`
	TransitionAt  *time.Time      `json:"transitionAt,omitempty"`
	PodConfig     *PodConfig      `json:"podConfiguration"`
	NodeTags      *Tags           `json:"virtualNodeTags,omitempty"`
	Cycling       json.RawMessage `json:"cycling,omitempty"`
	Tags          Tags            `json:"tags"`
	ID            string          `json:"id"`
	ClusterID     string          `json:"clusterId"`
	CompartmentID string          `json:"compartmentId"`
	Name          string          `json:"name"`
	Version       string          `json:"version"`
	State         string          `json:"state"`
	Etag          string          `json:"etag"`
	Placements    []VPlacement    `json:"placementConfigurations"`
	Labels        []KeyValue      `json:"initialVirtualNodeLabels,omitempty"`
	Taints        []Taint         `json:"taints,omitempty"`
	NsgIDs        []string        `json:"nsgIds,omitempty"`
	Nodes         []*VirtualNode  `json:"nodes"`
	Seq           int64           `json:"seq"`
	Size          int             `json:"size"`
}

// CreateVNPIn is the CreateVirtualNodePool request body.
type CreateVNPIn struct {
	Tags
	Size        *int            `json:"size"`
	PodConfig   *PodConfig      `json:"podConfiguration"`
	NodeTags    *Tags           `json:"virtualNodeTags"`
	Cycling     json.RawMessage `json:"virtualNodePoolCyclingDetails"`
	RetryToken  string          `json:"-"`
	Compartment string          `json:"compartmentId"`
	ClusterID   string          `json:"clusterId"`
	Name        string          `json:"displayName"`
	Placements  []VPlacement    `json:"placementConfigurations"`
	Labels      []KeyValue      `json:"initialVirtualNodeLabels"`
	Taints      []Taint         `json:"taints"`
	NsgIDs      []string        `json:"nsgIds"`
}

// UpdateVNPIn is the UpdateVirtualNodePool request body.
type UpdateVNPIn struct {
	Tags
	Name       *string         `json:"displayName"`
	Size       *int            `json:"size"`
	PodConfig  *PodConfig      `json:"podConfiguration"`
	NodeTags   *Tags           `json:"virtualNodeTags"`
	Cycling    json.RawMessage `json:"virtualNodePoolCyclingDetails"`
	Placements []VPlacement    `json:"placementConfigurations"`
	Labels     []KeyValue      `json:"initialVirtualNodeLabels"`
	Taints     []Taint         `json:"taints"`
	NsgIDs     []string        `json:"nsgIds"`
}

// VNPWire is the VirtualNodePool and VirtualNodePoolSummary body.
type VNPWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeUpdated    *time.Time                `json:"timeUpdated,omitempty"`
	PodConfig      *PodConfig                `json:"podConfiguration,omitempty"`
	NodeTags       *Tags                     `json:"virtualNodeTags,omitempty"`
	Cycling        json.RawMessage           `json:"virtualNodePoolCyclingDetails,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	CompartmentID  string                    `json:"compartmentId"`
	ClusterID      string                    `json:"clusterId"`
	Name           string                    `json:"displayName"`
	Version        string                    `json:"kubernetesVersion"`
	LifecycleState string                    `json:"lifecycleState"`
	Placements     []VPlacement              `json:"placementConfigurations"`
	Labels         []KeyValue                `json:"initialVirtualNodeLabels,omitempty"`
	Taints         []Taint                   `json:"taints,omitempty"`
	NsgIDs         []string                  `json:"nsgIds,omitempty"`
	Size           int                       `json:"size"`
}

// VirtualNodeWire is the VirtualNode and VirtualNodeSummary body.
type VirtualNodeWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	Name           string                    `json:"displayName"`
	PoolID         string                    `json:"virtualNodePoolId"`
	Version        string                    `json:"kubernetesVersion"`
	AD             string                    `json:"availabilityDomain"`
	FaultDomain    string                    `json:"faultDomain"`
	SubnetID       string                    `json:"subnetId"`
	PrivateIP      string                    `json:"privateIp,omitempty"`
	LifecycleState string                    `json:"lifecycleState"`
	NsgIDs         []string                  `json:"nsgIds,omitempty"`
}

func vnpView(p *VirtualNodePool) VNPWire {
	return VNPWire{
		ID: p.ID, CompartmentID: p.CompartmentID, ClusterID: p.ClusterID, Name: p.Name, Version: p.Version,
		LifecycleState: p.State, Placements: p.Placements, Labels: p.Labels, Taints: p.Taints, NsgIDs: p.NsgIDs,
		Size: p.Size, TimeCreated: p.TimeCreated, TimeUpdated: p.TimeUpdated, PodConfig: p.PodConfig,
		NodeTags: p.NodeTags, Cycling: p.Cycling, FreeformTags: p.Tags.FreeformTags, DefinedTags: p.Tags.DefinedTags,
	}
}

func virtualNodeView(n *VirtualNode, poolID string) VirtualNodeWire {
	return VirtualNodeWire{
		ID: n.ID, Name: n.Name, PoolID: poolID, Version: n.Version, AD: n.AD, FaultDomain: n.FaultDomain,
		SubnetID: n.SubnetID, PrivateIP: n.PrivateIP, LifecycleState: n.State, NsgIDs: n.NsgIDs,
		TimeCreated: n.TimeCreated, FreeformTags: n.Tags.FreeformTags, DefinedTags: n.Tags.DefinedTags,
	}
}

// validEviction checks an ISO 8601 PT duration between PT0M and PT60M.
func validEviction(name string, s *string) *APIError {
	if s == nil {
		return nil
	}

	m := evictionRe.FindStringSubmatch(*s)
	if m == nil || *s == "PT" {
		return errParam(name + " must be an ISO 8601 duration such as PT30M")
	}

	var total time.Duration

	for i, unit := range []time.Duration{time.Hour, time.Minute, time.Second} {
		if m[i+1] == "" {
			continue
		}

		total += time.Duration(atoiSmall(m[i+1])) * unit
	}

	if total > maxEvictionMin*time.Minute {
		return errParam(name + " must be between PT0M and PT60M")
	}

	return nil
}

// atoiSmall parses at most digitsMax digits, so the result cannot overflow.
func atoiSmall(s string) int {
	n := 0

	for _, r := range s[:min(len(s), digitsMax)] {
		n = n*decimalBase + int(r-'0')
	}

	return n
}

func (b *InMemoryBackend) checkVPlacements(ps []VPlacement, vcnID string) *APIError {
	if len(ps) == 0 {
		return errMissing("placementConfigurations is required")
	}

	for _, p := range ps {
		if p.AvailabilityDomain == "" {
			return errMissing("placementConfigurations.availabilityDomain is required")
		}

		sub, aerr := b.checkSubnet("placementConfigurations.subnetId", p.SubnetID, vcnID)
		if aerr != nil {
			return aerr
		}

		if sub.AvailabilityDomain != "" && sub.AvailabilityDomain != p.AvailabilityDomain {
			return errParam("subnet " + p.SubnetID + " is not in availability domain " + p.AvailabilityDomain)
		}
	}

	return nil
}

func (b *InMemoryBackend) checkPodConfig(pc *PodConfig, vcnID string) *APIError {
	if pc == nil || pc.Shape == "" || pc.SubnetID == "" {
		return errMissing("podConfiguration.subnetId and podConfiguration.shape are required")
	}

	_, aerr := b.checkSubnet("podConfiguration.subnetId", pc.SubnetID, vcnID)

	return aerr
}

func checkTaints(ts []Taint) *APIError {
	for _, t := range ts {
		if t.Key == "" || (t.Effect != "NoSchedule" && t.Effect != "PreferNoSchedule" && t.Effect != "NoExecute") {
			return errParam("taints need a key and an effect of NoSchedule, PreferNoSchedule or NoExecute")
		}
	}

	return nil
}

func checkSize(size int) *APIError {
	if size < 0 || size > maxVirtualNodes {
		return errParam("size must be between 0 and 1000")
	}

	return nil
}

func (b *InMemoryBackend) vnpNameTaken(clusterID, name, except string) bool {
	for _, p := range b.vpools {
		if p.ID != except && p.ClusterID == clusterID && p.State != stateDeleted && p.State != stateDeleting &&
			strs.Equal(p.Name, name) {
			return true
		}
	}

	return false
}

// replay returns the work request of an earlier request that used the same retry token.
func (b *InMemoryBackend) replay(scope, token string, alive func(id string) bool) (string, bool, *APIError) {
	v, ok := b.retry.Lookup(scope, token, b.nowUTC())
	if !ok {
		return "", false, nil
	}

	id, wr := ociretry.Split(v)
	if !alive(id) {
		return "", true, errInvalidatedToken()
	}

	return wr, true, nil
}

func (b *InMemoryBackend) clusterAlive(id string) bool {
	c, ok := b.clusters[id]

	return ok && c.State != stateDeleting && c.State != stateDeleted
}

func (b *InMemoryBackend) vnpAlive(id string) bool {
	p, ok := b.vpools[id]

	return ok && p.State != stateDeleting && p.State != stateDeleted
}

// CreateVirtualNodePool creates a virtual node pool and its virtual nodes; both start CREATING.
func (b *InMemoryBackend) CreateVirtualNodePool(in CreateVNPIn) (string, error) {
	b.mu.Lock("CreateVirtualNodePool")
	defer b.mu.Unlock()

	b.settleAll()

	if wr, hit, aerr := b.replay("CreateVirtualNodePool", in.RetryToken, b.vnpAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	if in.Size == nil {
		return "", errMissing("size is required")
	}

	if aerr := firstErr(b.checkCompartment(in.Compartment), validateName(in.Name), checkSize(*in.Size),
		b.checkTags(in.Tags), checkLabels(in.Labels), checkTaints(in.Taints)); aerr != nil {
		return "", aerr
	}

	c, aerr := b.activeCluster(in.ClusterID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(
		b.checkVPlacements(in.Placements, c.VcnID),
		b.checkPodConfig(in.PodConfig, c.VcnID),
	); aerr != nil {
		return "", aerr
	}

	if b.vnpNameTaken(c.ID, in.Name, "") {
		return "", errExists(in.Name)
	}

	now := b.nowUTC()
	p := &VirtualNodePool{
		ID: newOCID("virtualnodepool"), ClusterID: c.ID, CompartmentID: in.Compartment, Name: in.Name,
		Version: c.Version, State: stateCreating, Etag: newEtag(), Placements: in.Placements, PodConfig: in.PodConfig,
		Labels: in.Labels, Taints: in.Taints, NsgIDs: in.NsgIDs, Size: *in.Size, Tags: in.Tags, NodeTags: in.NodeTags,
		Cycling: in.Cycling, TimeCreated: now, Seq: b.nextSeq(), TransitionAt: b.transitionAt(),
	}
	b.vpools[p.ID] = p
	b.resizeVirtualNodes(p)

	wr := b.newWR(wrVNPCreate, p.CompartmentID, entityVNP, p.ID, actionCreated, p.TransitionAt)
	b.retry.Remember("CreateVirtualNodePool", in.RetryToken, ociretry.Join(p.ID, wr), now)

	return wr, nil
}

func liveVirtualNodes(p *VirtualNodePool) []*VirtualNode {
	return slices.DeleteFunc(slices.Clone(p.Nodes), func(n *VirtualNode) bool { return n.State == stateDeleting })
}

// resizeVirtualNodes adds or removes virtual nodes until the live count equals the pool size.
func (b *InMemoryBackend) resizeVirtualNodes(p *VirtualNodePool) {
	live := liveVirtualNodes(p)

	for i := len(live) - 1; i >= p.Size; i-- {
		live[i].State, live[i].TransitionAt = stateDeleting, p.TransitionAt
	}

	for i := len(live); i < p.Size; i++ {
		pl := p.Placements[i%len(p.Placements)]
		n := &VirtualNode{
			ID: newOCID("virtualnode"), Name: p.Name + "-" + ocidTail(newOCID("n"), shortTail), Version: p.Version,
			AD: pl.AvailabilityDomain, FaultDomain: faultDomainOne, SubnetID: pl.SubnetID, NsgIDs: p.NsgIDs,
			State: stateCreating, TransitionAt: p.TransitionAt, TimeCreated: b.nowUTC(), Seq: b.nextSeq(),
		}

		if len(pl.FaultDomain) > 0 {
			n.FaultDomain = pl.FaultDomain[0]
		}

		if p.NodeTags != nil {
			n.Tags = *p.NodeTags
		}

		n.PrivateIP = b.allocIP(pl.SubnetID)
		p.Nodes = append(p.Nodes, n)
	}
}

func (b *InMemoryBackend) vnp(id string) (*VirtualNodePool, *APIError) {
	p, ok := b.vpools[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return p, nil
}

// GetVirtualNodePool returns one virtual node pool and its etag; DELETED pools stay readable.
func (b *InMemoryBackend) GetVirtualNodePool(id string) (VNPWire, string, error) {
	b.mu.Lock("GetVirtualNodePool")
	defer b.mu.Unlock()

	b.settleAll()

	p, aerr := b.vnp(id)
	if aerr != nil {
		return VNPWire{}, "", aerr
	}

	return vnpView(p), p.Etag, nil
}

// VNPListIn are the ListVirtualNodePools filters.
type VNPListIn struct {
	Compartment string
	ClusterID   string
	Name        string
	States      []string
	ListOpts
}

// ListVirtualNodePools lists the virtual node pools of a compartment.
func (b *InMemoryBackend) ListVirtualNodePools(in VNPListIn) (page.Page[VNPWire], error) {
	if aerr := firstErr(b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, "ID", "NAME", "TIME_CREATED")); aerr != nil {
		return page.Page[VNPWire]{}, aerr
	}

	b.mu.Lock("ListVirtualNodePools")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*VirtualNodePool

	for _, p := range b.vpools {
		if p.CompartmentID != in.Compartment || (in.ClusterID != "" && p.ClusterID != in.ClusterID) ||
			(in.Name != "" && !strs.Equal(p.Name, in.Name)) {
			continue
		}

		if len(in.States) > 0 && !containsFold(in.States, p.State) {
			continue
		}

		all = append(all, p)
	}

	pg := paginate(all, in.ListOpts, func(p *VirtualNodePool) string { return p.ID },
		func(p *VirtualNodePool) string { return p.Name },
		func(p *VirtualNodePool) (time.Time, int64) { return p.TimeCreated, p.Seq })

	out := make([]VNPWire, 0, len(pg.Data))
	for _, p := range pg.Data {
		out = append(out, vnpView(p))
	}

	return page.Page[VNPWire]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) mutableVNP(id, ifMatch string) (*VirtualNodePool, *Cluster, *APIError) {
	b.settleAll()

	p, aerr := b.vnp(id)
	if aerr != nil {
		return nil, nil, aerr
	}

	if aerr = checkEtag(p.Etag, ifMatch); aerr != nil {
		return nil, nil, aerr
	}

	if p.State != stateActive {
		return nil, nil, errState(
			"Virtual node pool " + id + " is in state " + p.State + "; the operation requires ACTIVE",
		)
	}

	return p, b.clusters[p.ClusterID], nil
}

func (b *InMemoryBackend) applyVNPUpdate(p *VirtualNodePool, in UpdateVNPIn, c *Cluster) *APIError {
	vcn := ""
	if c != nil {
		vcn = c.VcnID
	}

	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return aerr
		}

		if b.vnpNameTaken(p.ClusterID, *in.Name, p.ID) {
			return errExists(*in.Name)
		}

		p.Name = *in.Name
	}

	if in.Size != nil {
		if aerr := checkSize(*in.Size); aerr != nil {
			return aerr
		}

		p.Size = *in.Size
	}

	if in.Placements != nil {
		if aerr := b.checkVPlacements(in.Placements, vcn); aerr != nil {
			return aerr
		}

		p.Placements = in.Placements
	}

	if in.PodConfig != nil {
		if aerr := b.checkPodConfig(in.PodConfig, vcn); aerr != nil {
			return aerr
		}

		p.PodConfig = in.PodConfig
	}

	if in.Labels != nil {
		p.Labels = in.Labels
	}

	if in.Taints != nil {
		p.Taints = in.Taints
	}

	p.NsgIDs, p.NodeTags, p.Cycling = orSlice(in.NsgIDs, p.NsgIDs), orPtr(in.NodeTags, p.NodeTags),
		orSlice(in.Cycling, p.Cycling)
	p.Tags.merge(in.Tags)

	return nil
}

// UpdateVirtualNodePool changes a virtual node pool; size changes add or remove virtual nodes.
func (b *InMemoryBackend) UpdateVirtualNodePool(id, ifMatch string, in UpdateVNPIn) (string, error) {
	b.mu.Lock("UpdateVirtualNodePool")
	defer b.mu.Unlock()

	p, c, aerr := b.mutableVNP(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	next := *p
	if aerr = firstErr(b.checkTags(in.Tags), checkLabels(in.Labels), checkTaints(in.Taints),
		b.applyVNPUpdate(&next, in, c)); aerr != nil {
		return "", aerr
	}

	*p = next
	now := b.nowUTC()
	p.State, p.Etag, p.TimeUpdated, p.TransitionAt = stateUpdating, newEtag(), &now, b.transitionAt()
	b.resizeVirtualNodes(p)

	return b.newWR(wrVNPUpdate, p.CompartmentID, entityVNP, p.ID, actionUpdated, p.TransitionAt), nil
}

// DeleteVirtualNodePool deletes a virtual node pool and its virtual nodes.
func (b *InMemoryBackend) DeleteVirtualNodePool(id, ifMatch, grace string) (string, error) {
	b.mu.Lock("DeleteVirtualNodePool")
	defer b.mu.Unlock()

	p, _, aerr := b.mutableVNP(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if grace != "" {
		if aerr = validEviction("overrideEvictionGraceDurationVnp", &grace); aerr != nil {
			return "", aerr
		}
	}

	b.markVNPDeleting(p, b.transitionAt())

	return b.newWR(wrVNPDelete, p.CompartmentID, entityVNP, p.ID, actionDeleted, p.TransitionAt), nil
}

func (b *InMemoryBackend) markVNPDeleting(p *VirtualNodePool, at *time.Time) {
	p.State, p.TransitionAt, p.Etag = stateDeleting, at, newEtag()

	for _, n := range p.Nodes {
		n.State, n.TransitionAt = stateDeleting, at
	}
}

func settleVNP(p *VirtualNodePool, now time.Time) {
	if due(p.TransitionAt, now) {
		p.TransitionAt = nil

		switch p.State {
		case stateCreating, stateUpdating:
			p.State = stateActive
		case stateDeleting:
			p.State = stateDeleted
		}
	}

	kept := p.Nodes[:0]

	for _, n := range p.Nodes {
		if due(n.TransitionAt, now) {
			n.TransitionAt = nil

			switch n.State {
			case stateCreating, stateUpdating:
				n.State = stateActive
			case stateDeleting:
				continue
			}
		}

		kept = append(kept, n)
	}

	clear(p.Nodes[len(kept):])
	p.Nodes = kept
}

// VNodeListIn are the ListVirtualNodes filters.
type VNodeListIn struct {
	PoolID string
	Name   string
	ListOpts
}

// ListVirtualNodes lists the virtual nodes of a virtual node pool.
func (b *InMemoryBackend) ListVirtualNodes(in VNodeListIn) (page.Page[VirtualNodeWire], error) {
	if aerr := validListOpts(in.ListOpts, "ID", "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[VirtualNodeWire]{}, aerr
	}

	b.mu.Lock("ListVirtualNodes")
	defer b.mu.Unlock()

	b.settleAll()

	p, aerr := b.vnp(in.PoolID)
	if aerr != nil {
		return page.Page[VirtualNodeWire]{}, aerr
	}

	var all []*VirtualNode

	for _, n := range p.Nodes {
		if in.Name == "" || strings.EqualFold(n.Name, in.Name) {
			all = append(all, n)
		}
	}

	pg := paginate(all, in.ListOpts, func(n *VirtualNode) string { return n.ID },
		func(n *VirtualNode) string { return n.Name },
		func(n *VirtualNode) (time.Time, int64) { return n.TimeCreated, n.Seq })

	out := make([]VirtualNodeWire, 0, len(pg.Data))
	for _, n := range pg.Data {
		out = append(out, virtualNodeView(n, p.ID))
	}

	return page.Page[VirtualNodeWire]{Data: out, Next: pg.Next}, nil
}

// GetVirtualNode returns one virtual node of a pool.
func (b *InMemoryBackend) GetVirtualNode(poolID, nodeID string) (VirtualNodeWire, error) {
	b.mu.Lock("GetVirtualNode")
	defer b.mu.Unlock()

	b.settleAll()

	p, aerr := b.vnp(poolID)
	if aerr != nil {
		return VirtualNodeWire{}, aerr
	}

	i := slices.IndexFunc(p.Nodes, func(n *VirtualNode) bool { return n.ID == nodeID })
	if i < 0 {
		return VirtualNodeWire{}, errNotFound(nodeID)
	}

	return virtualNodeView(p.Nodes[i], p.ID), nil
}

func (h *Handler) createVNP(q *req) error {
	var in CreateVNPIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	wr, err := h.Backend.CreateVirtualNodePool(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getVNP(q *req) error {
	w, etag, err := h.Backend.GetVirtualNodePool(q.param("virtualNodePoolId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listVNPs(q *req) error {
	pg, err := h.Backend.ListVirtualNodePools(VNPListIn{
		Compartment: q.query("compartmentId"), ClusterID: q.query("clusterId"), Name: q.query("name"),
		States: q.queryAll("lifecycleState"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) updateVNP(q *req) error {
	var in UpdateVNPIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateVirtualNodePool(q.param("virtualNodePoolId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteVNP(q *req) error {
	wr, err := h.Backend.DeleteVirtualNodePool(q.param("virtualNodePoolId"), q.ifMatch(),
		q.query("overrideEvictionGraceDurationVnp"))

	return acceptedWR(q, wr, err)
}

func (h *Handler) listVirtualNodes(q *req) error {
	pg, err := h.Backend.ListVirtualNodes(VNodeListIn{
		PoolID: q.param("virtualNodePoolId"), Name: q.query("name"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) getVirtualNode(q *req) error {
	w, err := h.Backend.GetVirtualNode(q.param("virtualNodePoolId"), q.param("virtualNodeId"))

	return reply(q, w, "", err)
}
