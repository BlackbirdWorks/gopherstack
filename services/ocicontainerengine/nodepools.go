package ocicontainerengine

import (
	"encoding/binary"
	"encoding/json"
	"math"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxPoolSize     = 1000
	faultDomains    = 3
	maxMemPerOcpu   = 64
	maxLabelsPerSet = 100
)

var adRe = regexp.MustCompile(`^[A-Za-z0-9]{4}:[A-Z0-9-]+-AD-\d+$`)

// NodeConfigIn is nodeConfigDetails on create and update.
type NodeConfigIn struct {
	Tags
	Size             *int        `json:"size"`
	KmsKeyID         *string     `json:"kmsKeyId"`
	IsPvEncryption   *bool       `json:"isPvEncryptionInTransitEnabled"`
	ComputeClusterID *string     `json:"computeClusterId"`
	PodNet           *PodNet     `json:"nodePoolPodNetworkOptionDetails"`
	Placements       []Placement `json:"placementConfigs"`
	NsgIDs           []string    `json:"nsgIds"`
}

// CreateNodePoolIn is the CreateNodePool request body.
type CreateNodePoolIn struct {
	Tags
	Version        *string           `json:"kubernetesVersion"`
	ImageName      *string           `json:"nodeImageName"`
	Source         *NodeSource       `json:"nodeSourceDetails"`
	ShapeConfig    *ShapeConfig      `json:"nodeShapeConfig"`
	SSHKey         *string           `json:"sshPublicKey"`
	Quantity       *int              `json:"quantityPerSubnet"`
	NodeConfig     *NodeConfigIn     `json:"nodeConfigDetails"`
	NodeMetadata   map[string]string `json:"nodeMetadata"`
	Eviction       json.RawMessage   `json:"nodeEvictionNodePoolSettings"`
	Cycling        json.RawMessage   `json:"nodePoolCyclingDetails"`
	SecondaryVnics json.RawMessage   `json:"secondaryVnics"`
	PrimaryVnic    json.RawMessage   `json:"primaryVnic"`
	Compartment    string            `json:"compartmentId"`
	ClusterID      string            `json:"clusterId"`
	Name           string            `json:"name"`
	Shape          string            `json:"nodeShape"`
	LaunchType     string            `json:"networkLaunchType"`
	Labels         []KeyValue        `json:"initialNodeLabels"`
	SubnetIDs      []string          `json:"subnetIds"`
}

// UpdateNodePoolIn is the UpdateNodePool request body.
type UpdateNodePoolIn struct {
	Tags
	Name           *string           `json:"name"`
	Version        *string           `json:"kubernetesVersion"`
	Quantity       *int              `json:"quantityPerSubnet"`
	NodeConfig     *NodeConfigIn     `json:"nodeConfigDetails"`
	Source         *NodeSource       `json:"nodeSourceDetails"`
	ShapeConfig    *ShapeConfig      `json:"nodeShapeConfig"`
	SSHKey         *string           `json:"sshPublicKey"`
	Shape          *string           `json:"nodeShape"`
	NodeMetadata   map[string]string `json:"nodeMetadata"`
	Eviction       json.RawMessage   `json:"nodeEvictionNodePoolSettings"`
	Cycling        json.RawMessage   `json:"nodePoolCyclingDetails"`
	SecondaryVnics json.RawMessage   `json:"secondaryVnics"`
	PrimaryVnic    json.RawMessage   `json:"primaryVnic"`
	LaunchType     string            `json:"networkLaunchType"`
	Labels         []KeyValue        `json:"initialNodeLabels"`
	SubnetIDs      []string          `json:"subnetIds"`
}

// NodeWire is a Node body.
type NodeWire struct {
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	ID                 string                    `json:"id"`
	Name               string                    `json:"name"`
	KubernetesVersion  string                    `json:"kubernetesVersion"`
	AvailabilityDomain string                    `json:"availabilityDomain"`
	SubnetID           string                    `json:"subnetId"`
	NodePoolID         string                    `json:"nodePoolId"`
	FaultDomain        string                    `json:"faultDomain"`
	PrivateIP          string                    `json:"privateIp,omitempty"`
	LifecycleState     string                    `json:"lifecycleState"`
}

// NodePoolWire is the NodePool and NodePoolSummary body.
type NodePoolWire struct {
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	NodeMetadata      map[string]string         `json:"nodeMetadata,omitempty"`
	NodeShapeConfig   *ShapeConfig              `json:"nodeShapeConfig,omitempty"`
	NodeSource        *nodeSourceOption         `json:"nodeSource,omitempty"`
	NodeSourceDetails *NodeSource               `json:"nodeSourceDetails,omitempty"`
	QuantityPerSubnet *int                      `json:"quantityPerSubnet,omitempty"`
	NodeConfigDetails *NodeConfig               `json:"nodeConfigDetails,omitempty"`
	Eviction          json.RawMessage           `json:"nodeEvictionNodePoolSettings,omitempty"`
	Cycling           json.RawMessage           `json:"nodePoolCyclingDetails,omitempty"`
	Nodes             []NodeWire                `json:"nodes,omitempty"`
	ID                string                    `json:"id"`
	ClusterID         string                    `json:"clusterId"`
	CompartmentID     string                    `json:"compartmentId"`
	Name              string                    `json:"name"`
	KubernetesVersion string                    `json:"kubernetesVersion"`
	NodeShape         string                    `json:"nodeShape"`
	NodeImageID       string                    `json:"nodeImageId,omitempty"`
	NodeImageName     string                    `json:"nodeImageName,omitempty"`
	SSHPublicKey      string                    `json:"sshPublicKey,omitempty"`
	LifecycleState    string                    `json:"lifecycleState"`
	InitialNodeLabels []KeyValue                `json:"initialNodeLabels"`
	SubnetIDs         []string                  `json:"subnetIds"`
}

func nodePoolView(np *NodePool, summary bool) NodePoolWire {
	w := NodePoolWire{
		ID: np.ID, ClusterID: np.ClusterID, CompartmentID: np.CompartmentID, Name: np.Name,
		KubernetesVersion: np.Version, NodeShape: np.Shape, NodeShapeConfig: np.ShapeConfig,
		NodeSourceDetails: np.NodeSource, NodeImageName: np.ImageName, SSHPublicKey: np.SSHKey,
		LifecycleState: np.State, InitialNodeLabels: np.Labels, NodeMetadata: np.NodeMetadata,
		QuantityPerSubnet: np.QuantityPerSub, SubnetIDs: np.SubnetIDs, Eviction: np.Eviction, Cycling: np.Cycling,
		FreeformTags: np.Tags.FreeformTags, DefinedTags: np.Tags.DefinedTags,
	}
	if w.InitialNodeLabels == nil {
		w.InitialNodeLabels = []KeyValue{}
	}

	if w.SubnetIDs == nil {
		w.SubnetIDs = []string{}
	}

	if np.NodeSource != nil {
		w.NodeImageID = np.NodeSource.ImageID
		w.NodeSource = &nodeSourceOption{
			SourceType: sourceImage,
			SourceName: np.ImageName,
			ImageID:    np.NodeSource.ImageID,
		}
	}

	if np.NodeConfig != nil {
		w.NodeConfigDetails = np.NodeConfig
		for _, p := range np.NodeConfig.Placements {
			if !slices.Contains(w.SubnetIDs, p.SubnetID) {
				w.SubnetIDs = append(w.SubnetIDs, p.SubnetID)
			}
		}
	}

	if !summary {
		w.Nodes = make([]NodeWire, 0, len(np.Nodes))
		for _, n := range np.Nodes {
			w.Nodes = append(w.Nodes, NodeWire{
				ID: n.ID, Name: n.Name, KubernetesVersion: n.Version, AvailabilityDomain: n.AD, SubnetID: n.SubnetID,
				NodePoolID: np.ID, FaultDomain: n.FaultDomain, PrivateIP: n.PrivateIP, LifecycleState: n.State,
				FreeformTags: n.Tags.FreeformTags, DefinedTags: n.Tags.DefinedTags,
			})
		}
	}

	return w
}

func rejectUnsupported(secondary, primary json.RawMessage, launch string) *APIError {
	if len(secondary) > 0 && string(secondary) != "null" && string(secondary) != "[]" {
		return errParam("secondaryVnics are not supported by this emulator")
	}

	if len(primary) > 0 && string(primary) != "null" {
		return errParam("primaryVnic is not supported by this emulator")
	}

	if launch != "" {
		return errParam("networkLaunchType is not supported by this emulator")
	}

	return nil
}

// checkShape validates a node shape and its shapeConfig and returns the resolved config.
func (b *InMemoryBackend) checkShape(name string, cfg *ShapeConfig) (ShapeInfo, *ShapeConfig, *APIError) {
	if strings.TrimSpace(name) == "" {
		return ShapeInfo{}, nil, errMissing("nodeShape is required")
	}

	if b.net == nil {
		return ShapeInfo{Name: name}, cfg, nil
	}

	sh, ok := b.net.Shape(name)
	if !ok {
		return ShapeInfo{}, nil, errParam("nodeShape " + name + " is not a known compute shape")
	}

	if !sh.Flexible {
		if cfg != nil && (cfg.Ocpus != nil || cfg.MemoryInGBs != nil) {
			return sh, nil, errParam("nodeShapeConfig is only valid for flexible shapes, " + name + " is not flexible")
		}

		return sh, nil, nil
	}

	resolved, aerr := flexConfig(sh, cfg)

	return sh, resolved, aerr
}

// flexConfig validates a flexible shape's ocpus and memory and fills the default memory.
func flexConfig(sh ShapeInfo, cfg *ShapeConfig) (*ShapeConfig, *APIError) {
	if cfg == nil || cfg.Ocpus == nil {
		return nil, errMissing("nodeShapeConfig.ocpus is required for the flexible shape " + sh.Name)
	}

	o := *cfg.Ocpus
	if o < sh.MinOcpus || o > sh.MaxOcpus || o != math.Trunc(o) {
		return nil, errParam("nodeShapeConfig.ocpus must be a whole number between " +
			strconv.FormatFloat(sh.MinOcpus, 'f', -1, 64) + " and " + strconv.FormatFloat(sh.MaxOcpus, 'f', -1, 64))
	}

	mem := o * sh.DefaultMemPerOcpu
	if cfg.MemoryInGBs != nil {
		mem = *cfg.MemoryInGBs
	}

	if mem < max(1, o) || mem > min(sh.MaxMemoryGBs, o*maxMemPerOcpu) {
		return nil, errParam("nodeShapeConfig.memoryInGBs is outside the range allowed for " + sh.Name)
	}

	return &ShapeConfig{Ocpus: &o, MemoryInGBs: &mem}, nil
}

// checkSource resolves the node image and verifies it runs on the shape's architecture.
func (b *InMemoryBackend) checkSource(src *NodeSource, name *string, sh ShapeInfo) (*NodeSource, string, *APIError) {
	out, aerr := b.resolveSource(src, name)
	if aerr != nil {
		return nil, "", aerr
	}

	if out.BootVolumeGBs != nil && *out.BootVolumeGBs < 1 {
		return nil, "", errParam("nodeSourceDetails.bootVolumeSizeInGBs must be positive")
	}

	if !strings.HasPrefix(out.ImageID, "ocid1.image.") {
		return nil, "", errParam("nodeSourceDetails.imageId must be the OCID of an image")
	}

	imgName, aerr := b.verifyImage(out.ImageID, sh)
	if aerr != nil {
		return nil, "", aerr
	}

	return &out, imgName, nil
}

func (b *InMemoryBackend) resolveSource(src *NodeSource, name *string) (NodeSource, *APIError) {
	switch {
	case src != nil:
		if src.SourceType != "" && src.SourceType != sourceImage {
			return NodeSource{}, errParam("nodeSourceDetails.sourceType must be IMAGE")
		}

		out := *src
		out.SourceType = sourceImage

		return out, nil
	case name != nil && *name != "":
		id := ""

		if b.net != nil {
			for _, img := range b.net.Images() {
				if img.Name == *name {
					id = img.ID
				}
			}

			if id == "" {
				return NodeSource{}, errParam("nodeImageName " + *name + " does not match a known image")
			}
		}

		return NodeSource{SourceType: sourceImage, ImageID: id}, nil
	}

	return NodeSource{}, errMissing("nodeSourceDetails is required")
}

// verifyImage checks the image exists in OCI Core and matches the shape architecture; it returns the image name.
func (b *InMemoryBackend) verifyImage(id string, sh ShapeInfo) (string, *APIError) {
	if b.net == nil {
		return "", nil
	}

	img, ok := b.net.Image(id)
	if !ok {
		return "", errParam("nodeSourceDetails.imageId " + id + " does not reference an existing image")
	}

	if sh.Arch != "" && img.Arch != sh.Arch {
		return "", errParam(
			"image " + img.Name + " (" + img.Arch + ") cannot run on shape " + sh.Name + " (" + sh.Arch + ")",
		)
	}

	return img.Name, nil
}

func (b *InMemoryBackend) checkPlacements(ps []Placement, vcnID string) *APIError {
	if len(ps) == 0 {
		return errMissing("nodeConfigDetails.placementConfigs is required")
	}

	for _, p := range ps {
		if !adRe.MatchString(p.AvailabilityDom) {
			return errParam(
				"placementConfigs.availabilityDomain " + p.AvailabilityDom + " is not a valid availability domain",
			)
		}

		sub, aerr := b.checkSubnet("placementConfigs.subnetId", p.SubnetID, vcnID)
		if aerr != nil {
			return aerr
		}

		if sub.AvailabilityDomain != "" && !strings.EqualFold(sub.AvailabilityDomain, p.AvailabilityDom) {
			return errParam("subnet " + p.SubnetID + " is in availability domain " + sub.AvailabilityDomain)
		}
	}

	return nil
}

// buildNodeConfig validates nodeConfigDetails and merges it over the current one.
func (b *InMemoryBackend) buildNodeConfig(in *NodeConfigIn, cur *NodeConfig, c *Cluster) (*NodeConfig, *APIError) {
	if in.ComputeClusterID != nil {
		return nil, errParam("computeClusterId is not supported by this emulator")
	}

	out := NodeConfig{Placements: []Placement{}}
	if cur != nil {
		out = *cur
	}

	if in.Size != nil {
		if *in.Size < 0 || *in.Size > maxPoolSize {
			return nil, errParam("nodeConfigDetails.size must be between 0 and " + strconv.Itoa(maxPoolSize))
		}

		out.Size = *in.Size
	} else if cur == nil {
		return nil, errMissing("nodeConfigDetails.size is required")
	}

	if in.Placements != nil || cur == nil {
		if aerr := b.checkPlacements(in.Placements, c.VcnID); aerr != nil {
			return nil, aerr
		}

		out.Placements = in.Placements
	}

	if in.KmsKeyID != nil {
		if aerr := b.checkKey("nodeConfigDetails.kmsKeyId", *in.KmsKeyID); aerr != nil {
			return nil, aerr
		}

		out.KmsKeyID = *in.KmsKeyID
	}

	if in.NsgIDs != nil {
		out.NsgIDs = in.NsgIDs
	}

	if in.IsPvEncryption != nil {
		out.IsPvEncryption = in.IsPvEncryption
	}

	out.Tags.merge(in.Tags)

	return b.applyPodNet(&out, in.PodNet, c)
}

func (b *InMemoryBackend) applyPodNet(out *NodeConfig, in *PodNet, c *Cluster) (*NodeConfig, *APIError) {
	clusterCNI := c.PodNetworks[0].CniType

	if in == nil {
		if out.PodNet == nil {
			out.PodNet = &PodNet{CniType: clusterCNI}
		}

		if clusterCNI == cniNative && len(out.PodNet.PodSubnetIDs) == 0 {
			return nil, errMissing(
				"nodeConfigDetails.nodePoolPodNetworkOptionDetails.podSubnetIds is required for " + cniNative,
			)
		}

		return out, nil
	}

	if in.CniType != clusterCNI {
		return nil, errParam("nodePoolPodNetworkOptionDetails.cniType must match the cluster CNI " + clusterCNI)
	}

	if in.CniType == cniNative {
		if len(in.PodSubnetIDs) == 0 {
			return nil, errMissing("nodePoolPodNetworkOptionDetails.podSubnetIds is required for " + cniNative)
		}

		for _, id := range in.PodSubnetIDs {
			if _, aerr := b.checkSubnet("podSubnetIds", id, c.VcnID); aerr != nil {
				return nil, aerr
			}
		}
	}

	out.PodNet = in

	return out, nil
}

func (b *InMemoryBackend) checkLegacy(ids []string, qty *int, vcnID string) *APIError {
	if len(ids) == 0 {
		return errMissing("nodeConfigDetails or subnetIds is required")
	}

	if qty == nil || *qty < 0 || *qty > maxPoolSize {
		return errParam("quantityPerSubnet must be between 0 and " + strconv.Itoa(maxPoolSize))
	}

	for _, id := range ids {
		if _, aerr := b.checkSubnet("subnetIds", id, vcnID); aerr != nil {
			return aerr
		}
	}

	return nil
}

func checkLabels(ls []KeyValue) *APIError {
	if len(ls) > maxLabelsPerSet {
		return errParam("initialNodeLabels must have at most 100 entries")
	}

	for _, l := range ls {
		if l.Key == "" {
			return errParam("initialNodeLabels.key is required")
		}
	}

	return nil
}

// activeCluster returns the cluster a node pool operation targets; it must be ACTIVE.
func (b *InMemoryBackend) activeCluster(id string) (*Cluster, *APIError) {
	if id == "" {
		return nil, errMissing("clusterId is required")
	}

	c, ok := b.clusters[id]
	if !ok || c.State == stateDeleted {
		return nil, errNotFound(id)
	}

	if c.State != stateActive {
		return nil, errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	return c, nil
}

// CreateNodePool creates a node pool and its nodes; both start CREATING.
func (b *InMemoryBackend) CreateNodePool(in CreateNodePoolIn) (string, error) {
	b.mu.Lock("CreateNodePool")
	defer b.mu.Unlock()

	b.settleAll()

	if aerr := firstErr(b.checkCompartment(in.Compartment), validateName(in.Name),
		rejectUnsupported(in.SecondaryVnics, in.PrimaryVnic, in.LaunchType), checkLabels(in.Labels)); aerr != nil {
		return "", aerr
	}

	c, aerr := b.activeCluster(in.ClusterID)
	if aerr != nil {
		return "", aerr
	}

	np, aerr := b.buildPool(in, c)
	if aerr != nil {
		return "", aerr
	}

	np.Etag, np.Seq = newEtag(), b.nextSeq()
	np.TransitionAt = b.transitionAt()
	b.pools[np.ID] = np
	b.growPool(np, c, b.targetSize(np))

	return b.newWR(wrNodePoolCreate, np.CompartmentID, entityNodePool, np.ID, actionCreated, np.TransitionAt), nil
}

func (b *InMemoryBackend) buildPool(in CreateNodePoolIn, c *Cluster) (*NodePool, *APIError) {
	ver := def(ptrStr(in.Version), c.Version)
	if aerr := poolVersionOK(ver, c.Version); aerr != nil {
		return nil, aerr
	}

	sh, scfg, aerr := b.checkShape(in.Shape, in.ShapeConfig)
	if aerr != nil {
		return nil, aerr
	}

	src, imgName, aerr := b.checkSource(in.Source, in.ImageName, sh)
	if aerr != nil {
		return nil, aerr
	}

	np := &NodePool{
		ID: newOCID("nodepool"), ClusterID: c.ID, CompartmentID: in.Compartment, Name: in.Name, Version: ver,
		Shape: in.Shape, ShapeConfig: scfg, NodeSource: src, ImageName: imgName, State: stateCreating,
		TimeCreated: b.nowUTC(), Labels: in.Labels, NodeMetadata: in.NodeMetadata, Tags: in.Tags,
		Eviction: in.Eviction, Cycling: in.Cycling, SSHKey: ptrStr(in.SSHKey),
	}

	switch {
	case in.NodeConfig != nil && len(in.SubnetIDs) > 0:
		return nil, errParam("nodeConfigDetails and subnetIds are mutually exclusive")
	case in.NodeConfig != nil:
		if np.NodeConfig, aerr = b.buildNodeConfig(in.NodeConfig, nil, c); aerr != nil {
			return nil, aerr
		}
	default:
		if aerr = b.checkLegacy(in.SubnetIDs, in.Quantity, c.VcnID); aerr != nil {
			return nil, aerr
		}

		np.SubnetIDs, np.QuantityPerSub = in.SubnetIDs, in.Quantity
	}

	return np, nil
}

func ptrStr(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}

// slot is one node placement target.
type slot struct{ ad, subnet string }

func (np *NodePool) slots(b *InMemoryBackend) []slot {
	if np.NodeConfig != nil {
		out := make([]slot, 0, len(np.NodeConfig.Placements))
		for _, p := range np.NodeConfig.Placements {
			out = append(out, slot{p.AvailabilityDom, p.SubnetID})
		}

		return out
	}

	out := make([]slot, 0, len(np.SubnetIDs))

	for _, id := range np.SubnetIDs {
		ad := ""
		if b.net != nil {
			if s, ok := b.net.Subnet(id); ok {
				ad = s.AvailabilityDomain
			}
		}

		out = append(out, slot{def(ad, "Uocm:US-ASHBURN-AD-1"), id})
	}

	return out
}

func (b *InMemoryBackend) targetSize(np *NodePool) int {
	if np.NodeConfig != nil {
		return np.NodeConfig.Size
	}

	return *np.QuantityPerSub * len(np.SubnetIDs)
}

func liveNodes(np *NodePool) []*Node {
	return slices.DeleteFunc(slices.Clone(np.Nodes), func(n *Node) bool { return n.State == stateDeleting })
}

// growPool adds nodes, always to the least populated placement, until the pool has size live nodes.
func (b *InMemoryBackend) growPool(np *NodePool, c *Cluster, size int) {
	slots := np.slots(b)
	if len(slots) == 0 {
		return
	}

	for len(liveNodes(np)) < size {
		np.Nodes = append(np.Nodes, b.newNode(np, c, b.leastLoaded(np, slots)))
	}
}

func (b *InMemoryBackend) leastLoaded(np *NodePool, slots []slot) slot {
	best, bestN := slots[0], math.MaxInt

	for _, s := range slots {
		n := 0

		for _, nd := range liveNodes(np) {
			if nd.AD == s.ad && nd.SubnetID == s.subnet {
				n++
			}
		}

		if n < bestN {
			best, bestN = s, n
		}
	}

	return best
}

func (b *InMemoryBackend) newNode(np *NodePool, c *Cluster, s slot) *Node {
	id := newOCID("instance")
	idx := len(np.Nodes)

	fd := "FAULT-DOMAIN-" + strconv.Itoa(idx%faultDomains+1)
	if np.NodeConfig != nil {
		for _, p := range np.NodeConfig.Placements {
			if p.AvailabilityDom == s.ad && p.SubnetID == s.subnet && len(p.FaultDomains) > 0 {
				fd = p.FaultDomains[idx%len(p.FaultDomains)]
			}
		}
	}

	return &Node{
		ID: id,
		Name: "oke-c" + ocidTail(
			c.ID,
			longTail,
		) + "-n" + ocidTail(
			np.ID,
			longTail,
		) + "-s" + ocidTail(
			s.subnet,
			longTail,
		) +
			"-" + ocidTail(
			id,
			shortTail,
		),
		Version:      np.Version,
		AD:           s.ad,
		SubnetID:     s.subnet,
		FaultDomain:  fd,
		PrivateIP:    b.allocIP(s.subnet),
		State:        stateCreating,
		TransitionAt: np.TransitionAt,
		Tags:         np.Tags,
	}
}

// allocIP hands out the highest free host address of the subnet, or "" when the subnet is unknown.
func (b *InMemoryBackend) allocIP(subnetID string) string {
	if b.net == nil {
		return ""
	}

	sub, ok := b.net.Subnet(subnetID)
	if !ok {
		return ""
	}

	p, err := netip.ParsePrefix(sub.CIDR)
	if err != nil || !p.Addr().Is4() {
		return ""
	}

	used := map[string]bool{}

	for _, np := range b.pools {
		for _, n := range np.Nodes {
			used[n.PrivateIP] = true
		}
	}

	a4 := p.Masked().Addr().As4()
	base := binary.BigEndian.Uint32(a4[:])
	last := base | (^uint32(0) >> p.Bits())

	for ip := last - 1; ip > base+privateEndpointOffset*2; ip-- {
		binary.BigEndian.PutUint32(a4[:], ip)

		if s := netip.AddrFrom4(a4).String(); !used[s] {
			return s
		}
	}

	return ""
}

func (b *InMemoryBackend) pool(id string) (*NodePool, *APIError) {
	np, ok := b.pools[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return np, nil
}

// GetNodePool returns one node pool and its etag; DELETED pools stay readable.
func (b *InMemoryBackend) GetNodePool(id string) (NodePoolWire, string, error) {
	b.mu.Lock("GetNodePool")
	defer b.mu.Unlock()

	b.settleAll()

	np, aerr := b.pool(id)
	if aerr != nil {
		return NodePoolWire{}, "", aerr
	}

	return nodePoolView(np, false), np.Etag, nil
}

// PoolListIn are the ListNodePools filters.
type PoolListIn struct {
	Compartment string
	ClusterID   string
	Name        string
	States      []string
	ListOpts
}

// ListNodePools lists node pools; DELETED ones only when asked for by state.
func (b *InMemoryBackend) ListNodePools(in PoolListIn) (page.Page[NodePoolWire], error) {
	if aerr := validListOpts(in.ListOpts, "ID", "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[NodePoolWire]{}, aerr
	}

	b.mu.Lock("ListNodePools")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.Compartment); aerr != nil {
		return page.Page[NodePoolWire]{}, aerr
	}

	b.settleAll()

	var all []*NodePool

	for _, np := range b.pools {
		if np.CompartmentID != in.Compartment || (in.ClusterID != "" && np.ClusterID != in.ClusterID) ||
			(in.Name != "" && np.Name != in.Name) {
			continue
		}

		if len(in.States) == 0 && np.State == stateDeleted || len(in.States) > 0 && !containsFold(in.States, np.State) {
			continue
		}

		all = append(all, np)
	}

	pg := paginate(
		all,
		in.ListOpts,
		func(n *NodePool) string { return n.ID },
		func(n *NodePool) string { return n.Name },
		func(n *NodePool) (time.Time, int64) { return n.TimeCreated, n.Seq },
	)

	out := make([]NodePoolWire, 0, len(pg.Data))
	for _, np := range pg.Data {
		out = append(out, nodePoolView(np, true))
	}

	return page.Page[NodePoolWire]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) mutablePool(id, ifMatch string) (*NodePool, *Cluster, *APIError) {
	b.settleAll()

	np, aerr := b.pool(id)
	if aerr != nil {
		return nil, nil, aerr
	}

	if aerr = checkEtag(np.Etag, ifMatch); aerr != nil {
		return nil, nil, aerr
	}

	if np.State != stateActive {
		return nil, nil, errState("Node pool " + id + " is in state " + np.State + "; the operation requires ACTIVE")
	}

	return np, b.clusters[np.ClusterID], nil
}

func (b *InMemoryBackend) touchPool(np *NodePool, op string) string {
	np.State, np.Etag, np.TransitionAt = stateUpdating, newEtag(), b.transitionAt()

	return b.newWR(op, np.CompartmentID, entityNodePool, np.ID, actionUpdated, np.TransitionAt)
}

// UpdateNodePool changes a node pool; size changes add or remove nodes.
func (b *InMemoryBackend) UpdateNodePool(id, ifMatch string, in UpdateNodePoolIn) (string, error) {
	b.mu.Lock("UpdateNodePool")
	defer b.mu.Unlock()

	np, c, aerr := b.mutablePool(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	next := *np
	if aerr = firstErr(rejectUnsupported(in.SecondaryVnics, in.PrimaryVnic, in.LaunchType), checkLabels(in.Labels),
		b.applyPoolUpdate(&next, in, c)); aerr != nil {
		return "", aerr
	}

	*np = next
	wr := b.touchPool(np, wrNodePoolUpdate)

	if c != nil {
		b.shrinkPool(np, b.targetSize(np))
		b.growPool(np, c, b.targetSize(np))
	}

	return wr, nil
}

func (b *InMemoryBackend) applyPoolUpdate(np *NodePool, in UpdateNodePoolIn, c *Cluster) *APIError {
	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return aerr
		}

		np.Name = *in.Name
	}

	if in.Version != nil {
		if aerr := poolVersionOK(*in.Version, c.Version); aerr != nil {
			return aerr
		}

		np.Version = *in.Version
	}

	if aerr := b.applyPoolCompute(np, in); aerr != nil {
		return aerr
	}

	if aerr := b.applyPoolPlacement(np, in, c); aerr != nil {
		return aerr
	}

	np.Labels = orSlice(in.Labels, np.Labels)
	np.NodeMetadata = orMap(in.NodeMetadata, np.NodeMetadata)
	np.Eviction = orSlice(in.Eviction, np.Eviction)
	np.Cycling = orSlice(in.Cycling, np.Cycling)

	if in.SSHKey != nil {
		np.SSHKey = *in.SSHKey
	}

	np.Tags.merge(in.Tags)

	return nil
}

func (b *InMemoryBackend) applyPoolCompute(np *NodePool, in UpdateNodePoolIn) *APIError {
	if in.Shape != nil || in.ShapeConfig != nil {
		name := def(ptrStr(in.Shape), np.Shape)

		cfg := in.ShapeConfig
		if cfg == nil && in.Shape == nil {
			cfg = np.ShapeConfig
		}

		sh, resolved, aerr := b.checkShape(name, cfg)
		if aerr != nil {
			return aerr
		}

		if aerr = b.checkArch(np, sh, in.Source); aerr != nil {
			return aerr
		}

		np.Shape, np.ShapeConfig = name, resolved
	}

	if in.Source != nil {
		sh, _, _ := b.checkShape(np.Shape, np.ShapeConfig)

		src, imgName, aerr := b.checkSource(in.Source, nil, sh)
		if aerr != nil {
			return aerr
		}

		np.NodeSource, np.ImageName = src, imgName
	}

	return nil
}

func (b *InMemoryBackend) checkArch(np *NodePool, sh ShapeInfo, src *NodeSource) *APIError {
	if src != nil || np.NodeSource == nil {
		return nil
	}

	_, _, aerr := b.checkSource(np.NodeSource, nil, sh)

	return aerr
}

func (b *InMemoryBackend) applyPoolPlacement(np *NodePool, in UpdateNodePoolIn, c *Cluster) *APIError {
	switch {
	case in.NodeConfig != nil && np.NodeConfig == nil:
		return errParam("nodeConfigDetails cannot be added to a node pool created with subnetIds")
	case in.NodeConfig != nil:
		cfg, aerr := b.buildNodeConfig(in.NodeConfig, np.NodeConfig, c)
		if aerr != nil {
			return aerr
		}

		np.NodeConfig = cfg
	case len(in.SubnetIDs) > 0 || in.Quantity != nil:
		if np.NodeConfig != nil {
			return errParam("subnetIds and quantityPerSubnet cannot be set on a node pool using nodeConfigDetails")
		}

		ids, qty := orSlice(in.SubnetIDs, np.SubnetIDs), orPtr(in.Quantity, np.QuantityPerSub)
		if aerr := b.checkLegacy(ids, qty, c.VcnID); aerr != nil {
			return aerr
		}

		np.SubnetIDs, np.QuantityPerSub = ids, qty
	}

	return nil
}

// shrinkPool marks the surplus nodes DELETING, newest first.
func (b *InMemoryBackend) shrinkPool(np *NodePool, size int) {
	live := liveNodes(np)
	for i := len(live) - 1; i >= size && i >= 0; i-- {
		live[i].State, live[i].TransitionAt = stateDeleting, np.TransitionAt
	}
}

func (b *InMemoryBackend) markPoolDeleting(np *NodePool, at *time.Time) {
	np.State, np.TransitionAt, np.Etag = stateDeleting, at, newEtag()

	for _, n := range np.Nodes {
		n.State, n.TransitionAt = stateDeleting, at
	}
}

// DeleteNodePool deletes a node pool and its nodes.
func (b *InMemoryBackend) DeleteNodePool(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteNodePool")
	defer b.mu.Unlock()

	np, _, aerr := b.mutablePool(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	b.markPoolDeleting(np, b.transitionAt())

	return b.newWR(wrNodePoolDelete, np.CompartmentID, entityNodePool, np.ID, actionDeleted, np.TransitionAt), nil
}

// DeleteNode removes one node; without decrement a replacement is created.
func (b *InMemoryBackend) DeleteNode(poolID, nodeID, ifMatch string, decrement bool) (string, error) {
	b.mu.Lock("DeleteNode")
	defer b.mu.Unlock()

	np, c, aerr := b.mutablePool(poolID, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	i := slices.IndexFunc(np.Nodes, func(n *Node) bool { return n.ID == nodeID && n.State != stateDeleting })
	if i < 0 {
		return "", errNotFound(nodeID)
	}

	if decrement && np.NodeConfig == nil {
		return "", errParam("isDecrementSize requires a node pool that uses nodeConfigDetails")
	}

	victim := np.Nodes[i]
	wr := b.touchPool(np, wrNodePoolUpdate)
	victim.State, victim.TransitionAt = stateDeleting, np.TransitionAt

	if decrement {
		np.NodeConfig.Size--

		return wr, nil
	}

	np.Nodes = append(np.Nodes, b.newNode(np, c, slot{victim.AD, victim.SubnetID}))

	return wr, nil
}

func (h *Handler) createNodePool(q *req) error {
	var in CreateNodePoolIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateNodePool(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getNodePool(q *req) error {
	w, etag, err := h.Backend.GetNodePool(q.param("nodePoolId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listNodePools(q *req) error {
	pg, err := h.Backend.ListNodePools(PoolListIn{
		Compartment: q.query("compartmentId"), ClusterID: q.query("clusterId"), Name: q.query("name"),
		States: q.queryAll("lifecycleState"), ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) updateNodePool(q *req) error {
	var in UpdateNodePoolIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateNodePool(q.param("nodePoolId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteNodePool(q *req) error {
	wr, err := h.Backend.DeleteNodePool(q.param("nodePoolId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteNode(q *req) error {
	dec, _ := strconv.ParseBool(q.query("isDecrementSize"))
	wr, err := h.Backend.DeleteNode(q.param("nodePoolId"), q.param("nodeId"), q.ifMatch(), dec)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getNodePoolOptions(q *req) error {
	all, _ := strconv.ParseBool(q.query("shouldListAllPatchVersions"))
	w, err := h.Backend.GetNodePoolOptions(NodePoolOptionsIn{
		ID: q.param("nodePoolOptionId"), OSType: q.query("nodePoolOsType"), OSArch: q.query("nodePoolOsArch"),
		K8sVersion: q.query("nodePoolK8sVersion"), AllPatches: all,
	})

	return reply(q, w, "", err)
}
