//nolint:staticcheck // legacy cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const allPools = "-"

type updatePlan struct {
	nodeAutoscaling *containerpb.NodePoolAutoscaling
	poolID          string
	nodeVersion     string
	imageType       string
	masterVersion   string
	locations       []string
	hasNodeVersion  bool
	hasMaster       bool
	hasAutoscaling  bool
	hasImage        bool
	hasLocations    bool
}

// UpdateCluster applies the Desired* fields of a ClusterUpdate. Every field with a matching cluster field is
// copied onto it; fields the emulator cannot honour are rejected rather than ignored.
func (b *InMemoryBackend) UpdateCluster(req *containerpb.UpdateClusterRequest) (*containerpb.Operation, error) {
	r, err := resolveCluster(req.GetName(), req.GetProjectId(), req.GetZone(), req.GetClusterId())
	if err != nil {
		return nil, err
	}

	u := req.GetUpdate()
	if u == nil || !u.ProtoReflect().IsValid() || proto.Equal(u, &containerpb.ClusterUpdate{}) {
		return nil, errInvalid("update", "No update was specified.")
	}

	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	b.sweepLocked()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	pool := ""
	if id := u.GetDesiredNodePoolId(); id != "" && id != allPools {
		pool = id
	}

	if err = b.conflictLocked(r, pool); err != nil {
		return nil, err
	}

	tmp := clone(c.Cluster)
	saved := clonePools(c.Pools)

	plan, err := collectUpdate(c, tmp, u)
	if err != nil {
		return nil, err
	}

	typ, err := b.applyPlan(c, tmp, plan)
	if err != nil {
		c.Pools = saved

		return nil, err
	}

	c.Cluster = tmp
	kind := kindClusterOp

	if pool != "" {
		kind, r.pool = kindPoolOp, pool
	}

	o := b.newOpLocked(r, typ, kind, tmp.GetSelfLink(), b.timing.Mutate, false)

	return b.opViewLocked(o), nil
}

func collectUpdate(c *clusterRec, tmp *containerpb.Cluster, u *containerpb.ClusterUpdate) (*updatePlan, error) {
	p := &updatePlan{}

	var err error

	u.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		err = p.take(c, tmp, fd, v)

		return err == nil
	})

	return p, err
}

const fieldEtag = "etag"

// takePlan records the fields that act on node pools, versions and locations; it reports whether name was one.
func (p *updatePlan) takePlan(name string, v protoreflect.Value) bool {
	switch name {
	case "desired_node_pool_id":
		p.poolID = v.String()
	case "desired_node_version":
		p.nodeVersion, p.hasNodeVersion = v.String(), true
	case "desired_master_version":
		p.masterVersion, p.hasMaster = v.String(), true
	case "desired_image_type":
		p.imageType, p.hasImage = v.String(), true
	case "desired_node_pool_autoscaling":
		p.nodeAutoscaling, p.hasAutoscaling = msgOf[*containerpb.NodePoolAutoscaling](v), true
	case "desired_locations":
		p.hasLocations = true

		for i := range v.List().Len() {
			p.locations = append(p.locations, v.List().Get(i).String())
		}
	default:
		return false
	}

	return true
}

// msgOf returns a copy of the message held by v.
func msgOf[T proto.Message](v protoreflect.Value) T {
	out, _ := proto.Clone(v.Message().Interface()).(T)

	return out
}

func (p *updatePlan) take(
	c *clusterRec,
	tmp *containerpb.Cluster,
	fd protoreflect.FieldDescriptor,
	v protoreflect.Value,
) error {
	name := string(fd.Name())
	if p.takePlan(name, v) {
		return nil
	}

	switch name {
	case fieldEtag:
		if v.String() != etagOf(c.Cluster) {
			return errPrecondition(
				tmp.GetSelfLink(),
				"The etag %q does not match the cluster's current etag.",
				v.String(),
			)
		}
	case "desired_logging_service":
		return setService(
			&tmp.LoggingService,
			v.String(),
			"logging.googleapis.com/kubernetes",
			"logging.googleapis.com",
			"none",
		)
	case "desired_monitoring_service":
		return setService(
			&tmp.MonitoringService,
			v.String(),
			"monitoring.googleapis.com/kubernetes",
			"monitoring.googleapis.com",
			"none",
		)
	case "desired_addons_config":
		return mergeAddons(tmp, msgOf[*containerpb.AddonsConfig](v))
	case "desired_cluster_autoscaling":
		return setClusterAutoscaling(tmp, msgOf[*containerpb.ClusterAutoscaling](v))
	case "desired_master_authorized_networks_config":
		return setAuthNets(tmp, msgOf[*containerpb.MasterAuthorizedNetworksConfig](v))
	case "desired_enable_private_endpoint", "desired_default_enable_private_nodes":
		return setPrivate(tmp, name, v.Bool())
	default:
		return setGeneric(tmp, fd, v)
	}

	return nil
}

func setService(dst *string, v string, allowed ...string) error {
	if err := validService(v, allowed...); err != nil {
		return err
	}

	*dst = v

	return nil
}

func setClusterAutoscaling(cl *containerpb.Cluster, a *containerpb.ClusterAutoscaling) error {
	for _, rl := range a.GetResourceLimits() {
		if rl.GetResourceType() == "" || rl.GetMaximum() < rl.GetMinimum() || rl.GetMinimum() < 0 {
			return errInvalid(
				"desired_cluster_autoscaling.resource_limits",
				"Invalid resource limit for %q.",
				rl.GetResourceType(),
			)
		}
	}

	cl.Autoscaling = a

	return nil
}

func setAuthNets(cl *containerpb.Cluster, m *containerpb.MasterAuthorizedNetworksConfig) error {
	if err := validMasterAuthNets(m); err != nil {
		return err
	}

	cl.MasterAuthorizedNetworksConfig = m

	return nil
}

func setPrivate(cl *containerpb.Cluster, name string, v bool) error {
	if !cl.GetPrivateClusterConfig().GetEnablePrivateNodes() {
		return errPrecondition(cl.GetSelfLink(), "%s requires a private cluster.", strings.TrimPrefix(name, "desired_"))
	}

	if name == "desired_enable_private_endpoint" {
		cl.PrivateClusterConfig.EnablePrivateEndpoint = v
	} else {
		cl.PrivateClusterConfig.EnablePrivateNodes = v
	}

	return nil
}

// setGeneric copies a ClusterUpdate field onto the cluster field (or NetworkConfig field) of the same name.
func setGeneric(cl *containerpb.Cluster, fd protoreflect.FieldDescriptor, v protoreflect.Value) error {
	name := string(fd.Name())
	target := strings.TrimPrefix(name, "desired_")

	if name == "desired_intra_node_visibility_config" || name == "desired_l4_ilb_subsetting_config" {
		return setNetworkFlag(cl, name, v)
	}

	if !fieldAccepts(cl.ProtoReflect(), target, fd) && !fieldAccepts(netConfig(cl).ProtoReflect(), target, fd) {
		return errUnsupported(name)
	}

	dst := cl.ProtoReflect()
	if !fieldAccepts(dst, target, fd) {
		dst = cl.GetNetworkConfig().ProtoReflect()
	}

	set(dst, target, fd, v)

	return nil
}

func netConfig(cl *containerpb.Cluster) *containerpb.NetworkConfig {
	cl.NetworkConfig = firstPtr(cl.GetNetworkConfig(), &containerpb.NetworkConfig{})

	return cl.GetNetworkConfig()
}

func setNetworkFlag(cl *containerpb.Cluster, name string, v protoreflect.Value) error {
	nc := netConfig(cl)
	on := v.Message().Get(v.Message().Descriptor().Fields().ByName("enabled")).Bool()

	if name == "desired_intra_node_visibility_config" {
		nc.EnableIntraNodeVisibility = on
	} else {
		nc.EnableL4IlbSubsetting = on
	}

	return nil
}

func fieldAccepts(m protoreflect.Message, name string, src protoreflect.FieldDescriptor) bool {
	dst := m.Descriptor().Fields().ByName(protoreflect.Name(name))
	if dst == nil || dst.Kind() != src.Kind() || dst.IsList() != src.IsList() || dst.IsMap() != src.IsMap() {
		return false
	}

	switch src.Kind() {
	case protoreflect.MessageKind, protoreflect.GroupKind:
		return dst.Message().FullName() == src.Message().FullName()
	case protoreflect.EnumKind:
		return dst.Enum().FullName() == src.Enum().FullName()
	default:
		return true
	}
}

func set(m protoreflect.Message, name string, src protoreflect.FieldDescriptor, v protoreflect.Value) {
	dst := m.Descriptor().Fields().ByName(protoreflect.Name(name))

	if src.Kind() == protoreflect.MessageKind && !src.IsList() && !src.IsMap() {
		m.Set(dst, protoreflect.ValueOfMessage(proto.Clone(v.Message().Interface()).ProtoReflect()))

		return
	}

	m.Set(dst, v)
}

// applyPlan applies the node-pool and version parts of an update and reports the operation type.
func (b *InMemoryBackend) applyPlan(
	c *clusterRec,
	tmp *containerpb.Cluster,
	p *updatePlan,
) (containerpb.Operation_Type, error) {
	typ := containerpb.Operation_UPDATE_CLUSTER

	if p.hasLocations {
		if err := b.setLocations(c, tmp, p.locations); err != nil {
			return typ, err
		}
	}

	if p.hasMaster {
		if err := upgradeMaster(tmp, p.masterVersion); err != nil {
			return typ, err
		}

		typ = containerpb.Operation_UPGRADE_MASTER
	}

	if p.hasNodeVersion || p.hasImage || p.hasAutoscaling {
		targets, err := c.targetPools(p.poolID)
		if err != nil {
			return typ, err
		}

		for _, pr := range targets {
			if err = b.applyPoolUpdate(c, tmp, pr, p); err != nil {
				return typ, err
			}
		}

		if p.hasNodeVersion && !p.hasMaster {
			typ = containerpb.Operation_UPGRADE_NODES
		}
	}

	return typ, nil
}

func (c *clusterRec) targetPools(id string) ([]*poolRec, error) {
	if id == "" || id == allPools {
		return c.Pools, nil
	}

	pr, ok := c.pool(id)
	if !ok {
		return nil, errNotFound(id)
	}

	return []*poolRec{pr}, nil
}

func (b *InMemoryBackend) applyPoolUpdate(_ *clusterRec, cl *containerpb.Cluster, pr *poolRec, p *updatePlan) error {
	if cl.GetAutopilot().GetEnabled() {
		return errPrecondition(cl.GetSelfLink(), "Node pools of Autopilot clusters are managed by Google.")
	}

	if p.hasNodeVersion {
		if err := upgradeNodes(pr, p.nodeVersion, cl.GetCurrentMasterVersion()); err != nil {
			return err
		}
	}

	if p.hasImage {
		if !validImageType(p.imageType) {
			return errInvalid("desired_image_type", "Image type %q is not supported.", p.imageType)
		}

		pr.Pool.Config.ImageType = p.imageType
	}

	if p.hasAutoscaling {
		return setPoolAutoscaling(pr, p.nodeAutoscaling)
	}

	return nil
}

func upgradeNodes(pr *poolRec, want, master string) error {
	v, err := resolveNodeVersion(firstNonEmpty(want, master), master)
	if err != nil {
		return err
	}

	if compareVersions(v, pr.Pool.GetVersion()) < 0 {
		return errInvalid(
			"node_version",
			"Node version %s is older than the current version %s.",
			v,
			pr.Pool.GetVersion(),
		)
	}

	if v != pr.Pool.GetVersion() {
		pr.PrevVersion, pr.Pool.Version = pr.Pool.GetVersion(), v
	}

	return nil
}

func clonePools(in []*poolRec) []*poolRec {
	out := make([]*poolRec, len(in))

	for i, p := range in {
		cp := *p
		cp.Pool = clone(p.Pool)
		out[i] = &cp
	}

	return out
}
