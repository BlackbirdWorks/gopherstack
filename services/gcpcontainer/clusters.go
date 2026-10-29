//nolint:staticcheck // legacy request and cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"encoding/base64"
	"fmt"
	"net/netip"
	"slices"
	"strings"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	defaultNetwork    = "default"
	loggingService    = "logging.googleapis.com/kubernetes"
	monitoringService = "monitoring.googleapis.com/kubernetes"
	nodeCIDRSize      = 24
	routesNodeCIDR    = 24
	privateEndpointIP = 2
)

type clusterPlan struct {
	in        *containerpb.Cluster
	ref       ref
	version   string
	zones     []string
	channel   containerpb.ReleaseChannel_Channel
	autopilot bool
}

func (b *InMemoryBackend) planCluster(project, location string, in *containerpb.Cluster) (*clusterPlan, error) {
	if in == nil {
		return nil, errInvalid("cluster", "Cluster must be specified.")
	}

	if err := validName("cluster.name", in.GetName()); err != nil {
		return nil, err
	}

	if !validLocation(location) {
		return nil, errInvalid("parent", "Location %q is not a valid zone or region.", location)
	}

	p := &clusterPlan{in: in, ref: ref{project: project, location: location, cluster: in.GetName()}}
	p.autopilot = in.GetAutopilot().GetEnabled()
	p.channel = in.GetReleaseChannel().GetChannel()

	if p.autopilot {
		if err := checkAutopilotInput(in, location); err != nil {
			return nil, err
		}

		if p.channel == containerpb.ReleaseChannel_UNSPECIFIED {
			p.channel = containerpb.ReleaseChannel_REGULAR
		}
	}

	var err error
	if p.version, err = resolveMasterVersion(in.GetInitialClusterVersion(), p.channel); err != nil {
		return nil, err
	}

	p.zones = clusterZones(location, in.GetLocations())
	if err = checkLocations(location, in.GetLocations()); err != nil {
		return nil, err
	}

	return p, validLabels("cluster.resource_labels", in.GetResourceLabels())
}

func clusterZones(location string, want []string) []string {
	if len(want) > 0 {
		out := slices.Clone(want)
		slices.Sort(out)

		return slices.Compact(out)
	}

	if isZone(location) {
		return []string{location}
	}

	return defaultZones(location)
}

func checkLocations(location string, want []string) error {
	region := regionOf(location)

	for _, z := range want {
		if !isZone(z) || regionOf(z) != region {
			return errInvalid("cluster.locations", "Location %q is not a zone of %s.", z, region)
		}
	}

	if isZone(location) && len(want) > 0 && !slices.Contains(want, location) {
		return errInvalid("cluster.locations", "Locations must include the cluster zone %s.", location)
	}

	return nil
}

func checkAutopilotInput(in *containerpb.Cluster, location string) error {
	switch {
	case !isRegion(location):
		return errInvalid("parent", "Autopilot clusters must be regional; %q is a zone.", location)
	case len(in.GetNodePools()) > 0,
		in.GetInitialNodeCount() != 0,
		in.GetNodeConfig() != nil:
		return errInvalid(
			"cluster.node_pools",
			"Autopilot clusters manage their own nodes; node pools and node configuration cannot be specified.",
		)
	case in.GetLegacyAbac().GetEnabled():
		return errInvalid("cluster.legacy_abac", "Legacy ABAC cannot be enabled on Autopilot clusters.")
	case in.GetNetworkPolicy().GetEnabled():
		return errInvalid("cluster.network_policy", "Network policy cannot be enabled on Autopilot clusters.")
	}

	return nil
}

type netSpec struct {
	network    string
	subnetwork string
	netPath    string
	subPath    string
}

func shortName(s string) string {
	if _, after, ok := strings.CutLast(s, "/"); ok {
		return after
	}

	return s
}

// pathProject extracts the project of a `projects/p/...` resource path, or def.
func pathProject(path, def string) string {
	rest, ok := strings.CutPrefix(path, "projects/")
	if !ok {
		return def
	}

	p, _, _ := strings.Cut(rest, "/")

	return p
}

func (b *InMemoryBackend) resolveNet(project, region string, in *containerpb.Cluster) (netSpec, error) {
	rawNet := firstNonEmpty(in.GetNetwork(), in.GetNetworkConfig().GetNetwork(), defaultNetwork)
	rawSub := firstNonEmpty(in.GetSubnetwork(), in.GetNetworkConfig().GetSubnetwork())
	n := netSpec{network: shortName(rawNet), subnetwork: shortName(rawSub)}
	netProject := pathProject(rawNet, project)
	subProject := pathProject(rawSub, project)

	if b.compute != nil {
		if n.network != defaultNetwork && !b.compute.NetworkExists(netProject, n.network) {
			return n, errInvalid("cluster.network", "Network %q does not exist.", n.network)
		}

		if n.subnetwork != "" && !(n.network == defaultNetwork && n.subnetwork == defaultNetwork) &&
			!b.compute.SubnetworkExists(subProject, region, n.subnetwork) {
			return n, errInvalid(
				"cluster.subnetwork",
				"Subnetwork %q does not exist in region %s.",
				n.subnetwork,
				region,
			)
		}
	}

	if n.subnetwork == "" {
		n.subnetwork = n.network
	}

	n.netPath = "projects/" + netProject + "/global/networks/" + n.network
	n.subPath = "projects/" + subProject + "/regions/" + region + "/subnetworks/" + n.subnetwork

	return n, nil
}

// CreateCluster creates a cluster and its node pools and returns the CREATE_CLUSTER operation.
func (b *InMemoryBackend) CreateCluster(req *containerpb.CreateClusterRequest) (*containerpb.Operation, error) {
	project, location, err := resolveLocation(
		req.GetParent(),
		req.GetProjectId(),
		req.GetZone(),
	)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	b.sweepLocked()

	plan, err := b.planCluster(project, location, req.GetCluster())
	if err != nil {
		return nil, err
	}

	if _, exists := b.clusters[plan.ref.clusterKey()]; exists {
		return nil, errExists(plan.ref.clusterName())
	}

	rec, err := b.buildClusterLocked(plan)
	if err != nil {
		return nil, err
	}

	manual := b.engine != nil
	o := b.newOpLocked(
		plan.ref,
		containerpb.Operation_CREATE_CLUSTER,
		kindCreate,
		rec.Cluster.GetSelfLink(),
		b.timing.Create,
		manual,
	)
	b.clusters[plan.ref.clusterKey()] = rec

	if manual {
		b.launchLocked(plan.ref, rec, o)
	}

	return b.opViewLocked(o), nil
}

func (b *InMemoryBackend) buildClusterLocked(p *clusterPlan) (*clusterRec, error) {
	in := p.in

	net, err := b.resolveNet(p.ref.project, regionOf(p.ref.location), in)
	if err != nil {
		return nil, err
	}

	rng, err := b.chooseRanges(p.ref.project, regionOf(p.ref.location), net.subnetwork, in)
	if err != nil {
		return nil, err
	}

	cl := b.baseCluster(p, net, rng)

	if err = b.applyAccessInput(cl, in); err != nil {
		return nil, err
	}

	rec := &clusterRec{Project: p.ref.project, Cluster: cl}

	if err = b.addInitialPools(rec, p); err != nil {
		return nil, err
	}

	if err = b.stubIdentity(cl, p.ref.cluster); err != nil {
		return nil, err
	}

	cl.Etag = ""

	return rec, nil
}

func (b *InMemoryBackend) baseCluster(p *clusterPlan, n netSpec, r ranges) *containerpb.Cluster {
	cl := clone(p.in)
	cl.NodePools = nil
	cl.NodeConfig = nil
	cl.Id = uuid.NewString()
	cl.SelfLink = clusterSelfLink(p.ref.project, p.ref.location, p.ref.cluster)
	cl.Location, cl.Zone = p.ref.location, p.ref.location
	cl.CreateTime = rfc3339(b.now())
	cl.Status, cl.StatusMessage, cl.Conditions, cl.ExpireTime = containerpb.Cluster_PROVISIONING, "", nil, ""
	cl.InitialClusterVersion, cl.CurrentMasterVersion, cl.CurrentNodeVersion = p.version, p.version, p.version
	cl.Locations = p.zones
	cl.Network, cl.Subnetwork = n.network, n.subnetwork
	cl.LoggingService = firstNonEmpty(cl.GetLoggingService(), loggingService)
	cl.MonitoringService = firstNonEmpty(cl.GetMonitoringService(), monitoringService)
	cl.ClusterIpv4Cidr, cl.ServicesIpv4Cidr = r.pod.String(), r.services.String()
	cl.NodeIpv4CidrSize = nodeCIDRSize
	cl.LabelFingerprint = fingerprint(cl.GetResourceLabels())
	cl.LegacyAbac = firstPtr(cl.GetLegacyAbac(), &containerpb.LegacyAbac{})
	cl.ShieldedNodes = firstPtr(cl.GetShieldedNodes(), &containerpb.ShieldedNodes{Enabled: true})
	cl.DefaultMaxPodsConstraint = firstPtr(
		cl.GetDefaultMaxPodsConstraint(),
		&containerpb.MaxPodsConstraint{MaxPodsPerNode: defaultPodsMax},
	)
	cl.AddonsConfig = withAddonDefaults(cl.GetAddonsConfig())
	cl.LoggingConfig = firstPtr(cl.GetLoggingConfig(), defaultLoggingConfig())
	cl.MonitoringConfig = firstPtr(cl.GetMonitoringConfig(), defaultMonitoringConfig())
	cl.MaintenancePolicy = withPolicyVersion(cl.GetMaintenancePolicy())
	cl.NetworkConfig = withNetworkConfig(cl.GetNetworkConfig(), n, p.autopilot)
	cl.IpAllocationPolicy = withIPPolicy(cl.GetIpAllocationPolicy(), n, r)

	if p.channel != containerpb.ReleaseChannel_UNSPECIFIED {
		cl.ReleaseChannel = &containerpb.ReleaseChannel{Channel: p.channel}
	}

	if p.autopilot {
		cl.Autopilot = &containerpb.Autopilot{Enabled: true}
		cl.VerticalPodAutoscaling = firstPtr(
			cl.GetVerticalPodAutoscaling(),
			&containerpb.VerticalPodAutoscaling{Enabled: true},
		)
		cl.WorkloadIdentityConfig = firstPtr(cl.GetWorkloadIdentityConfig(),
			&containerpb.WorkloadIdentityConfig{WorkloadPool: p.ref.project + ".svc.id.goog"})
	}

	return cl
}

func firstPtr[T any](cur, def *T) *T {
	if cur != nil {
		return cur
	}

	return def
}

func withAddonDefaults(a *containerpb.AddonsConfig) *containerpb.AddonsConfig {
	a = firstPtr(a, &containerpb.AddonsConfig{})
	a.HttpLoadBalancing = firstPtr(a.GetHttpLoadBalancing(), &containerpb.HttpLoadBalancing{})
	a.HorizontalPodAutoscaling = firstPtr(a.GetHorizontalPodAutoscaling(), &containerpb.HorizontalPodAutoscaling{})
	a.NetworkPolicyConfig = firstPtr(a.GetNetworkPolicyConfig(), &containerpb.NetworkPolicyConfig{Disabled: true})

	return a
}

func defaultLoggingConfig() *containerpb.LoggingConfig {
	return &containerpb.LoggingConfig{ComponentConfig: &containerpb.LoggingComponentConfig{
		EnableComponents: []containerpb.LoggingComponentConfig_Component{
			containerpb.LoggingComponentConfig_SYSTEM_COMPONENTS, containerpb.LoggingComponentConfig_WORKLOADS,
		},
	}}
}

func defaultMonitoringConfig() *containerpb.MonitoringConfig {
	return &containerpb.MonitoringConfig{
		ComponentConfig: &containerpb.MonitoringComponentConfig{
			EnableComponents: []containerpb.MonitoringComponentConfig_Component{
				containerpb.MonitoringComponentConfig_SYSTEM_COMPONENTS,
			},
		},
		ManagedPrometheusConfig: &containerpb.ManagedPrometheusConfig{Enabled: true},
	}
}

func withPolicyVersion(m *containerpb.MaintenancePolicy) *containerpb.MaintenancePolicy {
	m = firstPtr(m, &containerpb.MaintenancePolicy{})
	m.ResourceVersion = etagOf(&containerpb.MaintenancePolicy{Window: m.GetWindow()})

	return m
}

func withNetworkConfig(nc *containerpb.NetworkConfig, n netSpec, autopilot bool) *containerpb.NetworkConfig {
	nc = firstPtr(nc, &containerpb.NetworkConfig{})
	nc.Network, nc.Subnetwork = n.netPath, n.subPath

	if nc.GetDatapathProvider() == containerpb.DatapathProvider_DATAPATH_PROVIDER_UNSPECIFIED {
		nc.DatapathProvider = containerpb.DatapathProvider_LEGACY_DATAPATH
		if autopilot {
			nc.DatapathProvider = containerpb.DatapathProvider_ADVANCED_DATAPATH
		}
	}

	return nc
}

func withIPPolicy(ip *containerpb.IPAllocationPolicy, n netSpec, r ranges) *containerpb.IPAllocationPolicy {
	ip = firstPtr(ip, &containerpb.IPAllocationPolicy{UseIpAliases: true})
	if ip.GetUseRoutes() {
		return ip
	}

	ip.UseIpAliases = true
	ip.SubnetworkName = firstNonEmpty(ip.GetSubnetworkName(), n.subnetwork)
	ip.ClusterIpv4Cidr, ip.ServicesIpv4Cidr = r.pod.String(), r.services.String()
	ip.ClusterIpv4CidrBlock, ip.ServicesIpv4CidrBlock = r.pod.String(), r.services.String()

	if ip.GetStackType() == containerpb.StackType_STACK_TYPE_UNSPECIFIED {
		ip.StackType = containerpb.StackType_IPV4
	}

	return ip
}

// applyAccessInput validates the master-access settings of a create request.
func (b *InMemoryBackend) applyAccessInput(cl, in *containerpb.Cluster) error {
	if err := validMasterAuthNets(
		in.GetMasterAuthorizedNetworksConfig(),
	); err != nil {
		return err
	}

	if err := validMaintenance(in.GetMaintenancePolicy()); err != nil {
		return err
	}

	if pw := in.GetMasterAuth().
		GetPassword(); pw != "" {
		if err := validPassword(pw); err != nil {
			return err
		}
	}

	return b.applyPrivate(cl, in)
}

func (b *InMemoryBackend) applyPrivate(cl, in *containerpb.Cluster) error {
	pc := in.GetPrivateClusterConfig()
	if pc == nil {
		return nil
	}

	cl.PrivateClusterConfig = clone(pc)

	if !pc.GetEnablePrivateNodes() {
		if pc.GetEnablePrivateEndpoint() {
			return errInvalid("cluster.private_cluster_config", "A private endpoint requires private nodes.")
		}

		return nil
	}

	blk, err := parseCIDR(
		"cluster.private_cluster_config.master_ipv4_cidr_block",
		pc.GetMasterIpv4CidrBlock(),
		masterPrefix,
		masterPrefix,
	)
	if err != nil {
		return err
	}

	if pod, perr := netip.ParsePrefix(cl.GetClusterIpv4Cidr()); perr == nil && pod.Overlaps(blk) {
		return errInvalid(
			"cluster.private_cluster_config.master_ipv4_cidr_block",
			"The master range %s overlaps the pod range %s.",
			blk,
			pod,
		)
	}

	cl.PrivateClusterConfig.PrivateEndpoint = u32ToAddr(
		addrToU32(blk.Addr()) + privateEndpointIP,
	).String()
	//nolint:staticcheck // deprecated field kept for wire compatibility

	return nil
}

// addInitialPools creates the legacy default pool, the requested pools, or the Autopilot-managed pool.
func (b *InMemoryBackend) addInitialPools(rec *clusterRec, p *clusterPlan) error {
	in := p.in
	pc := poolCtx{
		ref:        p.ref,
		version:    p.version,
		zones:      p.zones,
		autoUpdate: p.channel != containerpb.ReleaseChannel_UNSPECIFIED,
	}

	if p.autopilot {
		rec.Pools = []*poolRec{autopilotPool(pc)}

		return nil
	}

	pools := in.GetNodePools()
	if len(pools) == 0 {
		if in.GetInitialNodeCount() == 0 {
			return errInvalid(
				"cluster.initial_node_count",
				"Either node_pools or initial_node_count must be specified.",
			)
		}

		pools = []*containerpb.NodePool{
			{
				Name:             defaultPoolName,
				Config:           in.GetNodeConfig(),
				InitialNodeCount: in.GetInitialNodeCount(),
			},
		}
		rec.Cluster.NodeConfig = withNodeDefaults(
			in.GetNodeConfig(),
		)
	}

	seen := map[string]bool{}

	for _, np := range pools {
		pr, err := b.buildPoolLocked(pc, np)
		if err != nil {
			return err
		}

		if seen[pr.Pool.GetName()] {
			return errInvalid("cluster.node_pools", "Duplicate node pool name %q.", pr.Pool.GetName())
		}

		seen[pr.Pool.GetName()] = true
		rec.Pools = append(rec.Pools, pr)
	}

	return nil
}

func autopilotPool(pc poolCtx) *poolRec {
	pc.ref.pool = defaultPoolName

	return &poolRec{Pool: &containerpb.NodePool{
		Name: defaultPoolName, Config: withNodeDefaults(nil), Version: pc.version, Locations: pc.zones,
		SelfLink:          poolSelfLink(pc.ref),
		InstanceGroupUrls: groupURLs(pc.ref.project, pc.zones, pc.ref.cluster, defaultPoolName),
		Autoscaling:       &containerpb.NodePoolAutoscaling{Enabled: true, Autoprovisioned: true},
		Management:        &containerpb.NodeManagement{AutoUpgrade: true, AutoRepair: true},
		MaxPodsConstraint: &containerpb.MaxPodsConstraint{MaxPodsPerNode: defaultPodsMax},
		PodIpv4CidrSize:   podPrefix(defaultPodsMax),
	}}
}

// stubIdentity gives the cluster a synthetic endpoint and a real CA certificate; the docker engine replaces both.
func (b *InMemoryBackend) stubIdentity(cl *containerpb.Cluster, name string) error {
	ca, err := k3sengine.NewCA("gopherstack-gke-" + name + "-ca")
	if err != nil {
		return fmt.Errorf("cluster CA: %w", err)
	}

	b.seq++
	cl.Endpoint = documentationAddr(b.seq)
	cl.MasterAuth = firstPtr(cl.GetMasterAuth(), &containerpb.MasterAuth{})
	cl.MasterAuth.ClusterCaCertificate = base64.StdEncoding.EncodeToString(ca.CertPEM)
	cl.MasterAuth.ClientCertificateConfig = firstPtr(
		cl.GetMasterAuth().GetClientCertificateConfig(),
		&containerpb.ClientCertificateConfig{},
	)

	return nil
}

// documentationAddr hands out addresses from the RFC 5737 documentation ranges.
func documentationAddr(n uint64) string {
	bases := []string{"203.0.113.", "198.51.100.", "192.0.2."}
	const hosts = 253

	i := (n - 1) % (hosts * uint64(len(bases)))

	return fmt.Sprintf("%s%d", bases[i/hosts], i%hosts+1)
}

// GetCluster returns one cluster.
func (b *InMemoryBackend) GetCluster(req *containerpb.GetClusterRequest) (*containerpb.Cluster, error) {
	r, err := resolveCluster(
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetCluster")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	return b.clusterViewLocked(c), nil
}

func (b *InMemoryBackend) liveClusterLocked(r ref) (*clusterRec, error) {
	c, ok := b.clusters[r.clusterKey()]
	if !ok || (c.DeleteOp != "" && b.deleteDoneLocked(r.clusterKey(), c.DeleteOp)) {
		return nil, errNotFound(r.clusterName())
	}

	return c, nil
}

// ListClusters lists the clusters of a location, or of every location for "-".
func (b *InMemoryBackend) ListClusters(
	req *containerpb.ListClustersRequest,
) (*containerpb.ListClustersResponse, error) {
	project, location, err := resolveLocation(
		req.GetParent(),
		req.GetProjectId(),
		req.GetZone(),
	)
	if err != nil {
		return nil, err
	}

	if location != wildcard && !validLocation(location) {
		return nil, errNotFound(locationPath(project, location))
	}

	b.mu.RLock("ListClusters")
	defer b.mu.RUnlock()

	resp := &containerpb.ListClustersResponse{}

	for k, c := range b.clusters {
		if c.Project != project || (location != wildcard && c.Cluster.GetLocation() != location) {
			continue
		}

		if c.DeleteOp != "" && b.deleteDoneLocked(k, c.DeleteOp) {
			continue
		}

		resp.Clusters = append(resp.Clusters, b.clusterViewLocked(c))
	}

	slices.SortFunc(resp.GetClusters(), func(a, c *containerpb.Cluster) int {
		return strings.Compare(a.GetSelfLink(), c.GetSelfLink())
	})

	return resp, nil
}

// DeleteCluster starts deletion and returns the DELETE_CLUSTER operation.
func (b *InMemoryBackend) DeleteCluster(req *containerpb.DeleteClusterRequest) (*containerpb.Operation, error) {
	r, err := resolveCluster(
		req.GetName(),
		req.GetProjectId(),
		req.GetZone(),
		req.GetClusterId(),
	)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	b.sweepLocked()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	if c.DeleteOp != "" {
		return nil, errBusy(r.clusterName(), c.DeleteOp)
	}

	for _, o := range b.activeOpsLocked(r.project, r.location, r.cluster) {
		b.finishOpLocked(o, "")
	}

	o := b.newOpLocked(
		r,
		containerpb.Operation_DELETE_CLUSTER,
		kindDelete,
		c.Cluster.GetSelfLink(),
		b.timing.Delete,
		false,
	)
	c.DeleteOp = o.Op.GetName()
	b.reapLocked(r.clusterKey())

	return b.opViewLocked(o), nil
}
