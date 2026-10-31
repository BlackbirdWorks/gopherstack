//nolint:staticcheck // legacy cluster fields are deprecated in the proto but still served
package gcpcontainer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"

	"cloud.google.com/go/container/apiv1/containerpb"
)

const (
	defaultMachineType = "e2-medium"
	defaultDiskType    = "pd-balanced"
	defaultPoolName    = "default-pool"
	defaultMaxSurge    = 1
	groupHashBytes     = 4
)

func defaultScopes() []string {
	return []string{
		"https://www.googleapis.com/auth/devstorage.read_only",
		"https://www.googleapis.com/auth/logging.write",
		"https://www.googleapis.com/auth/monitoring",
		"https://www.googleapis.com/auth/service.management.readonly",
		"https://www.googleapis.com/auth/servicecontrol",
		"https://www.googleapis.com/auth/trace.append",
	}
}

func withNodeDefaults(in *containerpb.NodeConfig) *containerpb.NodeConfig {
	nc := clone(in)
	if nc == nil {
		nc = &containerpb.NodeConfig{}
	}

	if nc.GetMachineType() == "" {
		nc.MachineType = defaultMachineType
	}

	if nc.GetDiskSizeGb() == 0 {
		nc.DiskSizeGb = defaultDiskGb
	}

	if nc.GetDiskType() == "" {
		nc.DiskType = defaultDiskType
	}

	if nc.GetImageType() == "" {
		nc.ImageType = defaultImageType
	}

	if len(nc.GetOauthScopes()) == 0 {
		nc.OauthScopes = defaultScopes()
	}

	if nc.GetServiceAccount() == "" {
		nc.ServiceAccount = "default"
	}

	if nc.Metadata == nil {
		nc.Metadata = map[string]string{"disable-legacy-endpoints": "true"}
	}

	if nc.GetShieldedInstanceConfig() == nil {
		nc.ShieldedInstanceConfig = &containerpb.ShieldedInstanceConfig{EnableIntegrityMonitoring: true}
	}

	return nc
}

func groupURL(project, zone, cluster, pool string) string {
	sum := sha256.Sum256([]byte(project + "/" + zone + "/" + cluster + "/" + pool))

	return fmt.Sprintf("%s/projects/%s/zones/%s/instanceGroupManagers/gke-%s-%s-%s-grp",
		computeV, project, zone, cluster, pool, hex.EncodeToString(sum[:groupHashBytes]))
}

func groupURLs(project string, zones []string, cluster, pool string) []string {
	out := make([]string, 0, len(zones))
	for _, z := range zones {
		out = append(out, groupURL(project, z, cluster, pool))
	}

	return out
}

func poolSelfLink(r ref) string { return apiHost + "/" + r.poolName() }

type poolCtx struct {
	ref        ref
	version    string
	zones      []string
	autoUpdate bool
}

// buildPoolLocked validates in and returns the node pool record created for it.
func (b *InMemoryBackend) buildPoolLocked(pc poolCtx, in *containerpb.NodePool) (*poolRec, error) {
	name := firstNonEmpty(in.GetName(), defaultPoolName)
	if err := validName("node_pool.name", name); err != nil {
		return nil, err
	}

	if err := b.validatePoolInput(in); err != nil {
		return nil, err
	}

	version, err := resolveNodeVersion(in.GetVersion(), pc.version)
	if err != nil {
		return nil, err
	}

	zones, err := poolZones(in.GetLocations(), pc.zones)
	if err != nil {
		return nil, err
	}

	pc.ref.pool = name
	p := clone(in)
	p.Name = name
	p.Config = withNodeDefaults(in.GetConfig())
	p.Version = version
	p.Locations = zones
	p.SelfLink = poolSelfLink(pc.ref)
	p.InstanceGroupUrls = groupURLs(pc.ref.project, zones, pc.ref.cluster, name)
	p.Status = containerpb.NodePool_PROVISIONING
	p.Etag = ""
	p.Conditions = nil
	p.StatusMessage = ""

	if p.GetManagement() == nil {
		p.Management = &containerpb.NodeManagement{AutoUpgrade: true, AutoRepair: true}
	}

	if pc.autoUpdate {
		p.Management.AutoUpgrade = true
	}

	if p.GetMaxPodsConstraint() == nil {
		p.MaxPodsConstraint = &containerpb.MaxPodsConstraint{MaxPodsPerNode: defaultPodsMax}
	}

	p.PodIpv4CidrSize = podPrefix(p.GetMaxPodsConstraint().GetMaxPodsPerNode())

	if p.GetUpgradeSettings() == nil {
		strategy := containerpb.NodePoolUpdateStrategy_SURGE
		p.UpgradeSettings = &containerpb.NodePool_UpgradeSettings{
			MaxSurge: defaultMaxSurge, Strategy: &strategy,
		}
	}

	if p.GetAutoscaling() == nil {
		p.Autoscaling = &containerpb.NodePoolAutoscaling{}
	}

	return &poolRec{Pool: p, Size: in.GetInitialNodeCount()}, nil
}

func (b *InMemoryBackend) validatePoolInput(in *containerpb.NodePool) error {
	if n := in.GetInitialNodeCount(); n < 0 || n > maxNodeCount {
		return errInvalid("node_pool.initial_node_count", "Initial node count must be between 0 and %d.", maxNodeCount)
	}

	if err := validAutoscaling(in.GetAutoscaling()); err != nil {
		return err
	}

	if in.GetInitialNodeCount() == 0 && !in.GetAutoscaling().GetEnabled() {
		return errInvalid("node_pool.initial_node_count", "Initial node count must be greater than 0.")
	}

	if err := validMaxPods("node_pool.max_pods_constraint", in.GetMaxPodsConstraint()); err != nil {
		return err
	}

	return validNodeConfig(b.compute, in.GetConfig())
}

func poolZones(want, clusterZones []string) ([]string, error) {
	if len(want) == 0 {
		return slices.Clone(clusterZones), nil
	}

	for _, z := range want {
		if !slices.Contains(clusterZones, z) {
			return nil, errInvalid(
				"node_pool.locations",
				"Node pool location %q is not one of the cluster locations %v.",
				z,
				clusterZones,
			)
		}
	}

	out := slices.Clone(want)
	slices.Sort(out)

	return slices.Compact(out), nil
}

func (c *clusterRec) pool(name string) (*poolRec, bool) {
	for _, p := range c.Pools {
		if p.Pool.GetName() == name {
			return p, true
		}
	}

	return nil, false
}
