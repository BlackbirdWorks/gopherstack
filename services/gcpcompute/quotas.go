package gcpcompute

import (
	"slices"
	"strings"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	localSSDGb     = 375
	scratchType    = "SCRATCH"
	allRegions     = ""
	quotaMetrics   = 64
	selfScopeParts = 5
)

type quotaDef struct {
	metric string
	limit  float64
}

// The limits are emulator choices: the emulator enforces none of them.
var regionQuotaDefs = []quotaDef{ //nolint:gochecknoglobals // read-only catalogue
	{"CPUS", 24}, {"PREEMPTIBLE_CPUS", 24}, {"E2_CPUS", 24}, {"N2_CPUS", 24}, {"N2D_CPUS", 24},
	{"C3_CPUS", 24}, {"T2D_CPUS", 24}, {"T2A_CPUS", 24},
	{"INSTANCES", 8000}, {"DISKS_TOTAL_GB", 4096}, {"SSD_TOTAL_GB", 500}, {"LOCAL_SSD_TOTAL_GB", 3000},
	{"STATIC_ADDRESSES", 8}, {"IN_USE_ADDRESSES", 8}, {"INTERNAL_ADDRESSES", 200}, {"SUBNETWORKS", 100},
	{"RESERVATIONS", 100}, {"RESOURCE_POLICIES", 100}, {"FORWARDING_RULES", 15}, {"HEALTH_CHECKS", 75},
	{"TARGET_POOLS", 50}, {"TARGET_INSTANCES", 50}, {"VPN_GATEWAYS", 5}, {"VPN_TUNNELS", 10},
	{"TARGET_VPN_GATEWAYS", 5}, {"SECURITY_POLICIES_PER_REGION", 10},
	{"INTERCONNECT_ATTACHMENTS_PER_REGION", 16}, {"NVIDIA_T4_GPUS", 8}, {"NVIDIA_P4_GPUS", 8},
	{"NVIDIA_V100_GPUS", 8}, {"ROUTERS", 20},
}

var projectQuotaDefs = []quotaDef{ //nolint:gochecknoglobals // read-only catalogue
	{"SNAPSHOTS", 1000}, {"NETWORKS", 15}, {"FIREWALLS", 200}, {"ROUTES", 250}, {"IMAGES", 100},
	{"INSTANCE_TEMPLATES", 100}, {"HEALTH_CHECKS", 75}, {"BACKEND_SERVICES", 50}, {"BACKEND_BUCKETS", 3},
	{"URL_MAPS", 10}, {"SSL_CERTIFICATES", 10}, {"SSL_POLICIES", 10}, {"SECURITY_POLICIES", 10},
	{"INTERCONNECTS", 6}, {"EXTERNAL_VPN_GATEWAYS", 5}, {"MACHINE_IMAGES", 100}, {"IN_PLACE_SNAPSHOTS", 1000},
	{"TARGET_HTTP_PROXIES", 10}, {"TARGET_HTTPS_PROXIES", 10}, {"TARGET_TCP_PROXIES", 10},
	{"TARGET_SSL_PROXIES", 10}, {"CPUS_ALL_REGIONS", 32}, {"GPUS_ALL_REGIONS", 0},
	{"GLOBAL_INTERNAL_ADDRESSES", 50}, {"STATIC_ADDRESSES", 8},
}

var gpuQuotaMetric = map[string]string{ //nolint:gochecknoglobals // read-only lookup table
	"nvidia-tesla-t4": "NVIDIA_T4_GPUS", "nvidia-tesla-p4": "NVIDIA_P4_GPUS", "nvidia-tesla-v100": "NVIDIA_V100_GPUS",
}

// selfScope splits a resource selfLink into project, scope kind and location.
func selfScope(self string) (string, string, string) {
	parts := strings.Split(trimURL(self), "/")
	if len(parts) < selfScopeParts || parts[0] != collProjects {
		return "", "", ""
	}

	if parts[2] == scopeGlobal {
		return parts[1], scopeGlobal, ""
	}

	return parts[1], parts[2], parts[3]
}

type quotaMeter struct {
	use     map[string]float64
	project string
	region  string
}

// add counts n of metric for a resource: project-wide meters see global resources, regional ones see the region.
func (m *quotaMeter) add(metric, self string, n float64) {
	p, sc, loc := selfScope(self)
	if p != m.project {
		return
	}

	if m.region == allRegions {
		if sc == scopeGlobal {
			m.use[metric] += n
		}

		return
	}

	if sc == scopeZone {
		loc = regionOfZone(loc)
	} else if sc != scopeRegion {
		return
	}

	if loc == m.region {
		m.use[metric] += n
	}
}

// addAll counts n of metric for every location of the project.
func (m *quotaMeter) addAll(metric, self string, n float64) {
	if p, _, _ := selfScope(self); p == m.project && m.region == allRegions {
		m.use[metric] += n
	}
}

func familyCPUMetric(machine string) string {
	family, _, _ := strings.Cut(lastSegment(machine), "-")

	metric := strings.ToUpper(family) + "_CPUS"
	for _, d := range regionQuotaDefs {
		if d.metric == metric {
			return metric
		}
	}

	return ""
}

func (b *InMemoryBackend) meterInstancesLocked(m *quotaMeter) {
	now := b.now().UTC()

	for _, r := range b.instances {
		in := r.Inst
		m.add("INSTANCES", in.SelfLink, 1)

		if st := r.status(now, b.timing); st == stTerminated || st == stSuspended {
			continue
		}

		cpus, _ := machineShape(in.MachineType)
		n := float64(cpus)
		m.add("CPUS", in.SelfLink, n)
		m.addAll("CPUS_ALL_REGIONS", in.SelfLink, n)

		if in.Scheduling != nil && (in.Scheduling.Preemptible || in.Scheduling.ProvisioningModel == provSpot) {
			m.add("PREEMPTIBLE_CPUS", in.SelfLink, n)
		}

		if metric := familyCPUMetric(in.MachineType); metric != "" {
			m.add(metric, in.SelfLink, n)
		}

		for _, g := range in.GuestAccelerators {
			c := float64(g.AcceleratorCount)
			m.addAll("GPUS_ALL_REGIONS", in.SelfLink, c)

			if metric, ok := gpuQuotaMetric[lastSegment(g.AcceleratorType)]; ok {
				m.add(metric, in.SelfLink, c)
			}
		}

		for _, d := range in.Disks {
			if d.Type == scratchType {
				m.add("LOCAL_SSD_TOTAL_GB", in.SelfLink, localSSDGb)
			}
		}
	}
}

func (b *InMemoryBackend) meterAddressesLocked(m *quotaMeter) {
	inUse := map[string]bool{}
	count := func(ip, self string) {
		if ip != "" && !inUse[ip] {
			inUse[ip] = true
			m.add("IN_USE_ADDRESSES", self, 1)
		}
	}

	for _, r := range b.instances {
		for _, nic := range r.Inst.NetworkInterfaces {
			for _, ac := range nic.AccessConfigs {
				count(ac.NatIP, r.Inst.SelfLink)
			}
		}
	}

	for _, rec := range b.routers {
		b.countRouterIPsLocked(rec, count)
	}

	for _, a := range b.addresses {
		switch {
		case a.AddressType == addrInternal && a.Region != "":
			m.add("INTERNAL_ADDRESSES", a.SelfLink, 1)
		case a.AddressType == addrInternal:
			m.add("GLOBAL_INTERNAL_ADDRESSES", a.SelfLink, 1)
		default:
			m.add("STATIC_ADDRESSES", a.SelfLink, 1)
		}
	}
}

func (b *InMemoryBackend) countRouterIPsLocked(rec *routerRec, count func(ip, self string)) {
	for _, ips := range rec.AutoIPs {
		for _, ip := range ips {
			count(ip, rec.Router.SelfLink)
		}
	}

	for _, nat := range rec.Router.Nats {
		for _, link := range nat.NatIps {
			if a := b.addressBySelfLinkLocked(link); a != nil {
				count(a.Address, rec.Router.SelfLink)
			}
		}
	}
}

func (b *InMemoryBackend) addressBySelfLinkLocked(self string) *computeapi.Address {
	for _, a := range b.addresses {
		if a.SelfLink == self {
			return a
		}
	}

	return nil
}

func (b *InMemoryBackend) meterStorageLocked(m *quotaMeter) {
	for _, d := range b.disks {
		m.add("DISKS_TOTAL_GB", d.SelfLink, float64(d.SizeGb))

		if lastSegment(d.Type) == "pd-ssd" {
			m.add("SSD_TOTAL_GB", d.SelfLink, float64(d.SizeGb))
		}
	}

	for _, s := range b.snapshots {
		m.add("SNAPSHOTS", s.SelfLink, 1)
	}

	for _, s := range b.instantSnaps {
		m.add("IN_PLACE_SNAPSHOTS", s.SelfLink, 1)
	}

	for _, i := range b.images {
		m.add("IMAGES", i.SelfLink, 1)
	}

	for _, i := range b.machineImages {
		m.add("MACHINE_IMAGES", i.SelfLink, 1)
	}

	for _, r := range b.reservations {
		m.add("RESERVATIONS", r.SelfLink, 1)
	}

	for _, p := range b.resPolicies {
		m.add("RESOURCE_POLICIES", p.SelfLink, 1)
	}
}

func (b *InMemoryBackend) meterNetworkingLocked(m *quotaMeter) {
	for _, n := range b.networks {
		m.add("NETWORKS", n.SelfLink, 1)
	}

	for _, s := range b.subnets {
		m.add("SUBNETWORKS", s.SelfLink, 1)
	}

	for _, f := range b.firewalls {
		m.add("FIREWALLS", f.SelfLink, 1)
	}

	for _, r := range b.routes {
		m.add("ROUTES", r.SelfLink, 1)
	}

	for _, r := range b.routers {
		m.add("ROUTERS", r.Router.SelfLink, 1)
	}

	for _, p := range b.secPolicies {
		metric := "SECURITY_POLICIES"
		if p.Region != "" {
			metric = "SECURITY_POLICIES_PER_REGION"
		}

		m.add(metric, p.SelfLink, 1)
	}

	for _, a := range b.icAttachments {
		m.add("INTERCONNECT_ATTACHMENTS_PER_REGION", a.SelfLink, 1)
	}

	for _, i := range b.interconnects {
		m.add("INTERCONNECTS", i.SelfLink, 1)
	}

	for _, v := range b.externalVPNs {
		m.add("EXTERNAL_VPN_GATEWAYS", v.SelfLink, 1)
	}

	for _, v := range b.vpnGateways {
		m.add("VPN_GATEWAYS", v.SelfLink, 1)
	}

	for _, v := range b.targetVPNs {
		m.add("TARGET_VPN_GATEWAYS", v.SelfLink, 1)
	}

	for _, v := range b.vpnTunnels {
		m.add("VPN_TUNNELS", v.SelfLink, 1)
	}
}

func (b *InMemoryBackend) meterLoadBalancingLocked(m *quotaMeter) {
	for _, v := range b.healths {
		m.add("HEALTH_CHECKS", v.SelfLink, 1)
	}

	for _, v := range b.backendSvcs {
		m.add("BACKEND_SERVICES", v.SelfLink, 1)
	}

	for _, v := range b.backendBuckets {
		m.add("BACKEND_BUCKETS", v.SelfLink, 1)
	}

	for _, v := range b.urlMaps {
		m.add("URL_MAPS", v.SelfLink, 1)
	}

	for _, v := range b.sslCerts {
		m.add("SSL_CERTIFICATES", v.SelfLink, 1)
	}

	for _, v := range b.sslPolicies {
		m.add("SSL_POLICIES", v.SelfLink, 1)
	}

	for _, v := range b.httpProxies {
		m.add("TARGET_HTTP_PROXIES", v.SelfLink, 1)
	}

	for _, v := range b.httpsProxies {
		m.add("TARGET_HTTPS_PROXIES", v.SelfLink, 1)
	}

	for _, v := range b.tcpProxies {
		m.add("TARGET_TCP_PROXIES", v.SelfLink, 1)
	}

	for _, v := range b.sslProxies {
		m.add("TARGET_SSL_PROXIES", v.SelfLink, 1)
	}

	for _, v := range b.fwdRules {
		m.add("FORWARDING_RULES", v.SelfLink, 1)
	}

	for _, v := range b.targetPools {
		m.add("TARGET_POOLS", v.SelfLink, 1)
	}

	for _, v := range b.targetInstances {
		m.add("TARGET_INSTANCES", v.SelfLink, 1)
	}

	for _, v := range b.templates {
		m.add("INSTANCE_TEMPLATES", v.SelfLink, 1)
	}
}

// quotasLocked reports every metric of defs with usage computed from the backend state.
func (b *InMemoryBackend) quotasLocked(project, region string, defs []quotaDef) []*computeapi.Quota {
	m := &quotaMeter{project: project, region: region, use: make(map[string]float64, quotaMetrics)}

	b.meterInstancesLocked(m)
	b.meterAddressesLocked(m)
	b.meterStorageLocked(m)
	b.meterNetworkingLocked(m)
	b.meterLoadBalancingLocked(m)

	out := make([]*computeapi.Quota, 0, len(defs))

	for _, d := range defs {
		out = append(out, &computeapi.Quota{
			Metric: d.metric, Limit: d.limit, Usage: m.use[d.metric],
			ForceSendFields: []string{"Limit", "Usage"},
		})
	}

	slices.SortFunc(out, func(a, z *computeapi.Quota) int { return strings.Compare(a.Metric, z.Metric) })

	return out
}
