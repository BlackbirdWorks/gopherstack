package azurearm

import (
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strings"
	"time"
)

const (
	aksModeSystem      = "System"
	aksModeUser        = "User"
	aksOSLinux         = "Linux"
	aksOSWindows       = "Windows"
	aksPriorityRegular = "Regular"
	aksPrioritySpot    = "Spot"
	aksMaxPools        = 100
	aksMaxCount        = 1000
	aksLinuxPoolName   = 12
	aksWinPoolName     = 6
	aksSystemMinCores  = 4
	aksSystemMinMemMB  = 4096
	aksMaxOSDiskGB     = 2048
	aksFieldVMSize     = "vmSize"
	aksFieldSubnet     = "vnetSubnetID"
	aksFieldPodSubnet  = "podSubnetID"
	aksFieldMode       = "mode"
	aksFieldOSType     = "osType"
	aksFieldCount      = "count"
	aksFieldMin        = "minCount"
	aksFieldMax        = "maxCount"
	aksFieldAutoscale  = "enableAutoScaling"
	aksFieldOrchVer    = "orchestratorVersion"
	aksFieldPriority   = "scaleSetPriority"
	aksImageUbuntu     = "AKSUbuntu-2204gen2containerd-"
	aksImageAzLinux    = "AKSAzureLinux-V3gen2-"
	aksImageWindows    = "AKSWindows-2022-containerd-20348."
	aksLatestImageTag  = "202610.12.0"
	aksCodeNotAllowed  = "OperationNotAllowed"
	aksDefaultOSDiskGB = 128
)

var aksPoolNameRe = regexp.MustCompile(`^[a-z][a-z0-9]*$`)

type storedAKSPool struct {
	Created     time.Time      `json:"created"`
	Modified    time.Time      `json:"modified"`
	Props       map[string]any `json:"props"`
	Name        string         `json:"name"`
	SpecVersion string         `json:"specVersion,omitempty"`
	Current     string         `json:"current"`
	NodeImage   string         `json:"nodeImage"`
	Seq         int            `json:"seq"`
}

// aksImmutablePool lists pool properties that cannot change after creation (Learn: use-system-pools, node pool
// resize guidance).
func aksImmutablePool() []string {
	return []string{
		aksFieldVMSize, aksFieldOSType, "osSKU", "osDiskSizeGB", "osDiskType", aksFieldSubnet, aksFieldPodSubnet,
		"maxPods", "availabilityZones", "enableNodePublicIP", aksFieldPriority, "scaleSetEvictionPolicy", fieldType,
	}
}

func aksPoolReadOnly() []string {
	return []string{
		fieldProvisioningState, "powerState", "currentOrchestratorVersion", "nodeImageVersion", fieldName,
		aksFieldETag,
	}
}

type aksClusterCtx struct {
	plugin  string
	mode    string
	current string
}

func (c aksClusterCtx) defaultMaxPods() int {
	const (
		kubenetDefault  = 110
		overlayDefault  = 250
		azureCNIDefault = 30
	)

	switch {
	case c.plugin == aksPluginKubenet:
		return kubenetDefault
	case c.mode == aksModeOverlay:
		return overlayDefault
	}

	return azureCNIDefault
}

func aksClusterCtxOf(np map[string]any, current string) aksClusterCtx {
	return aksClusterCtx{
		plugin: strings.ToLower(strOf(np, aksNetPluginKey)), mode: strings.ToLower(strOf(np, "networkPluginMode")),
		current: current,
	}
}

// checkPool validates one pool's properties (the agentPools body's properties or an agentPoolProfiles item) and
// returns the stored pool. prev is the existing pool, if any.
func (p *ContainerServiceProvider) checkPool(
	name string, in map[string]any, prev *storedAKSPool, cc aksClusterCtx, now time.Time,
) (*storedAKSPool, error) {
	props := acaClone(in)
	if props == nil {
		props = map[string]any{}
	}

	for _, k := range aksPoolReadOnly() {
		delete(props, k)
	}

	if prev != nil {
		effective := prev.profile(false)

		for _, k := range aksImmutablePool() {
			if err := aksKeepImmutable(name, k, props, effective); err != nil {
				return nil, err
			}
		}
	}

	if err := aksCheckPoolName(name, props); err != nil {
		return nil, err
	}

	if err := aksCheckPoolShape(name, props, cc); err != nil {
		return nil, err
	}

	rec := &storedAKSPool{Name: name, Props: props, Created: now, Modified: now}
	if prev != nil {
		rec.Created, rec.Seq, rec.NodeImage = prev.Created, prev.Seq, prev.NodeImage
	}

	if err := aksResolvePoolVersion(rec, prev, cc); err != nil {
		return nil, err
	}

	if rec.NodeImage == "" {
		rec.NodeImage = aksNodeImage(props)
	}

	return rec, nil
}

func aksKeepImmutable(pool, key string, next, old map[string]any) error {
	nv, has := next[key]
	if !has {
		if ov, ok := old[key]; ok {
			next[key] = ov
		}

		return nil
	}

	if !aksEqualValue(nv, old[key]) {
		return netErr(
			http.StatusBadRequest,
			aksCodeNotAllowed,
			"Changing property '%s' of node pool '%s' is not allowed.",
			key,
			pool,
		)
	}

	return nil
}

func aksEqualValue(a, b any) bool {
	as, aok := a.(string)
	bs, bok := b.(string)

	if aok && bok {
		return strings.EqualFold(as, bs)
	}

	return aksJSON(a) == aksJSON(b)
}

func aksCheckPoolName(name string, props map[string]any) error {
	limit := aksLinuxPoolName
	if strings.EqualFold(strOf(props, aksFieldOSType), aksOSWindows) {
		limit = aksWinPoolName
	}

	if name == "" || len(name) > limit || !aksPoolNameRe.MatchString(name) {
		return aksBad("Node pool name '%s' is invalid: it must contain only lowercase alphanumeric characters, begin "+
			"with a lowercase letter and be 1-%d characters long (12 for Linux, 6 for Windows).", name, limit)
	}

	return nil
}

func aksCheckPoolShape(name string, props map[string]any, cc aksClusterCtx) error {
	mode, err := aksCheckPoolKind(name, props)
	if err != nil {
		return err
	}

	if err = aksCheckPoolType(name, props); err != nil {
		return err
	}

	if err = aksCheckVMSize(name, mode, strOf(props, aksFieldVMSize)); err != nil {
		return err
	}

	if err = aksCheckCounts(name, mode, props); err != nil {
		return err
	}

	return aksCheckPodsAndZones(name, mode, props, cc)
}

func aksCheckPoolKind(name string, props map[string]any) (string, error) {
	mode := strOf(props, aksFieldMode)
	if mode != aksModeSystem && mode != aksModeUser {
		return "", aksBad("Node pool '%s' mode must be explicitly set to System or User.", name)
	}

	osType, ok := aksCanon(strOf(props, aksFieldOSType), aksOSLinux, aksOSWindows)
	if !ok {
		return "", aksBad("Node pool '%s' osType '%s' must be Linux or Windows.", name, strOf(props, aksFieldOSType))
	}

	props[aksFieldOSType] = osType

	prio, ok := aksCanon(strOf(props, aksFieldPriority), aksPriorityRegular, aksPrioritySpot)
	if !ok {
		return "", aksBad("Node pool '%s' scaleSetPriority must be Regular or Spot.", name)
	}

	if mode == aksModeSystem && (osType == aksOSWindows || prio == aksPrioritySpot) {
		return "", aksBad("Node pool '%s': system node pools must use Linux and cannot be Spot.", name)
	}

	return mode, nil
}

func aksCheckPoolType(name string, props map[string]any) error {
	if t := strOf(props, fieldType); t != "" {
		c, found := aksCanon(t, "VirtualMachineScaleSets", "AvailabilitySet", "VirtualMachines")
		if !found {
			return aksBad("Node pool '%s' type '%s' is not supported.", name, t)
		}

		props[fieldType] = c
	}

	if gb, set := acaNum(props, "osDiskSizeGB"); set && (gb < 0 || gb > aksMaxOSDiskGB) {
		return aksBad("Node pool '%s' osDiskSizeGB must be between 0 and %d.", name, aksMaxOSDiskGB)
	}

	return nil
}

func aksCanon(v string, allowed ...string) (string, bool) {
	if v == "" {
		return allowed[0], true
	}

	return canonicalIn(allowed, v)
}

func aksCheckVMSize(pool, mode, size string) error {
	if size == "" {
		return aksBad("Node pool '%s' requires vmSize.", pool)
	}

	def, ok := findVMSize(size)
	if !ok {
		return aksBad("The VM size of %s is not allowed in your subscription in this location.", size)
	}

	if mode != aksModeSystem {
		return nil
	}

	if def.family == familyBS {
		return aksBad("Node pool '%s': B-series VM sizes such as %s are not supported for system node pools.",
			pool, def.name)
	}

	if def.cores < aksSystemMinCores || def.memoryMB < aksSystemMinMemMB {
		return aksBad("Node pool '%s': system node pools require a VM size with at least %d vCPUs and %d GB of "+
			"memory, but %s has %d vCPUs and %d MB.", pool, aksSystemMinCores, aksSystemMinMemMB/mbPerGB,
			def.name, def.cores, def.memoryMB)
	}

	return nil
}

func aksCheckCounts(pool, mode string, props map[string]any) error {
	floor := 0
	if mode == aksModeSystem {
		floor = 1
	}

	if acaBool(props, aksFieldAutoscale) {
		return aksCheckAutoscale(pool, mode, floor, props)
	}

	_, hasMin := acaNum(props, aksFieldMin)
	_, hasMax := acaNum(props, aksFieldMax)

	if hasMin || hasMax {
		return aksBad("Node pool '%s': minCount and maxCount can only be set when enableAutoScaling is true.", pool)
	}

	count, hasCount := acaNum(props, aksFieldCount)
	if !hasCount {
		count = 1
		props[aksFieldCount] = 1
	}

	if int(count) < floor || int(count) > aksMaxCount {
		return aksBad("Node pool '%s' count must be between %d and %d for a %s pool.",
			pool, floor, aksMaxCount, strings.ToLower(mode))
	}

	return nil
}

func aksCheckAutoscale(pool, mode string, floor int, props map[string]any) error {
	count, hasCount := acaNum(props, aksFieldCount)
	minC, hasMin := acaNum(props, aksFieldMin)
	maxC, hasMax := acaNum(props, aksFieldMax)

	if !hasMin || !hasMax || int(minC) < floor || int(maxC) > aksMaxCount || minC > maxC {
		return aksBad("Node pool '%s': with autoscaling, minCount and maxCount are required, minCount (at least %d "+
			"for a %s pool) must not exceed maxCount (at most %d).", pool, floor, strings.ToLower(mode), aksMaxCount)
	}

	switch {
	case !hasCount:
		props[aksFieldCount] = int(minC)
	case count < minC || count > maxC:
		return aksBad("Node pool '%s' count %d must be between minCount %d and maxCount %d.",
			pool, int(count), int(minC), int(maxC))
	}

	return nil
}

func aksCheckPodsAndZones(pool, mode string, props map[string]any, cc aksClusterCtx) error {
	const minPodsForSystem = 30

	maxPods := cc.defaultMaxPods()
	if mp, set := acaNum(props, "maxPods"); set {
		maxPods = int(mp)
	}

	if maxPods < aksMaxPodsMin || maxPods > aksMaxPodsMax {
		return aksBad("Node pool '%s' maxPods must be between %d and %d.", pool, aksMaxPodsMin, aksMaxPodsMax)
	}

	nodes := 1
	if c, set := acaNum(props, aksFieldCount); set {
		nodes = int(c)
	}

	if acaBool(props, aksFieldAutoscale) {
		minC, _ := acaNum(props, aksFieldMin)
		nodes = int(minC)
	}

	if mode == aksModeSystem && maxPods*nodes < minPodsForSystem && maxPods < minPodsForSystem {
		return aksBad("Node pool '%s': system node pools must support at least %d pods (maxPods %d x %d nodes).",
			pool, minPodsForSystem, maxPods, nodes)
	}

	seen := map[string]bool{}

	for _, z := range strList(props["availabilityZones"]) {
		if !slices.Contains([]string{"1", "2", "3"}, z) || seen[z] {
			return aksBad("Node pool '%s' availabilityZones must be distinct values from 1, 2 and 3.", pool)
		}

		seen[z] = true
	}

	return nil
}

func aksResolvePoolVersion(rec, prev *storedAKSPool, cc aksClusterCtx) error {
	spec := strOf(rec.Props, aksFieldOrchVer)
	delete(rec.Props, aksFieldOrchVer)

	if spec == "" {
		rec.Current = cc.current

		return nil
	}

	cur, err := resolveAKSVersion(spec)
	if err != nil {
		return err
	}

	if err = aksPoolSkewOK(cur, cc.current); err != nil {
		return err
	}

	if prev != nil {
		pf, _ := parseAKSVersion(prev.Current)
		nf, _ := parseAKSVersion(cur)

		if nf.minor < pf.minor || (nf.minor == pf.minor && nf.patch < pf.patch) {
			return netErr(http.StatusBadRequest, aksCodeInvalidUpgrade,
				"Downgrading node pool Kubernetes version from %s to %s is not allowed.", prev.Current, cur)
		}
	}

	rec.SpecVersion, rec.Current = spec, cur

	return nil
}

func aksNodeImage(props map[string]any) string {
	switch {
	case strings.EqualFold(strOf(props, aksFieldOSType), aksOSWindows):
		return aksImageWindows + aksLatestImageTag
	case strings.EqualFold(strOf(props, "osSKU"), "AzureLinux"):
		return aksImageAzLinux + aksLatestImageTag
	}

	return aksImageUbuntu + aksLatestImageTag
}

// aksCheckPoolSet enforces the invariants across a cluster's pools: a system pool exists, all pools share one virtual
// network, and the network profile's ranges do not overlap their subnets.
func (p *ContainerServiceProvider) aksCheckPoolSet(pools map[string]*storedAKSPool, np map[string]any) error {
	system := false
	vnets := map[string]string{}
	subnets := map[string][]netip.Prefix{}

	for _, name := range sortedKeys(pools) {
		pool := pools[name]

		if strOf(pool.Props, aksFieldMode) == aksModeSystem {
			system = true
		}

		for _, field := range []string{aksFieldSubnet, aksFieldPodSubnet} {
			id := strOf(pool.Props, field)
			if id == "" {
				continue
			}

			prefixes, err := p.aksSubnetRef(field, id)
			if err != nil {
				return err
			}

			if field == aksFieldSubnet {
				subnets[id] = prefixes
			}

			vnet, _, _ := strings.Cut(strings.ToLower(id), "/subnets/")
			if first, seen := vnets[field]; seen && first != vnet {
				return aksBad("All node pools of a cluster must use subnets of the same virtual network (%s).", field)
			}

			vnets[field] = vnet
		}
	}

	if !system {
		return netErr(http.StatusBadRequest, aksCodeNotAllowed, "There has to be at least one system agent pool.")
	}

	return aksCheckSubnetOverlap(np, subnets)
}

func (s *storedAKSPool) sortedKey() string { return strings.ToLower(s.Name) }

func aksSortedPools(pools map[string]*storedAKSPool) []*storedAKSPool {
	out := make([]*storedAKSPool, 0, len(pools))
	for _, pool := range pools {
		out = append(out, pool)
	}

	slices.SortFunc(out, func(a, b *storedAKSPool) int {
		if a.Seq != b.Seq {
			return a.Seq - b.Seq
		}

		return strings.Compare(a.sortedKey(), b.sortedKey())
	})

	return out
}

// profile renders the pool as an agentPoolProfiles item or the properties of an AgentPool.
func (s *storedAKSPool) profile(stopped bool) map[string]any {
	props := acaClone(s.Props)

	props[fieldProvisioningState] = provisioningStateSucceeded
	props["currentOrchestratorVersion"] = s.Current
	props[aksFieldOrchVer] = s.Current
	props["nodeImageVersion"] = s.NodeImage

	power := aksPowerRunning
	if stopped {
		power = aksPowerStopped
	}

	props["powerState"] = map[string]any{aksFieldCode: power}

	for k, v := range map[string]any{
		aksFieldOSType: aksOSLinux, fieldType: "VirtualMachineScaleSets", "osDiskType": "Managed",
		"osDiskSizeGB": aksDefaultOSDiskGB, aksFieldPriority: aksPriorityRegular,
	} {
		if _, has := props[k]; !has {
			props[k] = v
		}
	}

	if s.SpecVersion != "" {
		props[aksFieldOrchVer] = s.SpecVersion
	}

	return props
}
