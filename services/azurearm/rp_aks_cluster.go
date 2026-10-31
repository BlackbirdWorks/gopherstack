package azurearm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

const (
	aksMaxClusterName = 63
	aksMaxDNSPrefix   = 54
	aksTokenBytes     = 16
	aksMaxAgentPools  = 100
	aksDefaultSkuName = "Base"
	aksDefaultTier    = "Free"
)

var (
	aksClusterNameRe = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9_-]*[a-zA-Z0-9])?$`)
	aksDNSPrefixRe   = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)
)

func aksClusterReadOnly() []string {
	return []string{
		fieldProvisioningState, "powerState", "fqdn", "privateFQDN", "azurePortalFQDN", "currentKubernetesVersion",
		"maxAgentPools", aksFieldETag,
	}
}

func aksRandomToken() string {
	buf := make([]byte, aksTokenBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

func (p *ContainerServiceProvider) lookup(rg, name string) (*storedAKSCluster, bool) {
	p.mu.RLock("lookup")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(rg, name)]

	return c, ok
}

func (p *ContainerServiceProvider) putCluster(
	ctx context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	out, drop, err := p.putClusterLocked(ctx, id, body)
	if drop != nil {
		drop()
	}

	return out, err
}

// putClusterLocked returns a cleanup to run once opMu is released: dropping a node resource group cascades back into
// this provider.
func (p *ContainerServiceProvider) putClusterLocked(
	ctx context.Context, id ResourceID, body map[string]any,
) (map[string]any, func(), error) {
	p.opMu.Lock("putCluster")
	defer p.opMu.Unlock()

	rec, prev, err := p.buildCluster(id, body)
	if err != nil {
		return nil, nil, err
	}

	if prev == nil {
		if p.backend.ResourceGroupExists(rec.NodeRG) {
			return nil, nil, aksBad("The node resource group '%s' already exists.", rec.NodeRG)
		}

		p.backend.PutResourceGroup(rec.NodeRG, rec.Location, nil)
	}

	p.mu.Lock("putCluster")
	p.clusters[aksKey(id.ResourceGroup, rec.Name)] = rec
	p.mu.Unlock()

	if !rec.Stopped {
		if err = p.bringUp(ctx, rec, prev == nil); err != nil {
			if prev == nil {
				return nil, p.detachCluster(ctx, rec), err
			}

			return nil, nil, err
		}
	}

	p.mu.RLock("putCluster")
	defer p.mu.RUnlock()

	return p.clusterBody(rec), nil, nil
}

// bringUp starts the cluster's node as a cancellable operation; an aborted start leaves the cluster Canceled.
func (p *ContainerServiceProvider) bringUp(ctx context.Context, rec *storedAKSCluster, created bool) error {
	if !p.eng.enabled() {
		return nil
	}

	state := aksStateUpdating
	if created {
		state = aksStateCreating
	}

	opCtx, end := p.beginOp(ctx, rec, state)
	err := p.ensureNode(opCtx, rec, created)

	if end() {
		return nil
	}

	return err
}

// detachCluster forgets a cluster and stops its node; the returned cleanup drops the node resource group and must
// run without opMu held.
func (p *ContainerServiceProvider) detachCluster(ctx context.Context, c *storedAKSCluster) func() {
	p.mu.Lock("detachCluster")
	delete(p.clusters, aksKey(c.ResourceGroup, c.Name))
	cascade := p.cascade
	p.mu.Unlock()

	p.eng.purge([]string{aksKey(c.ResourceGroup, c.Name)})

	return func() { p.dropResourceGroup(ctx, cascade, c.NodeRG) }
}

func (p *ContainerServiceProvider) buildCluster(
	id ResourceID, body map[string]any,
) (*storedAKSCluster, *storedAKSCluster, error) {
	name := id.LeafName()
	if len(name) > aksMaxClusterName || !aksClusterNameRe.MatchString(name) {
		return nil, nil, aksBad("Managed cluster name '%s' is invalid: use 1-%d alphanumerics, underscores or "+
			"hyphens, starting and ending with an alphanumeric.", name, aksMaxClusterName)
	}

	loc := strOf(body, fieldLocation)
	if loc == "" {
		return nil, nil, netErr(http.StatusBadRequest, "LocationRequired", "The location property is required for "+
			"this definition.")
	}

	prev, _ := p.lookup(id.ResourceGroup, name)
	now := time.Now().UTC()
	rec := &storedAKSCluster{
		Sub: id.SubscriptionID, Name: name, ResourceGroup: id.ResourceGroup, Location: loc, Created: now,
		Modified: now, Tags: stringTags(body[fieldTags]), Kind: strOf(body, "kind"), Pools: map[string]*storedAKSPool{},
	}

	var prevIdentity resourceIdentity

	if prev != nil {
		if prev.Stopped {
			return nil, nil, aksStoppedErr(name)
		}

		if !strings.EqualFold(loc, prev.Location) {
			return nil, nil, aksImmutableErr(name, fieldLocation)
		}

		rec.Created, rec.Location, rec.Token, rec.CACert = prev.Created, prev.Location, prev.Token, prev.CACert
		rec.NextSeq, prevIdentity = prev.NextSeq, prev.Identity
		rec.Maint, rec.Bindings, rec.Commands = prev.Maint, prev.Bindings, prev.Commands
	}

	props := acaClone(mapOf(body[fieldProperties]))
	if props == nil {
		props = map[string]any{}
	}

	for _, k := range aksClusterReadOnly() {
		delete(props, k)
	}

	_, hasIdentity := body[fieldIdentity]

	ident, err := p.ids.apply(body[fieldIdentity], hasIdentity, prevIdentity)
	if err != nil {
		return nil, nil, err
	}

	rec.Identity = ident

	if err = p.fillCluster(rec, prev, body, props); err != nil {
		return nil, nil, err
	}

	return rec, prev, nil
}

func aksStoppedErr(name string) error {
	return netErr(http.StatusBadRequest, aksCodeNotAllowed,
		"Operation is not allowed: managed cluster '%s' is stopped. Start the cluster first.", name)
}

func aksImmutableErr(name, field string) error {
	return netErr(http.StatusBadRequest, aksCodeNotAllowed,
		"Changing property '%s' of managed cluster '%s' is not allowed.", field, name)
}

func (p *ContainerServiceProvider) fillCluster(
	rec, prev *storedAKSCluster, body, props map[string]any,
) error {
	if err := aksCheckIdentity(rec, props); err != nil {
		return err
	}

	if err := aksCheckSKU(rec, body); err != nil {
		return err
	}

	if err := aksCheckNaming(rec, prev, props); err != nil {
		return err
	}

	if err := aksCheckVersion(rec, prev, props); err != nil {
		return err
	}

	np, err := aksNetworkFor(rec, prev, props)
	if err != nil {
		return err
	}

	props["networkProfile"] = np

	if err = p.fillPools(rec, prev, props, np); err != nil {
		return err
	}

	if rec.CACert == "" {
		ca, caErr := k3sengine.NewCA("gopherstack-aks-" + rec.Name + "-ca")
		if caErr != nil {
			return aksBad("Generating the cluster CA failed: %v", caErr)
		}

		rec.CACert, rec.Token = string(ca.CertPEM), aksRandomToken()
	}

	delete(props, aksFieldPools)
	delete(props, aksFieldK8sVersion)
	delete(props, aksFieldNodeRG)
	rec.Props = props

	return nil
}

func aksCheckIdentity(rec *storedAKSCluster, props map[string]any) error {
	sp := mapOf(props["servicePrincipalProfile"])

	if !rec.Identity.isSet() && strOf(sp, "clientId") == "" {
		return aksBad("Either identity or servicePrincipalProfile must be specified for managed cluster '%s'.",
			rec.Name)
	}

	if sp != nil && strOf(sp, "clientId") == "" {
		return aksBad("servicePrincipalProfile.clientId is required.")
	}

	return nil
}

func aksCheckSKU(rec *storedAKSCluster, body map[string]any) error {
	sku := mapOf(body[aksFieldSKU])
	rec.SkuName, rec.SkuTier = aksDefaultSkuName, aksDefaultTier

	if n := strOf(sku, fieldName); n != "" {
		c, ok := canonicalIn([]string{aksDefaultSkuName, "Automatic"}, n)
		if !ok {
			return aksBad("sku.name '%s' must be Base or Automatic.", n)
		}

		rec.SkuName = c
	}

	if t := strOf(sku, aksFieldTier); t != "" {
		c, ok := canonicalIn([]string{aksDefaultTier, aksTierStandard, aksTierPremium}, t)
		if !ok {
			return aksBad("sku.tier '%s' must be Free, Standard or Premium.", t)
		}

		rec.SkuTier = c
	}

	return nil
}

func aksCheckNaming(rec, prev *storedAKSCluster, props map[string]any) error {
	prefix := strOf(props, "dnsPrefix")
	if prefix == "" {
		prefix = strOf(props, "fqdnSubdomain")
	}

	if prefix == "" {
		return aksBad("Either dnsPrefix or fqdnSubdomain is required for managed cluster '%s'.", rec.Name)
	}

	if len(prefix) > aksMaxDNSPrefix || !aksDNSPrefixRe.MatchString(prefix) {
		return aksBad("DNS prefix '%s' is invalid: use 1-%d alphanumerics or hyphens, starting and ending with an "+
			"alphanumeric.", prefix, aksMaxDNSPrefix)
	}

	rec.Fqdn = aksFqdn(prefix, rec.Sub, rec.ResourceGroup, rec.Name, rec.Location, "hcp.")

	rec.NodeRG = strOf(props, aksFieldNodeRG)
	if rec.NodeRG == "" {
		rec.NodeRG = "MC_" + rec.ResourceGroup + "_" + rec.Name + "_" + acaRegion(rec.Location)
	}

	if prev == nil {
		return nil
	}

	if prefix != strOf(prev.Props, "dnsPrefix") && prefix != strOf(prev.Props, "fqdnSubdomain") {
		return aksImmutableErr(rec.Name, "dnsPrefix")
	}

	if !strings.EqualFold(rec.NodeRG, prev.NodeRG) {
		return aksImmutableErr(rec.Name, aksFieldNodeRG)
	}

	return nil
}

func aksCheckVersion(rec, prev *storedAKSCluster, props map[string]any) error {
	req := strOf(props, aksFieldK8sVersion)
	if req == "" && prev != nil {
		req = prev.Requested
	}

	cur, err := resolveAKSVersion(req)
	if err != nil {
		return err
	}

	if prev != nil {
		if err = aksUpgradeAllowed(prev.Current, cur); err != nil {
			return err
		}
	}

	rec.Requested, rec.Current = req, cur
	if req == "" {
		rec.Requested = cur
	}

	return nil
}

func aksNetworkFor(rec, prev *storedAKSCluster, props map[string]any) (map[string]any, error) {
	raw := mapOf(props["networkProfile"])
	if raw == nil && prev != nil {
		return acaClone(mapOf(prev.Props["networkProfile"])), nil
	}

	np, err := aksCheckNetwork(raw)
	if err != nil {
		return nil, err
	}

	if prev != nil {
		old := mapOf(prev.Props["networkProfile"])

		for _, k := range []string{aksNetPluginKey, "networkPluginMode", aksFieldPodCidr, aksFieldSvcCidr, "dnsServiceIP"} {
			if !strings.EqualFold(strOf(np, k), strOf(old, k)) {
				return nil, aksImmutableErr(rec.Name, "networkProfile."+k)
			}
		}
	}

	return np, nil
}

func (p *ContainerServiceProvider) fillPools(rec, prev *storedAKSCluster, props, np map[string]any) error {
	items := acaList(props[aksFieldPools])
	cc := aksClusterCtxOf(np, rec.Current)

	switch {
	case items == nil && prev != nil:
		for k, pool := range prev.Pools {
			clone := *pool
			clone.Props = acaClone(pool.Props)

			if pool.SpecVersion == "" {
				clone.Current = rec.Current
			} else if err := aksPoolSkewOK(clone.Current, rec.Current); err != nil {
				return err
			}

			rec.Pools[k] = &clone
		}
	case len(items) == 0:
		return aksBad("agentPoolProfiles must contain at least one node pool.")
	default:
		if err := p.poolsFromItems(rec, prev, items, cc); err != nil {
			return err
		}
	}

	if len(rec.Pools) > aksMaxAgentPools {
		return aksBad("A managed cluster can have at most %d node pools.", aksMaxAgentPools)
	}

	return p.aksCheckPoolSet(rec.Pools, np)
}

func (p *ContainerServiceProvider) poolsFromItems(
	rec, prev *storedAKSCluster, items []any, cc aksClusterCtx,
) error {
	for _, it := range items {
		m := mapOf(it)
		name := strOf(m, fieldName)
		key := strings.ToLower(name)

		if _, dup := rec.Pools[key]; dup {
			return aksBad("Node pool name '%s' is used more than once.", name)
		}

		var old *storedAKSPool
		if prev != nil {
			old = prev.Pools[key]
		}

		pool, err := p.checkPool(name, m, old, cc, rec.Modified)
		if err != nil {
			return err
		}

		if old == nil {
			rec.NextSeq++
			pool.Seq = rec.NextSeq
		}

		rec.Pools[key] = pool
	}

	return nil
}

func (p *ContainerServiceProvider) deleteCluster(ctx context.Context, id ResourceID, name string) error {
	p.opMu.Lock("deleteCluster")

	c, ok := p.lookup(id.ResourceGroup, name)
	if !ok {
		p.opMu.Unlock()

		return ErrResourceNotFound
	}

	drop := p.detachCluster(ctx, c)

	p.opMu.Unlock()
	drop()

	return nil
}

// PatchResource implements resourcePatcher: the Update Tags operation.
func (p *ContainerServiceProvider) PatchResource(id ResourceID, body map[string]any) (map[string]any, error) {
	t := aksTargetOf(id)
	if t.kind != aksKindCluster || t.listing {
		return nil, netErr(http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}

	p.opMu.Lock("PatchResource")
	defer p.opMu.Unlock()

	p.mu.Lock("PatchResource")
	defer p.mu.Unlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, t.names[0])
	}

	c.Tags = stringTags(body[fieldTags])
	c.Modified = time.Now().UTC()

	return p.clusterBody(c), nil
}

// setPower implements the start and stop actions.
func (p *ContainerServiceProvider) setPower(ctx context.Context, id ResourceID, stop bool) error {
	p.opMu.Lock("setPower")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.LeafName())
	if !ok {
		return aksNotFound(id.ResourceGroup, id.LeafName())
	}

	if c.Stopped == stop {
		return nil
	}

	if stop {
		if err := p.eng.pause(ctx, aksKey(c.ResourceGroup, c.Name)); err != nil {
			return err
		}
	} else {
		opCtx, end := p.beginOp(ctx, c, aksStateStarting)
		err := p.ensureNode(opCtx, c, false)

		if end() {
			return nil
		}

		if err != nil {
			return err
		}
	}

	p.mu.Lock("setPower")
	c.Stopped, c.Modified = stop, time.Now().UTC()
	p.mu.Unlock()

	return nil
}

// clusterBody renders a cluster; callers hold at least the read lock.
func (p *ContainerServiceProvider) clusterBody(c *storedAKSCluster) map[string]any {
	props := acaClone(c.Props)
	pools := make([]any, 0, len(c.Pools))

	for _, pool := range aksSortedPools(c.Pools) {
		prof := pool.profile(c.Stopped)
		prof[fieldName] = pool.Name
		pools = append(pools, prof)
	}

	power := aksPowerRunning
	if c.Stopped {
		power = aksPowerStopped
	}

	if sp := mapOf(props["servicePrincipalProfile"]); sp != nil {
		delete(sp, "secret")
	}

	if ap := mapOf(props["aadProfile"]); ap != nil {
		delete(ap, "serverAppSecret")
	}

	if wp := mapOf(props["windowsProfile"]); wp != nil {
		delete(wp, "adminPassword")
	}

	props[aksFieldPools] = pools
	props[aksFieldK8sVersion] = c.Requested
	props["currentKubernetesVersion"] = c.Current
	props[fieldProvisioningState] = aksProvisioning(c)
	props["powerState"] = map[string]any{aksFieldCode: power}
	props[aksFieldNodeRG] = c.NodeRG
	props["maxAgentPools"] = aksMaxAgentPools
	props["azurePortalFQDN"] = strings.Replace(c.Fqdn, ".hcp.", ".portal.hcp.", 1)

	if acaBool(mapOf(props["apiServerAccessProfile"]), "enablePrivateCluster") {
		props["privateFQDN"] = strings.Replace(c.Fqdn, ".hcp.", ".privatelink.", 1)
	} else {
		props["fqdn"] = c.Fqdn
	}

	if _, has := props["enableRBAC"]; !has {
		props["enableRBAC"] = true
	}

	out := map[string]any{
		"id": c.armID(), fieldName: c.Name, fieldType: namespaceMicrosoftContainerService + "/" + aksClusterType,
		fieldLocation: c.Location, fieldTags: tagsOrEmpty(c.Tags), fieldProperties: props,
		aksFieldSKU:        map[string]any{fieldName: c.SkuName, aksFieldTier: c.SkuTier},
		aksFieldETag:       aksEtag(c.Modified),
		acaFieldSystemData: acaSystemData(c.Created, c.Modified),
	}

	if c.Kind != "" {
		out["kind"] = c.Kind
	}

	if ident := p.ids.body(c.Identity); ident != nil {
		out[fieldIdentity] = ident
	}

	return out
}

func aksProvisioning(c *storedAKSCluster) string {
	if c.ProvState != "" {
		return c.ProvState
	}

	return provisioningStateSucceeded
}

func (p *ContainerServiceProvider) poolBody(c *storedAKSCluster, pool *storedAKSPool) map[string]any {
	return map[string]any{
		"id": aksResourceID(c.Sub, c.ResourceGroup, []string{aksClusterType, aksPoolType}, []string{c.Name, pool.Name}).
			ARMID(),
		fieldName:       pool.Name,
		fieldType:       namespaceMicrosoftContainerService + "/" + aksClusterType + "/" + aksPoolType,
		fieldProperties: pool.profile(c.Stopped),
	}
}

func (p *ContainerServiceProvider) putPool(id ResourceID, body map[string]any) (map[string]any, error) {
	p.opMu.Lock("putPool")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.Names[0])
	if !ok {
		return nil, aksNotFound(id.ResourceGroup, id.Names[0])
	}

	if c.Stopped {
		return nil, aksStoppedErr(c.Name)
	}

	name := id.Names[1]
	key := strings.ToLower(name)
	old := c.Pools[key]
	cc := aksClusterCtxOf(mapOf(c.Props["networkProfile"]), c.Current)

	pool, err := p.checkPool(name, mapOf(body[fieldProperties]), old, cc, time.Now().UTC())
	if err != nil {
		return nil, err
	}

	next := make(map[string]*storedAKSPool, len(c.Pools)+1)
	maps.Copy(next, c.Pools)

	next[key] = pool

	if len(next) > aksMaxAgentPools {
		return nil, aksBad("A managed cluster can have at most %d node pools.", aksMaxAgentPools)
	}

	if err = p.aksCheckPoolSet(next, mapOf(c.Props["networkProfile"])); err != nil {
		return nil, err
	}

	p.mu.Lock("putPool")
	defer p.mu.Unlock()

	if old == nil {
		c.NextSeq++
		pool.Seq = c.NextSeq
	}

	c.Pools, c.Modified = next, pool.Modified

	return p.poolBody(c, pool), nil
}

func (p *ContainerServiceProvider) deletePool(id ResourceID, t aksTarget) error {
	p.opMu.Lock("deletePool")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, t.names[0])
	if !ok {
		return ErrResourceNotFound
	}

	key := strings.ToLower(t.names[1])
	if _, found := c.Pools[key]; !found {
		return ErrResourceNotFound
	}

	if c.Stopped {
		return aksStoppedErr(c.Name)
	}

	next := make(map[string]*storedAKSPool, len(c.Pools))
	for k, v := range c.Pools {
		if k != key {
			next[k] = v
		}
	}

	if !slices.ContainsFunc(aksSortedPools(next), func(s *storedAKSPool) bool {
		return strOf(s.Props, aksFieldMode) == aksModeSystem
	}) {
		return netErr(http.StatusBadRequest, aksCodeNotAllowed, "There has to be at least one system agent pool.")
	}

	p.mu.Lock("deletePool")
	defer p.mu.Unlock()

	c.Pools, c.Modified = next, time.Now().UTC()

	return nil
}

// upgradeNodeImage moves a pool to the newest node image.
func (p *ContainerServiceProvider) upgradeNodeImage(id ResourceID) error {
	p.opMu.Lock("upgradeNodeImage")
	defer p.opMu.Unlock()

	c, ok := p.lookup(id.ResourceGroup, id.Names[0])
	if !ok {
		return aksNotFound(id.ResourceGroup, id.Names[0])
	}

	pool, found := c.Pools[strings.ToLower(id.Names[1])]
	if !found {
		return netErr(http.StatusNotFound, aksCodeNotFound, "The agent pool '%s' was not found.", id.Names[1])
	}

	if c.Stopped {
		return aksStoppedErr(c.Name)
	}

	p.mu.Lock("upgradeNodeImage")
	defer p.mu.Unlock()

	pool.NodeImage, pool.Modified, c.Modified = aksNodeImage(pool.Props), time.Now().UTC(), time.Now().UTC()

	return nil
}
