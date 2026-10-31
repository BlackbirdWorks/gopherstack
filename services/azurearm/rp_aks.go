package azurearm

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftContainerService = "Microsoft.ContainerService"
	aksClusterType                     = "managedClusters"
	aksPoolType                        = "agentPools"
	aksDomainSuffix                    = "azmk8s.io"
	aksHashLen                         = 8
	aksCodeNotFound                    = "ResourceNotFound"
	aksFieldNodeRG                     = "nodeResourceGroup"
	aksFieldPools                      = "agentPoolProfiles"
	aksFieldETag                       = "eTag"
	aksFieldCode                       = "code"
	aksFieldSKU                        = "sku"
	aksFieldK8sVersion                 = "kubernetesVersion"
	aksFieldSvcCidr                    = "serviceCidr"
	aksFieldTier                       = "tier"
	aksFieldPodCidr                    = "podCidr"
	aksDefaultName                     = "default"
	aksPowerRunning                    = "Running"
	aksPowerStopped                    = "Stopped"
	aksTierPremium                     = "Premium"
	aksTierStandard                    = "Standard"
)

func aksAPIVersions() []string {
	return []string{"2024-09-01", "2025-01-01", "2025-05-01", "2025-10-01", "2026-08-01"}
}

type aksKind int

const (
	aksKindOther aksKind = iota
	aksKindCluster
	aksKindPool
	aksKindMaint
	aksKindBinding
)

type aksTarget struct {
	names   []string
	kind    aksKind
	listing bool
}

func aksTargetOf(id ResourceID) aksTarget {
	t := aksTarget{names: id.Names, listing: len(id.Names) < len(id.Types)}

	types := make([]string, 0, len(id.Types))
	for _, s := range id.Types {
		types = append(types, strings.ToLower(s))
	}

	switch {
	case acaTypesAre(types, aksClusterType):
		t.kind = aksKindCluster
	case acaTypesAre(types, aksClusterType, aksPoolType):
		t.kind = aksKindPool
	case acaTypesAre(types, aksClusterType, aksMaintType):
		t.kind = aksKindMaint
	case acaTypesAre(types, aksClusterType, aksBindingType):
		t.kind = aksKindBinding
	}

	return t
}

func aksKey(rg, name string) string { return strings.ToLower(rg) + "/" + strings.ToLower(name) }

type storedAKSCluster struct {
	Created       time.Time                    `json:"created"`
	Modified      time.Time                    `json:"modified"`
	Tags          map[string]string            `json:"tags,omitempty"`
	Props         map[string]any               `json:"props"`
	Pools         map[string]*storedAKSPool    `json:"pools"`
	Commands      map[string]*storedAKSCommand `json:"commands,omitempty"`
	Bindings      map[string]*storedAKSBinding `json:"bindings,omitempty"`
	Maint         map[string]*storedAKSMaint   `json:"maint,omitempty"`
	Sub           string                       `json:"sub"`
	Token         string                       `json:"token"`
	Kind          string                       `json:"kind,omitempty"`
	SkuName       string                       `json:"skuName"`
	ResourceGroup string                       `json:"resourceGroup"`
	Requested     string                       `json:"requested"`
	Current       string                       `json:"current"`
	CACert        string                       `json:"caCert"`
	NodeRG        string                       `json:"nodeRG"`
	Location      string                       `json:"location"`
	Fqdn          string                       `json:"fqdn"`
	SkuTier       string                       `json:"skuTier"`
	Name          string                       `json:"name"`
	ProvState     string                       `json:"provState,omitempty"`
	Identity      resourceIdentity             `json:"identity"`
	NextSeq       int                          `json:"nextSeq"`
	Stopped       bool                         `json:"stopped,omitempty"`
}

func (c *storedAKSCluster) armID() string {
	return aksResourceID(c.Sub, c.ResourceGroup, []string{aksClusterType}, []string{c.Name}).ARMID()
}

func aksResourceID(sub, rg string, types, names []string) ResourceID {
	return ResourceID{
		SubscriptionID: sub, ResourceGroup: rg, Namespace: namespaceMicrosoftContainerService,
		Types: types, Names: names,
	}
}

func aksEtag(t time.Time) string { return acaEtag(t) }

func aksJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}

	return string(b)
}

func aksNotFound(rg, name string) error {
	return netErr(http.StatusNotFound, aksCodeNotFound,
		"The Resource '%s/%s/%s' under resource group '%s' was not found.",
		namespaceMicrosoftContainerService, aksClusterType, name, rg)
}

// aksFqdn is <prefix>-<8 hex>.hcp.<region>.azmk8s.io; the hash keeps names stable per cluster.
func aksFqdn(prefix, sub, rg, name, location, infix string) string {
	sum := sha256.Sum256([]byte(strings.ToLower(sub + "/" + rg + "/" + name)))

	return prefix + "-" + hex.EncodeToString(sum[:])[:aksHashLen] + "." + infix + acaRegion(location) + "." +
		aksDomainSuffix
}

// ContainerServiceProvider implements ResourceProvider for Microsoft.ContainerService (AKS). ARM keeps the
// configuration; with the opt-in engine each running cluster is a single-node k3s container.
type ContainerServiceProvider struct {
	now      func() time.Time
	backend  *InMemoryBackend
	net      *NetworkProvider
	ids      *identityService
	mu       *lockmetrics.RWMutex
	opMu     *lockmetrics.RWMutex
	clusters map[string]*storedAKSCluster
	ops      map[string]*aksOp
	cascade  func(ctx context.Context, resourceGroup string)
	eng      *aksEngine
	wg       sync.WaitGroup
}

// NewContainerServiceProvider creates the provider; network and ids may be nil.
func NewContainerServiceProvider(
	backend *InMemoryBackend, network *NetworkProvider, ids *identityService,
) *ContainerServiceProvider {
	return &ContainerServiceProvider{
		now: time.Now, backend: backend, net: network, ids: ids,
		mu: lockmetrics.New("azurearm.containerservice"), opMu: lockmetrics.New("azurearm.containerservice.ops"),
		clusters: map[string]*storedAKSCluster{}, ops: map[string]*aksOp{}, eng: newAKSEngine(),
	}
}

var _ ResourceProvider = (*ContainerServiceProvider)(nil)

// Namespace implements ResourceProvider.
func (p *ContainerServiceProvider) Namespace() string { return namespaceMicrosoftContainerService }

// ResourceTypes implements ResourceProvider.
func (p *ContainerServiceProvider) ResourceTypes() []ResourceTypeDef {
	return []ResourceTypeDef{{Type: aksClusterType, APIVersions: aksAPIVersions(), HasChildren: true}}
}

// SetResourceGroupCascade sets the callback that deletes the resources of a resource group in every provider; it
// removes the contents of a cluster's node resource group.
func (p *ContainerServiceProvider) SetResourceGroupCascade(fn func(ctx context.Context, resourceGroup string)) {
	p.mu.Lock("SetResourceGroupCascade")
	defer p.mu.Unlock()

	p.cascade = fn
}

// ListKeys implements ResourceProvider.
func (p *ContainerServiceProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Put implements ResourceProvider.
func (p *ContainerServiceProvider) Put(
	ctx context.Context,
	id ResourceID,
	body map[string]any,
) (map[string]any, error) {
	t := aksTargetOf(id)

	switch {
	case t.listing:
		return nil, ErrResourceNotFound
	case t.kind == aksKindCluster:
		return p.putCluster(ctx, id, body)
	case t.kind == aksKindPool:
		return p.putPool(id, body)
	case t.kind == aksKindMaint:
		return p.putMaint(id, body)
	case t.kind == aksKindBinding:
		return p.putBinding(id, body)
	}

	out, _, err := p.backend.putGenericResource(id, body)

	return out, err
}

// Get implements ResourceProvider.
func (p *ContainerServiceProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	t := aksTargetOf(id)

	if t.kind == aksKindOther {
		return p.backend.getGenericResource(id)
	}

	if t.listing {
		return nil, ErrResourceNotFound
	}

	switch t.kind {
	case aksKindMaint:
		return p.getMaint(id)
	case aksKindBinding:
		return p.getBinding(id)
	case aksKindOther, aksKindCluster, aksKindPool:
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return nil, ErrResourceNotFound
	}

	if t.kind == aksKindCluster {
		return p.clusterBody(c), nil
	}

	pool, found := c.Pools[strings.ToLower(t.names[1])]
	if !found {
		return nil, ErrResourceNotFound
	}

	return p.poolBody(c, pool), nil
}

// Delete implements ResourceProvider.
func (p *ContainerServiceProvider) Delete(ctx context.Context, id ResourceID) error {
	t := aksTargetOf(id)

	switch {
	case t.listing:
		return ErrResourceNotFound
	case t.kind == aksKindCluster:
		return p.deleteCluster(ctx, id, t.names[0])
	case t.kind == aksKindPool:
		return p.deletePool(id, t)
	case t.kind == aksKindMaint:
		return p.deleteMaint(id)
	case t.kind == aksKindBinding:
		return p.deleteBinding(id)
	}

	return p.backend.deleteGenericResource(id)
}

// List implements ResourceProvider.
func (p *ContainerServiceProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	t := aksTargetOf(id)

	if t.kind == aksKindOther {
		return p.backend.listGenericResources(id), nil
	}

	switch t.kind {
	case aksKindMaint:
		return p.listMaint(id)
	case aksKindBinding:
		return p.listBindings(id)
	case aksKindOther, aksKindCluster, aksKindPool:
	}

	p.mu.RLock("List")
	defer p.mu.RUnlock()

	if t.kind == aksKindPool {
		c, ok := p.clusters[aksKey(id.ResourceGroup, t.names[0])]
		if !ok {
			return nil, aksNotFound(id.ResourceGroup, t.names[0])
		}

		out := []map[string]any{}
		for _, pool := range aksSortedPools(c.Pools) {
			out = append(out, p.poolBody(c, pool))
		}

		return out, nil
	}

	out := []map[string]any{}

	for _, k := range sortedKeys(p.clusters) {
		if c := p.clusters[k]; id.ResourceGroup == "" || resourceGroupsEqual(c.ResourceGroup, id.ResourceGroup) {
			out = append(out, p.clusterBody(c))
		}
	}

	return out, nil
}

// Reset implements ResourceProvider.
func (p *ContainerServiceProvider) Reset() {
	p.opMu.Lock("Reset")
	defer p.opMu.Unlock()

	p.mu.Lock("Reset")
	p.clusters = map[string]*storedAKSCluster{}
	p.mu.Unlock()

	p.eng.removeAll()
	p.eng.sweepVolumes(nil)
}

// DeleteResourcesInGroup implements ResourceProvider; clusters of the group go with their node resource groups, and a
// deleted node resource group takes its cluster.
func (p *ContainerServiceProvider) DeleteResourcesInGroup(ctx context.Context, resourceGroup string) {
	p.opMu.Lock("DeleteResourcesInGroup")

	p.mu.Lock("DeleteResourcesInGroup")

	var nodeGroups, keys []string

	for k, c := range p.clusters {
		if resourceGroupsEqual(c.ResourceGroup, resourceGroup) || resourceGroupsEqual(c.NodeRG, resourceGroup) {
			keys = append(keys, k)

			if resourceGroupsEqual(c.ResourceGroup, resourceGroup) {
				nodeGroups = append(nodeGroups, c.NodeRG)
			}

			delete(p.clusters, k)
		}
	}

	cascade := p.cascade
	p.mu.Unlock()

	p.eng.purge(keys)
	p.opMu.Unlock()

	for _, rg := range nodeGroups {
		p.dropResourceGroup(ctx, cascade, rg)
	}
}

func (p *ContainerServiceProvider) dropResourceGroup(
	ctx context.Context, cascade func(context.Context, string), rg string,
) {
	if err := p.backend.DeleteResourceGroup(rg); err != nil {
		return
	}

	if cascade != nil {
		cascade(ctx, rg)
	}
}

// Close stops every cluster container.
func (p *ContainerServiceProvider) Close() {
	p.eng.shutdown()
	p.wg.Wait()
}

// etagOf implements etagSource for clusters and agent pools.
func (p *ContainerServiceProvider) etagOf(id ResourceID) (string, bool) {
	t := aksTargetOf(id)
	if t.listing || len(t.names) == 0 {
		return "", false
	}

	p.mu.RLock("etagOf")
	defer p.mu.RUnlock()

	c, ok := p.clusters[aksKey(id.ResourceGroup, t.names[0])]
	if !ok {
		return "", false
	}

	switch t.kind {
	case aksKindCluster:
		return aksEtag(c.Modified), true
	case aksKindPool:
		if pool, found := c.Pools[strings.ToLower(t.names[1])]; found {
			return aksEtag(pool.Modified), true
		}
	case aksKindOther, aksKindMaint, aksKindBinding:
	}

	return "", false
}
