package ocicontainerengine

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	apiPort         = "6443"
	defaultPodsCIDR = "10.244.0.0/16"
	defaultSvcCIDR  = "10.96.0.0/16"
	tokenBytes      = 24
	maxDecommission = 30
	hoursPerDay     = 24
)

var durationDaysRe = regexp.MustCompile(`^P(\d+)D$`)

// CreateClusterIn is the CreateCluster request body.
type CreateClusterIn struct {
	Tags
	VcnID          *string         `json:"vcnId"`
	KmsKeyID       *string         `json:"kmsKeyId"`
	EndpointConfig *EndpointConfig `json:"endpointConfig"`
	Options        *ClusterOptions `json:"options"`
	ImagePolicy    *ImagePolicy    `json:"imagePolicyConfig"`
	Name           string          `json:"name"`
	Compartment    string          `json:"compartmentId"`
	Version        string          `json:"kubernetesVersion"`
	Type           string          `json:"type"`
	PodNetworks    []PodNet        `json:"clusterPodNetworkOptions"`
}

// UpdateClusterIn is the UpdateCluster request body.
type UpdateClusterIn struct {
	Tags
	Name        *string         `json:"name"`
	Version     *string         `json:"kubernetesVersion"`
	Options     *ClusterOptions `json:"options"`
	ImagePolicy *ImagePolicy    `json:"imagePolicyConfig"`
	Type        string          `json:"type"`
}

// MigrateIn is the ClusterMigrateToNativeVcn request body.
type MigrateIn struct {
	EndpointConfig *EndpointConfig `json:"endpointConfig"`
	Decommission   *string         `json:"decommissionDelayDuration"`
}

type clusterMeta struct {
	TimeCreated            time.Time  `json:"timeCreated"`
	TimeUpdated            *time.Time `json:"timeUpdated,omitempty"`
	TimeDeleted            *time.Time `json:"timeDeleted,omitempty"`
	CreatedByWorkRequestID string     `json:"createdByWorkRequestId,omitempty"`
	UpdatedByWorkRequestID string     `json:"updatedByWorkRequestId,omitempty"`
	DeletedByWorkRequestID string     `json:"deletedByWorkRequestId,omitempty"`
}

// ClusterWire is the Cluster and ClusterSummary body.
type ClusterWire struct {
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	KmsKeyID          *string                   `json:"kmsKeyId,omitempty"`
	EndpointConfig    *EndpointConfig           `json:"endpointConfig,omitempty"`
	Endpoints         *Endpoints                `json:"endpoints,omitempty"`
	Options           ClusterOptions            `json:"options"`
	ImagePolicy       ImagePolicy               `json:"imagePolicyConfig"`
	Metadata          clusterMeta               `json:"metadata"`
	ID                string                    `json:"id"`
	Name              string                    `json:"name"`
	CompartmentID     string                    `json:"compartmentId"`
	VcnID             string                    `json:"vcnId,omitempty"`
	KubernetesVersion string                    `json:"kubernetesVersion"`
	LifecycleState    string                    `json:"lifecycleState"`
	LifecycleDetails  string                    `json:"lifecycleDetails,omitempty"`
	Type              string                    `json:"type"`
	Upgrades          []string                  `json:"availableKubernetesUpgrades"`
	PodNetworks       []PodNet                  `json:"clusterPodNetworkOptions"`
}

func clusterView(c *Cluster, summary bool) ClusterWire {
	w := ClusterWire{
		ID: c.ID, Name: c.Name, CompartmentID: c.CompartmentID, VcnID: c.VcnID, KubernetesVersion: c.Version,
		EndpointConfig: c.EndpointConfig, Options: c.Options, ImagePolicy: c.ImagePolicy,
		LifecycleState: c.State, LifecycleDetails: c.Details, Type: c.Type, PodNetworks: c.PodNetworks,
		FreeformTags: c.Tags.FreeformTags, DefinedTags: c.Tags.DefinedTags, Upgrades: upgradesFrom(c.Version),
		Metadata: clusterMeta{
			TimeCreated: c.TimeCreated, TimeUpdated: c.TimeUpdated, TimeDeleted: c.TimeDeleted,
			CreatedByWorkRequestID: c.CreatedByWR, UpdatedByWorkRequestID: c.UpdatedByWR,
			DeletedByWorkRequestID: c.DeletedByWR,
		},
	}
	if c.Endpoints != (Endpoints{}) {
		ep := c.Endpoints
		w.Endpoints = &ep
	}

	if !summary && c.KmsKeyID != "" {
		w.KmsKeyID = &c.KmsKeyID
	}

	return w
}

func (b *InMemoryBackend) checkSubnet(field, id, vcnID string) (SubnetInfo, *APIError) {
	if !strings.HasPrefix(id, "ocid1.subnet.") {
		return SubnetInfo{}, errParam(field + " must be the OCID of a subnet")
	}

	if b.net == nil {
		return SubnetInfo{ID: id, VcnID: vcnID}, nil
	}

	s, ok := b.net.Subnet(id)
	if !ok {
		return SubnetInfo{}, errParam(field + " " + id + " does not reference an existing subnet")
	}

	if vcnID != "" && s.VcnID != vcnID {
		return SubnetInfo{}, errParam(field + " " + id + " is not in VCN " + vcnID)
	}

	return s, nil
}

func (b *InMemoryBackend) checkKey(field, keyID string) *APIError {
	if !strings.HasPrefix(keyID, "ocid1.key.") {
		return errParam(field + " must be the OCID of a Vault key")
	}

	if b.keyChecker == nil {
		return nil
	}

	state, found := b.keyChecker(keyID)

	switch {
	case !found:
		return errParam(field + " " + keyID + " does not reference an existing key")
	case state != "ENABLED":
		return errParam(field + " " + keyID + " is in state " + state + "; the key must be ENABLED")
	}

	return nil
}

func validCIDR(field, v string) (netip.Prefix, *APIError) {
	p, err := netip.ParsePrefix(v)
	if err != nil || !p.Addr().Is4() || p.Masked() != p {
		return p, errParam(field + " must be an IPv4 CIDR block such as 10.244.0.0/16")
	}

	return p, nil
}

func (b *InMemoryBackend) checkPodNets(nets []PodNet) ([]PodNet, *APIError) {
	if len(nets) == 0 {
		return []PodNet{{CniType: cniFlannel}}, nil
	}

	if len(nets) > 1 {
		return nil, errParam("clusterPodNetworkOptions must contain at most one entry")
	}

	if nets[0].CniType != cniFlannel && nets[0].CniType != cniNative {
		return nil, errParam("clusterPodNetworkOptions.cniType must be " + cniFlannel + " or " + cniNative)
	}

	return []PodNet{{CniType: nets[0].CniType}}, nil
}

// checkOptions validates the create options and fills the documented defaults.
func (b *InMemoryBackend) checkOptions(in *ClusterOptions, vcnID string) (ClusterOptions, *APIError) {
	out := ClusterOptions{
		K8sNetwork:        &NetworkConfig{PodsCidr: defaultPodsCIDR, ServicesCidr: defaultSvcCIDR},
		AddOns:            &AddOns{IsKubernetesDashboardEnabled: new(false), IsTillerEnabled: new(false)},
		Admission:         &Admission{IsPodSecurityPolicyEnabled: new(false)},
		ServiceLbSubnetID: []string{},
	}

	if in == nil {
		return out, nil
	}

	if len(in.OIDCAuth) > 0 || len(in.OIDCDiscovery) > 0 {
		return out, errParam("OpenID Connect authentication and discovery are not supported by this emulator")
	}

	for _, id := range in.ServiceLbSubnetID {
		if _, aerr := b.checkSubnet("options.serviceLbSubnetIds", id, vcnID); aerr != nil {
			return out, aerr
		}
	}

	out.ServiceLbSubnetID = slices.Clone(in.ServiceLbSubnetID)
	if out.ServiceLbSubnetID == nil {
		out.ServiceLbSubnetID = []string{}
	}

	if aerr := mergeNetwork(&out, in.K8sNetwork); aerr != nil {
		return out, aerr
	}

	if in.AddOns != nil {
		out.AddOns = &AddOns{
			IsKubernetesDashboardEnabled: def(in.AddOns.IsKubernetesDashboardEnabled, new(false)),
			IsTillerEnabled:              def(in.AddOns.IsTillerEnabled, new(false)),
		}
	}

	applyUpdatable(&out, in)

	return out, nil
}

func mergeNetwork(out *ClusterOptions, in *NetworkConfig) *APIError {
	if in == nil {
		return nil
	}

	pods, svc := def(in.PodsCidr, defaultPodsCIDR), def(in.ServicesCidr, defaultSvcCIDR)

	pp, aerr := validCIDR("options.kubernetesNetworkConfig.podsCidr", pods)
	if aerr != nil {
		return aerr
	}

	sp, aerr := validCIDR("options.kubernetesNetworkConfig.servicesCidr", svc)
	if aerr != nil {
		return aerr
	}

	if pp.Overlaps(sp) {
		return errParam("options.kubernetesNetworkConfig podsCidr and servicesCidr must not overlap")
	}

	out.K8sNetwork = &NetworkConfig{PodsCidr: pods, ServicesCidr: svc}

	return nil
}

// applyUpdatable copies the option blocks UpdateCluster may change.
func applyUpdatable(out, in *ClusterOptions) {
	if in.Admission != nil {
		out.Admission = &Admission{IsPodSecurityPolicyEnabled: def(in.Admission.IsPodSecurityPolicyEnabled, new(false))}
	}

	if in.PersistentVolume != nil {
		out.PersistentVolume = in.PersistentVolume
	}

	if in.ServiceLb != nil {
		out.ServiceLb = in.ServiceLb
	}
}

func (b *InMemoryBackend) checkEndpointConfig(ec *EndpointConfig, vcnID string) (SubnetInfo, *APIError) {
	if ec == nil {
		return SubnetInfo{}, nil
	}

	if ec.SubnetID == "" {
		return SubnetInfo{}, errMissing("endpointConfig.subnetId is required")
	}

	return b.checkSubnet("endpointConfig.subnetId", ec.SubnetID, vcnID)
}

func (b *InMemoryBackend) checkImagePolicy(in *ImagePolicy) (ImagePolicy, *APIError) {
	if in == nil {
		return ImagePolicy{IsPolicyEnabled: new(false), KeyDetails: []KeyDetail{}}, nil
	}

	for _, k := range in.KeyDetails {
		if aerr := b.checkKey("imagePolicyConfig.keyDetails.kmsKeyId", k.KmsKeyID); aerr != nil {
			return ImagePolicy{}, aerr
		}
	}

	out := ImagePolicy{IsPolicyEnabled: def(in.IsPolicyEnabled, new(false)), KeyDetails: slices.Clone(in.KeyDetails)}
	if out.KeyDetails == nil {
		out.KeyDetails = []KeyDetail{}
	}

	return out, nil
}

// hostIn returns the address offset hosts above the start of a CIDR block, or "" when it does not fit.
func hostIn(cidr string, offset int) string {
	p, err := netip.ParsePrefix(cidr)
	if err != nil {
		return ""
	}

	a := p.Masked().Addr()
	for range offset {
		a = a.Next()
	}

	if !p.Contains(a) {
		return ""
	}

	return a.String()
}

// documentationAddr hands out addresses from the RFC 5737 documentation ranges.
func documentationAddr(n int64) string {
	bases := []string{"203.0.113.", "198.51.100.", "192.0.2."}
	const hosts = 253

	i := (n - 1) % (hosts * int64(len(bases)))

	return bases[i/hosts] + strconv.FormatInt(i%hosts+1, 10)
}

// assignEndpoints gives a cluster API endpoints; the docker engine replaces them with the real server.
func (b *InMemoryBackend) assignEndpoints(c *Cluster, sub SubnetInfo) {
	suffix := ":" + apiPort

	if c.EndpointConfig == nil {
		c.Endpoints = Endpoints{Kubernetes: documentationAddr(b.nextSeq()) + suffix}

		return
	}

	priv := hostIn(sub.CIDR, privateEndpointOffset+len(b.clusters))
	if priv == "" {
		priv = documentationAddr(b.nextSeq())
	}

	c.Endpoints = Endpoints{PrivateEndpoint: priv + suffix}
	if c.EndpointConfig.IsPublicIPEnabled != nil && *c.EndpointConfig.IsPublicIPEnabled {
		c.Endpoints.PublicEndpoint = documentationAddr(b.nextSeq()) + suffix
	}
}

const privateEndpointOffset = 5

func newToken() string {
	buf := make([]byte, tokenBytes)
	_, _ = rand.Read(buf)

	return "gopherstack-oke-" + hex.EncodeToString(buf)
}

func (b *InMemoryBackend) newIdentity(c *Cluster) *APIError {
	ca, err := k3sengine.NewCA("gopherstack-oke-" + ocidTail(c.ID, longTail) + "-ca")
	if err != nil {
		return newErr(http.StatusInternalServerError, "InternalServerError", "Failed to generate the cluster CA")
	}

	c.CAPEM, c.Token = string(ca.CertPEM), newToken()

	return nil
}

// CreateCluster creates a cluster in CREATING state and returns its work request.
func (b *InMemoryBackend) CreateCluster(in CreateClusterIn) (string, error) {
	b.mu.Lock("CreateCluster")
	defer b.mu.Unlock()

	if aerr := firstErr(b.checkCompartment(in.Compartment), validateName(in.Name)); aerr != nil {
		return "", aerr
	}

	switch {
	case in.Version == "":
		return "", errMissing("kubernetesVersion is required")
	case !isSupported(in.Version):
		return "", errParam("kubernetesVersion " + in.Version + " is not supported; see GetClusterOptions")
	case in.Type != "" && in.Type != typeBasic && in.Type != typeEnhanced:
		return "", errParam("type must be " + typeBasic + " or " + typeEnhanced)
	}

	c, sub, aerr := b.buildCluster(in)
	if aerr != nil {
		return "", aerr
	}

	b.assignEndpoints(c, sub)

	if aerr = b.newIdentity(c); aerr != nil {
		return "", aerr
	}

	var finish *time.Time
	if b.engine == nil {
		finish = c.TransitionAt
	} else {
		c.TransitionAt = nil
	}

	c.CreatedByWR = b.newWR(wrClusterCreate, c.CompartmentID, entityCluster, c.ID, actionCreated, finish)
	b.clusters[c.ID] = c

	if b.engine != nil {
		b.launchLocked(c, c.CreatedByWR)
	}

	return c.CreatedByWR, nil
}

func (b *InMemoryBackend) buildCluster(in CreateClusterIn) (*Cluster, SubnetInfo, *APIError) {
	vcnID := ""
	if in.VcnID != nil {
		vcnID = *in.VcnID
		if b.net != nil && !b.net.VCNExists(vcnID) {
			return nil, SubnetInfo{}, errParam("vcnId " + vcnID + " does not reference an existing VCN")
		}
	}

	sub, aerr := b.checkEndpointConfig(in.EndpointConfig, vcnID)
	if aerr != nil {
		return nil, sub, aerr
	}

	vcnID = def(vcnID, sub.VcnID)

	opts, aerr := b.checkOptions(in.Options, vcnID)
	if aerr != nil {
		return nil, sub, aerr
	}

	pol, aerr := b.checkImagePolicy(in.ImagePolicy)
	if aerr != nil {
		return nil, sub, aerr
	}

	nets, aerr := b.checkPodNets(in.PodNetworks)
	if aerr != nil {
		return nil, sub, aerr
	}

	if in.KmsKeyID != nil {
		if aerr = b.checkKey("kmsKeyId", *in.KmsKeyID); aerr != nil {
			return nil, sub, aerr
		}
	}

	now := b.nowUTC()
	c := &Cluster{
		ID: newOCID("cluster"), Name: in.Name, CompartmentID: in.Compartment, VcnID: vcnID, Version: in.Version,
		Type: def(in.Type, typeBasic), State: stateCreating, TimeCreated: now, TransitionAt: b.transitionAt(),
		EndpointConfig: in.EndpointConfig, Options: opts, ImagePolicy: pol, PodNetworks: nets, Tags: in.Tags,
		Etag: newEtag(), Seq: b.nextSeq(),
	}

	if in.KmsKeyID != nil {
		c.KmsKeyID = *in.KmsKeyID
	}

	return c, sub, nil
}

func (b *InMemoryBackend) liveCluster(id string) (*Cluster, *APIError) {
	c, ok := b.clusters[id]
	if !ok {
		return nil, errNotFound(id)
	}

	return c, nil
}

// GetCluster returns one cluster and its etag; DELETED clusters stay readable.
func (b *InMemoryBackend) GetCluster(id string) (ClusterWire, string, error) {
	b.mu.Lock("GetCluster")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return ClusterWire{}, "", aerr
	}

	return clusterView(c, false), c.Etag, nil
}

// ClusterListIn are the ListClusters filters.
type ClusterListIn struct {
	Compartment string
	Name        string
	States      []string
	ListOpts
}

// ListClusters lists the clusters of a compartment; DELETED ones only when asked for by state.
func (b *InMemoryBackend) ListClusters(in ClusterListIn) (page.Page[ClusterWire], error) {
	if aerr := validListOpts(in.ListOpts, "ID", "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[ClusterWire]{}, aerr
	}

	b.mu.Lock("ListClusters")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.Compartment); aerr != nil {
		return page.Page[ClusterWire]{}, aerr
	}

	b.settleAll()

	var all []*Cluster

	for _, c := range b.clusters {
		if c.CompartmentID != in.Compartment || (in.Name != "" && c.Name != in.Name) {
			continue
		}

		if len(in.States) == 0 && c.State == stateDeleted || len(in.States) > 0 && !containsFold(in.States, c.State) {
			continue
		}

		all = append(all, c)
	}

	pg := paginate(all, in.ListOpts, func(c *Cluster) string { return c.ID }, func(c *Cluster) string { return c.Name },
		func(c *Cluster) (time.Time, int64) { return c.TimeCreated, c.Seq })

	out := make([]ClusterWire, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, clusterView(c, true))
	}

	return page.Page[ClusterWire]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) mutableCluster(id, ifMatch string) (*Cluster, *APIError) {
	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = checkEtag(c.Etag, ifMatch); aerr != nil {
		return nil, aerr
	}

	if c.State != stateActive && c.State != stateFailed {
		return nil, errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	return c, nil
}

// UpdateCluster changes name, version, type, options and tags; the cluster reports UPDATING meanwhile.
func (b *InMemoryBackend) UpdateCluster(id, ifMatch string, in UpdateClusterIn) (string, error) {
	b.mu.Lock("UpdateCluster")
	defer b.mu.Unlock()

	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateActive {
		return "", errState("Cluster " + id + " is in state " + c.State + "; the operation requires ACTIVE")
	}

	if aerr = b.checkClusterUpdate(c, in); aerr != nil {
		return "", aerr
	}

	if in.Name != nil {
		c.Name = *in.Name
	}

	if in.Version != nil {
		c.Version = *in.Version
	}

	if in.Type != "" {
		c.Type = in.Type
	}

	if in.Options != nil {
		applyUpdatable(&c.Options, in.Options)
	}

	if in.ImagePolicy != nil {
		c.ImagePolicy, _ = b.checkImagePolicy(in.ImagePolicy)
	}

	c.Tags.merge(in.Tags)

	return b.beginTransition(c, wrClusterUpdate, stateUpdating, actionUpdated), nil
}

func (b *InMemoryBackend) checkClusterUpdate(c *Cluster, in UpdateClusterIn) *APIError {
	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return aerr
		}
	}

	if in.Options != nil && (len(in.Options.OIDCAuth) > 0 || len(in.Options.OIDCDiscovery) > 0) {
		return errParam("OpenID Connect authentication and discovery are not supported by this emulator")
	}

	if in.ImagePolicy != nil {
		if _, aerr := b.checkImagePolicy(in.ImagePolicy); aerr != nil {
			return aerr
		}
	}

	return firstErr(checkTypeChange(c.Type, in.Type), checkUpgrade(c.Version, in.Version))
}

func checkTypeChange(cur, want string) *APIError {
	switch {
	case want != "" && want != typeBasic && want != typeEnhanced:
		return errParam("type must be " + typeBasic + " or " + typeEnhanced)
	case want == typeBasic && cur == typeEnhanced:
		return errParam("An ENHANCED_CLUSTER cannot be changed to a BASIC_CLUSTER")
	}

	return nil
}

func checkUpgrade(cur string, want *string) *APIError {
	if want != nil && *want != cur && !slices.Contains(upgradesFrom(cur), *want) {
		return errParam("kubernetesVersion " + *want + " is not an available upgrade of " + cur)
	}

	return nil
}

// beginTransition moves a settled cluster into an interim state and records its work request.
func (b *InMemoryBackend) beginTransition(c *Cluster, op, state, action string) string {
	now := b.nowUTC()
	c.State, c.TimeUpdated, c.Etag = state, &now, newEtag()

	c.TransitionAt = b.transitionAt()
	wr := b.newWR(op, c.CompartmentID, entityCluster, c.ID, action, c.TransitionAt)
	if state == stateDeleting {
		c.DeletedByWR = wr
	} else {
		c.UpdatedByWR = wr
	}

	return wr
}

// DeleteCluster deletes a cluster and its node pools; the cluster goes DELETING then DELETED.
func (b *InMemoryBackend) DeleteCluster(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteCluster")
	defer b.mu.Unlock()

	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	wr := b.beginTransition(c, wrClusterDelete, stateDeleting, actionDeleted)

	for _, np := range b.pools {
		if np.ClusterID == id && np.State != stateDeleted {
			b.markPoolDeleting(np, c.TransitionAt)
		}
	}

	b.reapLocked(id)

	return wr, nil
}

// MigrateToNativeVcn moves a legacy cluster endpoint into a VCN subnet.
func (b *InMemoryBackend) MigrateToNativeVcn(id, ifMatch string, in MigrateIn) (string, error) {
	b.mu.Lock("MigrateToNativeVcn")
	defer b.mu.Unlock()

	c, aerr := b.mutableCluster(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if aerr = b.checkMigrate(c, in); aerr != nil {
		return "", aerr
	}

	sub, _ := b.checkEndpointConfig(in.EndpointConfig, c.VcnID)
	days := maxDecommission

	if in.Decommission != nil {
		days, _ = strconv.Atoi(durationDaysRe.FindStringSubmatch(*in.Decommission)[1])
	}

	c.VcnID, c.EndpointConfig = def(c.VcnID, sub.VcnID), in.EndpointConfig
	legacy := c.Endpoints.Kubernetes

	b.assignEndpoints(c, sub)

	if b.engine != nil {
		engineEndpoints(c, legacy)
	}

	c.Endpoints.Kubernetes = legacy
	c.MigrateState = "IN_PROGRESS"

	sched := b.nowUTC().Add(b.transition + time.Duration(days)*hoursPerDay*time.Hour)
	c.MigrateScheduled = &sched

	return b.beginTransition(c, wrClusterUpdate, stateUpdating, actionUpdated), nil
}

func (b *InMemoryBackend) checkMigrate(c *Cluster, in MigrateIn) *APIError {
	if c.EndpointConfig != nil && c.EndpointConfig.SubnetID != "" {
		return errParam("Cluster " + c.ID + " already uses a VCN-native endpoint")
	}

	if in.EndpointConfig == nil {
		return errMissing("endpointConfig is required")
	}

	if _, aerr := b.checkEndpointConfig(in.EndpointConfig, c.VcnID); aerr != nil {
		return aerr
	}

	if in.Decommission != nil {
		m := durationDaysRe.FindStringSubmatch(*in.Decommission)
		if m == nil {
			return errParam("decommissionDelayDuration must be an ISO 8601 day duration such as P30D")
		}

		if n, _ := strconv.Atoi(m[1]); n > maxDecommission {
			return errParam("decommissionDelayDuration must be at most P30D")
		}
	}

	return nil
}

// MigrateStatusWire is the GetClusterMigrateToNativeVcnStatus body.
type MigrateStatusWire struct {
	TimeDecommissionScheduled *time.Time `json:"timeDecommissionScheduled,omitempty"`
	State                     string     `json:"state"`
}

// MigrateStatus reports the endpoint migration state of a cluster.
func (b *InMemoryBackend) MigrateStatus(id string) (MigrateStatusWire, error) {
	b.mu.Lock("MigrateStatus")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(id)
	if aerr != nil {
		return MigrateStatusWire{}, aerr
	}

	return MigrateStatusWire{
		State:                     def(c.MigrateState, "NOT_STARTED"),
		TimeDecommissionScheduled: c.MigrateScheduled,
	}, nil
}

func (h *Handler) createCluster(q *req) error {
	var in CreateClusterIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateCluster(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getCluster(q *req) error {
	w, etag, err := h.Backend.GetCluster(q.param("clusterId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listClusters(q *req) error {
	pg, err := h.Backend.ListClusters(ClusterListIn{
		Compartment: q.query("compartmentId"), Name: q.query("name"), States: q.queryAll("lifecycleState"),
		ListOpts: q.listOpts(),
	})

	return collectionReply(q, pg, err)
}

func (h *Handler) updateCluster(q *req) error {
	var in UpdateClusterIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateCluster(q.param("clusterId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteCluster(q *req) error {
	wr, err := h.Backend.DeleteCluster(q.param("clusterId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) migrateNativeVcn(q *req) error {
	var in MigrateIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.MigrateToNativeVcn(q.param("clusterId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) migrateStatus(q *req) error {
	w, err := h.Backend.MigrateStatus(q.param("clusterId"))

	return reply(q, w, "", err)
}

func (h *Handler) getClusterOptions(q *req) error {
	all, _ := strconv.ParseBool(q.query("shouldListAllPatchVersions"))
	w, err := h.Backend.GetClusterOptions(q.param("clusterOptionId"), all)

	return reply(q, w, "", err)
}
