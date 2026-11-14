package ocicontainerengine

import (
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Addon is a cluster add-on installed through InstallAddon.
type Addon struct {
	TimeCreated  time.Time  `json:"timeCreated"`
	TransitionAt *time.Time `json:"transitionAt,omitempty"`
	Name         string     `json:"name"`
	Version      string     `json:"version"`
	State        string     `json:"state"`
	Etag         string     `json:"etag"`
	Configs      []KeyValue `json:"configurations,omitempty"`
}

type addonOpt struct {
	versions  map[string][]string
	name      string
	essential bool
}

func allMinors(vs ...string) map[string][]string {
	return map[string][]string{"1.34": vs, "1.35": vs, "1.36": vs}
}

// addonCatalog is the add-on table of docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/
// contengconfiguringclusteraddons-supportedversions.htm: latest image version per Kubernetes minor.
func addonCatalog() []addonOpt {
	return []addonOpt{
		{name: "kube-proxy", essential: true, versions: map[string][]string{
			"1.34": {"1.34.10"}, "1.35": {"1.35.2"}, "1.36": {"1.36.4"},
		}},
		{name: "CoreDNS", essential: true, versions: allMinors("1.12.2")},
		{name: "OCI VCN-Native Pod Networking CNI plugin", essential: true, versions: allMinors("3.3.1")},
		{name: "flannel", essential: true, versions: allMinors("0.27.3")},
		{name: "ObservabilityAgent", essential: true, versions: allMinors("0.135.0")},
		{name: "NodeProblemDetector", essential: true, versions: allMinors("0.8.20")},
		{name: "AMD GPU Operator", versions: allMinors("1.5.0")},
		{name: "Certificate Manager", versions: allMinors("1.17.1")},
		{name: "Cluster Autoscaler", versions: allMinors("1.34.3")},
		{name: "CSI Driver SMB", versions: allMinors("1.19.1")},
		{name: "Oracle Database Operator for Kubernetes", versions: allMinors("1.2.0")},
		{name: "GPU Health Monitoring", versions: allMinors("1.1.0")},
		{name: "Istio", versions: allMinors("1.29.5")},
		{name: "Kubernetes Dashboard", versions: allMinors("2.7.0")},
		{name: "Kubernetes Metrics Server", versions: allMinors("0.7.2")},
		{name: "OCI Native Ingress Controller", versions: allMinors("1.4.5")},
		{name: "Node Feature Discovery", versions: allMinors("0.17.3")},
		{name: "NVIDIA GPU Operator", versions: allMinors("25.3.3-2", "25.3.4-2", "25.10.0-1", "25.10.1-1")},
		{name: "NVIDIA GPU Plugin", versions: allMinors("0.17.0", "0.16.2", "0.15.1", "0.14.2")},
		{name: "NVIDIA Network Operator", versions: allMinors("25.4.0", "25.7.0", "25.10.0-1")},
		{name: "Tiller", versions: allMinors("2.16.0")},
		{name: "WebLogic Kubernetes Operator", versions: allMinors("4.3.10")},
	}
}

func addonKey(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}

		return -1
	}, s)
}

func findAddonOpt(name string) (addonOpt, bool) {
	k := addonKey(name)
	for _, o := range addonCatalog() {
		if addonKey(o.name) == k {
			return o, true
		}
	}

	return addonOpt{}, false
}

func minorOf(version string) string {
	v, ok := parseVersion(version)
	if !ok {
		return ""
	}

	return "1." + strconv.Itoa(v.minor)
}

// AddonWire is the Addon and AddonSummary body.
type AddonWire struct {
	TimeCreated             time.Time  `json:"timeCreated"`
	Name                    string     `json:"name"`
	LifecycleState          string     `json:"lifecycleState"`
	Version                 string     `json:"version,omitempty"`
	CurrentInstalledVersion string     `json:"currentInstalledVersion,omitempty"`
	Configurations          []KeyValue `json:"configurations,omitempty"`
}

func addonView(a *Addon, full bool) AddonWire {
	w := AddonWire{Name: a.Name, LifecycleState: a.State, Version: a.Version, TimeCreated: a.TimeCreated}
	if a.State == stateActive {
		w.CurrentInstalledVersion = a.Version
	}

	if full {
		w.Configurations = a.Configs
	}

	return w
}

// InstallAddonIn is the InstallAddon request body.
type InstallAddonIn struct {
	Version    *string    `json:"version"`
	Override   *bool      `json:"isOverrideExisting"`
	Name       string     `json:"addonName"`
	RetryToken string     `json:"-"`
	Configs    []KeyValue `json:"configurations"`
}

func checkAddonConfigs(cs []KeyValue) *APIError {
	seen := map[string]bool{}

	for _, c := range cs {
		if c.Key == "" || seen[c.Key] {
			return errParam("configurations need unique non-empty keys")
		}

		seen[c.Key] = true
	}

	return nil
}

func checkAddonVersion(o addonOpt, cluster *Cluster, version *string) (string, *APIError) {
	vs := o.versions[minorOf(cluster.Version)]
	if len(vs) == 0 {
		return "", errParam("add-on " + o.name + " is not available for Kubernetes " + cluster.Version)
	}

	if version == nil || *version == "" {
		return vs[len(vs)-1], nil
	}

	if !slices.Contains(vs, *version) {
		return "", errParam("version " + *version + " of add-on " + o.name + " is not available for Kubernetes " +
			cluster.Version + "; available: " + strings.Join(vs, ", "))
	}

	return *version, nil
}

func (b *InMemoryBackend) enhancedCluster(id, ifMatch string) (*Cluster, *APIError) {
	b.settleAll()

	c, aerr := b.activeCluster(id)
	if aerr != nil {
		return nil, aerr
	}

	if aerr = checkEtag(c.Etag, ifMatch); aerr != nil {
		return nil, aerr
	}

	if c.Type != typeEnhanced {
		return nil, errParam("Cluster add-ons can only be configured on enhanced clusters")
	}

	return c, nil
}

func findAddon(c *Cluster, name string) *Addon {
	k := addonKey(name)

	for _, a := range c.Addons {
		if addonKey(a.Name) == k && a.State != stateDeleted {
			return a
		}
	}

	return nil
}

func (b *InMemoryBackend) beginAddon(c *Cluster, op string, a *Addon, action string) string {
	a.Etag, a.TransitionAt = newEtag(), b.transitionAt()

	return b.newWR(op, c.CompartmentID, entityCluster, c.ID, action, a.TransitionAt)
}

// InstallAddon installs an add-on on an enhanced cluster; it starts CREATING.
func (b *InMemoryBackend) InstallAddon(clusterID, ifMatch string, in InstallAddonIn) (string, error) {
	b.mu.Lock("InstallAddon")
	defer b.mu.Unlock()

	if wr, hit, aerr := b.replay("InstallAddon", in.RetryToken, b.clusterAlive); hit {
		if aerr != nil {
			return "", aerr
		}

		return wr, nil
	}

	opt, ok := findAddonOpt(in.Name)
	if in.Name == "" || !ok {
		return "", errParam("addonName " + in.Name + " is not a supported add-on")
	}

	c, aerr := b.enhancedCluster(clusterID, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	version, aerr := checkAddonVersion(opt, c, in.Version)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkAddonConfigs(in.Configs); aerr != nil {
		return "", aerr
	}

	existing := findAddon(c, opt.name)
	if existing != nil && (in.Override == nil || !*in.Override) {
		return "", errExists(opt.name)
	}

	a, action := existing, actionUpdated

	if a == nil {
		a = &Addon{Name: opt.name, TimeCreated: b.nowUTC()}
		c.Addons, action = append(c.Addons, a), actionCreated
	}

	a.Version, a.Configs, a.State = version, in.Configs, stateCreating
	if existing != nil {
		a.State = stateUpdating
	}

	op := wrAddonEnable
	if existing != nil {
		op = wrAddonUpdate
	}

	wr := b.beginAddon(c, op, a, action)
	b.retry.Remember("InstallAddon", in.RetryToken, ociretry.Join(c.ID, wr), b.nowUTC())

	return wr, nil
}

func (b *InMemoryBackend) addonOf(clusterID, name string) (*Cluster, *Addon, *APIError) {
	b.settleAll()

	c, aerr := b.liveCluster(clusterID)
	if aerr != nil {
		return nil, nil, aerr
	}

	a := findAddon(c, name)
	if a == nil {
		return nil, nil, errNotFound(name)
	}

	return c, a, nil
}

// GetAddon returns one add-on and its etag.
func (b *InMemoryBackend) GetAddon(clusterID, name string) (AddonWire, string, error) {
	b.mu.Lock("GetAddon")
	defer b.mu.Unlock()

	_, a, aerr := b.addonOf(clusterID, name)
	if aerr != nil {
		return AddonWire{}, "", aerr
	}

	return addonView(a, true), a.Etag, nil
}

// ListAddons lists the add-ons installed on a cluster.
func (b *InMemoryBackend) ListAddons(clusterID string, o ListOpts) (page.Page[AddonWire], error) {
	if aerr := validListOpts(o, "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[AddonWire]{}, aerr
	}

	b.mu.Lock("ListAddons")
	defer b.mu.Unlock()

	b.settleAll()

	c, aerr := b.liveCluster(clusterID)
	if aerr != nil {
		return page.Page[AddonWire]{}, aerr
	}

	all := slices.Clone(c.Addons)
	pg := paginate(all, o, func(a *Addon) string { return a.Name }, func(a *Addon) string { return a.Name },
		func(a *Addon) (time.Time, int64) { return a.TimeCreated, 0 })

	out := make([]AddonWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		out = append(out, addonView(a, false))
	}

	return page.Page[AddonWire]{Data: out, Next: pg.Next}, nil
}

// UpdateAddonIn is the UpdateAddon request body.
type UpdateAddonIn struct {
	Version *string    `json:"version"`
	Configs []KeyValue `json:"configurations"`
}

// UpdateAddon changes the version or configuration of an installed add-on.
func (b *InMemoryBackend) UpdateAddon(clusterID, name, ifMatch string, in UpdateAddonIn) (string, error) {
	b.mu.Lock("UpdateAddon")
	defer b.mu.Unlock()

	c, a, aerr := b.addonOf(clusterID, name)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if a.State != stateActive {
		return "", errState("Add-on " + a.Name + " is in state " + a.State + "; the operation requires ACTIVE")
	}

	opt, _ := findAddonOpt(a.Name)

	version, aerr := checkAddonVersion(opt, c, orPtr(in.Version, &a.Version))
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkAddonConfigs(in.Configs); aerr != nil {
		return "", aerr
	}

	a.Version, a.Configs, a.State = version, orSlice(in.Configs, a.Configs), stateUpdating

	return b.beginAddon(c, wrAddonUpdate, a, actionUpdated), nil
}

// DisableAddon removes an add-on; isRemoveExistingAddOn only matters to the in-cluster resources.
func (b *InMemoryBackend) DisableAddon(clusterID, name, ifMatch string) (string, error) {
	b.mu.Lock("DisableAddon")
	defer b.mu.Unlock()

	c, a, aerr := b.addonOf(clusterID, name)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if a.State != stateActive {
		return "", errState("Add-on " + a.Name + " is in state " + a.State + "; the operation requires ACTIVE")
	}

	a.State = stateDeleting

	return b.beginAddon(c, wrAddonDisable, a, actionDeleted), nil
}

// AddonOptionWire is the AddonOptionSummary body.
type AddonOptionWire struct {
	Name           string             `json:"name"`
	LifecycleState string             `json:"lifecycleState"`
	Versions       []addonVersionWire `json:"versions"`
	IsEssential    bool               `json:"isEssential"`
}

type addonVersionWire struct {
	Status        string `json:"status"`
	VersionNumber string `json:"versionNumber"`
}

// ListAddonOptions lists the add-ons available for a Kubernetes version.
func (b *InMemoryBackend) ListAddonOptions(k8s, name string, o ListOpts) (page.Page[AddonOptionWire], error) {
	if aerr := validListOpts(o, "NAME", "TIME_CREATED"); aerr != nil {
		return page.Page[AddonOptionWire]{}, aerr
	}

	if k8s == "" {
		return page.Page[AddonOptionWire]{}, errMissing("kubernetesVersion is required")
	}

	minor := minorOf(k8s)
	if _, ok := parseVersion(k8s); !ok || !slices.ContainsFunc(supportedVersions, func(v string) bool {
		return minorOf(v) == minor
	}) {
		return page.Page[AddonOptionWire]{}, errParam("kubernetesVersion " + k8s + " is not supported")
	}

	var all []AddonOptionWire

	for _, opt := range addonCatalog() {
		if name != "" && addonKey(name) != addonKey(opt.name) {
			continue
		}

		w := AddonOptionWire{Name: opt.name, LifecycleState: "ACTIVE", IsEssential: opt.essential}
		for _, v := range opt.versions[minor] {
			w.Versions = append(w.Versions, addonVersionWire{Status: "ACTIVE", VersionNumber: v})
		}

		all = append(all, w)
	}

	pg := paginate(
		all,
		o,
		func(a AddonOptionWire) string { return a.Name },
		func(a AddonOptionWire) string { return a.Name },
		func(AddonOptionWire) (time.Time, int64) { return time.Time{}, 0 },
	)

	return page.Page[AddonOptionWire]{Data: pg.Data, Next: pg.Next}, nil
}

func settleAddons(c *Cluster, now time.Time) {
	kept := c.Addons[:0]

	for _, a := range c.Addons {
		if due(a.TransitionAt, now) {
			a.TransitionAt = nil

			switch a.State {
			case stateCreating, stateUpdating:
				a.State = stateActive
			case stateDeleting:
				continue
			}
		}

		kept = append(kept, a)
	}

	clear(c.Addons[len(kept):])
	c.Addons = kept
}

func (h *Handler) installAddon(q *req) error {
	var in InstallAddonIn
	if err := q.decode(&in); err != nil {
		return err
	}

	in.RetryToken = q.r.Header.Get(headerRetry)

	wr, err := h.Backend.InstallAddon(q.param("clusterId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getAddon(q *req) error {
	w, etag, err := h.Backend.GetAddon(q.param("clusterId"), q.param("addonName"))

	return reply(q, w, etag, err)
}

func (h *Handler) listAddons(q *req) error {
	pg, err := h.Backend.ListAddons(q.param("clusterId"), q.listOpts())

	return collectionReply(q, pg, err)
}

func (h *Handler) updateAddon(q *req) error {
	var in UpdateAddonIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateAddon(q.param("clusterId"), q.param("addonName"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) disableAddon(q *req) error {
	wr, err := h.Backend.DisableAddon(q.param("clusterId"), q.param("addonName"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) listAddonOptions(q *req) error {
	pg, err := h.Backend.ListAddonOptions(q.query("kubernetesVersion"), q.query("addonName"), q.listOpts())

	return collectionReply(q, pg, err)
}
