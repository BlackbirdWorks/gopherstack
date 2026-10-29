package azurearm

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	flexOpDuration   = 150 * time.Millisecond
	flexMaxOps       = 1024
	flexStateReady   = "Ready"
	flexStateStopped = "Stopped"
	flexCodeInvalid  = "InvalidParameterValue"
	flexCodeNotReady = "ServerIsNotReady"
	flexCodeBusy     = "SeverBusyWithOtherOperation"
	flexCodeConflict = "Conflict"
	flexOpFailedMsg  = "The database engine could not complete the operation."
)

type flexKind int

const (
	flexKindUnsupported flexKind = iota
	flexKindServer
	flexKindFirewall
	flexKindDatabase
	flexKindConfig
)

type flexFirewall struct {
	Name  string `json:"name"`
	Start string `json:"start"`
	End   string `json:"end"`
}

type flexDatabase struct {
	Name      string `json:"name"`
	Charset   string `json:"charset"`
	Collation string `json:"collation"`
}

// flexServer is the ARM-side state of one flexible server. The engine, its port and its data live behind the
// FlexEngine seam; the administrator password is never stored.
type flexServer struct {
	CreatedAt   time.Time               `json:"createdAt"`
	PointInTime time.Time               `json:"pointInTime,omitzero"`
	Tags        map[string]string       `json:"tags,omitempty"`
	Identity    map[string]any          `json:"identity,omitempty"`
	Maintenance map[string]any          `json:"maintenance,omitempty"`
	Encryption  map[string]any          `json:"encryption,omitempty"`
	AuthConfig  map[string]any          `json:"authConfig,omitempty"`
	Firewall    map[string]flexFirewall `json:"firewall,omitempty"`
	Databases   map[string]flexDatabase `json:"databases,omitempty"`
	Config      map[string]string       `json:"config,omitempty"`
	Pending     map[string]bool         `json:"pendingRestart,omitempty"`
	Name        string                  `json:"name"`
	ResourceGrp string                  `json:"resourceGroup"`
	Location    string                  `json:"location"`
	SKUName     string                  `json:"skuName"`
	SKUTier     string                  `json:"skuTier"`
	Version     string                  `json:"version"`
	Login       string                  `json:"login"`
	State       string                  `json:"state"`
	Transition  string                  `json:"transition,omitempty"`
	OpID        string                  `json:"-"`
	Zone        string                  `json:"zone,omitempty"`
	StandbyZone string                  `json:"standbyZone,omitempty"`
	HAMode      string                  `json:"haMode"`
	AutoGrow    string                  `json:"autoGrow"`
	GeoBackup   string                  `json:"geoBackup"`
	PublicAcc   string                  `json:"publicAccess"`
	Subnet      string                  `json:"subnet,omitempty"`
	DNSZone     string                  `json:"dnsZone,omitempty"`
	CreateMode  string                  `json:"createMode,omitempty"`
	SourceID    string                  `json:"sourceId,omitempty"`
	StorageGB   int                     `json:"storageGb"`
	Iops        int                     `json:"iops,omitempty"`
	Retention   int                     `json:"retentionDays"`
}

type flexOp struct {
	Started time.Time
	ReadyAt time.Time
	Target  string
	ErrMsg  string
	Returns bool
	Pending bool
}

// flexResult is the outcome of a write: the response body and the async operation to point the headers at.
type flexResult struct {
	body map[string]any
	opID string
}

// FlexDBProvider implements ResourceProvider for Microsoft.DBforPostgreSQL and Microsoft.DBforMySQL flexible
// servers (one instance per namespace). ARM keeps the server metadata and child resources; with an engine
// wired, real database servers run behind the FlexEngine seam.
type FlexDBProvider struct {
	now     func() time.Time
	flavor  *flexFlavor
	engine  FlexEngine
	network *NetworkProvider
	mu      *lockmetrics.RWMutex
	opMu    *lockmetrics.RWMutex
	res     map[string]*flexServer
	ops     map[string]*flexOp
	ctx     context.Context
	cancel  context.CancelFunc
	opOrder []string
	wg      sync.WaitGroup
}

func newFlexDBProvider(f *flexFlavor) *FlexDBProvider {
	ctx, cancel := context.WithCancel(context.Background())

	return &FlexDBProvider{
		now: time.Now, flavor: f, mu: lockmetrics.New("azurearm.flexdb." + f.kind),
		opMu: lockmetrics.New("azurearm.flexdb.ops." + f.kind), res: map[string]*flexServer{},
		ops: map[string]*flexOp{}, ctx: ctx, cancel: cancel,
	}
}

// NewPostgresFlexProvider creates the Microsoft.DBforPostgreSQL provider.
func NewPostgresFlexProvider() *FlexDBProvider { return newFlexDBProvider(newPostgresFlavor()) }

// NewMySQLFlexProvider creates the Microsoft.DBforMySQL provider.
func NewMySQLFlexProvider() *FlexDBProvider { return newFlexDBProvider(newMySQLFlavor()) }

var _ ResourceProvider = (*FlexDBProvider)(nil)

// SetClock replaces the provider's clock.
func (p *FlexDBProvider) SetClock(now func() time.Time) {
	p.mu.Lock("SetClock")
	p.now = now
	p.mu.Unlock()
}

// SetEngine wires the database engine; nil keeps servers ARM-only.
func (p *FlexDBProvider) SetEngine(e FlexEngine) {
	p.mu.Lock("SetEngine")
	p.engine = e
	p.mu.Unlock()
}

// SetNetwork lets the provider validate delegated subnets against Microsoft.Network.
func (p *FlexDBProvider) SetNetwork(n *NetworkProvider) {
	p.mu.Lock("SetNetwork")
	p.network = n
	p.mu.Unlock()
}

// Namespace implements ResourceProvider.
func (p *FlexDBProvider) Namespace() string { return p.flavor.ns }

// ResourceTypes implements ResourceProvider.
func (p *FlexDBProvider) ResourceTypes() []ResourceTypeDef {
	v := p.flavor.apiVersions

	return []ResourceTypeDef{
		{Type: flexServersType, APIVersions: v, HasChildren: true},
		{Type: flexServersType + "/" + flexFirewallType, APIVersions: v},
		{Type: flexServersType + "/" + flexDatabasesType, APIVersions: v},
		{Type: flexServersType + "/" + flexConfigsType, APIVersions: v},
		{Type: flexServersType + "/" + flexReplicasType, APIVersions: v},
		{Type: "checkNameAvailability", APIVersions: v},
		{Type: "locations/capabilities", APIVersions: v},
	}
}

func classifyFlexPath(id ResourceID) flexKind {
	switch strings.ToLower(strings.Join(id.Types, "/")) {
	case strings.ToLower(flexServersType):
		return flexKindServer
	case strings.ToLower(flexServersType + "/" + flexFirewallType):
		return flexKindFirewall
	case strings.ToLower(flexServersType + "/" + flexDatabasesType):
		return flexKindDatabase
	case strings.ToLower(flexServersType + "/" + flexConfigsType):
		return flexKindConfig
	}

	return flexKindUnsupported
}

func flexServerID(ns string, id ResourceID) ResourceID {
	return ResourceID{
		SubscriptionID: id.SubscriptionID, ResourceGroup: id.ResourceGroup, Namespace: ns,
		Types: []string{flexServersType}, Names: id.Names[:1],
	}
}

func (p *FlexDBProvider) key(id ResourceID) string {
	return strings.ToLower(flexServerID(p.flavor.ns, id).ARMID())
}

func (p *FlexDBProvider) recID(sub string, rec *flexServer) ResourceID {
	return ResourceID{
		SubscriptionID: sub, ResourceGroup: rec.ResourceGrp, Namespace: p.flavor.ns,
		Types: []string{flexServersType}, Names: []string{rec.Name},
	}
}

// flexError is a refusal with an ARM error code; the message is returned to the caller verbatim.
type flexError struct {
	code   string
	msg    string
	status int
}

func (e *flexError) Error() string { return e.code + ": " + e.msg }

func flexInvalid(format string, args ...any) error {
	return &flexError{flexCodeInvalid, fmt.Sprintf(format, args...), http.StatusBadRequest}
}

func flexConflict(code, msg string) error { return &flexError{code, msg, http.StatusConflict} }

func (p *FlexDBProvider) notFound(id ResourceID, what string) error {
	return &flexError{
		armErrorCodeResourceNotFound,
		fmt.Sprintf("The Resource '%s/%s/%s' under resource group '%s' was not found.",
			p.flavor.ns, flexServersType, id.Names[0], id.ResourceGroup) + what,
		http.StatusNotFound,
	}
}

func (p *FlexDBProvider) childNotFound(id ResourceID) error {
	return &flexError{
		armErrorCodeResourceNotFound,
		fmt.Sprintf(
			"The Resource '%s/%s/%s/%s' under resource group '%s' was not found.",
			p.flavor.ns,
			flexServersType,
			strings.Join(id.Names, "/"),
			strings.Join(id.Types[1:], "/"),
			id.ResourceGroup,
		),
		http.StatusNotFound,
	}
}

func flexErrorDetails(err error) (errorEntry, bool) {
	if fe, ok := errors.AsType[*flexError](err); ok {
		return errorEntry{fe.code, fe.msg, fe.status}, true
	}

	return errorEntry{}, false
}

func errUnsupportedFlexType(id ResourceID) error {
	return fmt.Errorf("%w: unsupported %s resource type %q", ErrResourceNotFound, id.Namespace, id.ResourceType())
}

func (p *FlexDBProvider) server(id ResourceID) (*flexServer, error) {
	rec, ok := p.res[p.key(id)]
	if !ok {
		return nil, p.notFound(id, "")
	}

	return rec, nil
}

// Put implements ResourceProvider.
func (p *FlexDBProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	r, err := p.Write(id, http.MethodPut, body)

	return r.body, err
}

// Write applies a PUT or PATCH and returns the body and the async operation it started.
func (p *FlexDBProvider) Write(id ResourceID, method string, body map[string]any) (flexResult, error) {
	patch := method == http.MethodPatch

	switch classifyFlexPath(id) {
	case flexKindServer:
		return p.writeServer(id, body, patch)
	case flexKindFirewall:
		return p.writeFirewall(id, body)
	case flexKindDatabase:
		return p.writeDatabase(id, body)
	case flexKindConfig:
		return p.writeConfig(id, body)
	case flexKindUnsupported:
	}

	return flexResult{}, errUnsupportedFlexType(id)
}

// Get implements ResourceProvider.
func (p *FlexDBProvider) Get(_ context.Context, id ResourceID) (map[string]any, error) {
	kind := classifyFlexPath(id)
	if kind == flexKindUnsupported {
		return nil, errUnsupportedFlexType(id)
	}

	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	rec, err := p.server(id)
	if err != nil {
		return nil, err
	}

	switch kind {
	case flexKindFirewall:
		fw, ok := rec.Firewall[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, p.childNotFound(id)
		}

		return p.firewallBody(id, fw), nil
	case flexKindDatabase:
		db, ok := rec.Databases[strings.ToLower(id.LeafName())]
		if !ok {
			return nil, p.childNotFound(id)
		}

		return p.databaseBody(id, db), nil
	case flexKindConfig:
		return p.configBody(id, rec)
	case flexKindServer, flexKindUnsupported:
	}

	return p.serverBody(id, rec), nil
}

// Delete implements ResourceProvider.
func (p *FlexDBProvider) Delete(_ context.Context, id ResourceID) error {
	_, err := p.Remove(id)

	return err
}

// Remove deletes a resource and returns the async operation it started.
func (p *FlexDBProvider) Remove(id ResourceID) (string, error) {
	switch classifyFlexPath(id) {
	case flexKindServer:
		return p.deleteServer(id)
	case flexKindFirewall:
		return p.deleteFirewall(id)
	case flexKindDatabase:
		return p.deleteDatabase(id)
	case flexKindConfig:
		return "", flexInvalid("Server parameters cannot be deleted; set source to system-default to reset one.")
	case flexKindUnsupported:
	}

	return "", errUnsupportedFlexType(id)
}

// List implements ResourceProvider; only servers are listable here, children use ListChildren.
func (p *FlexDBProvider) List(_ context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	out := []map[string]any{}

	if classifyFlexPath(id) == flexKindServer {
		for _, rec := range p.res {
			if id.ResourceGroup != "" && !resourceGroupsEqual(rec.ResourceGrp, id.ResourceGroup) {
				continue
			}

			out = append(out, p.serverBody(p.recID(id.SubscriptionID, rec), rec))
		}
	}

	slices.SortFunc(out, func(a, b map[string]any) int { return strings.Compare(nameField(a), nameField(b)) })

	return out, nil
}

// ListKeys implements ResourceProvider; flexible servers have no keys.
func (p *FlexDBProvider) ListKeys(context.Context, ResourceID) (map[string]any, error) {
	return nil, ErrResourceNotFound
}

// Reset implements ResourceProvider.
func (p *FlexDBProvider) Reset() {
	p.mu.Lock("Reset")
	keys := slices.Collect(maps.Keys(p.res))
	p.res = map[string]*flexServer{}
	eng := p.engine
	p.mu.Unlock()

	p.opMu.Lock("Reset")
	p.ops, p.opOrder = map[string]*flexOp{}, nil
	p.opMu.Unlock()

	p.dropEngines(eng, keys)
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *FlexDBProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")

	var keys []string

	for key, rec := range p.res {
		if resourceGroupsEqual(rec.ResourceGrp, resourceGroup) {
			keys = append(keys, key)
			delete(p.res, key)
		}
	}

	eng := p.engine
	p.mu.Unlock()

	p.dropEngines(eng, keys)
}

func (p *FlexDBProvider) dropEngines(eng FlexEngine, keys []string) {
	if eng == nil {
		return
	}

	for _, k := range keys {
		_ = eng.Delete(p.ctx, k)
	}
}

// Close stops background work and the engine's servers.
func (p *FlexDBProvider) Close() {
	p.cancel()
	p.wg.Wait()

	p.mu.RLock("Close")
	eng := p.engine
	p.mu.RUnlock()

	if c, ok := eng.(interface{ Close() }); ok {
		c.Close()
	}
}

// newOp registers an operation. With run set and an engine wired it completes when run returns; otherwise it
// completes after flexOpDuration of clock time. A failed run marks the target server Disabled.
func (p *FlexDBProvider) newOp(target string, returns bool, key string, run func(context.Context) error) string {
	opID := newOperationID()
	now := p.now()
	op := &flexOp{Started: now, ReadyAt: now.Add(flexOpDuration), Target: target, Returns: returns}

	if run != nil {
		op.Pending = true
	}

	p.opMu.Lock("newOp")
	p.ops[opID] = op

	if p.opOrder = append(p.opOrder, opID); len(p.opOrder) > flexMaxOps {
		delete(p.ops, p.opOrder[0])
		p.opOrder = p.opOrder[1:]
	}

	p.opMu.Unlock()

	if run != nil {
		p.wg.Go(func() { p.finishOp(opID, key, run(p.ctx)) })
	}

	return opID
}

func (p *FlexDBProvider) finishOp(opID, key string, err error) {
	if err != nil && p.ctx.Err() == nil {
		p.mu.Lock("finishOp")

		if rec, ok := p.res[key]; ok {
			rec.State = "Disabled"
		}

		p.mu.Unlock()
	}

	p.opMu.Lock("finishOp")
	defer p.opMu.Unlock()

	if op, ok := p.ops[opID]; ok {
		op.Pending = false
		op.ReadyAt = p.now()

		if err != nil {
			op.ErrMsg = flexOpFailedMsg
		}
	}
}

// OperationStatus reports whether opID is known, finished, and its failure message.
func (p *FlexDBProvider) OperationStatus(opID string) (flexOp, bool, bool) {
	p.opMu.RLock("OperationStatus")
	defer p.opMu.RUnlock()

	o, ok := p.ops[opID]
	if !ok {
		return flexOp{}, false, false
	}

	return *o, true, !o.Pending && !p.now().Before(o.ReadyAt)
}

func (p *FlexDBProvider) opPending(opID string) bool {
	if opID == "" {
		return false
	}

	_, known, done := p.OperationStatus(opID)

	return known && !done
}

func (p *FlexDBProvider) stateOf(rec *flexServer) string {
	if rec.Transition != "" && p.opPending(rec.OpID) {
		return rec.Transition
	}

	return rec.State
}

func cloneFlexServer(r *flexServer) *flexServer {
	c := *r
	c.Tags, c.Identity, c.Maintenance = maps.Clone(r.Tags), maps.Clone(r.Identity), maps.Clone(r.Maintenance)
	c.Encryption, c.AuthConfig = maps.Clone(r.Encryption), maps.Clone(r.AuthConfig)
	c.Firewall, c.Databases = maps.Clone(r.Firewall), maps.Clone(r.Databases)
	c.Config, c.Pending = maps.Clone(r.Config), maps.Clone(r.Pending)

	return &c
}
