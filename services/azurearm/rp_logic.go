package azurearm

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	namespaceMicrosoftLogic = "Microsoft.Logic"
	logicFieldID            = "id"
	logicDataPrefix         = "/_logic/workflows/"
	logicWorkflowsType      = "workflows"
	logicStateEnabled       = "Enabled"
	logicStateDisabled      = "Disabled"
	logicMaxRuns            = 1000
	logicMaxHistories       = 1000
	logicMaxVersions        = 100
	logicDefaultTop         = 30
	logicMaxTop             = 250
	logicNameMax            = 43
	logicKeyBytes           = 32
	logicIDBytes            = 16
	logicVersionDigits      = 20
)

func logicAPIVersions() []string { return []string{"2019-05-01", "2018-07-01-preview", "2016-06-01"} }

var logicNameRE = regexp.MustCompile(`^[A-Za-z0-9_.()-]+$`)

// Run, action and trigger statuses.
const (
	logicStSucceeded = "Succeeded"
	logicStFailed    = "Failed"
	logicStSkipped   = "Skipped"
	logicStTimedOut  = "TimedOut"
	logicStRunning   = "Running"
	logicStCancelled = "Cancelled"
	logicStWaiting   = "Waiting"
)

type logicVersion struct {
	Created    time.Time      `json:"created"`
	Definition map[string]any `json:"definition"`
	Parameters map[string]any `json:"parameters,omitempty"`
	Version    string         `json:"version"`
}

type logicTriggerState struct {
	Anchor  time.Time  `json:"anchor"`
	Created time.Time  `json:"created"`
	Changed time.Time  `json:"changed"`
	Last    *time.Time `json:"last,omitempty"`
	Next    *time.Time `json:"next,omitempty"`
	State   string     `json:"state"`
}

type logicContent struct {
	Value any  `json:"value,omitempty"`
	Set   bool `json:"set,omitempty"`
}

type logicRunTrigger struct {
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	Scheduled time.Time    `json:"scheduled"`
	Error     any          `json:"error,omitempty"`
	Name      string       `json:"name"`
	Status    string       `json:"status"`
	Code      string       `json:"code,omitempty"`
	Tracking  string       `json:"tracking,omitempty"`
	Inputs    logicContent `json:"inputs"`
	Outputs   logicContent `json:"outputs"`
}

type logicActionRec struct {
	Start    time.Time    `json:"start"`
	End      time.Time    `json:"end"`
	Error    any          `json:"error,omitempty"`
	Name     string       `json:"name"`
	Type     string       `json:"type"`
	Status   string       `json:"status"`
	Code     string       `json:"code,omitempty"`
	Tracking string       `json:"tracking,omitempty"`
	Inputs   logicContent `json:"inputs"`
	Outputs  logicContent `json:"outputs"`
}

type logicRunRec struct {
	Start       time.Time                  `json:"start"`
	End         time.Time                  `json:"end"`
	Error       any                        `json:"error,omitempty"`
	Outputs     map[string]any             `json:"outputs,omitempty"`
	Actions     map[string]*logicActionRec `json:"actions"`
	Trigger     logicRunTrigger            `json:"trigger"`
	Name        string                     `json:"name"`
	Status      string                     `json:"status"`
	Code        string                     `json:"code,omitempty"`
	Version     string                     `json:"version"`
	Correlation string                     `json:"correlation"`
	ClientID    string                     `json:"clientTrackingId,omitempty"`
	Order       []string                   `json:"order"`
}

type logicHistory struct {
	Start     time.Time    `json:"start"`
	End       time.Time    `json:"end"`
	Scheduled time.Time    `json:"scheduled"`
	Error     any          `json:"error,omitempty"`
	Name      string       `json:"name"`
	Trigger   string       `json:"trigger"`
	Status    string       `json:"status"`
	Code      string       `json:"code,omitempty"`
	Run       string       `json:"run,omitempty"`
	Tracking  string       `json:"tracking"`
	Inputs    logicContent `json:"inputs"`
	Outputs   logicContent `json:"outputs"`
	Fired     bool         `json:"fired"`
}

type logicWorkflow struct {
	Created      time.Time                     `json:"created"`
	Changed      time.Time                     `json:"changed"`
	Tags         map[string]string             `json:"tags,omitempty"`
	Identity     map[string]any                `json:"identity,omitempty"`
	Definition   map[string]any                `json:"definition"`
	Parameters   map[string]any                `json:"parameters,omitempty"`
	Extra        map[string]any                `json:"extra,omitempty"`
	Triggers     map[string]*logicTriggerState `json:"triggers"`
	Name         string                        `json:"name"`
	Subscription string                        `json:"subscription"`
	ResourceGrp  string                        `json:"resourceGroup"`
	Location     string                        `json:"location"`
	FlowID       string                        `json:"flowId"`
	State        string                        `json:"state"`
	Version      string                        `json:"version"`
	KeyPrimary   string                        `json:"keyPrimary"`
	KeySecondary string                        `json:"keySecondary"`
	Versions     []*logicVersion               `json:"versions"`
	Runs         []*logicRunRec                `json:"runs"`
	Histories    []*logicHistory               `json:"histories"`
}

// LogicProvider implements ResourceProvider for Microsoft.Logic Consumption workflows: ARM CRUD, run history and
// the workflow engine that executes definitions.
type LogicProvider struct {
	clock    gcpscale.Clock
	http     *http.Client
	mu       *lockmetrics.RWMutex
	wf       map[string]*logicWorkflow
	live     map[string]*logicLive
	timers   map[string]gcpscale.Timer
	rootCtx  context.Context //nolint:containedctx // lifetime of every run goroutine
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	seq      atomic.Int64
	external atomic.Bool
}

// NewLogicProvider creates a LogicProvider on the wall clock.
func NewLogicProvider() *LogicProvider {
	ctx, cancel := context.WithCancel(context.Background())
	p := &LogicProvider{
		clock: gcpscale.RealClock{}, mu: lockmetrics.New("azurearm.logicprovider"),
		wf: map[string]*logicWorkflow{}, live: map[string]*logicLive{}, timers: map[string]gcpscale.Timer{},
		rootCtx: ctx, cancel: cancel,
	}
	p.http = p.newHTTPClient()

	return p
}

var _ ResourceProvider = (*LogicProvider)(nil)

// SetClock replaces the clock that drives Wait, Until, retries and Recurrence triggers.
func (p *LogicProvider) SetClock(c gcpscale.Clock) {
	p.mu.Lock("SetClock")
	p.clock = c
	p.mu.Unlock()
}

// SetAllowExternal lets the HTTP action reach hosts outside loopback.
func (p *LogicProvider) SetAllowExternal(allow bool) { p.external.Store(allow) }

// WaitRuns blocks until every started run has finished; runs parked on a clock timer finish once the clock advances.
func (p *LogicProvider) WaitRuns() { p.wg.Wait() }

// Close cancels every run and recurrence timer.
func (p *LogicProvider) Close() {
	p.cancel()
	p.stopAllTimers()
	p.wg.Wait()
}

// Namespace implements ResourceProvider.
func (p *LogicProvider) Namespace() string { return namespaceMicrosoftLogic }

// ResourceTypes implements ResourceProvider.
func (p *LogicProvider) ResourceTypes() []ResourceTypeDef {
	v := logicAPIVersions()

	return []ResourceTypeDef{
		{Type: logicWorkflowsType, APIVersions: v, HasChildren: true},
		{Type: logicWorkflowsType + "/triggers", APIVersions: v},
		{Type: logicWorkflowsType + "/triggers/histories", APIVersions: v},
		{Type: logicWorkflowsType + "/runs", APIVersions: v},
		{Type: logicWorkflowsType + "/runs/actions", APIVersions: v},
		{Type: logicWorkflowsType + "/versions", APIVersions: v},
	}
}

func logicKey(sub, rg, name string) string {
	return strings.ToLower(ResourceID{
		SubscriptionID: sub, ResourceGroup: rg, Namespace: namespaceMicrosoftLogic,
		Types: []string{logicWorkflowsType}, Names: []string{name},
	}.ARMID())
}

func (w *logicWorkflow) armID() string {
	return ResourceID{
		SubscriptionID: w.Subscription, ResourceGroup: w.ResourceGrp, Namespace: namespaceMicrosoftLogic,
		Types: []string{logicWorkflowsType}, Names: []string{w.Name},
	}.ARMID()
}

func (w *logicWorkflow) key() string { return strings.ToLower(w.armID()) }

func randomLogicToken() string {
	b := make([]byte, logicKeyBytes)
	_, _ = rand.Read(b)

	return base64.RawURLEncoding.EncodeToString(b)
}

func randomLogicHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

// newVersionID returns a 20-digit decimal identifier, ordered by creation time.
func (p *LogicProvider) newVersionID(now time.Time) string {
	return fmt.Sprintf(
		"%0*d",
		logicVersionDigits,
		now.UnixNano()/100%1e17*10+p.seq.Add(1)%10,
	)
}

// newRunName returns an identifier that sorts newest-first like the service's reverse-tick names.
func (p *LogicProvider) newRunName(now time.Time) string {
	const maxTicks = int64(1) << 62

	return fmt.Sprintf("%019dCU%d", maxTicks-now.UnixNano(), p.seq.Add(1))
}

// Put implements ResourceProvider.
func (p *LogicProvider) Put(_ context.Context, id ResourceID, body map[string]any) (map[string]any, error) {
	out, _, err := p.putWorkflow(id, body, false, "")
	if err != nil {
		return nil, err
	}

	return out, nil
}

// Get implements ResourceProvider.
func (p *LogicProvider) Get(ctx context.Context, id ResourceID) (map[string]any, error) {
	p.mu.RLock("Get")
	defer p.mu.RUnlock()

	w, err := p.workflowLocked(id)
	if err != nil {
		return nil, err
	}

	return p.workflowBody(w, RequestHostFromContext(ctx)), nil
}

func (p *LogicProvider) workflowLocked(id ResourceID) (*logicWorkflow, error) {
	if len(id.Names) == 0 || !strings.EqualFold(id.Types[0], logicWorkflowsType) {
		return nil, fmt.Errorf(
			"%w: unsupported Microsoft.Logic resource type %q",
			ErrResourceNotFound,
			id.ResourceType(),
		)
	}

	w, ok := p.wf[logicKey(id.SubscriptionID, id.ResourceGroup, id.Names[0])]
	if !ok {
		return nil, ErrLogicWorkflowNotFound
	}

	return w, nil
}

// Delete implements ResourceProvider.
func (p *LogicProvider) Delete(_ context.Context, id ResourceID) error {
	p.mu.Lock("Delete")
	w, err := p.workflowLocked(id)

	if err != nil {
		p.mu.Unlock()

		return err
	}

	live := p.detachLocked(w)
	p.mu.Unlock()

	live.cancelAll()

	return nil
}

// detachLocked removes w and returns its live runs for cancellation; caller holds p.mu.
func (p *LogicProvider) detachLocked(w *logicWorkflow) logicLiveSet {
	key := w.key()
	delete(p.wf, key)

	var set logicLiveSet

	for rk, l := range p.live {
		if strings.HasPrefix(rk, key+"|") {
			set = append(set, l)
			delete(p.live, rk)
		}
	}

	for tk, t := range p.timers {
		if strings.HasPrefix(tk, key+"|") {
			t.Stop()
			delete(p.timers, tk)
		}
	}

	return set
}

// List implements ResourceProvider.
func (p *LogicProvider) List(ctx context.Context, id ResourceID) ([]map[string]any, error) {
	p.mu.RLock("List")
	defer p.mu.RUnlock()

	all := p.sortedLocked(id.SubscriptionID, id.ResourceGroup)
	out := make([]map[string]any, 0, len(all))

	for _, w := range all {
		out = append(out, p.workflowBody(w, RequestHostFromContext(ctx)))
	}

	return out, nil
}

func (p *LogicProvider) sortedLocked(sub, rg string) []*logicWorkflow {
	var all []*logicWorkflow

	for _, w := range p.wf {
		if sub != "" && !strings.EqualFold(w.Subscription, sub) {
			continue
		}

		if rg != "" && !resourceGroupsEqual(w.ResourceGrp, rg) {
			continue
		}

		all = append(all, w)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].key() < all[j].key() })

	return all
}

// ListKeys implements ResourceProvider; Logic workflows have no listKeys action.
func (p *LogicProvider) ListKeys(_ context.Context, id ResourceID) (map[string]any, error) {
	return nil, fmt.Errorf("%w: listKeys is not supported on %q", ErrResourceNotFound, id.ResourceType())
}

// Reset implements ResourceProvider.
func (p *LogicProvider) Reset() {
	p.mu.Lock("Reset")
	p.wf = map[string]*logicWorkflow{}
	live := p.live
	p.live = map[string]*logicLive{}
	p.mu.Unlock()

	p.stopAllTimers()

	for _, l := range live {
		l.cancel()
	}
}

// DeleteResourcesInGroup implements ResourceProvider.
func (p *LogicProvider) DeleteResourcesInGroup(_ context.Context, resourceGroup string) {
	p.mu.Lock("DeleteResourcesInGroup")

	var set logicLiveSet

	for _, w := range p.wf {
		if resourceGroupsEqual(w.ResourceGrp, resourceGroup) {
			set = append(set, p.detachLocked(w)...)
		}
	}

	p.mu.Unlock()
	set.cancelAll()
}

func (p *LogicProvider) stopAllTimers() {
	p.mu.Lock("stopAllTimers")
	defer p.mu.Unlock()

	for k, t := range p.timers {
		t.Stop()
		delete(p.timers, k)
	}
}

func (p *LogicProvider) newHTTPClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = httpclient.LoopbackDialer(p.external.Load, ErrLogicDestinationNotPermitted).DialContext

	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func cloneStringMap(m map[string]string) map[string]string { return maps.Clone(m) }
