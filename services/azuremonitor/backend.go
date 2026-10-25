package azuremonitor

import (
	"encoding/base64"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// Backend holds workspaces, data collection endpoints and rules.
type Backend struct {
	now        func() time.Time
	mu         *lockmetrics.RWMutex
	workspaces map[string]*Workspace
	dces       map[string]*DCE
	dcrs       map[string]*DCR
	settings   Settings
}

// NewBackend creates an empty backend.
func NewBackend(settings Settings) *Backend {
	return &Backend{
		mu:         lockmetrics.New("azuremonitor"),
		workspaces: map[string]*Workspace{},
		dces:       map[string]*DCE{},
		dcrs:       map[string]*DCR{},
		now:        func() time.Time { return time.Now().UTC() },
		settings:   settings,
	}
}

// SetClock replaces the time source.
func (b *Backend) SetClock(now func() time.Time) { b.now = now }

// Configure replaces the settings after construction.
func (b *Backend) Configure(s Settings) { b.settings = s }

// Reset clears all state.
func (b *Backend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.workspaces = map[string]*Workspace{}
	b.dces = map[string]*DCE{}
	b.dcrs = map[string]*DCR{}
}

func newKey() string {
	buf := make([]byte, 64)
	copy(buf, []byte(uuid.NewString()+uuid.NewString()))

	return base64.StdEncoding.EncodeToString(buf)
}

func newImmutableID(prefix string) string { return awsid.NewN(prefix, 32) }

var (
	workspaceNameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9-]{2,61}[A-Za-z0-9]$`)
	tableNameRe     = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,44}_CL$`)
	columnNameRe    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,44}$`)
	ruleNameRe      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
)

const (
	minRetention      = 4
	maxRetention      = 730
	maxTotalRetention = 4383
	defaultRetention  = 30
	defaultSKU        = "PerGB2018"
	timeGenerated     = "TimeGenerated"
)

var workspaceSKUs = []string{ //nolint:gochecknoglobals // static lookup
	"Free", "Standalone", "PerNode", "PerGB2018", "Standard", "Premium", "CapacityReservation", "LACluster",
}

func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func strOf(v any) string {
	s, _ := v.(string)

	return s
}

func cloneJSON(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}

	raw, err := json.Marshal(m)
	if err != nil {
		return nil
	}

	var out map[string]any

	_ = json.Unmarshal(raw, &out)

	return out
}

func stringMap(v any) map[string]string {
	in := mapOf(v)
	out := make(map[string]string, len(in))

	for k, e := range in {
		if s, ok := e.(string); ok {
			out[k] = s
		}
	}

	return out
}

func omit(m map[string]any, keys ...string) map[string]any {
	out := cloneJSON(m)
	if out == nil {
		return map[string]any{}
	}

	for _, k := range keys {
		delete(out, k)
	}

	return out
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func (b *Backend) workspaceByRef(sub, rg, name string) (*Workspace, bool) {
	w, ok := b.workspaces[key(sub, rg, name)]

	return w, ok
}

// FindWorkspace resolves a customer ID, workspace name or ARM resource ID.
func (b *Backend) FindWorkspace(idOrName string) (*Workspace, bool) {
	b.mu.RLock("FindWorkspace")
	defer b.mu.RUnlock()

	return b.findWorkspaceLocked(idOrName)
}

func (b *Backend) findWorkspaceLocked(idOrName string) (*Workspace, bool) {
	needle := strings.ToLower(strings.Trim(idOrName, "/"))

	var byName *Workspace

	for _, w := range b.workspaces {
		switch {
		case strings.EqualFold(w.CustomerID, needle):
			return w, true
		case strings.EqualFold(strings.Trim(w.armID(), "/"), needle):
			return w, true
		case strings.EqualFold(w.Name, needle):
			if byName != nil {
				return nil, false
			}

			byName = w
		}
	}

	return byName, byName != nil
}

func (w *Workspace) table(name string) (*Table, bool) {
	if t, ok := w.Tables[name]; ok {
		return t, true
	}

	return nil, false
}
