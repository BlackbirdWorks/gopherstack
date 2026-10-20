package cosmosdb

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

type scriptKind string

const (
	kindSproc   scriptKind = "sprocs"
	kindTrigger scriptKind = "triggers"
	kindUDF     scriptKind = "udfs"

	triggerPre  = "Pre"
	triggerPost = "Post"

	trigOpAll     = "All"
	trigOpCreate  = "Create"
	trigOpDelete  = "Delete"
	trigOpReplace = "Replace"
	trigOpUpdate  = "Update"
)

// storedScript is a stored procedure, trigger or user-defined function.
type storedScript struct {
	Timestamp        time.Time
	ID               string
	Body             string
	TriggerType      string `json:",omitempty"`
	TriggerOperation string `json:",omitempty"`
}

// ScriptSpec is the client-supplied definition of a script.
type ScriptSpec struct {
	ID               string
	Body             string
	TriggerType      string
	TriggerOperation string
}

// ScriptInfo is a read-only snapshot of a stored script.
type ScriptInfo struct {
	Timestamp        time.Time
	ID               string
	Body             string
	TriggerType      string
	TriggerOperation string
	RID              string
	Self             string
	ETag             string
}

// canonicalTriggerOperation maps a case-insensitive triggerOperation to its canonical spelling.
func canonicalTriggerOperation(op string) (string, bool) {
	for _, canonical := range [...]string{trigOpAll, trigOpCreate, trigOpDelete, trigOpReplace, trigOpUpdate} {
		if strings.EqualFold(op, canonical) {
			return canonical, true
		}
	}

	return "", false
}

// scriptMap returns the container's map for kind, allocating it when create is set.
func (c *storedContainer) scriptMap(kind scriptKind, create bool) map[string]*storedScript {
	p := &c.Sprocs

	switch kind {
	case kindTrigger:
		p = &c.Triggers
	case kindUDF:
		p = &c.UDFs
	case kindSproc:
	}

	if *p == nil && create {
		*p = make(map[string]*storedScript)
	}

	return *p
}

// normalizeScript validates spec for kind and returns its canonical form.
func normalizeScript(kind scriptKind, spec ScriptSpec) (ScriptSpec, error) {
	if spec.ID == "" {
		return spec, fmt.Errorf("%w: id must not be empty", ErrInvalidScript)
	}

	if strings.TrimSpace(spec.Body) == "" {
		return spec, fmt.Errorf("%w: body must not be empty", ErrInvalidScript)
	}

	if err := compileScript(spec.Body); err != nil {
		return spec, err
	}

	if kind != kindTrigger {
		spec.TriggerType, spec.TriggerOperation = "", ""

		return spec, nil
	}

	switch {
	case strings.EqualFold(spec.TriggerType, triggerPre):
		spec.TriggerType = triggerPre
	case strings.EqualFold(spec.TriggerType, triggerPost):
		spec.TriggerType = triggerPost
	default:
		return spec, fmt.Errorf("%w: triggerType must be Pre or Post", ErrInvalidScript)
	}

	op, ok := canonicalTriggerOperation(spec.TriggerOperation)
	if !ok {
		return spec, fmt.Errorf("%w: triggerOperation must be All, Create, Delete, Replace or Update", ErrInvalidScript)
	}

	spec.TriggerOperation = op

	return spec, nil
}

func (b *InMemoryBackend) scriptInfo(dbID, collID string, kind scriptKind, s *storedScript) ScriptInfo {
	path := "dbs/" + dbID + "/colls/" + collID + "/" + string(kind) + "/" + s.ID

	return ScriptInfo{
		ID: s.ID, Body: s.Body, TriggerType: s.TriggerType, TriggerOperation: s.TriggerOperation,
		RID: fakeRID(path), Self: path + "/", ETag: b.etagFunc(s.Timestamp), Timestamp: s.Timestamp,
	}
}

// CreateScript stores a new script; ErrScriptAlreadyExists if the id is taken.
func (b *InMemoryBackend) CreateScript(dbID, collID string, kind scriptKind, spec ScriptSpec) (ScriptInfo, error) {
	spec, err := normalizeScript(kind, spec)
	if err != nil {
		return ScriptInfo{}, err
	}

	b.mu.Lock("CreateScript")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return ScriptInfo{}, err
	}

	m := c.scriptMap(kind, true)
	if _, exists := m[spec.ID]; exists {
		return ScriptInfo{}, ErrScriptAlreadyExists
	}

	s := &storedScript{
		ID: spec.ID, Body: spec.Body, TriggerType: spec.TriggerType, TriggerOperation: spec.TriggerOperation,
		Timestamp: b.now(),
	}
	m[spec.ID] = s

	return b.scriptInfo(dbID, collID, kind, s), nil
}

// GetScript returns one script.
func (b *InMemoryBackend) GetScript(dbID, collID string, kind scriptKind, id string) (ScriptInfo, error) {
	b.mu.RLock("GetScript")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return ScriptInfo{}, err
	}

	s, ok := c.scriptMap(kind, false)[id]
	if !ok {
		return ScriptInfo{}, ErrScriptNotFound
	}

	return b.scriptInfo(dbID, collID, kind, s), nil
}

// ListScripts returns every script of kind, sorted by id.
func (b *InMemoryBackend) ListScripts(dbID, collID string, kind scriptKind) ([]ScriptInfo, error) {
	b.mu.RLock("ListScripts")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return nil, err
	}

	m := c.scriptMap(kind, false)
	out := make([]ScriptInfo, 0, len(m))

	for _, s := range m {
		out = append(out, b.scriptInfo(dbID, collID, kind, s))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out, nil
}

// ReplaceScript overwrites an existing script's definition.
func (b *InMemoryBackend) ReplaceScript(dbID, collID string, kind scriptKind, spec ScriptSpec) (ScriptInfo, error) {
	spec, err := normalizeScript(kind, spec)
	if err != nil {
		return ScriptInfo{}, err
	}

	b.mu.Lock("ReplaceScript")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return ScriptInfo{}, err
	}

	s, ok := c.scriptMap(kind, false)[spec.ID]
	if !ok {
		return ScriptInfo{}, ErrScriptNotFound
	}

	s.Body, s.TriggerType, s.TriggerOperation = spec.Body, spec.TriggerType, spec.TriggerOperation
	s.Timestamp = bumpTimestampFrom(b.now(), s.Timestamp, true)

	return b.scriptInfo(dbID, collID, kind, s), nil
}

// DeleteScript removes a script.
func (b *InMemoryBackend) DeleteScript(dbID, collID string, kind scriptKind, id string) error {
	b.mu.Lock("DeleteScript")
	defer b.mu.Unlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return err
	}

	m := c.scriptMap(kind, false)
	if _, ok := m[id]; !ok {
		return ErrScriptNotFound
	}

	delete(m, id)

	return nil
}
