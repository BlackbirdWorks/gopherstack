package ocifunctions

import (
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	minMemoryMB    = 128
	maxMemoryMB    = 3072
	minTimeoutSec  = 5
	maxTimeoutSec  = 300
	defaultTimeout = 30
	maxDetachedSec = 3600
	sourceImage    = "CONTAINER_IMAGE"
	concNone       = "NONE"
	concConstant   = "CONSTANT"
)

type sourceIn struct {
	SourceType  string `json:"sourceType"`
	Image       string `json:"image"`
	ImageDigest string `json:"imageDigest"`
}

type fnIn struct {
	Tags
	Config      map[string]string `json:"config"`
	Memory      *int64            `json:"memoryInMBs"`
	Timeout     *int              `json:"timeoutInSeconds"`
	Detached    *int              `json:"detachedModeTimeoutInSeconds"`
	Provisioned *Concurrency      `json:"provisionedConcurrencyConfig"`
	Trace       *TraceConfig      `json:"traceConfig"`
	Source      *sourceIn         `json:"sourceDetails"`
	Name        string            `json:"displayName"`
	AppID       string            `json:"applicationId"`
	Failure     json.RawMessage   `json:"failureDestination"`
	Success     json.RawMessage   `json:"successDestination"`
}

type fnWire struct {
	TimeUpdated    time.Time                 `json:"timeUpdated"`
	TimeCreated    time.Time                 `json:"timeCreated"`
	TraceConfig    *TraceConfig              `json:"traceConfig,omitempty"`
	Detached       *int                      `json:"detachedModeTimeoutInSeconds,omitempty"`
	Config         map[string]string         `json:"config,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	Source         sourceIn                  `json:"sourceDetails"`
	Provisioned    Concurrency               `json:"provisionedConcurrencyConfig"`
	ID             string                    `json:"id"`
	AppID          string                    `json:"applicationId"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"displayName"`
	LifecycleState string                    `json:"lifecycleState"`
	Shape          string                    `json:"shape,omitempty"`
	InvokeEndpoint string                    `json:"invokeEndpoint"`
	Success        json.RawMessage           `json:"successDestination,omitempty"`
	Failure        json.RawMessage           `json:"failureDestination,omitempty"`
	Memory         int64                     `json:"memoryInMBs"`
	Timeout        int                       `json:"timeoutInSeconds"`
}

func (b *InMemoryBackend) fnView(f *Function, endpoint string, summary bool) fnWire {
	w := fnWire{
		ID: f.ID, AppID: f.AppID, CompartmentID: f.CompartmentID, Name: f.Name, LifecycleState: f.State,
		Source: sourceIn{SourceType: sourceImage, Image: f.Image, ImageDigest: f.ImageDigest},
		Memory: f.Memory, Timeout: f.Timeout, Detached: f.Detached, Provisioned: f.Provisioned,
		Failure: f.Failure, Success: f.Success, TraceConfig: f.TraceConfig, InvokeEndpoint: endpoint,
		TimeCreated: f.TimeCreated, TimeUpdated: f.TimeUpdated,
		FreeformTags: f.Tags.FreeformTags, DefinedTags: f.Tags.DefinedTags,
	}
	if a, ok := b.apps[f.AppID]; ok {
		w.Shape = a.Shape
	}

	if !summary {
		w.Config = f.Config
	}

	return w
}

func validateSource(s *sourceIn) *APIError {
	switch {
	case s == nil:
		return errMissing("sourceDetails is required")
	case s.SourceType != sourceImage:
		return errParam("sourceDetails.sourceType " + s.SourceType + " is not supported; use CONTAINER_IMAGE")
	case strings.TrimSpace(s.Image) == "":
		return errMissing("sourceDetails.image is required")
	case s.ImageDigest != "" && !strings.HasPrefix(s.ImageDigest, "sha256:"):
		return errParam("sourceDetails.imageDigest must be a sha256:<hex> digest")
	}

	return nil
}

func validateLimits(in *fnIn) *APIError {
	if in.Memory != nil && (*in.Memory < minMemoryMB || *in.Memory > maxMemoryMB) {
		return errParam("memoryInMBs must be between 128 and 3072")
	}

	if in.Timeout != nil && (*in.Timeout < minTimeoutSec || *in.Timeout > maxTimeoutSec) {
		return errParam("timeoutInSeconds must be between 5 and 300")
	}

	if in.Detached != nil && (*in.Detached < minTimeoutSec || *in.Detached > maxDetachedSec) {
		return errParam("detachedModeTimeoutInSeconds must be between 5 and 3600")
	}

	if aerr := validateConcurrency(in.Provisioned); aerr != nil {
		return aerr
	}

	return validateConfig(in.Config)
}

func validateConcurrency(p *Concurrency) *APIError {
	switch {
	case p == nil:
		return nil
	case p.Strategy != concNone && p.Strategy != concConstant:
		return errParam("provisionedConcurrencyConfig.strategy must be NONE or CONSTANT")
	case p.Strategy == concConstant && (p.Count == nil || *p.Count < 0):
		return errParam("provisionedConcurrencyConfig.count is required for CONSTANT and must not be negative")
	}

	return nil
}

func (b *InMemoryBackend) fnNameTaken(appID, name, except string) bool {
	for _, f := range b.funcs {
		if f.ID != except && f.State != stateDeleted && f.AppID == appID && strs.Equal(f.Name, name) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) activeFn(id string) (*Function, *APIError) {
	f, ok := b.funcs[id]
	if !ok || f.State == stateDeleted {
		return nil, errNotFound(id)
	}

	if f.State != stateActive {
		return nil, errState("Function " + id + " is in state " + f.State + "; the operation requires ACTIVE")
	}

	return f, nil
}

// CreateFunction creates a function in an application.
func (b *InMemoryBackend) CreateFunction(in fnIn, endpoint string) (fnWire, string, string, error) {
	if aerr := validateName(in.Name); aerr != nil {
		return fnWire{}, "", "", aerr
	}

	if in.AppID == "" {
		return fnWire{}, "", "", errMissing("applicationId is required")
	}

	if in.Memory == nil {
		return fnWire{}, "", "", errMissing("memoryInMBs is required")
	}

	if aerr := validateSource(in.Source); aerr != nil {
		return fnWire{}, "", "", aerr
	}

	if aerr := validateLimits(&in); aerr != nil {
		return fnWire{}, "", "", aerr
	}

	defer b.flushEvents()

	b.mu.Lock("CreateFunction")
	defer b.mu.Unlock()

	app, aerr := b.activeApp(in.AppID)
	if aerr != nil {
		return fnWire{}, "", "", aerr
	}

	if b.fnNameTaken(app.ID, in.Name, "") {
		return fnWire{}, "", "", errExists("A function named " + in.Name + " already exists in the application")
	}

	now := b.nowUTC()
	f := &Function{
		ID: newOCID("fnfunc"), AppID: app.ID, CompartmentID: app.CompartmentID, Name: in.Name, State: stateCreating,
		Image: in.Source.Image, ImageDigest: in.Source.ImageDigest, Memory: *in.Memory, Timeout: defaultTimeout,
		Detached: in.Detached, Config: maps.Clone(in.Config), TraceConfig: in.Trace, Failure: in.Failure,
		Success: in.Success, Provisioned: Concurrency{Strategy: concNone}, Tags: in.Tags,
		TimeCreated: now, TimeUpdated: now, TransitionAt: b.transitionAt(), Etag: newEtag(), Seq: b.nextSeq(),
	}
	if in.Timeout != nil {
		f.Timeout = *in.Timeout
	}

	if in.Provisioned != nil {
		f.Provisioned = *in.Provisioned
	}

	b.funcs[f.ID] = f
	b.recordEvent(EventCreateFunction, f.CompartmentID, f.Name, f.ID)
	wr := b.newWR("CREATE_FUNCTION", f.CompartmentID, "function", f.ID, "CREATED", "/functions/"+f.ID, *f.TransitionAt)

	return b.fnView(f, endpoint, false), f.Etag, wr, nil
}

// GetFunction returns one function.
func (b *InMemoryBackend) GetFunction(id, endpoint string) (fnWire, string, error) {
	b.mu.RLock("GetFunction")
	defer b.mu.RUnlock()

	f, ok := b.funcs[id]
	if !ok || f.State == stateDeleted {
		return fnWire{}, "", errNotFound(id)
	}

	return b.fnView(f, endpoint, false), f.Etag, nil
}

// ListFunctions lists the functions of an application.
func (b *InMemoryBackend) ListFunctions(
	appID, id, name, state, endpoint string,
	o ListOpts,
) (page.Page[fnWire], error) {
	if appID == "" {
		return page.Page[fnWire]{}, errMissing("applicationId is required")
	}

	b.mu.RLock("ListFunctions")
	defer b.mu.RUnlock()

	if _, aerr := b.liveApp(appID); aerr != nil {
		return page.Page[fnWire]{}, aerr
	}

	var all []*Function

	for _, f := range b.funcs {
		if f.AppID == appID && f.State != stateDeleted && (id == "" || f.ID == id) &&
			(name == "" || f.Name == name) && (state == "" || strings.EqualFold(f.State, state)) {
			all = append(all, f)
		}
	}

	pg := paginate(all, o, func(f *Function) string { return f.Name }, func(f *Function) string { return f.ID },
		func(f *Function) (time.Time, int64) { return f.TimeCreated, f.Seq })

	out := make([]fnWire, 0, len(pg.Data))
	for _, f := range pg.Data {
		out = append(out, b.fnView(f, endpoint, true))
	}

	return page.Page[fnWire]{Data: out, Next: pg.Next}, nil
}

// UpdateFunction (202, no body) changes the mutable fields of a function; a new image replaces the warm containers.
func (b *InMemoryBackend) UpdateFunction(id, ifMatch string, in fnIn) (string, error) {
	if aerr := validateLimits(&in); aerr != nil {
		return "", aerr
	}

	if in.Source != nil {
		if aerr := validateSource(in.Source); aerr != nil {
			return "", aerr
		}
	}

	b.mu.Lock("UpdateFunction")

	f, aerr := b.activeFn(id)
	if aerr == nil {
		aerr = checkEtag(f.Etag, ifMatch)
	}

	if aerr != nil {
		b.mu.Unlock()

		return "", aerr
	}

	applyFnUpdate(f, in)
	f.TimeUpdated, f.Etag = b.nowUTC(), newEtag()
	wr := b.newWR("UPDATE_FUNCTION", f.CompartmentID, "function", f.ID, "UPDATED", "/functions/"+f.ID, f.TimeUpdated)
	b.mu.Unlock()

	b.exec.dropFunction(id)

	return wr, nil
}

func applyFnUpdate(f *Function, in fnIn) {
	if in.Memory != nil {
		f.Memory = *in.Memory
	}

	if in.Timeout != nil {
		f.Timeout = *in.Timeout
	}

	if in.Detached != nil {
		f.Detached = in.Detached
	}

	if in.Provisioned != nil {
		f.Provisioned = *in.Provisioned
	}

	if in.Config != nil {
		f.Config = maps.Clone(in.Config)
	}

	if in.Source != nil {
		f.Image, f.ImageDigest = in.Source.Image, in.Source.ImageDigest
	}

	if in.Failure != nil {
		f.Failure = in.Failure
	}

	if in.Success != nil {
		f.Success = in.Success
	}

	f.TraceConfig = cmpPtr(in.Trace, f.TraceConfig)
	f.Tags.replace(in.Tags)
}

// DeleteFunction moves a function to DELETING and removes its warm containers.
func (b *InMemoryBackend) DeleteFunction(id, ifMatch string) (string, error) {
	defer b.flushEvents()

	b.mu.Lock("DeleteFunction")

	f, aerr := b.activeFn(id)
	if aerr == nil {
		aerr = checkEtag(f.Etag, ifMatch)
	}

	if aerr != nil {
		b.mu.Unlock()

		return "", aerr
	}

	f.State, f.TransitionAt, f.Etag = stateDeleting, b.transitionAt(), newEtag()
	wr := b.newWR("DELETE_FUNCTION", f.CompartmentID, "function", f.ID, "DELETED", "/functions/"+f.ID, *f.TransitionAt)
	b.recordEvent(EventDeleteFunction, f.CompartmentID, f.Name, f.ID)
	b.mu.Unlock()

	b.exec.dropFunction(id)

	return wr, nil
}

func (h *Handler) createFunction(q *req) error {
	var in fnIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, wr, err := h.Backend.CreateFunction(in, q.endpoint())

	return replyWR(q, v, etag, wr, err)
}

func (h *Handler) getFunction(q *req) error {
	v, etag, err := h.Backend.GetFunction(q.param("functionId"), q.endpoint())

	return reply(q, v, etag, err)
}

func (h *Handler) listFunctions(q *req) error {
	pg, err := h.Backend.ListFunctions(q.query("applicationId"), q.query("id"), q.query("displayName"),
		q.query("lifecycleState"), q.endpoint(), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateFunction(q *req) error {
	var in fnIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateFunction(q.param("functionId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteFunction(q *req) error {
	wr, err := h.Backend.DeleteFunction(q.param("functionId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}
