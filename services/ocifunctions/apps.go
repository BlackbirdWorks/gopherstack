package ocifunctions

import (
	"maps"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const maxConfigBytes = 4096

//nolint:gochecknoglobals // immutable
var (
	validShapes  = []string{"GENERIC_X86", "GENERIC_ARM", "GENERIC_X86_ARM"}
	syslogScheme = []string{"tcp", "udp", "https"}
)

type appIn struct {
	Tags
	Config      map[string]string `json:"config"`
	TraceConfig *TraceConfig      `json:"traceConfig"`
	Logging     *LoggingConfig    `json:"logging"`
	ImagePolicy *ImagePolicy      `json:"imagePolicyConfig"`
	SyslogURL   *string           `json:"syslogUrl"`
	Compartment string            `json:"compartmentId"`
	Name        string            `json:"displayName"`
	Shape       string            `json:"shape"`
	SubnetIDs   []string          `json:"subnetIds"`
	NSGIDs      []string          `json:"networkSecurityGroupIds"`
}

type appWire struct {
	TimeCreated        time.Time                 `json:"timeCreated"`
	TimeUpdated        time.Time                 `json:"timeUpdated"`
	TraceConfig        *TraceConfig              `json:"traceConfig,omitempty"`
	Logging            *LoggingConfig            `json:"logging,omitempty"`
	ImagePolicy        *ImagePolicy              `json:"imagePolicyConfig,omitempty"`
	Config             map[string]string         `json:"config,omitempty"`
	FreeformTags       map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags        map[string]map[string]any `json:"definedTags,omitempty"`
	SecurityAttributes map[string]map[string]any `json:"securityAttributes,omitempty"`
	ID                 string                    `json:"id"`
	CompartmentID      string                    `json:"compartmentId"`
	Name               string                    `json:"displayName"`
	LifecycleState     string                    `json:"lifecycleState"`
	Shape              string                    `json:"shape"`
	SyslogURL          *string                   `json:"syslogUrl,omitempty"`
	SubnetIDs          []string                  `json:"subnetIds"`
	NSGIDs             []string                  `json:"networkSecurityGroupIds,omitempty"`
}

func appView(a *Application, summary bool) appWire {
	w := appWire{
		ID: a.ID, CompartmentID: a.CompartmentID, Name: a.Name, LifecycleState: a.State, Shape: a.Shape,
		SubnetIDs: a.SubnetIDs, NSGIDs: a.NSGIDs, TraceConfig: a.TraceConfig, Logging: a.Logging,
		ImagePolicy: a.ImagePolicy, TimeCreated: a.TimeCreated, TimeUpdated: a.TimeUpdated,
		FreeformTags: a.Tags.FreeformTags, DefinedTags: a.Tags.DefinedTags,
		SecurityAttributes: a.Tags.SecurityAttributes,
	}

	if !summary {
		w.Config = a.Config
		if a.SyslogURL != "" {
			w.SyslogURL = &a.SyslogURL
		}
	}

	return w
}

func validateConfig(cfg map[string]string) *APIError {
	size := 0
	for k, v := range cfg {
		size += len(k) + len(v)
	}

	if size > maxConfigBytes {
		return errParam("config must be at most 4096 bytes in total")
	}

	return nil
}

func validateSyslog(raw string) *APIError {
	u, err := url.Parse(raw)
	if raw == "" || err != nil || u.Host == "" || !slices.Contains(syslogScheme, u.Scheme) {
		return errParam("syslogUrl must be a tcp://, udp:// or https:// URL with a host")
	}

	return nil
}

func validateShape(shape string) *APIError {
	if !slices.Contains(validShapes, shape) {
		return errParam("shape must be one of " + strings.Join(validShapes, ", "))
	}

	return nil
}

func validateImagePolicy(p *ImagePolicy) *APIError {
	if p == nil {
		return nil
	}

	if p.IsPolicyEnabled == nil {
		return errMissing("imagePolicyConfig.isPolicyEnabled is required")
	}

	for _, k := range p.KeyDetails {
		if k.KmsKeyID == "" {
			return errMissing("imagePolicyConfig.keyDetails.kmsKeyId is required")
		}
	}

	return nil
}

func validateAppFields(in *appIn) *APIError {
	for _, aerr := range []*APIError{
		validateConfig(in.Config), validateImagePolicy(in.ImagePolicy),
	} {
		if aerr != nil {
			return aerr
		}
	}

	if in.SyslogURL != nil && *in.SyslogURL != "" {
		return validateSyslog(*in.SyslogURL)
	}

	return nil
}

func (b *InMemoryBackend) liveApp(id string) (*Application, *APIError) {
	a, ok := b.apps[id]
	if !ok || a.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return a, nil
}

func (b *InMemoryBackend) activeApp(id string) (*Application, *APIError) {
	a, aerr := b.liveApp(id)
	if aerr != nil {
		return nil, aerr
	}

	if a.State != stateActive {
		return nil, errState("Application " + id + " is in state " + a.State + "; the operation requires ACTIVE")
	}

	return a, nil
}

func (b *InMemoryBackend) appNameTaken(compartment, name, except string) bool {
	for _, a := range b.apps {
		if a.ID != except && a.State != stateDeleted && a.CompartmentID == compartment && strs.Equal(a.Name, name) {
			return true
		}
	}

	return false
}

// CreateApplication creates an application.
func (b *InMemoryBackend) CreateApplication(in appIn) (appWire, string, string, error) {
	if in.Compartment == "" {
		return appWire{}, "", "", errMissing("compartmentId is required")
	}

	if aerr := validateName(in.Name); aerr != nil {
		return appWire{}, "", "", aerr
	}

	if len(in.SubnetIDs) == 0 {
		return appWire{}, "", "", errMissing("subnetIds is required")
	}

	if in.Shape == "" {
		in.Shape = validShapes[0]
	}

	if aerr := validateShape(in.Shape); aerr != nil {
		return appWire{}, "", "", aerr
	}

	if aerr := validateAppFields(&in); aerr != nil {
		return appWire{}, "", "", aerr
	}

	defer b.flushEvents()

	b.mu.Lock("CreateApplication")
	defer b.mu.Unlock()

	if b.appNameTaken(in.Compartment, in.Name, "") {
		return appWire{}, "", "", errExists("An application named " + in.Name + " already exists in the compartment")
	}

	now := b.nowUTC()
	a := &Application{
		ID: newOCID("fnapp"), CompartmentID: in.Compartment, Name: in.Name, State: stateCreating, Shape: in.Shape,
		SubnetIDs: slices.Clone(in.SubnetIDs), NSGIDs: slices.Clone(in.NSGIDs), Config: maps.Clone(in.Config),
		TraceConfig: in.TraceConfig, Logging: in.Logging, ImagePolicy: in.ImagePolicy, Tags: in.Tags,
		TimeCreated: now, TimeUpdated: now, TransitionAt: b.transitionAt(), Etag: newEtag(), Seq: b.nextSeq(),
	}
	if in.SyslogURL != nil {
		a.SyslogURL = *in.SyslogURL
	}

	b.apps[a.ID] = a
	b.recordEvent(EventCreateApplication, a.CompartmentID, a.Name, a.ID)
	wr := b.newWR("CREATE_APPLICATION", a.CompartmentID, "application", a.ID, "CREATED",
		"/applications/"+a.ID, *a.TransitionAt)

	return appView(a, false), a.Etag, wr, nil
}

// GetApplication returns one application.
func (b *InMemoryBackend) GetApplication(id string) (appWire, string, error) {
	b.mu.RLock("GetApplication")
	defer b.mu.RUnlock()

	a, ok := b.apps[id]
	if !ok || a.State == stateDeleted {
		return appWire{}, "", errNotFound(id)
	}

	return appView(a, false), a.Etag, nil
}

// ListApplications lists the applications of a compartment.
func (b *InMemoryBackend) ListApplications(
	compartment, id, name, state string,
	o ListOpts,
) (page.Page[appWire], error) {
	if compartment == "" {
		return page.Page[appWire]{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListApplications")
	defer b.mu.RUnlock()

	var all []*Application

	for _, a := range b.apps {
		if a.CompartmentID == compartment && a.State != stateDeleted && (id == "" || a.ID == id) &&
			(name == "" || a.Name == name) && (state == "" || strings.EqualFold(a.State, state)) {
			all = append(all, a)
		}
	}

	pg := paginate(all, o, func(a *Application) string { return a.Name }, func(a *Application) string { return a.ID },
		func(a *Application) (time.Time, int64) { return a.TimeCreated, a.Seq })

	out := make([]appWire, 0, len(pg.Data))
	for _, a := range pg.Data {
		out = append(out, appView(a, true))
	}

	return page.Page[appWire]{Data: out, Next: pg.Next}, nil
}

// UpdateApplication (202, no body) changes the mutable fields of an application.
func (b *InMemoryBackend) UpdateApplication(id, ifMatch string, in appIn) (string, error) {
	if aerr := validateAppFields(&in); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("UpdateApplication")
	defer b.mu.Unlock()

	a, aerr := b.activeApp(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.Config != nil {
		a.Config = maps.Clone(in.Config)
	}

	if in.NSGIDs != nil {
		a.NSGIDs = slices.Clone(in.NSGIDs)
	}

	if in.SyslogURL != nil {
		a.SyslogURL = *in.SyslogURL
	}

	a.TraceConfig = cmpPtr(in.TraceConfig, a.TraceConfig)
	a.Logging = cmpPtr(in.Logging, a.Logging)
	a.ImagePolicy = cmpPtr(in.ImagePolicy, a.ImagePolicy)
	a.Tags.replace(in.Tags)
	a.TimeUpdated, a.Etag = b.nowUTC(), newEtag()

	return b.newWR("UPDATE_APPLICATION", a.CompartmentID, "application", a.ID, "UPDATED",
		"/applications/"+a.ID, a.TimeUpdated), nil
}

func cmpPtr[T any](in, cur *T) *T {
	if in != nil {
		return in
	}

	return cur
}

// DeleteApplication moves an empty application to DELETING.
func (b *InMemoryBackend) DeleteApplication(id, ifMatch string) (string, error) {
	defer b.flushEvents()

	b.mu.Lock("DeleteApplication")
	defer b.mu.Unlock()

	a, aerr := b.activeApp(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	for _, f := range b.funcs {
		if f.AppID == id && f.State != stateDeleted {
			return "", errState("Application " + id + " still contains functions; delete them first")
		}
	}

	a.State, a.TransitionAt, a.Etag = stateDeleting, b.transitionAt(), newEtag()
	b.recordEvent(EventDeleteApplication, a.CompartmentID, a.Name, a.ID)

	return b.newWR("DELETE_APPLICATION", a.CompartmentID, "application", a.ID, "DELETED",
		"/applications/"+a.ID, *a.TransitionAt), nil
}

// ChangeApplicationCompartment moves an application and its functions to another compartment.
func (b *InMemoryBackend) ChangeApplicationCompartment(id, ifMatch, compartment string) (string, error) {
	if compartment == "" {
		return "", errMissing("compartmentId is required")
	}

	b.mu.Lock("ChangeApplicationCompartment")
	defer b.mu.Unlock()

	a, aerr := b.activeApp(id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(a.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if b.appNameTaken(compartment, a.Name, a.ID) {
		return "", errExists("An application named " + a.Name + " already exists in the target compartment")
	}

	a.CompartmentID, a.Etag, a.TimeUpdated = compartment, newEtag(), b.nowUTC()

	for _, f := range b.funcs {
		if f.AppID == id {
			f.CompartmentID = compartment
		}
	}

	return b.newWR("UPDATE_APPLICATION", compartment, "application", a.ID, "UPDATED",
		"/applications/"+a.ID, a.TimeUpdated), nil
}

func (h *Handler) createApplication(q *req) error {
	var in appIn
	if err := q.decode(&in); err != nil {
		return err
	}

	v, etag, wr, err := h.Backend.CreateApplication(in)

	return replyWR(q, v, etag, wr, err)
}

func (h *Handler) getApplication(q *req) error {
	v, etag, err := h.Backend.GetApplication(q.param("applicationId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listApplications(q *req) error {
	pg, err := h.Backend.ListApplications(q.query("compartmentId"), q.query("id"), q.query("displayName"),
		q.query("lifecycleState"), q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateApplication(q *req) error {
	var in appIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateApplication(q.param("applicationId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteApplication(q *req) error {
	wr, err := h.Backend.DeleteApplication(q.param("applicationId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

type changeCompartmentIn struct {
	CompartmentID string `json:"compartmentId"`
}

func (h *Handler) changeApplicationCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeApplicationCompartment(q.param("applicationId"), q.ifMatch(), in.CompartmentID)

	return acceptedWR(q, wr, err)
}
