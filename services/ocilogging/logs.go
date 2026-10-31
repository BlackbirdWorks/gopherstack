package ocilogging

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultRetentionDays = 30
	sourceTypeOCIService = "OCISERVICE"
)

type SourceWire struct {
	Parameters map[string]string `json:"parameters,omitempty"`
	SourceType string            `json:"sourceType"`
	Service    string            `json:"service"`
	Resource   string            `json:"resource"`
	Category   string            `json:"category"`
}

type archivingWire struct {
	IsEnabled *bool `json:"isEnabled,omitempty"`
}

// ConfigWire is the log configuration as sent and returned over the wire.
type ConfigWire struct {
	Source        *SourceWire    `json:"source,omitempty"`
	Archiving     *archivingWire `json:"archiving,omitempty"`
	CompartmentID string         `json:"compartmentId,omitempty"`
}

// CreateLogIn is the CreateLog request body.
type CreateLogIn struct {
	Tags
	Enabled       *bool       `json:"isEnabled"`
	Retention     *int        `json:"retentionDuration"`
	Configuration *ConfigWire `json:"configuration"`
	Name          string      `json:"displayName"`
	LogType       string      `json:"logType"`
}

// UpdateLogIn is the UpdateLog request body.
type UpdateLogIn struct {
	Tags
	Name          *string     `json:"displayName"`
	Enabled       *bool       `json:"isEnabled"`
	Retention     *int        `json:"retentionDuration"`
	Configuration *ConfigWire `json:"configuration"`
}

// ChangeLogGroupIn is the ChangeLogLogGroup request body.
type ChangeLogGroupIn struct {
	TargetLogGroupID string `json:"targetLogGroupId"`
}

// LogWire is the GetLog response and the ListLogs item.
type LogWire struct {
	TimeCreated    time.Time                 `json:"timeCreated"`
	TimeModified   time.Time                 `json:"timeLastModified"`
	Configuration  *ConfigWire               `json:"configuration,omitempty"`
	FreeformTags   map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags    map[string]map[string]any `json:"definedTags,omitempty"`
	ID             string                    `json:"id"`
	LogGroupID     string                    `json:"logGroupId"`
	CompartmentID  string                    `json:"compartmentId"`
	Name           string                    `json:"displayName"`
	LogType        string                    `json:"logType"`
	LifecycleState string                    `json:"lifecycleState"`
	Retention      int                       `json:"retentionDuration"`
	Enabled        bool                      `json:"isEnabled"`
}

func configView(c *LogConfig) *ConfigWire {
	if c == nil {
		return nil
	}

	w := &ConfigWire{CompartmentID: c.CompartmentID}
	if c.Archiving != nil {
		w.Archiving = &archivingWire{IsEnabled: c.Archiving}
	}

	if c.Source != nil {
		w.Source = &SourceWire{
			SourceType: sourceTypeOCIService, Service: c.Source.Service, Resource: c.Source.Resource,
			Category: c.Source.Category, Parameters: c.Source.Parameters,
		}
	}

	return w
}

func (b *InMemoryBackend) logView(l *Log) LogWire {
	comp := ""
	if g, ok := b.groups[l.GroupID]; ok {
		comp = g.CompartmentID
	}

	return LogWire{
		ID: l.ID, LogGroupID: l.GroupID, CompartmentID: comp, Name: l.Name, LogType: l.LogType,
		LifecycleState: l.State, Retention: l.Retention, Enabled: l.Enabled, Configuration: configView(l.Config),
		TimeCreated: l.TimeCreated, TimeModified: l.TimeModified, FreeformTags: l.Tags.FreeformTags,
		DefinedTags: l.Tags.DefinedTags,
	}
}

func (b *InMemoryBackend) liveLog(groupID, id string) (*Log, *APIError) {
	l, ok := b.logs[id]
	if !ok || (groupID != "" && l.GroupID != groupID) {
		return nil, errNotFound(id)
	}

	return l, nil
}

func (b *InMemoryBackend) mutableLog(groupID, id string) (*Log, *APIError) {
	l, aerr := b.liveLog(groupID, id)
	if aerr != nil {
		return nil, aerr
	}

	if l.State == stateCreating || l.State == stateUpdating || l.State == stateDeleting {
		return nil, errState("Log " + id + " is in state " + l.State + "; the operation requires ACTIVE or INACTIVE")
	}

	return l, nil
}

func (b *InMemoryBackend) logNameTaken(groupID, name, except string) bool {
	for _, l := range b.logs {
		if l.ID != except && l.GroupID == groupID && l.Name == name {
			return true
		}
	}

	return false
}

func validRetention(v *int) *APIError {
	if v == nil {
		return nil
	}

	if *v < retentionStep || *v > maxRetentionDays || *v%retentionStep != 0 {
		return errParam("retentionDuration must be 30, 60, 90, 120, 150 or 180 days")
	}

	return nil
}

// buildConfig validates a configuration for the given log type and converts it to its stored form.
func buildConfig(logType string, w *ConfigWire) (*LogConfig, *APIError) {
	if w == nil {
		if logType == logTypeService {
			return nil, errMissing("configuration is required for a SERVICE log")
		}

		return nil, nil
	}

	c := &LogConfig{CompartmentID: w.CompartmentID}
	if w.Archiving != nil {
		c.Archiving = w.Archiving.IsEnabled
	}

	if logType == logTypeCustom {
		if w.Source != nil {
			return nil, errParam("configuration.source is only valid for a SERVICE log")
		}

		return c, nil
	}

	s := w.Source
	if s == nil || s.Service == "" || s.Resource == "" || s.Category == "" {
		return nil, errMissing("configuration.source with service, resource and category is required for a SERVICE log")
	}

	if s.SourceType != "" && !strs.Equal(s.SourceType, sourceTypeOCIService) {
		return nil, errParam("configuration.source.sourceType must be OCISERVICE")
	}

	if aerr := findService(s.Service, s.Category); aerr != nil {
		return nil, aerr
	}

	c.Source = &ServiceSource{Service: s.Service, Resource: s.Resource, Category: s.Category, Parameters: s.Parameters}

	return c, nil
}

func (b *InMemoryBackend) serviceLogTaken(src *ServiceSource, except string) bool {
	for _, l := range b.logs {
		if l.ID != except && l.Config != nil && l.Config.Source != nil && sameSource(l.Config.Source, src) &&
			l.State != stateDeleting {
			return true
		}
	}

	return false
}

func normalizeType(t string) (string, *APIError) {
	switch {
	case t == "":
		return "", errMissing("logType is required")
	case strs.Equal(t, logTypeCustom):
		return logTypeCustom, nil
	case strs.Equal(t, logTypeService):
		return logTypeService, nil
	}

	return "", errParam("logType must be CUSTOM or SERVICE")
}

// CreateLog creates a log inside a log group.
func (b *InMemoryBackend) CreateLog(groupID string, in CreateLogIn) (string, error) {
	logType, aerr := normalizeType(in.LogType)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(validateName(in.Name), validRetention(in.Retention)); aerr != nil {
		return "", aerr
	}

	cfg, aerr := buildConfig(logType, in.Configuration)
	if aerr != nil {
		return "", aerr
	}

	b.mu.Lock("CreateLog")
	defer b.mu.Unlock()

	b.settleAll()

	g, aerr := b.mutableGroup(groupID)
	if aerr != nil {
		return "", aerr
	}

	if b.logNameTaken(groupID, in.Name, "") {
		return "", errConflict("A log named " + in.Name + " already exists in the log group")
	}

	if cfg != nil && cfg.Source != nil && b.serviceLogTaken(cfg.Source, "") {
		return "", errConflict("A service log for this service, resource and category already exists")
	}

	now := b.nowUTC()
	l := &Log{
		ID: newOCID("log"), GroupID: g.ID, Name: in.Name, LogType: logType, State: stateCreating, Config: cfg,
		Enabled: in.Enabled == nil || *in.Enabled, Retention: defaultRetentionDays, TimeCreated: now,
		TimeModified: now, TransitionAt: b.transitionAt(), Tags: in.Tags, Etag: newEtag(), Seq: b.nextSeq(),
	}
	if in.Retention != nil {
		l.Retention = *in.Retention
	}

	b.logs[l.ID] = l

	return b.newWR("CREATE_LOG", "log", g.CompartmentID, l.ID, "CREATED", *l.TransitionAt), nil
}

// GetLog returns one log and its etag.
func (b *InMemoryBackend) GetLog(groupID, id string) (LogWire, string, error) {
	b.mu.Lock("GetLog")
	defer b.mu.Unlock()

	b.settleAll()

	l, aerr := b.liveLog(groupID, id)
	if aerr != nil {
		return LogWire{}, "", aerr
	}

	return b.logView(l), l.Etag, nil
}

// LogFilter narrows ListLogs.
type LogFilter struct {
	LogType, Name, State, Service, Resource string
}

func (f LogFilter) matches(l *Log) bool {
	if (f.Name != "" && l.Name != f.Name) || (f.LogType != "" && !strs.Equal(l.LogType, f.LogType)) ||
		(f.State != "" && !strs.Equal(l.State, f.State)) {
		return false
	}

	if f.Service == "" && f.Resource == "" {
		return true
	}

	if l.Config == nil || l.Config.Source == nil {
		return false
	}

	src := l.Config.Source

	return (f.Service == "" || src.Service == f.Service) && (f.Resource == "" || src.Resource == f.Resource)
}

// ListLogs lists the logs of a log group.
func (b *InMemoryBackend) ListLogs(groupID string, f LogFilter, o ListOpts) (page.Page[LogWire], error) {
	b.mu.Lock("ListLogs")
	defer b.mu.Unlock()

	b.settleAll()

	if _, ok := b.groups[groupID]; !ok {
		return page.Page[LogWire]{}, errNotFound(groupID)
	}

	var all []*Log

	for _, l := range b.logs {
		if l.GroupID == groupID && f.matches(l) {
			all = append(all, l)
		}
	}

	pg := paginate(all, o, func(l *Log) string { return l.Name },
		func(l *Log) (time.Time, int64) { return l.TimeCreated, l.Seq })

	out := make([]LogWire, 0, len(pg.Data))
	for _, l := range pg.Data {
		out = append(out, b.logView(l))
	}

	return page.Page[LogWire]{Data: out, Next: pg.Next}, nil
}

// UpdateLog changes the mutable fields of a log; it passes through UPDATING.
func (b *InMemoryBackend) UpdateLog(groupID, id, ifMatch string, in UpdateLogIn) (string, error) {
	if in.Name != nil {
		if aerr := validateName(*in.Name); aerr != nil {
			return "", aerr
		}
	}

	if aerr := validRetention(in.Retention); aerr != nil {
		return "", aerr
	}

	b.mu.Lock("UpdateLog")
	defer b.mu.Unlock()

	b.settleAll()

	l, aerr := b.mutableLog(groupID, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(l.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	if in.Name != nil {
		if b.logNameTaken(l.GroupID, *in.Name, l.ID) {
			return "", errConflict("A log named " + *in.Name + " already exists in the log group")
		}

		l.Name = *in.Name
	}

	if aerr = applyConfigUpdate(l, in.Configuration); aerr != nil {
		return "", aerr
	}

	if in.Enabled != nil {
		l.Enabled = *in.Enabled
	}

	if in.Retention != nil {
		l.Retention = *in.Retention
	}

	l.Tags.merge(in.Tags)
	l.State, l.TransitionAt, l.TimeModified, l.Etag = stateUpdating, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("UPDATE_LOG", "log", b.groups[l.GroupID].CompartmentID, l.ID, "UPDATED", *l.TransitionAt), nil
}

// applyConfigUpdate applies archiving and source parameters; a service log keeps its service, resource and category.
func applyConfigUpdate(l *Log, w *ConfigWire) *APIError {
	if w == nil {
		return nil
	}

	if l.LogType == logTypeCustom && w.Source != nil {
		return errParam("configuration.source is only valid for a SERVICE log")
	}

	if l.Config == nil {
		l.Config = &LogConfig{}
	}

	if w.Archiving != nil {
		l.Config.Archiving = w.Archiving.IsEnabled
	}

	if w.Source != nil && l.Config.Source != nil {
		l.Config.Source.Parameters = w.Source.Parameters
	}

	return nil
}

// DeleteLog moves a log to DELETING; its entries are dropped when the transition completes.
func (b *InMemoryBackend) DeleteLog(groupID, id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteLog")
	defer b.mu.Unlock()

	b.settleAll()

	l, aerr := b.mutableLog(groupID, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(l.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	l.State, l.TransitionAt, l.TimeModified, l.Etag = stateDeleting, b.transitionAt(), b.nowUTC(), newEtag()

	return b.newWR("DELETE_LOG", "log", b.groups[l.GroupID].CompartmentID, l.ID, "DELETED", *l.TransitionAt), nil
}

// ChangeLogLogGroup moves a log to another log group of the same compartment.
func (b *InMemoryBackend) ChangeLogLogGroup(groupID, id, ifMatch, target string) (string, error) {
	if target == "" {
		return "", errMissing("targetLogGroupId is required")
	}

	b.mu.Lock("ChangeLogLogGroup")
	defer b.mu.Unlock()

	b.settleAll()

	l, aerr := b.mutableLog(groupID, id)
	if aerr != nil {
		return "", aerr
	}

	if aerr = checkEtag(l.Etag, ifMatch); aerr != nil {
		return "", aerr
	}

	tg, aerr := b.mutableGroup(target)
	if aerr != nil {
		return "", aerr
	}

	if tg.CompartmentID != b.groups[l.GroupID].CompartmentID {
		return "", errParam("the target log group must be in the same compartment as the log")
	}

	if b.logNameTaken(target, l.Name, l.ID) {
		return "", errConflict("A log named " + l.Name + " already exists in the target log group")
	}

	l.GroupID, l.Etag, l.TimeModified = target, newEtag(), b.nowUTC()

	return b.newWR("MOVE_LOG", "log", tg.CompartmentID, l.ID, "UPDATED", b.nowUTC()), nil
}

func (h *Handler) createLog(q *req) error {
	var in CreateLogIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateLog(q.param("logGroupId"), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getLog(q *req) error {
	v, etag, err := h.Backend.GetLog(q.param("logGroupId"), q.param("logId"))

	return reply(q, v, etag, err)
}

func (h *Handler) listLogs(q *req) error {
	pg, err := h.Backend.ListLogs(q.param("logGroupId"), LogFilter{
		LogType: q.query("logType"), Name: q.query("displayName"), State: q.query("lifecycleState"),
		Service: q.query("sourceService"), Resource: q.query("sourceResource"),
	}, q.listOpts())

	return listReply(q, pg, err)
}

func (h *Handler) updateLog(q *req) error {
	var in UpdateLogIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateLog(q.param("logGroupId"), q.param("logId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteLog(q *req) error {
	wr, err := h.Backend.DeleteLog(q.param("logGroupId"), q.param("logId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeLogLogGroup(q *req) error {
	var in ChangeLogGroupIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeLogLogGroup(q.param("logGroupId"), q.param("logId"), q.ifMatch(), in.TargetLogGroupID)

	return acceptedWR(q, wr, err)
}

func sameSource(a, c *ServiceSource) bool {
	return a.Service == c.Service && a.Resource == c.Resource && a.Category == c.Category
}
