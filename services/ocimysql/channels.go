package ocimysql

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	stateNeedsAttention = "NEEDS_ATTENTION"
	targetDBSystem      = "DBSYSTEM"
	sourceMysql         = "MYSQL"
	defaultChannelPort  = 3306
	channelRaise        = "RAISE_ERROR"
)

// ChannelSourceIn is the channel source details; the password is validated and discarded.
type ChannelSourceIn struct {
	Port       *int   `json:"port"`
	SourceType string `json:"sourceType"`
	Hostname   string `json:"hostname"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	SslMode    string `json:"sslMode"`
}

// ChannelTargetIn is the channel target details.
type ChannelTargetIn struct {
	DelayInSeconds                  *int            `json:"delayInSeconds"`
	TargetType                      string          `json:"targetType"`
	DBSystemID                      string          `json:"dbSystemId"`
	ChannelName                     string          `json:"channelName"`
	ApplierUsername                 string          `json:"applierUsername"`
	TablesWithoutPrimaryKeyHandling string          `json:"tablesWithoutPrimaryKeyHandling"`
	Filters                         json.RawMessage `json:"filters"`
}

// CreateChannelIn is the CreateChannel body.
type CreateChannelIn struct {
	Tags
	IsEnabled     *bool           `json:"isEnabled"`
	Source        ChannelSourceIn `json:"source"`
	CompartmentID string          `json:"compartmentId"`
	DisplayName   string          `json:"displayName"`
	Description   string          `json:"description"`
	Target        ChannelTargetIn `json:"target"`
}

//nolint:gochecknoglobals // immutable enum value lists
var (
	sslModes      = []string{"VERIFY_IDENTITY", "VERIFY_CA", "REQUIRED", "DISABLED"}
	tableHandling = []string{channelRaise, "ALLOW", "GENERATE_IMPLICIT_PRIMARY_KEY"}
)

func (b *InMemoryBackend) channelSource(in ChannelSourceIn, cur *ChannelSource) (ChannelSource, *APIError) {
	var out ChannelSource
	if cur != nil {
		out = *cur
	}

	if in.SourceType != "" && !strings.EqualFold(in.SourceType, sourceMysql) {
		return out, errParam("source.sourceType must be " + sourceMysql)
	}

	out.SourceType = sourceMysql
	setNonEmpty(&out.Hostname, in.Hostname)
	setNonEmpty(&out.Username, in.Username)
	setNonEmpty(&out.SslMode, strings.ToUpper(in.SslMode))

	if in.Password != "" {
		out.HasPassword = true
	}

	if in.Port != nil {
		out.Port = *in.Port
	}

	if cur == nil && out.Port == 0 {
		out.Port = defaultChannelPort
	}

	switch {
	case out.Hostname == "":
		return out, errMissing("source.hostname is required")
	case out.Username == "":
		return out, errMissing("source.username is required")
	case !out.HasPassword:
		return out, errMissing("source.password is required")
	case out.SslMode == "":
		return out, errMissing("source.sslMode is required")
	case !containsFold(sslModes, out.SslMode):
		return out, errParam("source.sslMode must be one of " + strings.Join(sslModes, ", "))
	case out.Port < 1 || out.Port > 65535:
		return out, errParam("source.port must be between 1 and 65535")
	}

	return out, nil
}

func (b *InMemoryBackend) channelTarget(in ChannelTargetIn, cur *ChannelTarget) (ChannelTarget, *APIError) {
	var out ChannelTarget
	if cur != nil {
		out = *cur
	}

	if in.TargetType != "" && !strings.EqualFold(in.TargetType, targetDBSystem) {
		return out, errParam("target.targetType must be " + targetDBSystem)
	}

	out.TargetType = targetDBSystem

	if cur == nil {
		out.TablesWithoutPrimaryKeyHandling = channelRaise
		out.ApplierUsername = "applier"

		if in.DBSystemID == "" {
			return out, errMissing("target.dbSystemId is required")
		}

		out.DBSystemID = in.DBSystemID
	}

	setNonEmpty(&out.ChannelName, in.ChannelName)
	setNonEmpty(&out.ApplierUsername, in.ApplierUsername)
	setNonEmpty(&out.TablesWithoutPrimaryKeyHandling, strings.ToUpper(in.TablesWithoutPrimaryKeyHandling))

	if in.DelayInSeconds != nil {
		if *in.DelayInSeconds < 0 {
			return out, errParam("target.delayInSeconds must not be negative")
		}

		out.DelayInSeconds = *in.DelayInSeconds
	}

	if in.Filters != nil {
		out.Filters = in.Filters
	}

	if !containsFold(tableHandling, out.TablesWithoutPrimaryKeyHandling) {
		return out, errParam(
			"target.tablesWithoutPrimaryKeyHandling must be one of " + strings.Join(tableHandling, ", "),
		)
	}

	return out, nil
}

func (b *InMemoryBackend) channelDocs(c *Channel) (Doc, Doc) {
	src := Doc{
		keySourceType: c.Source.SourceType, "hostname": c.Source.Hostname, "port": c.Source.Port,
		"username": c.Source.Username, "sslMode": c.Source.SslMode,
	}
	tgt := Doc{
		"targetType": c.Target.TargetType, paramDBSystem: c.Target.DBSystemID, "channelName": c.Target.ChannelName,
		"applierUsername": c.Target.ApplierUsername, "delayInSeconds": c.Target.DelayInSeconds,
		"tablesWithoutPrimaryKeyHandling": c.Target.TablesWithoutPrimaryKeyHandling,
	}

	if c.Target.Filters != nil {
		tgt["filters"] = c.Target.Filters
	}

	return src, tgt
}

func (b *InMemoryBackend) channelSummary(c *Channel) Doc {
	src, tgt := b.channelDocs(c)
	d := Doc{
		"id": c.ID, keyCompartment: c.CompartmentID, keyIsEnabled: c.Enabled, "source": src, "target": tgt,
		keyLifecycle: c.State, keyDisplayName: c.Name, keyTimeCreated: c.TimeCreated, keyTimeUpdated: c.TimeUpdated,
	}
	if c.Details != "" {
		d["lifecycleDetails"] = c.Details
	}

	putTags(d, c.Tags)

	return d
}

func (b *InMemoryBackend) channelDoc(c *Channel) Doc {
	d := b.channelSummary(c)
	if c.Description != "" {
		d["description"] = c.Description
	}

	return d
}

// CreateChannel creates a replication channel record.
func (b *InMemoryBackend) CreateChannel(in CreateChannelIn) (Doc, string, string, error) {
	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	src, aerr := b.channelSource(in.Source, nil)
	tgt, aerr2 := b.channelTarget(in.Target, nil)

	if aerr = firstErr(aerr, aerr2); aerr != nil {
		return nil, "", "", aerr
	}

	b.settleID(tgt.DBSystemID)

	s, ok := b.systems[tgt.DBSystemID]
	if !ok || s.State == stateDeleted {
		return nil, "", "", errParam(
			"target.dbSystemId " + tgt.DBSystemID + " does not reference an existing DB system",
		)
	}

	if s.State != stateActive {
		return nil, "", "", errState("DB system " + s.ID + " is in state " + s.State + "; channels require ACTIVE")
	}

	compartment := orStr(in.CompartmentID, s.CompartmentID)
	if aerr = b.checkCompartment(compartment); aerr != nil {
		return nil, "", "", aerr
	}

	name := orStr(in.DisplayName, "MySQLChannel"+b.nowUTC().Format("20060102150405"))
	if aerr = validateName(name); aerr != nil {
		return nil, "", "", aerr
	}

	if tgt.ChannelName == "" {
		tgt.ChannelName = "channel" + b.nowUTC().Format("150405")
	}

	now := b.nowUTC()
	enabledNow := in.IsEnabled == nil || *in.IsEnabled
	c := &Channel{
		ID: newOCID("mysqlchannel"), CompartmentID: compartment, Name: name, Description: in.Description,
		Enabled: enabledNow, State: stateCreating, After: stateInactive, Source: src, Target: tgt, Tags: in.Tags,
		Etag: newEtag(), TimeCreated: now, TimeUpdated: now, TransitionAt: b.transitionAt(), Seq: b.nextSeq(),
	}
	if enabledNow {
		c.After = stateActive
	}

	b.channels[c.ID] = c
	wr := b.newWR(wrCreateChannel, compartment, entityChannel, c.ID, actionCreated, c.TransitionAt)

	return b.channelDoc(c), c.Etag, wr, nil
}

func (b *InMemoryBackend) channelFor(id, ifMatch string) (*Channel, *APIError) {
	b.settleID(id)

	c, ok := b.channels[id]
	if !ok || c.State == stateDeleted {
		return nil, errNotFound(id)
	}

	return c, checkEtag(c.Etag, ifMatch)
}

// GetChannel returns one channel.
func (b *InMemoryBackend) GetChannel(id string) (Doc, string, error) {
	b.mu.Lock("GetChannel")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, "")
	if aerr != nil {
		return nil, "", aerr
	}

	return b.channelDoc(c), c.Etag, nil
}

// ChannelListIn are the ListChannels filters.
type ChannelListIn struct {
	Compartment string
	DBSystemID  string
	ID          string
	Name        string
	State       string
	IsEnabled   *bool
	ListOpts
}

func (in ChannelListIn) matches(c *Channel) bool {
	if c.CompartmentID != in.Compartment || (in.DBSystemID != "" && c.Target.DBSystemID != in.DBSystemID) ||
		(in.ID != "" && c.ID != in.ID) || (in.Name != "" && c.Name != in.Name) ||
		(in.IsEnabled != nil && c.Enabled != *in.IsEnabled) {
		return false
	}

	return stateMatches(c.State, in.State)
}

// ListChannels lists the channels of a compartment.
func (b *InMemoryBackend) ListChannels(in ChannelListIn) (page.Page[Doc], error) {
	if aerr := firstErr(b.checkCompartmentLocked(in.Compartment),
		validListOpts(in.ListOpts, keyDisplayName, keyTimeCreated)); aerr != nil {
		return page.Page[Doc]{}, aerr
	}

	b.mu.Lock("ListChannels")
	defer b.mu.Unlock()

	b.settleAll()

	var all []*Channel

	for _, c := range b.channels {
		if in.matches(c) {
			all = append(all, c)
		}
	}

	pg := paginate(all, in.ListOpts, func(c *Channel) sortKeys {
		return sortKeys{created: c.TimeCreated, updated: c.TimeUpdated, name: c.Name, seq: c.Seq}
	})

	out := make([]Doc, 0, len(pg.Data))
	for _, c := range pg.Data {
		out = append(out, b.channelSummary(c))
	}

	return page.Page[Doc]{Data: out, Next: pg.Next}, nil
}

func (b *InMemoryBackend) channelTransition(c *Channel, op, after string) string {
	c.State, c.After, c.TimeUpdated, c.Etag = stateUpdating, after, b.nowUTC(), newEtag()
	c.TransitionAt = b.transitionAt()

	return b.newWR(op, c.CompartmentID, entityChannel, c.ID, actionUpdated, c.TransitionAt)
}

// UpdateChannelIn is the UpdateChannel body.
type UpdateChannelIn struct {
	Tags
	IsEnabled   *bool           `json:"isEnabled"`
	DisplayName *string         `json:"displayName"`
	Description *string         `json:"description"`
	Source      ChannelSourceIn `json:"source"`
	Target      ChannelTargetIn `json:"target"`
}

// UpdateChannel updates a channel and returns its work request.
func (b *InMemoryBackend) UpdateChannel(id, ifMatch string, in UpdateChannelIn) (string, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateActive && c.State != stateInactive && c.State != stateNeedsAttention {
		return "", errState("channel " + id + " is in state " + c.State)
	}

	src, aerr := b.channelSource(in.Source, &c.Source)
	tgt, aerr2 := b.channelTarget(in.Target, &c.Target)

	if aerr = firstErr(aerr, aerr2); aerr != nil {
		return "", aerr
	}

	if in.DisplayName != nil {
		if aerr = validateName(*in.DisplayName); aerr != nil {
			return "", aerr
		}

		c.Name = *in.DisplayName
	}

	if in.Description != nil {
		c.Description = *in.Description
	}

	if in.IsEnabled != nil {
		c.Enabled = *in.IsEnabled
	}

	c.Source, c.Target = src, tgt
	c.Tags.merge(in.Tags)

	return b.channelTransition(c, wrUpdateChannel, enabledState(c.Enabled)), nil
}

func enabledState(on bool) string {
	if on {
		return stateActive
	}

	return stateInactive
}

// ResumeChannel enables a channel.
func (b *InMemoryBackend) ResumeChannel(id, ifMatch string) (string, error) {
	b.mu.Lock("ResumeChannel")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateInactive && c.State != stateNeedsAttention {
		return "", errState(
			"channel " + id + " is in state " + c.State + "; resume requires INACTIVE or NEEDS_ATTENTION",
		)
	}

	c.Enabled = true

	return b.channelTransition(c, wrResumeChannel, stateActive), nil
}

// ResetChannel resets a channel's replication state.
func (b *InMemoryBackend) ResetChannel(id, ifMatch string) (string, error) {
	b.mu.Lock("ResetChannel")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State != stateInactive && c.State != stateNeedsAttention {
		return "", errState(
			"channel " + id + " is in state " + c.State + "; reset requires INACTIVE or NEEDS_ATTENTION",
		)
	}

	return b.channelTransition(c, wrResetChannel, enabledState(c.Enabled)), nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(id, ifMatch string) (string, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	c, aerr := b.channelFor(id, ifMatch)
	if aerr != nil {
		return "", aerr
	}

	if c.State == stateCreating || c.State == stateUpdating || c.State == stateDeleting {
		return "", errState("channel " + id + " is in state " + c.State)
	}

	c.State, c.After, c.TimeUpdated, c.Etag = stateDeleting, "", b.nowUTC(), newEtag()
	c.TransitionAt = b.transitionAt()

	return b.newWR(wrDeleteChannel, c.CompartmentID, entityChannel, id, actionDeleted, c.TransitionAt), nil
}

func (h *Handler) createChannel(q *req) error {
	var in CreateChannelIn
	if err := q.decode(&in); err != nil {
		return err
	}

	d, etag, wr, err := h.Backend.CreateChannel(in)
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", wr)

	return q.json(http.StatusOK, d, etag)
}

func (h *Handler) getChannel(q *req) error {
	d, etag, err := h.Backend.GetChannel(q.param("channelId"))

	return reply(q, d, etag, err)
}

func (h *Handler) listChannels(q *req) error {
	in := ChannelListIn{
		Compartment: q.query(keyCompartment), DBSystemID: q.query(paramDBSystem), ID: q.query("channelId"),
		Name: q.query(keyDisplayName), State: q.query(keyLifecycle), ListOpts: q.listOpts(),
	}
	if v := q.query(keyIsEnabled); v != "" {
		t := strings.EqualFold(v, "true")
		in.IsEnabled = &t
	}

	pg, err := h.Backend.ListChannels(in)

	return collectionReply(q, pg, err)
}

func (h *Handler) updateChannel(q *req) error {
	var in UpdateChannelIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateChannel(q.param("channelId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) resumeChannel(q *req) error {
	wr, err := h.Backend.ResumeChannel(q.param("channelId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) resetChannel(q *req) error {
	wr, err := h.Backend.ResetChannel(q.param("channelId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteChannel(q *req) error {
	wr, err := h.Backend.DeleteChannel(q.param("channelId"), q.ifMatch())

	return acceptedWR(q, wr, err)
}
