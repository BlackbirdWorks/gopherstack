package ocinosql

import (
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// Emulator limit constants; Oracle documents the per-table maximums as service limits.
const (
	maxReadUnits    = 40000
	maxWriteUnits   = 20000
	maxStorageGB    = 5000
	reclaimReadWrit = 50
	reclaimStorage  = 25
	ocidPrefix      = "ocid1."
)

type limitsIn struct {
	MaxReadUnits    *int   `json:"maxReadUnits"`
	MaxWriteUnits   *int   `json:"maxWriteUnits"`
	MaxStorageInGBs *int   `json:"maxStorageInGBs"`
	CapacityMode    string `json:"capacityMode"`
}

type createTableIn struct {
	Tags
	TableLimits   *limitsIn `json:"tableLimits"`
	AutoReclaim   *bool     `json:"isAutoReclaimable"`
	Name          string    `json:"name"`
	CompartmentID string    `json:"compartmentId"`
	DDL           string    `json:"ddlStatement"`
}

type updateTableIn struct {
	Tags
	TableLimits   *limitsIn `json:"tableLimits"`
	CompartmentID string    `json:"compartmentId"`
	DDL           string    `json:"ddlStatement"`
}

type changeCompartmentIn struct {
	ToCompartmentID   string `json:"toCompartmentId"`
	FromCompartmentID string `json:"fromCompartmentId"`
}

func buildLimits(in *limitsIn, autoReclaim bool) (Limits, *APIError) {
	if in == nil {
		if autoReclaim {
			return Limits{Mode: modeProvisioned, ReadUnits: reclaimReadWrit, WriteUnits: reclaimReadWrit,
				StorageGB: reclaimStorage}, nil
		}

		return Limits{}, errMissing("tableLimits is required unless isAutoReclaimable is true")
	}

	mode := in.CapacityMode
	if mode == "" {
		mode = modeProvisioned
	}

	if mode != modeProvisioned && mode != modeOnDemand {
		return Limits{}, errParam("capacityMode must be PROVISIONED or ON_DEMAND")
	}

	if in.MaxStorageInGBs == nil || *in.MaxStorageInGBs < 1 || *in.MaxStorageInGBs > maxStorageGB {
		return Limits{}, errParam("maxStorageInGBs must be between 1 and 5000")
	}

	lim := Limits{Mode: mode, StorageGB: *in.MaxStorageInGBs}
	if mode == modeOnDemand {
		return lim, nil
	}

	for _, u := range []struct {
		v    *int
		name string
		max  int
	}{{in.MaxReadUnits, "maxReadUnits", maxReadUnits}, {in.MaxWriteUnits, "maxWriteUnits", maxWriteUnits}} {
		if u.v == nil || *u.v < 1 || *u.v > u.max {
			return Limits{}, errParam(u.name + " must be between 1 and " + strconv.Itoa(u.max) +
				" for PROVISIONED capacity")
		}
	}

	lim.ReadUnits, lim.WriteUnits = *in.MaxReadUnits, *in.MaxWriteUnits

	return lim, nil
}

// resolve finds a live table by OCID or by name within a compartment.
func (b *InMemoryBackend) resolve(ref, compartment string) (*Table, *APIError) {
	if strings.HasPrefix(ref, ocidPrefix) {
		t, ok := b.tables[ref]
		if !ok || t.State == stateDeleted {
			return nil, errNotFound(ref)
		}

		if compartment != "" && compartment != t.CompartmentID {
			return nil, errParam("compartmentId does not match the compartment of table " + ref)
		}

		return t, nil
	}

	if compartment == "" {
		return nil, errMissing("compartmentId is required when tableNameOrId is a table name")
	}

	for _, t := range b.tables {
		if t.CompartmentID == compartment && t.State != stateDeleted && strs.Equal(t.Name, ref) {
			return t, nil
		}
	}

	return nil, errNotFound(ref)
}

func (b *InMemoryBackend) nameTaken(compartment, name, except string) bool {
	for _, t := range b.tables {
		if t.ID != except && t.CompartmentID == compartment && t.State != stateDeleted && strs.Equal(t.Name, name) {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) finishAt(t *Table) time.Time { return *t.TransitionAt }

// CreateTable creates a table from a CREATE TABLE statement and returns its work request id.
func (b *InMemoryBackend) CreateTable(in createTableIn) (string, error) {
	b.mu.Lock("CreateTable")
	defer b.mu.Unlock()

	if aerr := b.checkCompartment(in.CompartmentID); aerr != nil {
		return "", aerr
	}

	spec, aerr := parseCreate(in.DDL)
	if aerr != nil {
		return "", aerr
	}

	if in.Name == "" || !strs.Equal(in.Name, spec.name) {
		return "", errParam("name must match the table name in ddlStatement")
	}

	if aerr = validIdent("table", in.Name); aerr != nil {
		return "", aerr
	}

	if b.nameTaken(in.CompartmentID, in.Name, "") {
		return b.createExisting(in, spec)
	}

	reclaim := in.AutoReclaim != nil && *in.AutoReclaim

	lim, aerr := buildLimits(in.TableLimits, reclaim)
	if aerr != nil {
		return "", aerr
	}

	now := b.nowUTC()
	t := &Table{
		ID: newOCID("nosqltable"), CompartmentID: in.CompartmentID, Name: in.Name, State: stateCreating,
		Etag: newEtag(), Limits: lim, Columns: spec.cols, PrimaryKey: spec.pk,
		ShardKey: spec.shard, Identity: spec.identity, AutoReclaim: reclaim, TimeCreated: now, TimeUpdated: now,
		Rows: map[string]*Row{}, Indexes: map[string]*Index{}, Usage: map[int64]*Usage{}, Tags: in.Tags,
		TransitionAt: b.transitionAt(),
	}

	if len(t.ShardKey) == 0 {
		t.ShardKey = slices.Clone(spec.pk)
	}

	if spec.ttlHours != nil {
		t.TTLHours = *spec.ttlHours
	}

	t.DDL = renderDDL(t)
	b.tables[t.ID] = t

	return b.newWR(wrCreate, t.CompartmentID, t.ID, "CREATED", b.finishAt(t)), nil
}

func (b *InMemoryBackend) createExisting(in createTableIn, spec *createSpec) (string, error) {
	if !spec.ifNotExists {
		return "", errConflict("Table " + in.Name + " already exists in compartment " + in.CompartmentID)
	}

	t, aerr := b.resolve(in.Name, in.CompartmentID)
	if aerr != nil {
		return "", aerr
	}

	return b.newWR(wrCreate, t.CompartmentID, t.ID, "CREATED", b.nowUTC()), nil
}

func (b *InMemoryBackend) modifiable(t *Table) *APIError {
	if t.State != stateActive {
		return errState("Table " + t.Name + " is in state " + t.State + "; it must be ACTIVE for this operation")
	}

	return nil
}

// UpdateTable alters a table's schema, limits or tags.
func (b *InMemoryBackend) UpdateTable(ref, ifMatch string, in updateTableIn) (string, error) {
	b.mu.Lock("UpdateTable")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, in.CompartmentID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(b.modifiable(t), checkEtag(t.Etag, ifMatch)); aerr != nil {
		return "", aerr
	}

	if in.DDL != "" && in.TableLimits != nil {
		return "", errParam("ddlStatement and tableLimits cannot both be specified in one update")
	}

	changed := false

	switch {
	case in.DDL != "":
		if aerr = b.applyAlter(t, in.DDL); aerr != nil {
			return "", aerr
		}

		changed = true
	case in.TableLimits != nil:
		lim, e := buildLimits(in.TableLimits, false)
		if e != nil {
			return "", e
		}

		t.Limits, changed = lim, true
	}

	t.Tags.merge(in.Tags)
	t.Etag, t.TimeUpdated = newEtag(), b.nowUTC()

	if !changed {
		return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", b.nowUTC()), nil
	}

	t.State, t.TransitionAt = stateUpdating, b.transitionAt()

	return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", b.finishAt(t)), nil
}

// DeleteTable drops a table; ifExists turns a missing table into a no-op (empty work request id).
func (b *InMemoryBackend) DeleteTable(ref, compartment, ifMatch string, ifExists bool) (string, error) {
	b.mu.Lock("DeleteTable")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		if ifExists && aerr.Status == http.StatusNotFound {
			return "", nil
		}

		return "", aerr
	}

	if aerr = firstErr(b.modifiable(t), checkEtag(t.Etag, ifMatch)); aerr != nil {
		return "", aerr
	}

	t.State, t.TransitionAt, t.TimeUpdated = stateDeleting, b.transitionAt(), b.nowUTC()

	return b.newWR(wrDelete, t.CompartmentID, t.ID, "DELETED", b.finishAt(t)), nil
}

// ChangeTableCompartment moves a table to another compartment.
func (b *InMemoryBackend) ChangeTableCompartment(ref, ifMatch string, in changeCompartmentIn) (string, error) {
	b.mu.Lock("ChangeTableCompartment")
	defer b.mu.Unlock()

	t, aerr := b.resolve(ref, in.FromCompartmentID)
	if aerr != nil {
		return "", aerr
	}

	if aerr = firstErr(b.checkCompartment(in.ToCompartmentID), b.modifiable(t),
		checkEtag(t.Etag, ifMatch)); aerr != nil {
		return "", aerr
	}

	if b.nameTaken(in.ToCompartmentID, t.Name, t.ID) {
		return "", errConflict("Table " + t.Name + " already exists in compartment " + in.ToCompartmentID)
	}

	t.CompartmentID, t.Etag, t.TimeUpdated = in.ToCompartmentID, newEtag(), b.nowUTC()

	return b.newWR(wrUpdate, t.CompartmentID, t.ID, "UPDATED", b.nowUTC()), nil
}

// Table wire types.

type limitsWire struct {
	CapacityMode    string `json:"capacityMode"`
	MaxReadUnits    int    `json:"maxReadUnits"`
	MaxWriteUnits   int    `json:"maxWriteUnits"`
	MaxStorageInGBs int    `json:"maxStorageInGBs"`
}

type columnWire struct {
	DefaultValue *string `json:"defaultValue,omitempty"`
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	IsNullable   bool    `json:"isNullable"`
	IsAsUUID     bool    `json:"isAsUuid"`
	IsGenerated  bool    `json:"isGenerated"`
}

type identityWire struct {
	ColumnName string `json:"columnName"`
	IsAlways   bool   `json:"isAlways"`
	IsNull     bool   `json:"isNull"`
}

type schemaWire struct {
	Identity   *identityWire `json:"identity,omitempty"`
	Columns    []columnWire  `json:"columns"`
	PrimaryKey []string      `json:"primaryKey"`
	ShardKey   []string      `json:"shardKey"`
	TTL        int           `json:"ttl"`
}

// TableWire is the full table representation.
type TableWire struct {
	TimeCreated       time.Time                 `json:"timeCreated"`
	TimeUpdated       time.Time                 `json:"timeUpdated"`
	Schema            *schemaWire               `json:"schema,omitempty"`
	FreeformTags      map[string]string         `json:"freeformTags,omitempty"`
	DefinedTags       map[string]map[string]any `json:"definedTags,omitempty"`
	DDLStatement      string                    `json:"ddlStatement,omitempty"`
	ID                string                    `json:"id"`
	CompartmentID     string                    `json:"compartmentId"`
	Name              string                    `json:"name"`
	LifecycleState    string                    `json:"lifecycleState"`
	SchemaState       string                    `json:"schemaState"`
	TableLimits       limitsWire                `json:"tableLimits"`
	IsAutoReclaimable bool                      `json:"isAutoReclaimable"`
	IsMultiRegion     bool                      `json:"isMultiRegion"`
}

func limitsView(l Limits) limitsWire {
	return limitsWire{MaxReadUnits: l.ReadUnits, MaxWriteUnits: l.WriteUnits, MaxStorageInGBs: l.StorageGB,
		CapacityMode: l.Mode}
}

func schemaView2(t *Table) *schemaWire {
	cols := make([]columnWire, len(t.Columns))

	for i, c := range t.Columns {
		cols[i] = columnWire{Name: c.Name, Type: c.Type, IsNullable: !c.NotNull, IsAsUUID: c.AsUUID, IsGenerated: c.Gen}

		if len(c.Default) > 0 {
			var s string
			if json.Unmarshal(c.Default, &s) != nil {
				s = string(c.Default)
			}

			cols[i].DefaultValue = &s
		}
	}

	out := &schemaWire{Columns: cols, PrimaryKey: t.PrimaryKey, ShardKey: t.ShardKey, TTL: t.TTLHours / hoursPerDay}
	if t.Identity != nil {
		out.Identity = &identityWire{
			ColumnName: t.Identity.Column,
			IsAlways:   t.Identity.Always,
			IsNull:     t.Identity.OnNull,
		}
	}

	return out
}

func tableView(t *Table, full bool) TableWire {
	w := TableWire{
		ID: t.ID, CompartmentID: t.CompartmentID, Name: t.Name, LifecycleState: t.State, SchemaState: "MUTABLE",
		TimeCreated: t.TimeCreated, TimeUpdated: t.TimeUpdated, TableLimits: limitsView(t.Limits),
		IsAutoReclaimable: t.AutoReclaim, FreeformTags: t.Tags.FreeformTags, DefinedTags: t.Tags.DefinedTags,
	}

	if full {
		w.Schema, w.DDLStatement = schemaView2(t), t.DDL
	}

	return w
}

// GetTable returns one table and its etag.
func (b *InMemoryBackend) GetTable(ref, compartment string) (TableWire, string, error) {
	b.mu.RLock("GetTable")
	defer b.mu.RUnlock()

	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return TableWire{}, "", aerr
	}

	return tableView(t, true), t.Etag, nil
}

// TableList is the ListTables result body.
type TableList struct {
	Items                  []TableWire `json:"items"`
	AutoReclaimableTables  int         `json:"autoReclaimableTables"`
	OnDemandCapacityTables int         `json:"onDemandCapacityTables"`
}

func (l *TableList) count(t *Table) {
	if t.State == stateDeleted {
		return
	}

	if t.AutoReclaim {
		l.AutoReclaimableTables++
	}

	if t.Limits.Mode == modeOnDemand {
		l.OnDemandCapacityTables++
	}
}

func tableMatches(t *Table, name, state string) bool {
	if name != "" && !strs.Equal(t.Name, name) {
		return false
	}

	return state == "" || state == filterAll || t.State == state
}

// ListTables lists tables of a compartment.
func (b *InMemoryBackend) ListTables(
	compartment, name, state string,
	o ListOpts,
) (page.Page[TableWire], TableList, error) {
	if compartment == "" {
		return page.Page[TableWire]{}, TableList{}, errMissing("compartmentId is required")
	}

	b.mu.RLock("ListTables")
	defer b.mu.RUnlock()

	var (
		all   []*Table
		stats TableList
	)

	for _, t := range b.tables {
		if t.CompartmentID != compartment || (t.State == stateDeleted && state != filterAll && state != stateDeleted) {
			continue
		}

		stats.count(t)

		if tableMatches(t, name, state) {
			all = append(all, t)
		}
	}

	pg := paginate(all, o, func(t *Table) string { return t.Name },
		func(t *Table) (time.Time, int64) { return t.TimeCreated, 0 })

	out := make([]TableWire, 0, len(pg.Data))
	for _, t := range pg.Data {
		out = append(out, tableView(t, false))
	}

	return page.Page[TableWire]{Data: out, Next: pg.Next}, stats, nil
}

func (h *Handler) createTable(q *req) error {
	var in createTableIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.CreateTable(in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) getTable(q *req) error {
	w, etag, err := h.Backend.GetTable(q.param("tableNameOrId"), q.query("compartmentId"))

	return reply(q, w, etag, err)
}

func (h *Handler) listTables(q *req) error {
	pg, stats, err := h.Backend.ListTables(q.query("compartmentId"), q.query("name"), q.query("lifecycleState"),
		q.listOpts())
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)
	stats.Items = pg.Data

	return q.json(http.StatusOK, stats, "")
}

func (h *Handler) updateTable(q *req) error {
	var in updateTableIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.UpdateTable(q.param("tableNameOrId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}

func (h *Handler) deleteTable(q *req) error {
	wr, err := h.Backend.DeleteTable(q.param("tableNameOrId"), q.query("compartmentId"), q.ifMatch(),
		q.flag("isIfExists"))
	if err == nil && wr == "" {
		q.w.WriteHeader(http.StatusNoContent)

		return nil
	}

	return acceptedWR(q, wr, err)
}

func (h *Handler) changeTableCompartment(q *req) error {
	var in changeCompartmentIn
	if err := q.decode(&in); err != nil {
		return err
	}

	wr, err := h.Backend.ChangeTableCompartment(q.param("tableNameOrId"), q.ifMatch(), in)

	return acceptedWR(q, wr, err)
}
