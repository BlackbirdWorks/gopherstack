package ocinosql

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	bytesPerUnit  = 1024
	secondsPerMin = 60
	optIfAbsent   = "IF_ABSENT"
	optIfPresent  = "IF_PRESENT"
)

type updateRowIn struct {
	Value           map[string]any `json:"value"`
	IsGetReturnRow  *bool          `json:"isGetReturnRow"`
	TTL             *int           `json:"ttl"`
	IsTTLUseDefault *bool          `json:"isTtlUseTableDefault"`
	IsExactMatch    *bool          `json:"isExactMatch"`
	CompartmentID   string         `json:"compartmentId"`
	Option          string         `json:"option"`
}

type usageWire struct {
	ReadUnitsConsumed  int `json:"readUnitsConsumed"`
	WriteUnitsConsumed int `json:"writeUnitsConsumed"`
}

// UpdateRowResult is the UpdateRow response body.
type UpdateRowResult struct {
	Version         *string         `json:"version"`
	ExistingVersion *string         `json:"existingVersion,omitempty"`
	GeneratedValue  *string         `json:"generatedValue,omitempty"`
	ExistingValue   json.RawMessage `json:"existingValue,omitempty"`
	Usage           usageWire       `json:"usage"`
}

// RowResult is the GetRow response body.
type RowResult struct {
	TimeOfExpiration *time.Time      `json:"timeOfExpiration,omitempty"`
	Value            json.RawMessage `json:"value"`
	Usage            usageWire       `json:"usage"`
}

// DeleteRowResult is the DeleteRow response body.
type DeleteRowResult struct {
	ExistingVersion *string         `json:"existingVersion,omitempty"`
	ExistingValue   json.RawMessage `json:"existingValue,omitempty"`
	IsSuccess       bool            `json:"isSuccess"`
	Usage           usageWire       `json:"usage"`
}

func unitsFor(size int) int { return max(1, (size+bytesPerUnit-1)/bytesPerUnit) }

func (t *Table) record(now time.Time, reads, writes int) {
	if t.Usage == nil {
		t.Usage = map[int64]*Usage{}
	}

	minute := now.Unix() / secondsPerMin * secondsPerMin

	u := t.Usage[minute]
	if u == nil {
		u = &Usage{}
		t.Usage[minute] = u
	}

	u.Reads += reads
	u.Writes += writes
}

func (t *Table) nextVersion() string {
	t.RowSeq++

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(t.RowSeq)) //nolint:gosec // counter is positive

	return base64.StdEncoding.EncodeToString(buf[:])
}

// rowTable resolves a table whose data plane is available.
func (b *InMemoryBackend) rowTable(ref, compartment string) (*Table, *APIError) {
	t, aerr := b.resolve(ref, compartment)
	if aerr != nil {
		return nil, aerr
	}

	if t.State != stateActive && t.State != stateUpdating {
		return nil, errState(
			"Table " + t.Name + " is in state " + t.State + "; rows are only available in ACTIVE or UPDATING",
		)
	}

	return t, nil
}

func (t *Table) live(key string, now time.Time) *Row {
	r := t.Rows[key]
	if r == nil {
		return nil
	}

	if r.Expire != nil && !r.Expire.After(now) {
		delete(t.Rows, key)

		return nil
	}

	return r
}

// prepareRow validates and completes a row value, generating identity and UUID columns.
func (t *Table) prepareRow(in map[string]any, exact bool) (map[string]any, *string, *APIError) {
	sv := newSchemaView(t.Columns)
	out := make(map[string]any, len(sv.cols))
	given := map[string]bool{}

	for k, v := range in {
		i, ok := sv.lookup(k)
		if !ok {
			if exact {
				return nil, nil, errParam("column '" + k + "' is not part of table " + t.Name)
			}

			continue
		}

		n, msg := coerce(sv.types[i], v)
		if msg != "" {
			return nil, nil, errParam("value of column '" + sv.cols[i].Name + "' " + msg)
		}

		out[sv.cols[i].Name], given[sv.cols[i].Name] = n, true
	}

	gen, aerr := t.generate(sv, out, given)
	if aerr != nil {
		return nil, nil, aerr
	}

	return out, gen, t.fillDefaults(sv, out, given, exact)
}

func (t *Table) generate(sv *schemaView, out map[string]any, given map[string]bool) (*string, *APIError) {
	var generated *string

	for _, c := range sv.cols {
		switch {
		case t.Identity != nil && strs.Equal(t.Identity.Column, c.Name):
			g, aerr := t.generateIdentity(c.Name, out, given)
			if aerr != nil {
				return nil, aerr
			}

			generated = g
		case c.AsUUID && c.Gen && (!given[c.Name] || out[c.Name] == nil):
			out[c.Name], given[c.Name] = newUUID(), true
		case c.AsUUID && out[c.Name] != nil:
			if s, _ := out[c.Name].(string); !validUUID(s) {
				return nil, errParam("value of column '" + c.Name + "' is not a valid UUID")
			}
		}
	}

	return generated, nil
}

func (t *Table) generateIdentity(col string, out map[string]any, given map[string]bool) (*string, *APIError) {
	id := t.Identity
	if id.Always && given[col] && out[col] != nil {
		return nil, errParam("column '" + col + "' is GENERATED ALWAYS AS IDENTITY and cannot be assigned a value")
	}

	if given[col] && out[col] != nil {
		return nil, nil
	}

	if given[col] && !id.OnNull && !id.Always {
		return nil, errParam("column '" + col + "' cannot be null")
	}

	v := strconv.FormatInt(id.Next, 10)
	id.Next += id.Increment
	out[col], given[col] = json.Number(v), true

	return &v, nil
}

func (t *Table) fillDefaults(sv *schemaView, out map[string]any, given map[string]bool, exact bool) *APIError {
	for i, c := range sv.cols {
		if given[c.Name] {
			if out[c.Name] == nil && c.NotNull {
				return errParam("column '" + c.Name + "' cannot be null")
			}

			continue
		}

		if len(c.Default) > 0 {
			v, _ := coerceDefault(sv.types[i], c.Default)
			out[c.Name] = v

			continue
		}

		if c.NotNull || (exact && !c.Gen) {
			return errParam("column '" + c.Name + "' requires a value")
		}

		out[c.Name] = nil
	}

	return nil
}

func (t *Table) rowExpiry(in updateRowIn, now time.Time) (*time.Time, *APIError) {
	hours := t.TTLHours

	if in.TTL != nil && (in.IsTTLUseDefault == nil || !*in.IsTTLUseDefault) {
		if *in.TTL < 0 {
			return nil, errParam("ttl must not be negative")
		}

		hours = *in.TTL * hoursPerDay
	}

	if hours == 0 {
		return nil, nil
	}

	at := now.Add(time.Duration(hours) * time.Hour)

	return &at, nil
}

func ptrIf(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

// UpdateRow puts a row into a table.
func (b *InMemoryBackend) UpdateRow(ref, ifMatch string, in updateRowIn) (UpdateRowResult, error) {
	b.mu.Lock("UpdateRow")
	defer b.mu.Unlock()

	t, aerr := b.rowTable(ref, in.CompartmentID)
	if aerr != nil {
		return UpdateRowResult{}, aerr
	}

	if in.Value == nil {
		return UpdateRowResult{}, errMissing("value is required")
	}

	if in.Option != "" && in.Option != optIfAbsent && in.Option != optIfPresent {
		return UpdateRowResult{}, errParam("option must be IF_ABSENT or IF_PRESENT")
	}

	now := b.nowUTC()

	expire, aerr := t.rowExpiry(in, now)
	if aerr != nil {
		return UpdateRowResult{}, aerr
	}

	row, gen, aerr := t.prepareRow(in.Value, in.IsExactMatch != nil && *in.IsExactMatch)
	if aerr != nil {
		return UpdateRowResult{}, aerr
	}

	key, aerr := rowKey(t.PrimaryKey, row)
	if aerr != nil {
		return UpdateRowResult{}, aerr
	}

	if aerr = checkRowSize(t.PrimaryKey, row); aerr != nil {
		return UpdateRowResult{}, aerr
	}

	existing := t.live(key, now)
	if refused(in.Option, ifMatch, existing) {
		return b.refusedPut(t, existing, in, now), nil
	}

	raw, _ := json.Marshal(row)
	t.Rows[key] = &Row{Value: raw, Version: t.nextVersion(), Expire: expire}
	w := unitsFor(len(raw))
	t.record(now, 0, w)

	return UpdateRowResult{
		Version:        ptrIf(t.Rows[key].Version),
		GeneratedValue: gen,
		Usage:          usageWire{WriteUnitsConsumed: w},
	}, nil
}

func refused(option, ifMatch string, existing *Row) bool {
	switch {
	case option == optIfAbsent && existing != nil, option == optIfPresent && existing == nil:
		return true
	case ifMatch != "" && (existing == nil || existing.Version != ifMatch):
		return true
	}

	return false
}

func (b *InMemoryBackend) refusedPut(t *Table, existing *Row, in updateRowIn, now time.Time) UpdateRowResult {
	res := UpdateRowResult{}

	if existing != nil && in.IsGetReturnRow != nil && *in.IsGetReturnRow {
		res.ExistingVersion, res.ExistingValue = ptrIf(existing.Version), existing.Value
		res.Usage.ReadUnitsConsumed = unitsFor(len(existing.Value))
	}

	t.record(now, res.Usage.ReadUnitsConsumed, 0)

	return res
}

func parseKey(t *Table, parts []string) (map[string]any, *APIError) {
	sv := newSchemaView(t.Columns)
	row := map[string]any{}

	for _, p := range parts {
		name, val, ok := strings.Cut(p, ":")
		if !ok {
			return nil, errParam("key '" + p + "' must have the form column-name:value")
		}

		i, found := sv.lookup(name)
		if !found || !strs.ContainsFold(t.PrimaryKey, name) {
			return nil, errParam("'" + name + "' is not a primary key column of table " + t.Name)
		}

		var raw any = val
		if sv.types[i].isNumeric() {
			raw = json.Number(val)
		}

		n, msg := coerce(sv.types[i], raw)
		if msg != "" {
			return nil, errParam("key value of column '" + sv.cols[i].Name + "' " + msg)
		}

		row[sv.cols[i].Name] = n
	}

	if len(row) != len(t.PrimaryKey) {
		return nil, errParam("key must contain a value for every primary key column")
	}

	return row, nil
}

// GetRow reads one row by primary key.
func (b *InMemoryBackend) GetRow(ref, compartment string, key []string, consistency string) (RowResult, string, error) {
	b.mu.Lock("GetRow")
	defer b.mu.Unlock()

	t, aerr := b.rowTable(ref, compartment)
	if aerr != nil {
		return RowResult{}, "", aerr
	}

	if consistency != "" && consistency != consEventual && consistency != consAbsolute {
		return RowResult{}, "", errParam("consistency must be EVENTUAL or ABSOLUTE")
	}

	kv, aerr := parseKey(t, key)
	if aerr != nil {
		return RowResult{}, "", aerr
	}

	ks, _ := rowKey(t.PrimaryKey, kv)
	now := b.nowUTC()
	r := t.live(ks, now)

	size := 0
	if r != nil {
		size = len(r.Value)
	}

	units := unitsFor(size)
	if consistency == consAbsolute {
		units *= 2
	}

	t.record(now, units, 0)

	if r == nil {
		return RowResult{Usage: usageWire{ReadUnitsConsumed: units}}, "", nil
	}

	return RowResult{
		Value:            r.Value,
		TimeOfExpiration: r.Expire,
		Usage:            usageWire{ReadUnitsConsumed: units},
	}, r.Version, nil
}

// DeleteRow removes one row by primary key.
func (b *InMemoryBackend) DeleteRow(
	ref, compartment string,
	key []string,
	ifMatch string,
	returnRow bool,
) (DeleteRowResult, error) {
	b.mu.Lock("DeleteRow")
	defer b.mu.Unlock()

	t, aerr := b.rowTable(ref, compartment)
	if aerr != nil {
		return DeleteRowResult{}, aerr
	}

	kv, aerr := parseKey(t, key)
	if aerr != nil {
		return DeleteRowResult{}, aerr
	}

	ks, _ := rowKey(t.PrimaryKey, kv)
	now := b.nowUTC()
	r := t.live(ks, now)
	res := DeleteRowResult{}

	if r != nil && returnRow {
		res.ExistingVersion, res.ExistingValue = ptrIf(r.Version), r.Value
		res.Usage.ReadUnitsConsumed = unitsFor(len(r.Value))
	}

	if r != nil && (ifMatch == "" || ifMatch == r.Version) {
		delete(t.Rows, ks)
		res.IsSuccess = true
		res.Usage.WriteUnitsConsumed = unitsFor(len(r.Value))
	} else if r == nil {
		res.Usage.WriteUnitsConsumed = 1
	}

	t.record(now, res.Usage.ReadUnitsConsumed, res.Usage.WriteUnitsConsumed)

	return res, nil
}

func (h *Handler) updateRow(q *req) error {
	var in updateRowIn
	if err := q.decode(&in); err != nil {
		return err
	}

	res, err := h.Backend.UpdateRow(q.param("tableNameOrId"), q.ifMatch(), in)

	return reply(q, res, "", err)
}

func (h *Handler) getRow(q *req) error {
	res, etag, err := h.Backend.GetRow(q.param("tableNameOrId"), q.query("compartmentId"), q.r.URL.Query()["key"],
		q.query("consistency"))

	return reply(q, res, etag, err)
}

func (h *Handler) deleteRow(q *req) error {
	res, err := h.Backend.DeleteRow(q.param("tableNameOrId"), q.query("compartmentId"), q.r.URL.Query()["key"],
		q.ifMatch(), q.flag("isGetReturnRow"))

	return reply(q, res, "", err)
}
