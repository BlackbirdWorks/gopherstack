package connect

import (
	"context"
	"slices"
	"strings"
	"time"
)

// PrimaryValue names a primary attribute and the value that identifies a record by it.
type PrimaryValue struct {
	AttributeName string `json:"AttributeName"`
	Value         string `json:"Value"`
}

// PrimaryValueResponse is a primary value with its attribute identifier.
type PrimaryValueResponse struct {
	AttributeID   string `json:"AttributeId"`
	AttributeName string `json:"AttributeName"`
	Value         string `json:"Value"`
}

// PrimaryAttributeValueFilter selects records by the values of one primary attribute.
type PrimaryAttributeValueFilter struct {
	AttributeName string   `json:"AttributeName"`
	Values        []string `json:"Values"`
}

// DataTableValueIn is one value of a batch create or update.
type DataTableValueIn struct {
	LockVersion   *DataTableLockVersion `json:"LockVersion"`
	AttributeName string                `json:"AttributeName"`
	Value         string                `json:"Value"`
	PrimaryValues []PrimaryValue        `json:"PrimaryValues"`
}

// DataTableValueIDIn identifies one value of a batch describe or delete.
type DataTableValueIDIn struct {
	LockVersion   *DataTableLockVersion `json:"LockVersion"`
	AttributeName string                `json:"AttributeName"`
	PrimaryValues []PrimaryValue        `json:"PrimaryValues"`
}

// DataTableBatchFailure reports one item a batch operation rejected.
type DataTableBatchFailure struct {
	AttributeName string         `json:"AttributeName"`
	Message       string         `json:"Message"`
	PrimaryValues []PrimaryValue `json:"PrimaryValues,omitempty"`
}

// DataTableBatchSuccess reports one item a batch create, update or delete applied.
type DataTableBatchSuccess struct {
	LockVersion   *DataTableLockVersion `json:"LockVersion,omitempty"`
	AttributeName string                `json:"AttributeName"`
	RecordID      string                `json:"RecordId,omitempty"`
	PrimaryValues []PrimaryValue        `json:"PrimaryValues,omitempty"`
}

// DataTableBatchDescribed is one value returned by BatchDescribeDataTableValue.
type DataTableBatchDescribed struct {
	Stamp
	LockVersion   *DataTableLockVersion  `json:"LockVersion,omitempty"`
	AttributeID   string                 `json:"AttributeId"`
	AttributeName string                 `json:"AttributeName"`
	RecordID      string                 `json:"RecordId"`
	Value         string                 `json:"Value"`
	PrimaryValues []PrimaryValueResponse `json:"PrimaryValues"`
}

// DataTableBatchResult is the body of a batch value operation.
type DataTableBatchResult[S any] struct {
	Successful []S                     `json:"Successful"`
	Failed     []DataTableBatchFailure `json:"Failed"`
}

type batchValuesIn struct {
	dtRef
	Values []DataTableValueIn `json:"Values"`
}

type batchIDsIn struct {
	dtRef
	Values []DataTableValueIDIn `json:"Values"`
}

func checkBatchSize(n int) error {
	if n == 0 || n > maxDTBatch {
		return invalidParamErr("Values must contain 1-%d entries", maxDTBatch)
	}

	return nil
}

func (t *dataTableRec) findRecord(primary map[string]string) *dtRecord {
	key := t.recordKey(primary)

	for i := range t.Records {
		if t.recordKey(t.Records[i].Primary) == key {
			return &t.Records[i]
		}
	}

	return nil
}

// resolvePrimary maps primary values by attribute name to a record key; every primary attribute must be given once.
func (t *dataTableRec) resolvePrimary(pvs []PrimaryValue, validate bool) (map[string]string, error) {
	out := map[string]string{}

	for _, p := range pvs {
		i := t.attrIndex(p.AttributeName)
		if i < 0 || !t.Attributes[i].Primary {
			return nil, invalidParamErr("attribute %q is not a primary attribute", p.AttributeName)
		}

		a := &t.Attributes[i]
		if _, dup := out[a.AttributeID]; dup {
			return nil, invalidParamErr("primary value %q is given twice", p.AttributeName)
		}

		if validate {
			if err := checkDTValue(a, p.Value); err != nil {
				return nil, invalidParamErr("primary value %q is invalid: %v", p.AttributeName, err)
			}
		}

		out[a.AttributeID] = p.Value
	}

	if want := t.primaryAttrs(); len(out) != len(want) {
		names := make([]string, len(want))
		for i, a := range want {
			names[i] = a.Name
		}

		return nil, invalidParamErr(
			"primary values must be given for every primary attribute: %s",
			strings.Join(names, ", "),
		)
	}

	return out, nil
}

// dtTarget is the attribute and record key an item refers to.
type dtTarget struct {
	attr    *DataTableAttribute
	primary map[string]string
}

func (t *dataTableRec) target(attrName string, pvs []PrimaryValue, validate bool) (*dtTarget, error) {
	i := t.attrIndex(attrName)
	if i < 0 {
		return nil, notFoundErr(dtLabelAttribute, attrName)
	}

	primary, err := t.resolvePrimary(pvs, validate)
	if err != nil {
		return nil, err
	}

	return &dtTarget{attr: &t.Attributes[i], primary: primary}, nil
}

func (t *dataTableRec) lockMatches(given *DataTableLockVersion, a *DataTableAttribute, rec *dtRecord, v *dtValue) bool {
	if given == nil {
		return true
	}

	level := t.Table.ValueLockLevel
	same := func(have, want string) bool { return want == "" || have == want }

	switch level {
	case dtLockTable:
		return same(t.Table.LockVersion.DataTable, given.DataTable)
	case dtLockPrimary:
		return rec == nil || same(rec.Lock, given.PrimaryValues)
	case dtLockAttribute:
		return a == nil || same(a.LockVersion.Attribute, given.Attribute)
	case dtLockValue:
		return v == nil || same(v.Lock, given.Value)
	default:
		return true
	}
}

func (t *dataTableRec) fullLock(a *DataTableAttribute, rec *dtRecord, v *dtValue) *DataTableLockVersion {
	lv := &DataTableLockVersion{DataTable: t.Table.LockVersion.DataTable, Attribute: a.LockVersion.Attribute}
	if rec != nil {
		lv.PrimaryValues = rec.Lock
	}

	if v != nil {
		lv.Value = v.Lock
	}

	return lv
}

// touchValue bumps the lock versions a value change affects at the table's lock level.
func (t *dataTableRec) touchValue(s Stamp, a *DataTableAttribute, rec *dtRecord) {
	t.Table.Stamp, a.Stamp, rec.Stamp = s, s, s
	rec.Lock = newID()

	switch t.Table.ValueLockLevel {
	case dtLockTable:
		t.Table.LockVersion = &DataTableLockVersion{DataTable: newID()}
	case dtLockAttribute:
		a.LockVersion = &DataTableLockVersion{Attribute: newID()}
	}
}

func (t *dataTableRec) setValue(s Stamp, a *DataTableAttribute, rec *dtRecord, value string) *dtValue {
	v := dtValue{Stamp: s, Value: value, Lock: newID()}
	rec.Values[a.AttributeID] = v
	t.touchValue(s, a, rec)

	return &v
}

func batchFail(attr, msg string, pvs []PrimaryValue) DataTableBatchFailure {
	return DataTableBatchFailure{AttributeName: attr, Message: msg, PrimaryValues: pvs}
}

func (t *dataTableRec) createValue(s Stamp, in *DataTableValueIn) (*DataTableBatchSuccess, error) {
	tg, err := t.target(in.AttributeName, in.PrimaryValues, true)
	if err != nil {
		return nil, err
	}

	if tg.attr.Primary {
		return nil, invalidParamErr(
			"attribute %q is a primary attribute; its values are the primary values of the record",
			tg.attr.Name,
		)
	}

	if verr := checkDTValue(tg.attr, in.Value); verr != nil {
		return nil, invalidParamErr("%v", verr)
	}

	rec := t.findRecord(tg.primary)
	if rec != nil {
		if _, ok := rec.Values[tg.attr.AttributeID]; ok {
			return nil, duplicateErr("value of attribute", tg.attr.Name)
		}
	} else {
		t.Records = append(t.Records, dtRecord{
			ID: newID(), Primary: tg.primary, Values: map[string]dtValue{}, Lock: newID(), Stamp: s,
		})
		rec = &t.Records[len(t.Records)-1]
	}

	v := t.setValue(s, tg.attr, rec, in.Value)

	return &DataTableBatchSuccess{
		AttributeName: tg.attr.Name, RecordID: rec.ID, PrimaryValues: in.PrimaryValues,
		LockVersion: t.fullLock(tg.attr, rec, v),
	}, nil
}

func (t *dataTableRec) existing(attrName string, pvs []PrimaryValue) (*dtTarget, *dtRecord, *dtValue, error) {
	tg, err := t.target(attrName, pvs, false)
	if err != nil {
		return nil, nil, nil, err
	}

	rec := t.findRecord(tg.primary)
	if rec == nil {
		return nil, nil, nil, notFoundErr("data table record", attrName)
	}

	v, ok := rec.Values[tg.attr.AttributeID]
	if !ok {
		return nil, nil, nil, notFoundErr("value of attribute", attrName)
	}

	return tg, rec, &v, nil
}

func (t *dataTableRec) updateValue(s Stamp, in *DataTableValueIn) (*DataTableBatchSuccess, error) {
	tg, rec, old, err := t.existing(in.AttributeName, in.PrimaryValues)
	if err != nil {
		return nil, err
	}

	if !t.lockMatches(in.LockVersion, tg.attr, rec, old) {
		return nil, conditionErr("the lock version does not match the current one")
	}

	if verr := checkDTValue(tg.attr, in.Value); verr != nil {
		return nil, invalidParamErr("%v", verr)
	}

	v := t.setValue(s, tg.attr, rec, in.Value)

	return &DataTableBatchSuccess{
		AttributeName: tg.attr.Name, PrimaryValues: in.PrimaryValues, LockVersion: t.fullLock(tg.attr, rec, v),
	}, nil
}

func (t *dataTableRec) deleteValue(s Stamp, in *DataTableValueIDIn) (*DataTableBatchSuccess, error) {
	tg, rec, old, err := t.existing(in.AttributeName, in.PrimaryValues)
	if err != nil {
		return nil, err
	}

	if !t.lockMatches(in.LockVersion, tg.attr, rec, old) {
		return nil, conditionErr("the lock version does not match the current one")
	}

	delete(rec.Values, tg.attr.AttributeID)
	t.touchValue(s, tg.attr, rec)

	out := &DataTableBatchSuccess{
		AttributeName: tg.attr.Name, PrimaryValues: in.PrimaryValues, LockVersion: t.fullLock(tg.attr, rec, nil),
	}
	t.dropEmptyRecords()

	return out, nil
}

func (t *dataTableRec) primaryResponses(rec *dtRecord) []PrimaryValueResponse {
	attrs := t.primaryAttrs()
	out := make([]PrimaryValueResponse, 0, len(attrs))

	for _, a := range attrs {
		out = append(
			out,
			PrimaryValueResponse{AttributeID: a.AttributeID, AttributeName: a.Name, Value: rec.Primary[a.AttributeID]},
		)
	}

	return out
}

func (t *dataTableRec) describeValue(in *DataTableValueIDIn) (*DataTableBatchDescribed, error) {
	tg, err := t.target(in.AttributeName, in.PrimaryValues, false)
	if err != nil {
		return nil, err
	}

	rec := t.findRecord(tg.primary)
	if rec == nil {
		return nil, notFoundErr("data table record", in.AttributeName)
	}

	out := &DataTableBatchDescribed{
		AttributeID: tg.attr.AttributeID, AttributeName: tg.attr.Name, RecordID: rec.ID,
		PrimaryValues: t.primaryResponses(rec), Stamp: rec.Stamp,
	}

	if tg.attr.Primary {
		out.Value, out.LockVersion = rec.Primary[tg.attr.AttributeID], t.fullLock(tg.attr, rec, nil)

		return out, nil
	}

	v, ok := rec.Values[tg.attr.AttributeID]
	if !ok {
		return nil, notFoundErr("value of attribute", in.AttributeName)
	}

	out.Value, out.Stamp, out.LockVersion = v.Value, v.Stamp, t.fullLock(tg.attr, rec, &v)

	return out, nil
}

func runBatch[In, Out any](
	items []In, attrOf func(*In) (string, []PrimaryValue), do func(*In) (*Out, error),
) *DataTableBatchResult[Out] {
	res := &DataTableBatchResult[Out]{Successful: []Out{}, Failed: []DataTableBatchFailure{}}

	for i := range items {
		out, err := do(&items[i])
		if err != nil {
			name, pvs := attrOf(&items[i])
			res.Failed = append(res.Failed, batchFail(name, err.Error(), pvs))

			continue
		}

		res.Successful = append(res.Successful, *out)
	}

	return res
}

func writeBatch[In, Out any](
	b *Backend, op, instRef, tableRef string, items []In,
	attrOf func(*In) (string, []PrimaryValue), do func(*dataTableRec, *In) (*Out, error),
) (*DataTableBatchResult[Out], error) {
	if err := checkBatchSize(len(items)); err != nil {
		return nil, err
	}

	return writeInstance(b, op, instRef, func(inst string) (*DataTableBatchResult[Out], error) {
		t, err := b.tableLocked(inst, tableRef)
		if err != nil {
			return nil, err
		}

		return runBatch(items, attrOf, func(in *In) (*Out, error) { return do(t, in) }), nil
	})
}

func valueAttr(in *DataTableValueIn) (string, []PrimaryValue) {
	return in.AttributeName, in.PrimaryValues
}
func valueIDAttr(in *DataTableValueIDIn) (string, []PrimaryValue) {
	return in.AttributeName, in.PrimaryValues
}

// BatchCreateDataTableValue creates values; each item succeeds or fails on its own.
func (b *Backend) BatchCreateDataTableValue(
	ctx context.Context, in *batchValuesIn,
) (*DataTableBatchResult[DataTableBatchSuccess], error) {
	s := b.stamp(ctx)

	return writeBatch(b, "BatchCreateDataTableValue", in.InstanceID, in.TableID, in.Values, valueAttr,
		func(t *dataTableRec, v *DataTableValueIn) (*DataTableBatchSuccess, error) { return t.createValue(s, v) })
}

// BatchUpdateDataTableValue updates existing values, honoring the table's lock level.
func (b *Backend) BatchUpdateDataTableValue(
	ctx context.Context, in *batchValuesIn,
) (*DataTableBatchResult[DataTableBatchSuccess], error) {
	s := b.stamp(ctx)

	return writeBatch(b, "BatchUpdateDataTableValue", in.InstanceID, in.TableID, in.Values, valueAttr,
		func(t *dataTableRec, v *DataTableValueIn) (*DataTableBatchSuccess, error) { return t.updateValue(s, v) })
}

// BatchDeleteDataTableValue deletes values; a record without values disappears.
func (b *Backend) BatchDeleteDataTableValue(
	ctx context.Context, in *batchIDsIn,
) (*DataTableBatchResult[DataTableBatchSuccess], error) {
	s := b.stamp(ctx)

	return writeBatch(b, "BatchDeleteDataTableValue", in.InstanceID, in.TableID, in.Values, valueIDAttr,
		func(t *dataTableRec, v *DataTableValueIDIn) (*DataTableBatchSuccess, error) {
			return t.deleteValue(s, v)
		})
}

// BatchDescribeDataTableValue reads values by record and attribute.
func (b *Backend) BatchDescribeDataTableValue(
	in *batchIDsIn,
) (*DataTableBatchResult[DataTableBatchDescribed], error) {
	if err := checkBatchSize(len(in.Values)); err != nil {
		return nil, err
	}

	return readInstance(b, "BatchDescribeDataTableValue", in.InstanceID,
		func(inst string) (*DataTableBatchResult[DataTableBatchDescribed], error) {
			t, err := b.tableLocked(inst, in.TableID)
			if err != nil {
				return nil, err
			}

			return runBatch(in.Values, valueIDAttr,
				func(v *DataTableValueIDIn) (*DataTableBatchDescribed, error) { return t.describeValue(v) }), nil
		})
}

// DataTableValueSummary is the list shape of a value.
type DataTableValueSummary struct {
	Stamp
	LockVersion   *DataTableLockVersion  `json:"LockVersion,omitempty"`
	AttributeName string                 `json:"AttributeName"`
	AttributeID   string                 `json:"AttributeId"`
	RecordID      string                 `json:"RecordId"`
	Value         string                 `json:"Value"`
	ValueType     string                 `json:"ValueType"`
	PrimaryValues []PrimaryValueResponse `json:"PrimaryValues"`
}

// RecordPrimaryValue is the list shape of a record key.
type RecordPrimaryValue struct {
	Stamp
	RecordID      string                 `json:"RecordId"`
	PrimaryValues []PrimaryValueResponse `json:"PrimaryValues"`
}

// ListDataTableValuesInput is the ListDataTableValues and ListDataTablePrimaryValues request.
type ListDataTableValuesInput struct {
	dtRef
	NextToken              string `json:"-"         query:"nextToken"`
	PrimaryAttributeValues []PrimaryAttributeValueFilter
	RecordIDs              []string `json:"RecordIds"`
	MaxResults             int32    `json:"-"         query:"maxResults"`
}

func (t *dataTableRec) selectRecords(in *ListDataTableValuesInput) ([]*dtRecord, error) {
	filters := map[string][]string{}

	for _, f := range in.PrimaryAttributeValues {
		i := t.attrIndex(f.AttributeName)
		if i < 0 || !t.Attributes[i].Primary {
			return nil, invalidParamErr("attribute %q is not a primary attribute", f.AttributeName)
		}

		filters[t.Attributes[i].AttributeID] = f.Values
	}

	ids := in.RecordIDs
	if in.NextToken != "" {
		ids = nil
	}

	var out []*dtRecord

	for i := range t.Records {
		rec := &t.Records[i]
		if len(ids) > 0 && !slices.Contains(ids, rec.ID) {
			continue
		}

		ok := true
		for attrID, vals := range filters {
			ok = ok && slices.Contains(vals, rec.Primary[attrID])
		}

		if ok {
			out = append(out, rec)
		}
	}

	return out, nil
}

func listFromTable[T any](
	b *Backend, op string, in *ListDataTableValuesInput, conv func(*dataTableRec, []*dtRecord) []T,
) ([]T, string, error) {
	all, err := readInstance(b, op, in.InstanceID, func(inst string) ([]T, error) {
		t, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return nil, err
		}

		recs, err := t.selectRecords(in)
		if err != nil {
			return nil, err
		}

		return conv(t, recs), nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// ListDataTableValues pages through the values of a data table, optionally for selected records.
func (b *Backend) ListDataTableValues(in *ListDataTableValuesInput) ([]DataTableValueSummary, string, error) {
	return listFromTable(b, "ListDataTableValues", in, func(t *dataTableRec, recs []*dtRecord) []DataTableValueSummary {
		var out []DataTableValueSummary

		for _, rec := range recs {
			for i := range t.Attributes {
				a := &t.Attributes[i]

				v, ok := rec.Values[a.AttributeID]
				if !ok {
					continue
				}

				out = append(out, DataTableValueSummary{
					Stamp: v.Stamp, AttributeName: a.Name, AttributeID: a.AttributeID, RecordID: rec.ID, Value: v.Value,
					ValueType: a.ValueType, PrimaryValues: t.primaryResponses(rec), LockVersion: t.fullLock(a, rec, &v),
				})
			}
		}

		return out
	})
}

// ListDataTablePrimaryValues pages through the record keys of a data table.
func (b *Backend) ListDataTablePrimaryValues(in *ListDataTableValuesInput) ([]RecordPrimaryValue, string, error) {
	return listFromTable(
		b,
		"ListDataTablePrimaryValues",
		in,
		func(t *dataTableRec, recs []*dtRecord) []RecordPrimaryValue {
			out := make([]RecordPrimaryValue, 0, len(recs))

			for _, rec := range recs {
				out = append(
					out,
					RecordPrimaryValue{Stamp: rec.Stamp, RecordID: rec.ID, PrimaryValues: t.primaryResponses(rec)},
				)
			}

			return out
		},
	)
}

// UpdateDataTablePrimaryValuesInput is the UpdateDataTablePrimaryValues request.
type UpdateDataTablePrimaryValuesInput struct {
	LockVersion *DataTableLockVersion
	dtRef
	NewPrimaryValues []PrimaryValue
	PrimaryValues    []PrimaryValue
}

// UpdateDataTablePrimaryValues re-keys a record; its values move with it.
func (b *Backend) UpdateDataTablePrimaryValues(
	ctx context.Context, in *UpdateDataTablePrimaryValuesInput,
) (*lockOut, error) {
	return writeInstance(b, "UpdateDataTablePrimaryValues", in.InstanceID, func(inst string) (*lockOut, error) {
		t, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return nil, err
		}

		if len(t.primaryAttrs()) == 0 {
			return nil, invalidRequestErr("the data table has no primary attributes")
		}

		cur, err := t.resolvePrimary(in.PrimaryValues, false)
		if err != nil {
			return nil, err
		}

		next, err := t.resolvePrimary(in.NewPrimaryValues, true)
		if err != nil {
			return nil, err
		}

		rec := t.findRecord(cur)
		if rec == nil {
			return nil, notFoundErr("data table record", t.recordKey(cur))
		}

		if other := t.findRecord(next); other != nil && other != rec {
			return nil, duplicateErr("data table record", t.recordKey(next))
		}

		if !t.lockMatches(in.LockVersion, nil, rec, nil) {
			return nil, conditionErr("the lock version does not match the current one")
		}

		s := b.stamp(ctx)
		rec.Primary, rec.Lock, rec.Stamp = next, newID(), s
		t.touch(s)

		return &lockOut{
			LockVersion: &DataTableLockVersion{DataTable: t.Table.LockVersion.DataTable, PrimaryValues: rec.Lock},
		}, nil
	})
}

// DataTableEvaluationSet names the attributes to evaluate for one record.
type DataTableEvaluationSet struct {
	AttributeNames []string       `json:"AttributeNames"`
	PrimaryValues  []PrimaryValue `json:"PrimaryValues"`
}

// DataTableEvaluatedValue is one evaluated value.
type DataTableEvaluatedValue struct {
	EvaluatedValue *string        `json:"EvaluatedValue,omitempty"`
	AttributeName  string         `json:"AttributeName"`
	RecordID       string         `json:"RecordId,omitempty"`
	ValueType      string         `json:"ValueType,omitempty"`
	PrimaryValues  []PrimaryValue `json:"PrimaryValues"`
	Error          bool           `json:"Error"`
	Found          bool           `json:"Found"`
}

// EvaluateDataTableValuesInput is the EvaluateDataTableValues request.
type EvaluateDataTableValuesInput struct {
	dtRef
	TimeZone   string `json:"TimeZone"`
	NextToken  string `json:"-"        query:"nextToken"`
	Values     []DataTableEvaluationSet
	MaxResults int32 `json:"-"        query:"maxResults"`
}

func (t *dataTableRec) evaluate(set *DataTableEvaluationSet) []DataTableEvaluatedValue {
	names := set.AttributeNames
	if len(names) == 0 {
		for _, a := range t.Attributes {
			names = append(names, a.Name)
		}
	}

	primary, perr := t.resolvePrimary(set.PrimaryValues, false)
	out := make([]DataTableEvaluatedValue, 0, len(names))

	for _, name := range names {
		ev := DataTableEvaluatedValue{AttributeName: name, PrimaryValues: set.PrimaryValues}

		i := t.attrIndex(name)
		if i < 0 || perr != nil {
			ev.Error = true
			out = append(out, ev)

			continue
		}

		a := &t.Attributes[i]
		ev.ValueType = a.ValueType

		if rec := t.findRecord(primary); rec != nil {
			value, ok := rec.Values[a.AttributeID]
			if a.Primary {
				value, ok = dtValue{Value: rec.Primary[a.AttributeID]}, true
			}

			if ok {
				ev.Found, ev.RecordID, ev.EvaluatedValue = true, rec.ID, &value.Value
			}
		}

		out = append(out, ev)
	}

	return out
}

// EvaluateDataTableValues returns the current value of each requested attribute; values here are never expressions.
func (b *Backend) EvaluateDataTableValues(in *EvaluateDataTableValuesInput) ([]DataTableEvaluatedValue, string, error) {
	if in.TimeZone != "" {
		if _, err := time.LoadLocation(in.TimeZone); err != nil {
			return nil, "", invalidParamErr("TimeZone %q is not a valid time zone", in.TimeZone)
		}
	}

	all, err := readInstance(
		b,
		"EvaluateDataTableValues",
		in.InstanceID,
		func(inst string) ([]DataTableEvaluatedValue, error) {
			t, err := b.tableLocked(inst, in.TableID)
			if err != nil {
				return nil, err
			}

			var out []DataTableEvaluatedValue

			for i := range in.Values {
				out = append(out, t.evaluate(&in.Values[i])...)
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}
