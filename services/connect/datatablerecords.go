package connect

import (
	"context"
	"slices"
)

// DataTableRecordValue is one value of a record, addressed by attribute identifier.
type DataTableRecordValue struct {
	AttributeID string
	Value       string
}

// DataTableRecordInput is a whole record: its primary values and its other values.
type DataTableRecordInput struct {
	InstanceID    string
	TableID       string
	RecordID      string
	PrimaryValues []DataTableRecordValue
	Values        []DataTableRecordValue
}

func (t *dataTableRec) attrByID(id string) *DataTableAttribute {
	i := slices.IndexFunc(t.Attributes, func(a DataTableAttribute) bool { return a.AttributeID == id })
	if i < 0 {
		return nil
	}

	return &t.Attributes[i]
}

func (t *dataTableRec) recordByID(id string) *dtRecord {
	i := slices.IndexFunc(t.Records, func(r dtRecord) bool { return r.ID == id })
	if i < 0 {
		return nil
	}

	return &t.Records[i]
}

// recordPlan is a validated record write: nothing is mutated until every value checked out.
type recordPlan struct {
	primary map[string]string
	values  map[string]string
	attrs   map[string]*DataTableAttribute
}

func (t *dataTableRec) planRecord(in *DataTableRecordInput) (*recordPlan, error) {
	if len(in.Values) == 0 {
		return nil, invalidParamErr("Values must contain at least one value")
	}

	pvs := make([]PrimaryValue, 0, len(in.PrimaryValues))

	for _, p := range in.PrimaryValues {
		a := t.attrByID(p.AttributeID)
		if a == nil {
			return nil, notFoundErr(dtLabelAttribute, p.AttributeID)
		}

		pvs = append(pvs, PrimaryValue{AttributeName: a.Name, Value: p.Value})
	}

	primary, err := t.resolvePrimary(pvs, true)
	if err != nil {
		return nil, err
	}

	plan := &recordPlan{
		primary: primary, values: map[string]string{}, attrs: map[string]*DataTableAttribute{},
	}

	for _, v := range in.Values {
		a := t.attrByID(v.AttributeID)
		if a == nil {
			return nil, notFoundErr(dtLabelAttribute, v.AttributeID)
		}

		if a.Primary {
			return nil, invalidParamErr("attribute %q is a primary attribute; give it in PrimaryValues", a.Name)
		}

		if _, dup := plan.values[a.AttributeID]; dup {
			return nil, invalidParamErr("the value of attribute %q is given twice", a.Name)
		}

		if verr := checkDTValue(a, v.Value); verr != nil {
			return nil, invalidParamErr("%v", verr)
		}

		plan.values[a.AttributeID], plan.attrs[a.AttributeID] = v.Value, a
	}

	return plan, nil
}

// PutDataTableRecord creates a record, or with RecordID replaces the primary values and the values of that record;
// values absent from the request are removed. It returns the record identifier.
func (b *Backend) PutDataTableRecord(ctx context.Context, in *DataTableRecordInput) (string, error) {
	return writeInstance(b, "PutDataTableRecord", in.InstanceID, func(inst string) (string, error) {
		t, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return "", err
		}

		plan, err := t.planRecord(in)
		if err != nil {
			return "", err
		}

		other := t.findRecord(plan.primary)

		var rec *dtRecord

		if in.RecordID == "" {
			if other != nil {
				return "", duplicateErr("data table record", t.recordKey(plan.primary))
			}

			s := b.stamp(ctx)
			t.Records = append(t.Records, dtRecord{
				ID: newID(), Primary: plan.primary, Values: map[string]dtValue{}, Lock: newID(), Stamp: s,
			})
			rec = &t.Records[len(t.Records)-1]
		} else {
			if rec = t.recordByID(in.RecordID); rec == nil {
				return "", notFoundErr("data table record", in.RecordID)
			}

			if other != nil && other != rec {
				return "", duplicateErr("data table record", t.recordKey(plan.primary))
			}

			rec.Primary, rec.Values = plan.primary, map[string]dtValue{}
		}

		s := b.stamp(ctx)
		for id, value := range plan.values {
			t.setValue(s, plan.attrs[id], rec, value)
		}

		return rec.ID, nil
	})
}

// DeleteDataTableRecord removes a record with all its values.
func (b *Backend) DeleteDataTableRecord(ctx context.Context, inst, tableRef, recordID string) error {
	_, err := writeInstance(b, "DeleteDataTableRecord", inst, func(inst string) (struct{}, error) {
		t, err := b.tableLocked(inst, tableRef)
		if err != nil {
			return struct{}{}, err
		}

		if t.recordByID(recordID) == nil {
			return struct{}{}, notFoundErr("data table record", recordID)
		}

		t.Records = slices.DeleteFunc(t.Records, func(r dtRecord) bool { return r.ID == recordID })
		t.touch(b.stamp(ctx))

		return struct{}{}, nil
	})

	return err
}
