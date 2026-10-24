package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxDataTables      = 100
	maxDTDescription   = 250
	maxDTBatch         = 100
	dtStatusPublished  = "PUBLISHED"
	dtStatusSaved      = "SAVED"
	dtFalse            = "false"
	dtLockNone         = "NONE"
	dtLockTable        = "DATA_TABLE"
	dtLockPrimary      = "PRIMARY_VALUE"
	dtLockAttribute    = "ATTRIBUTE"
	dtLockValue        = "VALUE"
	dtTypeText         = "TEXT"
	dtTypeNumber       = "NUMBER"
	dtTypeBoolean      = "BOOLEAN"
	dtTypeTextList     = "TEXT_LIST"
	dtTypeNumberList   = "NUMBER_LIST"
	aliasLatest        = "$LATEST"
	aliasSaved         = "$SAVED"
	dtReservedConnect  = "connect:"
	dtReservedAWS      = "aws:"
	dtLabelTable       = "DataTable"
	dtLabelAttribute   = "DataTableAttribute"
	dtFieldDescription = "description"
)

func dtLockLevels() []string {
	return []string{dtLockNone, dtLockTable, dtLockPrimary, dtLockAttribute, dtLockValue}
}

func dtValueTypes() []string {
	return []string{dtTypeText, dtTypeNumber, dtTypeBoolean, dtTypeTextList, dtTypeNumberList}
}

func conditionErr(format string, args ...any) error {
	return apiErr(http.StatusConflict, "ConditionalOperationFailedException", format, args...)
}

// DataTableLockVersion carries the optimistic-locking versions of the levels of a data table.
type DataTableLockVersion struct {
	Attribute     string `json:"Attribute,omitempty"`
	DataTable     string `json:"DataTable,omitempty"`
	PrimaryValues string `json:"PrimaryValues,omitempty"`
	Value         string `json:"Value,omitempty"`
}

// DataTableValidationEnum is the Enum member of a validation.
type DataTableValidationEnum struct {
	Values []string `json:"Values,omitempty"`
	Strict bool     `json:"Strict"`
}

// DataTableValidation holds the validation rules of an attribute.
type DataTableValidation struct {
	Enum             *DataTableValidationEnum `json:"Enum,omitempty"`
	ExclusiveMaximum float64                  `json:"ExclusiveMaximum,omitempty"`
	ExclusiveMinimum float64                  `json:"ExclusiveMinimum,omitempty"`
	Maximum          float64                  `json:"Maximum,omitempty"`
	Minimum          float64                  `json:"Minimum,omitempty"`
	MultipleOf       float64                  `json:"MultipleOf,omitempty"`
	MaxLength        int32                    `json:"MaxLength,omitempty"`
	MaxValues        int32                    `json:"MaxValues,omitempty"`
	MinLength        int32                    `json:"MinLength,omitempty"`
	MinValues        int32                    `json:"MinValues,omitempty"`
	IgnoreCase       bool                     `json:"IgnoreCase,omitempty"`
}

// DataTable is a Connect data table.
type DataTable struct {
	Stamp
	Tags           map[string]string     `json:"Tags,omitempty"`
	LockVersion    *DataTableLockVersion `json:"LockVersion,omitempty"`
	CreatedTime    awstime.EpochTime     `json:"CreatedTime"`
	Arn            string                `json:"Arn"`
	ID             string                `json:"Id"`
	Name           string                `json:"Name"`
	TimeZone       string                `json:"TimeZone"`
	Description    string                `json:"Description,omitempty"`
	Status         string                `json:"Status"`
	ValueLockLevel string                `json:"ValueLockLevel"`
}

// DataTableSummary is the list shape of a data table.
type DataTableSummary struct {
	Stamp
	Arn  string `json:"Arn"`
	ID   string `json:"Id"`
	Name string `json:"Name"`
}

// DataTableAttribute is a column of a data table.
type DataTableAttribute struct {
	Stamp
	LockVersion  *DataTableLockVersion `json:"LockVersion,omitempty"`
	Validation   *DataTableValidation  `json:"Validation,omitempty"`
	Name         string                `json:"Name"`
	ValueType    string                `json:"ValueType"`
	AttributeID  string                `json:"AttributeId"`
	DataTableArn string                `json:"DataTableArn"`
	DataTableID  string                `json:"DataTableId"`
	Description  string                `json:"Description,omitempty"`
	Primary      bool                  `json:"Primary"`
}

type dtValue struct {
	Stamp
	Value string `json:"value"`
	Lock  string `json:"lock"`
}

type dtRecord struct {
	Stamp
	Primary map[string]string  `json:"primary"`
	Values  map[string]dtValue `json:"values"`
	ID      string             `json:"id"`
	Lock    string             `json:"lock"`
}

type dataTableRec struct {
	InstanceID string               `json:"instanceId"`
	Table      DataTable            `json:"table"`
	Attributes []DataTableAttribute `json:"attributes,omitempty"`
	Records    []dtRecord           `json:"records,omitempty"`
}

type dtRef struct {
	TableID string `json:"-" path:"DataTableId"`
	instPath
}

// dataTableID strips the ARN and the $LATEST/$SAVED alias from a data table identifier.
func dataTableID(ref string) (string, error) {
	id := ref
	if strings.HasPrefix(id, "arn:") {
		if i := strings.LastIndex(id, "/"); i >= 0 {
			id = id[i+1:]
		}
	}

	if base, alias, ok := strings.Cut(id, ":"); ok {
		if alias != aliasLatest && alias != aliasSaved {
			return "", invalidParamErr("DataTableId alias must be %s or %s", aliasLatest, aliasSaved)
		}

		id = base
	}

	return id, nil
}

func (b *Backend) tableLocked(inst, ref string) (*dataTableRec, error) {
	id, err := dataTableID(ref)
	if err != nil {
		return nil, err
	}

	return b.dataTables.get(inst, id)
}

func checkDTName(field, v string) error {
	if err := checkName(field, v, maxNameLen); err != nil {
		return err
	}

	l := strs.Fold(v)
	if strings.HasPrefix(l, dtReservedConnect) || strings.HasPrefix(l, dtReservedAWS) {
		return invalidParamErr("%s must not start with %q or %q", field, dtReservedConnect, dtReservedAWS)
	}

	return nil
}

func checkDTDescription(v string) error {
	if len(v) > maxDTDescription {
		return invalidParamErr("Description must be at most %d characters", maxDTDescription)
	}

	return nil
}

func (t *dataTableRec) attrIndex(name string) int {
	return slices.IndexFunc(t.Attributes, func(a DataTableAttribute) bool { return strs.Equal(a.Name, name) })
}

func (t *dataTableRec) primaryAttrs() []*DataTableAttribute {
	var out []*DataTableAttribute

	for i := range t.Attributes {
		if t.Attributes[i].Primary {
			out = append(out, &t.Attributes[i])
		}
	}

	return out
}

func (t *dataTableRec) clone() *dataTableRec {
	raw, _ := json.Marshal(t)

	var out dataTableRec

	_ = json.Unmarshal(raw, &out)

	return &out
}

func (t *dataTableRec) touch(s Stamp) {
	t.Table.Stamp = s
	t.Table.LockVersion = &DataTableLockVersion{DataTable: newID()}
}

func (t *dataTableRec) lockVersion(a *DataTableAttribute) *DataTableLockVersion {
	return &DataTableLockVersion{DataTable: t.Table.LockVersion.DataTable, Attribute: a.LockVersion.Attribute}
}

func emptyDTValue(valueType string) string {
	switch valueType {
	case dtTypeNumber:
		return "0"
	case dtTypeBoolean:
		return dtFalse
	case dtTypeTextList, dtTypeNumberList:
		return "[]"
	default:
		return ""
	}
}

func (v *DataTableValidation) rangeInverted() bool {
	return (v.MaxLength > 0 && v.MinLength > v.MaxLength) || (v.MaxValues > 0 && v.MinValues > v.MaxValues) ||
		(v.Maximum != 0 && v.Minimum > v.Maximum) || (v.ExclusiveMaximum != 0 && v.ExclusiveMinimum > v.ExclusiveMaximum)
}

func checkDTValidation(v *DataTableValidation) error {
	if v == nil {
		return nil
	}

	if v.MinLength < 0 || v.MaxLength < 0 || v.MinValues < 0 || v.MaxValues < 0 || v.MultipleOf < 0 {
		return invalidParamErr("Validation lengths, value counts and MultipleOf must not be negative")
	}

	if v.rangeInverted() {
		return invalidParamErr("Validation minimum is greater than its maximum")
	}

	if v.Enum != nil && len(v.Enum.Values) == 0 {
		return invalidParamErr("Validation Enum must list at least one value")
	}

	return nil
}

func checkDTType(valueType string) error { return oneOf("ValueType", valueType, dtValueTypes()...) }

// CreateDataTableInput is the CreateDataTable request.
type CreateDataTableInput struct {
	Tags           map[string]string
	Name           string
	TimeZone       string
	Status         string
	ValueLockLevel string
	Description    string
	instPath
}

type createDataTableOut struct {
	LockVersion *DataTableLockVersion `json:"LockVersion"`
	Arn         string                `json:"Arn"`
	ID          string                `json:"Id"`
}

// CreateDataTable creates a data table without attributes or values.
func (b *Backend) CreateDataTable(ctx context.Context, in *CreateDataTableInput) (*createDataTableOut, error) {
	name, desc := strings.TrimSpace(in.Name), strings.TrimSpace(in.Description)
	level := in.ValueLockLevel

	if level == "" {
		level = dtLockNone
	}

	if err := firstError(
		checkDTName("Name", name), checkDTDescription(desc), checkTimeZone(in.TimeZone), checkTags(in.Tags),
		oneOf("Status", in.Status, dtStatusPublished, dtStatusSaved), oneOf("ValueLockLevel", level, dtLockLevels()...),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateDataTable", in.InstanceID, func(inst string) (*createDataTableOut, error) {
		if nameExists(b.dataTables, inst, func(r *dataTableRec) string { return r.Table.Name }, name, "") {
			return nil, duplicateErr(dtLabelTable, name)
		}

		if len(b.dataTables.in(inst)) >= maxDataTables {
			return nil, limitErr("an instance can have at most %d data tables", maxDataTables)
		}

		id := newID()
		rec := &dataTableRec{InstanceID: inst, Table: DataTable{
			Arn: b.buildARN(ctx, "instance/"+inst+"/data-table/"+id), ID: id, Name: name, Description: desc,
			TimeZone: in.TimeZone, Status: in.Status, ValueLockLevel: level, Tags: cloneTags(in.Tags),
			CreatedTime: awstime.NewEpochTime(b.now()),
		}}
		rec.touch(b.stamp(ctx))
		b.dataTables.put(rec)

		return &createDataTableOut{Arn: rec.Table.Arn, ID: id, LockVersion: rec.Table.LockVersion}, nil
	})
}

func (t *dataTableRec) view() DataTable {
	out := t.Table
	out.Tags = cloneTags(out.Tags)
	lv := *out.LockVersion
	out.LockVersion = &lv

	return out
}

// DescribeDataTable returns a data table.
func (b *Backend) DescribeDataTable(inst, ref string) (*DataTable, error) {
	return readInstance(b, "DescribeDataTable", inst, func(inst string) (*DataTable, error) {
		r, err := b.tableLocked(inst, ref)
		if err != nil {
			return nil, err
		}

		out := r.view()

		return &out, nil
	})
}

// ListDataTables pages through the data tables of an instance.
func (b *Backend) ListDataTables(inst, token string, maxResults int32) ([]DataTableSummary, string, error) {
	return listIn(b, "ListDataTables", inst, b.dataTables, func(r *dataTableRec) (DataTableSummary, bool) {
		return DataTableSummary{Stamp: r.Table.Stamp, Arn: r.Table.Arn, ID: r.Table.ID, Name: r.Table.Name}, true
	}, token, maxResults)
}

// UpdateDataTableMetadataInput is the UpdateDataTableMetadata request.
type UpdateDataTableMetadataInput struct {
	Name           *string
	TimeZone       *string
	ValueLockLevel *string
	Description    *string
	dtRef
}

type lockOut struct {
	LockVersion *DataTableLockVersion `json:"LockVersion"`
}

// UpdateDataTableMetadata changes the supplied properties of a data table.
func (b *Backend) UpdateDataTableMetadata(ctx context.Context, in *UpdateDataTableMetadataInput) (*lockOut, error) {
	return writeInstance(b, "UpdateDataTableMetadata", in.InstanceID, func(inst string) (*lockOut, error) {
		r, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return nil, err
		}

		next := r.Table

		if in.Name != nil {
			name := strings.TrimSpace(*in.Name)
			if nerr := checkDTName("Name", name); nerr != nil {
				return nil, nerr
			}

			if nameExists(b.dataTables, inst, func(x *dataTableRec) string { return x.Table.Name }, name, r.Table.ID) {
				return nil, duplicateErr(dtLabelTable, name)
			}

			next.Name = name
		}

		if in.Description != nil {
			next.Description = strings.TrimSpace(*in.Description)
		}

		if in.TimeZone != nil {
			next.TimeZone = *in.TimeZone
		}

		if in.ValueLockLevel != nil {
			next.ValueLockLevel = *in.ValueLockLevel
		}

		if verr := firstError(
			checkDTDescription(next.Description), checkTimeZone(next.TimeZone),
			oneOf("ValueLockLevel", next.ValueLockLevel, dtLockLevels()...),
		); verr != nil {
			return nil, verr
		}

		r.Table = next
		r.touch(b.stamp(ctx))

		return &lockOut{LockVersion: r.Table.LockVersion}, nil
	})
}

// DeleteDataTable removes a data table with its attributes and values.
func (b *Backend) DeleteDataTable(inst, ref string) (*empty, error) {
	return writeInstance(b, "DeleteDataTable", inst, func(inst string) (*empty, error) {
		r, err := b.tableLocked(inst, ref)
		if err != nil {
			return nil, err
		}

		return nil, b.dataTables.del(inst, r.Table.ID)
	})
}

// SearchDataTables searches data tables by name, description and tags.
func (b *Backend) SearchDataTables(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchDataTables", "DataTables", in, b.dataTables, nameDescSpec(),
		func(r *dataTableRec) searchSubject {
			return searchSubject{fields: nameDesc(r.Table.Name, r.Table.Description), tags: r.Table.Tags}
		},
		func(r *dataTableRec) DataTable { return r.view() })
}

// CreateDataTableAttributeInput is the CreateDataTableAttribute request.
type CreateDataTableAttributeInput struct {
	Validation  *DataTableValidation
	Name        string
	ValueType   string
	Description string
	dtRef
	Primary bool
}

type createAttributeOut struct {
	LockVersion *DataTableLockVersion `json:"LockVersion"`
	Name        string                `json:"Name"`
	AttributeID string                `json:"AttributeId"`
}

func (b *Backend) checkAttribute(name, valueType, desc string, v *DataTableValidation) error {
	return firstError(checkDTName("Name", name), checkDTDescription(desc), checkDTType(valueType),
		checkDTValidation(v))
}

// CreateDataTableAttribute adds an attribute; a new primary attribute gives existing records its empty value.
func (b *Backend) CreateDataTableAttribute(
	ctx context.Context,
	in *CreateDataTableAttributeInput,
) (*createAttributeOut, error) {
	name, desc := strings.TrimSpace(in.Name), strings.TrimSpace(in.Description)
	if err := b.checkAttribute(name, in.ValueType, desc, in.Validation); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateDataTableAttribute", in.InstanceID, func(inst string) (*createAttributeOut, error) {
		r, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return nil, err
		}

		if r.attrIndex(name) >= 0 {
			return nil, duplicateErr(dtLabelAttribute, name)
		}

		stamp := b.stamp(ctx)
		attr := DataTableAttribute{
			Stamp: stamp, Name: name, ValueType: in.ValueType, AttributeID: newID(), DataTableArn: r.Table.Arn,
			DataTableID: r.Table.ID, Description: desc, Primary: in.Primary, Validation: in.Validation,
			LockVersion: &DataTableLockVersion{Attribute: newID()},
		}
		r.Attributes = append(r.Attributes, attr)

		if attr.Primary {
			for i := range r.Records {
				r.Records[i].Primary[attr.AttributeID] = emptyDTValue(attr.ValueType)
				r.Records[i].Lock = newID()
			}
		}

		r.touch(stamp)

		return &createAttributeOut{Name: name, AttributeID: attr.AttributeID, LockVersion: r.lockVersion(&attr)}, nil
	})
}

// DescribeDataTableAttribute returns one attribute by name.
func (b *Backend) DescribeDataTableAttribute(inst, ref, name string) (*DataTableAttribute, error) {
	return readInstance(b, "DescribeDataTableAttribute", inst, func(inst string) (*DataTableAttribute, error) {
		r, err := b.tableLocked(inst, ref)
		if err != nil {
			return nil, err
		}

		i := r.attrIndex(name)
		if i < 0 {
			return nil, notFoundErr(dtLabelAttribute, name)
		}

		out := r.Attributes[i]

		return &out, nil
	})
}

// ListDataTableAttributesInput is the ListDataTableAttributes request.
type ListDataTableAttributesInput struct {
	dtRef
	NextToken    string   `json:"-"            query:"nextToken"`
	AttributeIDs []string `json:"AttributeIds"`
	MaxResults   int32    `json:"-"            query:"maxResults"`
}

// ListDataTableAttributes pages through the attributes of a data table in creation order.
func (b *Backend) ListDataTableAttributes(in *ListDataTableAttributesInput) ([]DataTableAttribute, string, error) {
	all, err := readInstance(
		b,
		"ListDataTableAttributes",
		in.InstanceID,
		func(inst string) ([]DataTableAttribute, error) {
			r, err := b.tableLocked(inst, in.TableID)
			if err != nil {
				return nil, err
			}

			var out []DataTableAttribute

			for _, a := range r.Attributes {
				if len(in.AttributeIDs) == 0 || slices.Contains(in.AttributeIDs, a.AttributeID) {
					out = append(out, a)
				}
			}

			return out, nil
		},
	)
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// UpdateDataTableAttributeInput is the UpdateDataTableAttribute request; it replaces every property.
type UpdateDataTableAttributeInput struct {
	Validation    *DataTableValidation
	AttributeName string `json:"-" path:"AttributeName"`
	Name          string
	ValueType     string
	Description   string
	dtRef
	Primary bool
}

type updateAttributeOut struct {
	LockVersion *DataTableLockVersion `json:"LockVersion"`
	Name        string                `json:"Name"`
}

// UpdateDataTableAttribute replaces the properties of an attribute, re-keying records when Primary changes.
func (b *Backend) UpdateDataTableAttribute(
	ctx context.Context,
	in *UpdateDataTableAttributeInput,
) (*updateAttributeOut, error) {
	name, desc := strings.TrimSpace(in.Name), strings.TrimSpace(in.Description)
	if err := b.checkAttribute(name, in.ValueType, desc, in.Validation); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateDataTableAttribute", in.InstanceID, func(inst string) (*updateAttributeOut, error) {
		r, err := b.tableLocked(inst, in.TableID)
		if err != nil {
			return nil, err
		}

		i := r.attrIndex(in.AttributeName)
		if i < 0 {
			return nil, notFoundErr(dtLabelAttribute, in.AttributeName)
		}

		if j := r.attrIndex(name); j >= 0 && j != i {
			return nil, duplicateErr(dtLabelAttribute, name)
		}

		next := r.clone()
		a := &next.Attributes[i]
		wasPrimary := a.Primary
		a.Name, a.ValueType, a.Description, a.Validation, a.Primary = name, in.ValueType, desc, in.Validation, in.Primary

		if wasPrimary != in.Primary {
			if kerr := next.rekey(a, in.Primary); kerr != nil {
				return nil, kerr
			}
		}

		stamp := b.stamp(ctx)
		a.Stamp, a.LockVersion = stamp, &DataTableLockVersion{Attribute: newID()}
		next.touch(stamp)
		b.dataTables.put(next)

		return &updateAttributeOut{Name: name, LockVersion: next.lockVersion(a)}, nil
	})
}

// rekey moves attribute a in or out of the record key and fails when two records would then collide.
func (t *dataTableRec) rekey(a *DataTableAttribute, nowPrimary bool) error {
	for i := range t.Records {
		rec := &t.Records[i]

		if nowPrimary {
			v, ok := rec.Values[a.AttributeID]
			if !ok {
				v.Value = emptyDTValue(a.ValueType)
			}

			delete(rec.Values, a.AttributeID)
			rec.Primary[a.AttributeID] = v.Value
		} else {
			rec.Values[a.AttributeID] = dtValue{Stamp: rec.Stamp, Value: rec.Primary[a.AttributeID], Lock: newID()}
			delete(rec.Primary, a.AttributeID)
		}

		rec.Lock = newID()
	}

	return t.checkUnique()
}

func (t *dataTableRec) checkUnique() error {
	seen := map[string]bool{}

	for i := range t.Records {
		k := t.recordKey(t.Records[i].Primary)
		if seen[k] {
			return apiErr(http.StatusConflict, "ResourceConflictException",
				"the primary values of two records would become identical")
		}

		seen[k] = true
	}

	return nil
}

func (t *dataTableRec) recordKey(primary map[string]string) string {
	var sb strings.Builder

	for _, a := range t.primaryAttrs() {
		v := primary[a.AttributeID]
		fmt.Fprintf(&sb, "%d:%s|", len(v), v)
	}

	return sb.String()
}

// DeleteDataTableAttribute removes an attribute with its values.
func (b *Backend) DeleteDataTableAttribute(ctx context.Context, inst, ref, name string) (*lockOut, error) {
	return writeInstance(b, "DeleteDataTableAttribute", inst, func(inst string) (*lockOut, error) {
		r, err := b.tableLocked(inst, ref)
		if err != nil {
			return nil, err
		}

		i := r.attrIndex(name)
		if i < 0 {
			return nil, notFoundErr(dtLabelAttribute, name)
		}

		next := r.clone()
		gone := next.Attributes[i]
		next.Attributes = slices.Delete(next.Attributes, i, i+1)

		for j := range next.Records {
			rec := &next.Records[j]
			delete(rec.Primary, gone.AttributeID)
			delete(rec.Values, gone.AttributeID)
		}

		if gone.Primary {
			if uerr := next.checkUnique(); uerr != nil {
				return nil, uerr
			}
		}

		next.dropEmptyRecords()
		next.touch(b.stamp(ctx))
		b.dataTables.put(next)

		return &lockOut{LockVersion: &DataTableLockVersion{DataTable: next.Table.LockVersion.DataTable}}, nil
	})
}

func (t *dataTableRec) dropEmptyRecords() {
	t.Records = slices.DeleteFunc(t.Records, func(r dtRecord) bool { return len(r.Values) == 0 })
}

type dtValueError struct{ msg string }

func (e *dtValueError) Error() string { return e.msg }

func valueErr(format string, args ...any) error {
	return &dtValueError{msg: fmt.Sprintf(format, args...)}
}

func trimmedFloat(v string) (float64, bool) {
	f, err := strconv.ParseFloat(strings.TrimSpace(v), 64)

	return f, err == nil && !math.IsNaN(f) && !math.IsInf(f, 0)
}

func checkDTNumber(v *DataTableValidation, f float64) error {
	switch {
	case v == nil:
		return nil
	case v.Maximum != 0 && f > v.Maximum:
		return valueErr("%v is greater than the maximum %v", f, v.Maximum)
	case v.Minimum != 0 && f < v.Minimum:
		return valueErr("%v is less than the minimum %v", f, v.Minimum)
	case v.ExclusiveMaximum != 0 && f >= v.ExclusiveMaximum:
		return valueErr("%v is not less than %v", f, v.ExclusiveMaximum)
	case v.ExclusiveMinimum != 0 && f <= v.ExclusiveMinimum:
		return valueErr("%v is not greater than %v", f, v.ExclusiveMinimum)
	case v.MultipleOf != 0 && math.Abs(math.Remainder(f, v.MultipleOf)) > 1e-9:
		return valueErr("%v is not a multiple of %v", f, v.MultipleOf)
	}

	return checkDTEnum(v, strconv.FormatFloat(f, 'f', -1, 64), true)
}

func checkDTEnum(v *DataTableValidation, s string, numeric bool) error {
	if v == nil || v.Enum == nil || !v.Enum.Strict {
		return nil
	}

	for _, e := range v.Enum.Values {
		if numeric {
			if ef, ok := trimmedFloat(e); ok && strconv.FormatFloat(ef, 'f', -1, 64) == s {
				return nil
			}
		} else if e == s || (v.IgnoreCase && strs.Equal(e, s)) {
			return nil
		}
	}

	return valueErr("%q is not one of the enum values", s)
}

func checkDTText(v *DataTableValidation, s string) error {
	if v == nil {
		return nil
	}

	n := int32(min(len(s), math.MaxInt32))

	if v.MaxLength > 0 && n > v.MaxLength {
		return valueErr("value is longer than %d characters", v.MaxLength)
	}

	if n < v.MinLength {
		return valueErr("value is shorter than %d characters", v.MinLength)
	}

	return checkDTEnum(v, s, false)
}

func checkDTList(a *DataTableAttribute, s string) error {
	var items []json.RawMessage
	if err := json.Unmarshal([]byte(s), &items); err != nil {
		return valueErr("value must be a JSON array")
	}

	if v := a.Validation; v != nil {
		n := int32(min(len(items), math.MaxInt32))
		if v.MaxValues > 0 && n > v.MaxValues {
			return valueErr("at most %d items are allowed", v.MaxValues)
		}

		if n < v.MinValues {
			return valueErr("value has fewer than %d items", v.MinValues)
		}
	}

	for _, raw := range items {
		if a.ValueType == dtTypeNumberList {
			var f float64
			if json.Unmarshal(raw, &f) != nil {
				return valueErr("list item %s is not a number", raw)
			}

			if err := checkDTNumber(a.Validation, f); err != nil {
				return err
			}

			continue
		}

		var str string
		if json.Unmarshal(raw, &str) != nil {
			return valueErr("list item %s is not text", raw)
		}

		if err := checkDTText(a.Validation, str); err != nil {
			return err
		}
	}

	return nil
}

// checkDTValue validates a value against the type and validation rules of its attribute.
func checkDTValue(a *DataTableAttribute, s string) error {
	switch a.ValueType {
	case dtTypeNumber:
		f, ok := trimmedFloat(s)
		if !ok {
			return valueErr("%q is not a number", s)
		}

		return checkDTNumber(a.Validation, f)
	case dtTypeBoolean:
		if s != "true" && s != dtFalse {
			return valueErr("%q is not a boolean", s)
		}

		return nil
	case dtTypeTextList, dtTypeNumberList:
		return checkDTList(a, s)
	default:
		return checkDTText(a.Validation, s)
	}
}
