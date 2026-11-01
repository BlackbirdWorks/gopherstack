package gcpfirestore

import (
	"math"
	"slices"
	"time"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// overlay stages the effect of one atomic batch of writes on top of the stored documents.
type overlay struct {
	b          *InMemoryBackend
	staged     map[string]*firestorepb.Document
	commitTime time.Time
	order      []string
}

func (o *overlay) get(name string) *firestorepb.Document {
	if d, ok := o.staged[name]; ok {
		return d
	}

	if rec := o.b.docs[name]; rec != nil {
		return rec.cur()
	}

	return nil
}

func (o *overlay) put(name string, d *firestorepb.Document) {
	if _, ok := o.staged[name]; !ok {
		o.order = append(o.order, name)
	}

	o.staged[name] = d
}

func checkPrecondition(name string, cur *firestorepb.Document, pre *firestorepb.Precondition) error {
	if pre == nil {
		return nil
	}

	switch c := pre.GetConditionType().(type) {
	case *firestorepb.Precondition_Exists:
		switch {
		case c.Exists && cur == nil:
			return errNotFound("No document to update: %s", name)
		case !c.Exists && cur != nil:
			return errExists(name)
		}
	case *firestorepb.Precondition_UpdateTime:
		if cur == nil {
			return errNotFound("No document to update: %s", name)
		}

		if !cur.GetUpdateTime().AsTime().Equal(c.UpdateTime.AsTime()) {
			return errPrecondition("The stored version (%s) does not match the required base version (%s).",
				cur.GetUpdateTime().AsTime().Format(time.RFC3339Nano), c.UpdateTime.AsTime().Format(time.RFC3339Nano))
		}
	}

	return nil
}

// applyWrite stages one write and returns its WriteResult.
func (o *overlay) applyWrite(w *firestorepb.Write) (*firestorepb.WriteResult, error) {
	switch op := w.GetOperation().(type) {
	case *firestorepb.Write_Update:
		return o.applyUpdate(op.Update, w.GetUpdateMask(), w.GetUpdateTransforms(), w.GetCurrentDocument())
	case *firestorepb.Write_Delete:
		return o.applyDelete(op.Delete, w.GetCurrentDocument())
	case *firestorepb.Write_Transform:
		return o.applyUpdate(&firestorepb.Document{Name: op.Transform.GetDocument()},
			&firestorepb.DocumentMask{}, op.Transform.GetFieldTransforms(), w.GetCurrentDocument())
	}

	return nil, errInvalid("Write has no operation.")
}

func (o *overlay) applyDelete(name string, pre *firestorepb.Precondition) (*firestorepb.WriteResult, error) {
	if err := validateDocName(name); err != nil {
		return nil, err
	}

	cur := o.get(name)
	if err := checkPrecondition(name, cur, pre); err != nil {
		return nil, err
	}

	if cur != nil {
		o.put(name, nil)
	}

	return &firestorepb.WriteResult{UpdateTime: timestamppb.New(o.commitTime)}, nil
}

func (o *overlay) applyUpdate(
	in *firestorepb.Document,
	mask *firestorepb.DocumentMask,
	transforms []*firestorepb.DocumentTransform_FieldTransform,
	pre *firestorepb.Precondition,
) (*firestorepb.WriteResult, error) {
	name := in.GetName()
	if err := validateDocName(name); err != nil {
		return nil, err
	}

	cur := o.get(name)
	if err := checkPrecondition(name, cur, pre); err != nil {
		return nil, err
	}

	fields, err := mergeFields(cur, in.GetFields(), mask)
	if err != nil {
		return nil, err
	}

	results, err := applyTransforms(fields, transforms, o.commitTime)
	if err != nil {
		return nil, err
	}

	created := o.commitTime
	if cur != nil {
		created = cur.GetCreateTime().AsTime()
	}

	next := &firestorepb.Document{
		Name:       name,
		Fields:     fields,
		CreateTime: timestamppb.New(created),
		UpdateTime: timestamppb.New(o.commitTime),
	}

	if cur != nil && proto.Equal(&firestorepb.Value{ValueType: &firestorepb.Value_MapValue{
		MapValue: &firestorepb.MapValue{Fields: cur.GetFields()},
	}}, mapVal(fields)) {
		next.UpdateTime = cur.GetUpdateTime()
	}

	o.put(name, next)

	return &firestorepb.WriteResult{UpdateTime: next.GetUpdateTime(), TransformResults: results}, nil
}

// mergeFields builds the new field map: a replacement without a mask, otherwise only the masked paths.
func mergeFields(
	cur *firestorepb.Document,
	in map[string]*firestorepb.Value,
	mask *firestorepb.DocumentMask,
) (map[string]*firestorepb.Value, error) {
	norm, err := normalizeFields(in)
	if err != nil {
		return nil, err
	}

	if mask == nil {
		return norm, nil
	}

	out := map[string]*firestorepb.Value{}
	if cur != nil {
		out = cloneDoc(cur).GetFields()
	}

	for _, p := range mask.GetFieldPaths() {
		path, perr := parseFieldPath(p)
		if perr != nil {
			return nil, perr
		}

		if v, ok := getPath(norm, path); ok {
			setPath(out, path, v)
		} else {
			deletePath(out, path)
		}
	}

	return out, nil
}

func applyTransforms(
	fields map[string]*firestorepb.Value,
	transforms []*firestorepb.DocumentTransform_FieldTransform,
	commit time.Time,
) ([]*firestorepb.Value, error) {
	if len(transforms) == 0 {
		return nil, nil
	}

	results := make([]*firestorepb.Value, 0, len(transforms))

	for _, ft := range transforms {
		path, err := parseFieldPath(ft.GetFieldPath())
		if err != nil {
			return nil, err
		}

		old, _ := getPath(fields, path)
		res, nv, err := transformValue(ft, old, commit)
		if err != nil {
			return nil, err
		}

		setPath(fields, path, nv)
		results = append(results, res)
	}

	return results, nil
}

// transformValue returns the transform result reported to the client and the value to store.
func transformValue(
	ft *firestorepb.DocumentTransform_FieldTransform,
	old *firestorepb.Value,
	commit time.Time,
) (*firestorepb.Value, *firestorepb.Value, error) {
	switch t := ft.GetTransformType().(type) {
	case *firestorepb.DocumentTransform_FieldTransform_SetToServerValue:
		if t.SetToServerValue != firestorepb.DocumentTransform_FieldTransform_REQUEST_TIME {
			return nil, nil, errInvalid("Unsupported server value %v.", t.SetToServerValue)
		}

		v := tsVal(commit)

		return v, v, nil
	case *firestorepb.DocumentTransform_FieldTransform_Increment:
		v := addNumbers(old, t.Increment)

		return v, v, nil
	case *firestorepb.DocumentTransform_FieldTransform_Maximum:
		v := extremum(old, t.Maximum, 1)

		return v, v, nil
	case *firestorepb.DocumentTransform_FieldTransform_Minimum:
		v := extremum(old, t.Minimum, -1)

		return v, v, nil
	case *firestorepb.DocumentTransform_FieldTransform_AppendMissingElements:
		nv, err := appendMissing(old, t.AppendMissingElements.GetValues())

		return nullVal(), nv, err
	case *firestorepb.DocumentTransform_FieldTransform_RemoveAllFromArray:
		return nullVal(), removeAll(old, t.RemoveAllFromArray.GetValues()), nil
	}

	return nil, nil, errInvalid("Field transform has no type.")
}

func addNumbers(old, delta *firestorepb.Value) *firestorepb.Value {
	if !isNumber(old) {
		return delta
	}

	oi, oInt := old.GetValueType().(*firestorepb.Value_IntegerValue)
	di, dInt := delta.GetValueType().(*firestorepb.Value_IntegerValue)

	if oInt && dInt {
		sum := oi.IntegerValue + di.IntegerValue
		if (di.IntegerValue > 0 && sum < oi.IntegerValue) || (di.IntegerValue < 0 && sum > oi.IntegerValue) {
			if di.IntegerValue > 0 {
				return intVal(math.MaxInt64)
			}

			return intVal(math.MinInt64)
		}

		return intVal(sum)
	}

	return doubleVal(numberFloat(old) + numberFloat(delta))
}

func numberFloat(v *firestorepb.Value) float64 {
	if i, ok := v.GetValueType().(*firestorepb.Value_IntegerValue); ok {
		return float64(i.IntegerValue)
	}

	return v.GetDoubleValue()
}

// extremum implements maximum (dir 1) and minimum (dir -1); NaN wins either way and ties keep the stored value.
func extremum(old, operand *firestorepb.Value, dir int) *firestorepb.Value {
	if !isNumber(old) {
		return operand
	}

	if isNaN(old) {
		return old
	}

	if isNaN(operand) {
		return operand
	}

	if compareNumbers(operand, old)*dir > 0 {
		return operand
	}

	return old
}

func arrayOf(v *firestorepb.Value) []*firestorepb.Value {
	if _, ok := v.GetValueType().(*firestorepb.Value_ArrayValue); ok {
		return v.GetArrayValue().GetValues()
	}

	return nil
}

func containsValue(vs []*firestorepb.Value, v *firestorepb.Value) bool {
	return slices.ContainsFunc(
		vs,
		func(e *firestorepb.Value) bool { return valuesEqual(e, v) || compareValues(e, v) == 0 },
	)
}

func appendMissing(old *firestorepb.Value, add []*firestorepb.Value) (*firestorepb.Value, error) {
	out := slices.Clone(arrayOf(old))

	for _, a := range add {
		na, err := normalizeValue(a, 1)
		if err != nil {
			return nil, err
		}

		if !containsValue(out, na) {
			out = append(out, na)
		}
	}

	return arrayVal(out), nil
}

func removeAll(old *firestorepb.Value, rm []*firestorepb.Value) *firestorepb.Value {
	out := make([]*firestorepb.Value, 0)

	for _, e := range arrayOf(old) {
		if !containsValue(rm, e) {
			out = append(out, e)
		}
	}

	return arrayVal(out)
}

// commitLocked applies writes atomically at a fresh commit time.
func (b *InMemoryBackend) commitLocked(writes []*firestorepb.Write) ([]*firestorepb.WriteResult, time.Time, error) {
	ct := b.wallLocked()
	if !ct.After(b.lastCommit) {
		ct = b.lastCommit.Add(time.Microsecond)
	}

	o := &overlay{b: b, staged: map[string]*firestorepb.Document{}, commitTime: ct}
	results := make([]*firestorepb.WriteResult, 0, len(writes))

	for _, w := range writes {
		r, err := o.applyWrite(w)
		if err != nil {
			return nil, time.Time{}, err
		}

		results = append(results, r)
	}

	b.lastCommit = ct

	for _, name := range o.order {
		b.storeLocked(name, o.staged[name], ct)
	}

	b.sweepLocked(ct)
	b.notifyLocked()

	return results, ct, nil
}

func (b *InMemoryBackend) storeLocked(name string, d *firestorepb.Document, ct time.Time) {
	rec := b.docs[name]
	if rec == nil {
		if d == nil {
			return
		}

		rec = &docRec{}
		b.docs[name] = rec
	}

	if cur := rec.cur(); cur != nil && d != nil && cur.GetUpdateTime().AsTime().Equal(d.GetUpdateTime().AsTime()) {
		return
	}

	if d == nil && rec.cur() == nil {
		return
	}

	old := rec.cur()
	rec.vers = append(rec.vers, docVer{at: ct, doc: d})
	rec.prune(ct)
	b.queueDocLocked(old, d, ct)
}

func cloneDoc(d *firestorepb.Document) *firestorepb.Document {
	c, _ := proto.Clone(d).(*firestorepb.Document)

	return c
}
