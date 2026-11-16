package gcpbigquery

import (
	"context"
	"net/http"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	litRoutineType  = "routineType"
	litArguments    = "arguments"
	litTypeKind     = "typeKind"
	litDataType     = "dataType"
	litRoutineID    = "routineId"
	litName         = "name"
	litLastModified = "lastModifiedTime"
	litDatasetID    = "datasetId"
)

// Routine is a stored function or procedure; Resource holds the writable REST fields.
type Routine struct {
	Resource map[string]any `json:"resource"`
	Created  int64          `json:"created"`
	Modified int64          `json:"modified"`
	Seq      int64          `json:"seq"`
}

func routineManaged() map[string]bool {
	return map[string]bool{
		litEtag: true, "routineReference": true, litCreationtime: true, litLastModified: true, "buildStatus": true,
	}
}

func (d *Dataset) routine(id string) (*Routine, string, bool) {
	if r, ok := d.Routines[id]; ok {
		return r, id, true
	}

	for k, r := range d.Routines {
		if strings.EqualFold(k, id) {
			return r, k, true
		}
	}

	return nil, "", false
}

func stdFromType(t bqsql.Type) map[string]any {
	switch t.Kind {
	case bqsql.KindArray:
		out := map[string]any{litTypeKind: bqsql.KindArray.SQLName()}
		if t.Elem != nil {
			out["arrayElementType"] = stdFromType(*t.Elem)
		}

		return out
	case bqsql.KindStruct:
		fields := make([]any, len(t.Fields))
		for i, f := range t.Fields {
			fields[i] = map[string]any{litName: f.Name, litType: stdFromType(f.Type)}
		}

		return map[string]any{litTypeKind: bqsql.KindStruct.SQLName(), "structType": map[string]any{"fields": fields}}
	case bqsql.KindUnknown:
		return map[string]any{litTypeKind: "TYPE_KIND_UNSPECIFIED"}
	case bqsql.KindInt, bqsql.KindFloat, bqsql.KindBool, bqsql.KindString, bqsql.KindBytes, bqsql.KindTimestamp,
		bqsql.KindDate, bqsql.KindTime, bqsql.KindDatetime, bqsql.KindNumeric, bqsql.KindBigNumeric, bqsql.KindJSON,
		bqsql.KindGeography, bqsql.KindInterval:
	}

	return map[string]any{litTypeKind: t.Kind.SQLName()}
}

func typeFromStd(raw any) (bqsql.Type, error) {
	m, _ := raw.(map[string]any)
	kind, _ := m[litTypeKind].(string)

	switch kind {
	case bqsql.KindArray.SQLName():
		elem, err := typeFromStd(m["arrayElementType"])
		if err != nil {
			return bqsql.Type{}, err
		}

		return bqsql.Type{Kind: bqsql.KindArray, Elem: &elem}, nil
	case bqsql.KindStruct.SQLName():
		st, _ := m["structType"].(map[string]any)
		list, _ := st["fields"].([]any)
		out := bqsql.Type{Kind: bqsql.KindStruct}

		for _, f := range list {
			fm, _ := f.(map[string]any)
			name, _ := fm[litName].(string)

			ft, err := typeFromStd(fm[litType])
			if err != nil {
				return bqsql.Type{}, err
			}

			out.Fields = append(out.Fields, bqsql.Column{Name: name, Type: ft})
		}

		return out, nil
	}

	if k, ok := bqsql.KindOf(kind); ok && k != bqsql.KindStruct && kind != "" {
		return bqsql.Type{Kind: k}, nil
	}

	return bqsql.Type{}, errInvalid("Unsupported routine data type %q", kind)
}

func tableTypeFromStd(raw any) ([]bqsql.Column, error) {
	m, _ := raw.(map[string]any)
	list, _ := m["columns"].([]any)
	out := make([]bqsql.Column, 0, len(list))

	for _, c := range list {
		cm, _ := c.(map[string]any)
		name, _ := cm[litName].(string)

		t, err := typeFromStd(cm[litType])
		if err != nil {
			return nil, err
		}

		out = append(out, bqsql.Column{Name: name, Type: t})
	}

	return out, nil
}

func tableTypeToStd(cols []bqsql.Column) map[string]any {
	list := make([]any, len(cols))
	for i, c := range cols {
		list[i] = map[string]any{litName: c.Name, litType: stdFromType(c.Type)}
	}

	return map[string]any{"columns": list}
}

func routineArgs(res map[string]any) ([]bqsql.RoutineArg, error) {
	list, _ := res[litArguments].([]any)
	out := make([]bqsql.RoutineArg, 0, len(list))

	for _, item := range list {
		am, _ := item.(map[string]any)
		a := bqsql.RoutineArg{}
		a.Name, _ = am[litName].(string)
		a.Mode, _ = am["mode"].(string)

		var err error

		switch kind, _ := am["argumentKind"].(string); kind {
		case "ANY_TYPE":
			a.AnyType = true
		case "ANY_TABLE":
			a.AnyTable, a.IsTable = true, true
		case "FIXED_TABLE":
			a.IsTable = true
			a.Table, err = tableTypeFromStd(am["tableType"])
		default:
			a.Type, err = typeFromStd(am[litDataType])
		}

		if err != nil {
			return nil, err
		}

		out = append(out, a)
	}

	return out, nil
}

// routineData converts a stored resource into the engine's view of it.
func routineData(project, ds, id string, res map[string]any) (*bqsql.RoutineData, error) {
	r := &bqsql.RoutineData{Ref: bqsql.TableRef{Project: project, Dataset: ds, Table: id}, Language: "SQL"}
	r.Kind, _ = res[litRoutineType].(string)
	r.Body, _ = res["definitionBody"].(string)
	r.Description, _ = res["description"].(string)

	if lang, _ := res["language"].(string); lang != "" {
		r.Language = strings.ToUpper(lang)
	}

	var err error

	if r.Args, err = routineArgs(res); err != nil {
		return nil, err
	}

	if res["returnType"] != nil {
		t, terr := typeFromStd(res["returnType"])
		if terr != nil {
			return nil, terr
		}

		r.Returns = &t
	}

	if res["returnTableType"] != nil {
		if r.ReturnTable, err = tableTypeFromStd(res["returnTableType"]); err != nil {
			return nil, err
		}
	}

	return r, nil
}

// resourceFromData renders a routine created through SQL as the REST resource body.
func resourceFromData(r *bqsql.RoutineData) map[string]any {
	res := map[string]any{litRoutineType: r.Kind, "language": r.Language, "definitionBody": r.Body}

	if r.Description != "" {
		res["description"] = r.Description
	}

	if len(r.Args) > 0 {
		args := make([]any, len(r.Args))

		for i, a := range r.Args {
			am := map[string]any{litName: a.Name}

			switch {
			case a.AnyTable:
				am["argumentKind"] = "ANY_TABLE"
			case a.IsTable:
				am["argumentKind"] = "FIXED_TABLE"
				am["tableType"] = tableTypeToStd(a.Table)
			case a.AnyType:
				am["argumentKind"] = "ANY_TYPE"
			default:
				am["argumentKind"] = "FIXED_TYPE"
				am[litDataType] = stdFromType(a.Type)
			}

			if a.Mode != "" {
				am["mode"] = a.Mode
			}

			args[i] = am
		}

		res[litArguments] = args
	}

	if r.Returns != nil {
		res["returnType"] = stdFromType(*r.Returns)
	}

	if len(r.ReturnTable) > 0 {
		res["returnTableType"] = tableTypeToStd(r.ReturnTable)
	}

	return res
}

func routineWire(project, ds, id string, r *Routine) map[string]any {
	out := cloneMap(r.Resource)
	out["routineReference"] = map[string]any{litProjectid: project, litDatasetID: ds, litRoutineID: id}
	out[litCreationtime] = msString(r.Created)
	out[litLastModified] = msString(r.Modified)
	out[litEtag] = etagOf("routine", project, ds, id, msString(r.Seq))

	if _, ok := out["language"]; !ok {
		out["language"] = "SQL"
	}

	return out
}

func routineSummary(project, ds, id string, r *Routine, mask []string) map[string]any {
	full := routineWire(project, ds, id, r)
	out := map[string]any{}

	for _, k := range []string{
		litEtag, "routineReference", litRoutineType, litCreationtime, litLastModified, "language",
		"remoteFunctionOptions",
	} {
		if v, ok := full[k]; ok {
			out[k] = v
		}
	}

	for _, k := range mask {
		if v, ok := full[k]; ok {
			out[k] = v
		}
	}

	return out
}

func (b *InMemoryBackend) routineBody(project, ds string, body map[string]any, pathID string) (string, error) {
	ref := refIDs(body, "routineReference")
	id := ref[litRoutineID]

	if pathID != "" {
		if id != "" && id != pathID {
			return "", errInvalid(
				"Routine ID in the routine reference (%s) does not match the request (%s)",
				id,
				pathID,
			)
		}

		id = pathID
	}

	if id == "" {
		return "", errInvalid("Routine ID is required")
	}

	if len(id) > maxIDLen || !datasetIDPattern().MatchString(id) {
		return "", errInvalid("Invalid routine ID %q.", id)
	}

	if p := ref[litProjectid]; p != "" && p != project {
		return "", errInvalid(
			"Project ID in the routine reference (%s) does not match the request project (%s)",
			p,
			project,
		)
	}

	if d := ref[litDatasetID]; d != "" && d != ds {
		return "", errInvalid("Dataset ID in the routine reference (%s) does not match the request dataset (%s)", d, ds)
	}

	return id, nil
}

func validRoutineType(t string) bool {
	return t == bqsql.RoutineScalar || t == bqsql.RoutineTable || t == bqsql.RoutineProcedure
}

// checkRoutineBody analyses a routine definition against the live catalog; the caller holds the lock.
func (b *InMemoryBackend) checkRoutineBody(ctx context.Context, project, ds, id string, res map[string]any) error {
	_ = ctx

	typ, _ := res[litRoutineType].(string)
	if !validRoutineType(typ) {
		return errInvalid(
			"Routine type %q is not supported; use SCALAR_FUNCTION, TABLE_VALUED_FUNCTION or PROCEDURE",
			typ,
		)
	}

	if body, _ := res["definitionBody"].(string); strings.TrimSpace(body) == "" {
		return errInvalid("Routine definitionBody is required")
	}

	rd, err := routineData(project, ds, id, res)
	if err != nil {
		return err
	}

	in := &bqsql.Input{Catalog: catalog{b: b}, Project: project, Dataset: ds}
	if err = bqsql.CheckRoutine(in, rd); err != nil {
		return fromSQL(err)
	}

	return nil
}

// CreateRoutine implements routines.insert.
func (b *InMemoryBackend) CreateRoutine(project, ds string, body map[string]any) (map[string]any, error) {
	id, err := b.routineBody(project, ds, body, "")
	if err != nil {
		return nil, err
	}

	b.mu.Lock("CreateRoutine")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return nil, err
	}

	if _, _, ok := d.routine(id); ok {
		return nil, errDuplicate("Already Exists: Routine %s:%s.%s", project, ds, id)
	}

	res := metaFrom(body, routineManaged())
	if err = b.checkRoutineBody(context.Background(), project, ds, id, res); err != nil {
		return nil, err
	}

	now := nowMs()
	r := &Routine{Resource: res, Created: now, Modified: now, Seq: b.nextSeq()}
	d.Routines[id] = r

	b.auditCall("google.cloud.bigquery.v2.RoutineService.InsertRoutine",
		"projects/"+project+"/datasets/"+ds+"/routines/"+id, project)

	return routineWire(project, ds, id, r), nil
}

// GetRoutine implements routines.get.
func (b *InMemoryBackend) GetRoutine(project, ds, id string) (map[string]any, error) {
	b.mu.RLock("GetRoutine")
	defer b.mu.RUnlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return nil, err
	}

	r, key, ok := d.routine(id)
	if !ok {
		return nil, errNotFound("Not found: Routine %s:%s.%s", project, ds, id)
	}

	return routineWire(project, ds, key, r), nil
}

// UpdateRoutine implements routines.update. The Go client sends only the changed fields, so the body merges into the
// stored routine (a null removes a field); a non-empty ifMatch must equal the current etag.
func (b *InMemoryBackend) UpdateRoutine(
	project, ds, id, ifMatch string, body map[string]any,
) (map[string]any, error) {
	if _, err := b.routineBody(project, ds, body, id); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateRoutine")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return nil, err
	}

	r, key, ok := d.routine(id)
	if !ok {
		return nil, errNotFound("Not found: Routine %s:%s.%s", project, ds, id)
	}

	if ifMatch != "" && ifMatch != etagOf("routine", project, ds, key, msString(r.Seq)) {
		return nil, newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition check failed.")
	}

	res := cloneMap(r.Resource)
	patch := cloneMap(body)

	for k := range routineManaged() {
		delete(patch, k)
	}

	mergePatch(res, patch)

	if err = b.checkRoutineBody(context.Background(), project, ds, key, res); err != nil {
		return nil, err
	}

	r.Resource, r.Modified, r.Seq = res, nowMs(), b.nextSeq()

	return routineWire(project, ds, key, r), nil
}

// DeleteRoutine implements routines.delete.
func (b *InMemoryBackend) DeleteRoutine(project, ds, id string) error {
	b.mu.Lock("DeleteRoutine")
	defer b.mu.Unlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return err
	}

	_, key, ok := d.routine(id)
	if !ok {
		return errNotFound("Not found: Routine %s:%s.%s", project, ds, id)
	}

	delete(d.Routines, key)

	b.auditCall("google.cloud.bigquery.v2.RoutineService.DeleteRoutine",
		"projects/"+project+"/datasets/"+ds+"/routines/"+key, project)

	return nil
}

// ListRoutines implements routines.list; filter is routineType:{type} and readMask adds fields to the summary.
func (b *InMemoryBackend) ListRoutines(
	project, ds string, o ListOptions, readMask string,
) (page.Page[map[string]any], error) {
	b.mu.RLock("ListRoutines")
	defer b.mu.RUnlock()

	d, err := b.datasetLocked(project, ds)
	if err != nil {
		return page.Page[map[string]any]{}, err
	}

	want := ""

	if o.Filter != "" {
		key, val, found := strings.Cut(strings.TrimSpace(o.Filter), ":")
		if !found || key != litRoutineType || !validRoutineType(val) {
			return page.Page[map[string]any]{}, errInvalid("Invalid filter %q; use routineType:{type}", o.Filter)
		}

		want = val
	}

	var mask []string

	for f := range strings.SplitSeq(readMask, ",") {
		if f = strings.TrimSpace(f); f != "" {
			mask = append(mask, f)
		}
	}

	ids := make([]string, 0, len(d.Routines))

	for id, r := range d.Routines {
		if t, _ := r.Resource[litRoutineType].(string); want == "" || t == want {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	all := make([]map[string]any, len(ids))
	for i, id := range ids {
		all[i] = routineSummary(project, ds, id, d.Routines[id], mask)
	}

	return page.New(all, o.PageToken, o.Max, defaultListMax), nil
}

// applyRoutineEffect applies CREATE or DROP of a routine from a SQL statement; temporary routines never reach it.
func (b *InMemoryBackend) applyRoutineEffect(e bqsql.Effect) error {
	if e.Routine != nil && e.Routine.Temp {
		return nil
	}

	d, ok := b.datasets[dsKey(e.Ref.Project, e.Ref.Dataset)]
	if !ok {
		return errNotFound("Not found: Dataset %s:%s", e.Ref.Project, e.Ref.Dataset)
	}

	if e.Op == bqsql.OpDropRoutine {
		_, key, found := d.routine(e.Ref.Table)
		if !found {
			return errNotFound("Not found: Routine %s", e.Ref)
		}

		delete(d.Routines, key)

		return nil
	}

	now := nowMs()
	r := &Routine{Resource: resourceFromData(e.Routine), Created: now, Modified: now, Seq: b.nextSeq()}

	if old, key, found := d.routine(e.Ref.Table); found {
		if !e.OrReplace {
			return errDuplicate("Already Exists: Routine %s", e.Ref)
		}

		r.Created = old.Created
		delete(d.Routines, key)
	}

	d.Routines[e.Ref.Table] = r

	return nil
}
