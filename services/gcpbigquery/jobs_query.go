package gcpbigquery

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	createIfNeeded = "CREATE_IF_NEEDED"
	createNever    = "CREATE_NEVER"
	writeTruncate  = "WRITE_TRUNCATE"
	writeAppend    = "WRITE_APPEND"
	writeEmpty     = "WRITE_EMPTY"
)

type queryConfig struct {
	UseLegacySQL     *bool        `json:"useLegacySql"`
	DefaultDataset   *datasetRef  `json:"defaultDataset"`
	DestinationTable *tableRef    `json:"destinationTable"`
	Query            string       `json:"query"`
	ParameterMode    string       `json:"parameterMode"`
	CreateDisp       string       `json:"createDisposition"`
	WriteDisp        string       `json:"writeDisposition"`
	QueryParameters  []queryParam `json:"queryParameters"`
}

type datasetRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

func intOf(v any) (int, bool) {
	switch t := v.(type) {
	case json.Number:
		n, err := t.Int64()

		return int(n), err == nil
	case float64:
		return int(t), true
	case string:
		n, err := strconv.Atoi(t)

		return n, err == nil
	}

	return 0, false
}

func decodeQueryConfig(raw any) (queryConfig, error) {
	var qc queryConfig

	b, err := json.Marshal(raw)
	if err != nil {
		return qc, errInvalid("Invalid query configuration")
	}

	if err = json.Unmarshal(b, &qc); err != nil {
		return qc, errInvalid("Invalid query configuration: %v", err)
	}

	return qc, nil
}

// execQuery runs a query-job statement; the caller holds the write lock.
func (b *InMemoryBackend) execQuery(
	ctx context.Context,
	project string,
	qc queryConfig,
	dryRun bool,
) (*outcome, error) {
	if qc.UseLegacySQL != nil && *qc.UseLegacySQL {
		return nil, errInvalidQuery("Legacy SQL is not supported; set useLegacySql to false")
	}

	if strings.TrimSpace(qc.Query) == "" {
		return nil, errInvalidQuery("Query text is required")
	}

	st, err := bqsql.ParseStatement(qc.Query)
	if err != nil {
		return nil, fromSQL(err)
	}

	params, err := engineParams(qc.QueryParameters, qc.ParameterMode)
	if err != nil {
		return nil, err
	}

	in := &bqsql.Input{Catalog: catalog{b: b}, Project: project, Params: params}
	if qc.DefaultDataset != nil {
		in.Dataset = qc.DefaultDataset.DatasetID
		if qc.DefaultDataset.ProjectID != "" {
			in.Project = qc.DefaultDataset.ProjectID
		}
	}

	res, err := bqsql.Run(ctx, st, in, dryRun)
	if err != nil {
		return b.queryFailure(err)
	}

	out := &outcome{Fields: res.Fields, Rows: res.Rows, IsQuery: st.Kind == bqsql.StmtQuery, NumDML: res.Affected}
	out.Stats = queryStats(res)

	if dryRun {
		return out, nil
	}

	if aerr := b.applyEffects(res.Effects); aerr != nil {
		return &outcome{
			Err:   &jobError{Reason: apiReason(aerr), Message: apiMessage(aerr)},
			Stats: map[string]any{},
		}, nil
	}

	if st.Kind == bqsql.StmtQuery && qc.DestinationTable != nil {
		if jerr := b.writeDestination(
			*qc.DestinationTable,
			res.Fields,
			res.Rows,
			qc.CreateDisp,
			qc.WriteDisp,
			true,
		); jerr != nil {
			out.Err = jerr
		} else {
			out.Dest = qc.DestinationTable
		}
	}

	return out, nil
}

// queryFailure maps runtime failures into the job and everything else into an HTTP error.
func (b *InMemoryBackend) queryFailure(err error) (*outcome, error) {
	e := bqsql.AsError(err)
	switch e.Kind {
	case bqsql.ErrRuntime:
		return &outcome{Err: &jobError{Reason: "invalidQuery", Message: e.Message}, Stats: map[string]any{}}, nil
	case bqsql.ErrExists:
		return &outcome{Err: &jobError{Reason: litDuplicate, Message: e.Message}, Stats: map[string]any{}}, nil
	case bqsql.ErrInvalid, bqsql.ErrNotFound, bqsql.ErrUnsupported:
	}

	return nil, fromSQL(err)
}

func queryStats(res *bqsql.Result) map[string]any {
	stats := map[string]any{
		"statementType": res.Statement, litTotalbytesprocessed: "0", "totalBytesBilled": "0", "cacheHit": false,
		"billingTier": 1,
	}

	if res.Kind.DML() {
		stats["numDmlAffectedRows"] = strconv.FormatInt(res.Affected, 10)
		stats["dmlStats"] = map[string]any{
			"insertedRowCount": strconv.FormatInt(res.Inserted, 10),
			"updatedRowCount":  strconv.FormatInt(res.Updated, 10),
			"deletedRowCount":  strconv.FormatInt(res.Deleted, 10),
		}
	}

	return stats
}

// writeDestination stores rows into a destination table honouring the create and write dispositions.
func (b *InMemoryBackend) writeDestination(
	ref tableRef, fields []bqsql.Field, rows [][]any, create, write string, replaceSchema bool,
) *jobError {
	if create == "" {
		create = createIfNeeded
	}

	if write == "" {
		write = writeEmpty
	}

	d, ok := b.datasets[dsKey(ref.ProjectID, ref.DatasetID)]
	if !ok {
		return &jobError{Reason: litNotfound, Message: "Not found: Dataset " + ref.ProjectID + ":" + ref.DatasetID}
	}

	t, exists := d.Tables[ref.TableID]
	if !exists {
		if create == createNever {
			return &jobError{
				Reason:  litNotfound,
				Message: "Not found: Table " + ref.ProjectID + ":" + ref.DatasetID + "." + ref.TableID,
			}
		}

		now := nowMs()
		d.Tables[ref.TableID] = &Table{
			Meta:     map[string]any{},
			Schema:   fields,
			Rows:     rows,
			Created:  now,
			Modified: now,
			Seq:      b.nextSeq(),
		}

		return nil
	}

	if t.ViewQuery != "" {
		return &jobError{Reason: litInvalid, Message: "Cannot write to view " + ref.TableID}
	}

	return b.fillExisting(t, ref, fields, rows, write, replaceSchema)
}

func (b *InMemoryBackend) fillExisting(
	t *Table,
	ref tableRef,
	fields []bqsql.Field,
	rows [][]any,
	write string,
	replaceSchema bool,
) *jobError {
	switch write {
	case writeEmpty:
		if len(t.Rows) > 0 {
			return &jobError{
				Reason:  litDuplicate,
				Message: "Already Exists: Table " + ref.ProjectID + ":" + ref.DatasetID + "." + ref.TableID,
			}
		}

		fallthrough
	case writeTruncate:
		if replaceSchema || len(t.Schema) == 0 {
			t.Schema = fields
		} else if len(fields) != len(t.Schema) {
			return schemaMismatch(ref)
		}

		t.Rows = rows
	case writeAppend:
		if len(fields) != len(t.Schema) {
			return schemaMismatch(ref)
		}

		t.Rows = append(t.Rows, rows...)
	default:
		return &jobError{Reason: litInvalid, Message: "Invalid writeDisposition " + write}
	}

	t.Modified = nowMs()
	t.Seq = b.nextSeq()

	return nil
}

func schemaMismatch(ref tableRef) *jobError {
	return &jobError{
		Reason:  litInvalid,
		Message: "Provided Schema does not match Table " + ref.ProjectID + ":" + ref.DatasetID + "." + ref.TableID,
	}
}

// applyEffects commits the catalog changes a statement produced.
func (b *InMemoryBackend) applyEffects(effects []bqsql.Effect) error {
	for _, e := range effects {
		if err := b.applyEffect(e); err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) applyEffect(e bqsql.Effect) error {
	switch e.Op {
	case bqsql.OpWrite:
		return b.effectWrite(e)
	case bqsql.OpCreateTable, bqsql.OpCreateView:
		return b.effectCreate(e)
	case bqsql.OpCreateSchema:
		return b.effectCreateSchema(e)
	case bqsql.OpDropTable, bqsql.OpDropView:
		d := b.datasets[dsKey(e.Ref.Project, e.Ref.Dataset)]
		if d != nil {
			delete(d.Tables, e.Ref.Table)
		}

		return nil
	case bqsql.OpDropSchema:
		return b.effectDropSchema(e)
	case bqsql.OpAlter:
		return b.effectAlter(e)
	}

	return errInvalidQuery("Unsupported catalog operation " + e.Op)
}

func (b *InMemoryBackend) effectTable(e bqsql.Effect) (*Table, error) {
	d, ok := b.datasets[dsKey(e.Ref.Project, e.Ref.Dataset)]
	if !ok {
		return nil, errNotFound("Not found: Dataset %s:%s", e.Ref.Project, e.Ref.Dataset)
	}

	t, ok := d.Tables[e.Ref.Table]
	if !ok {
		return nil, errNotFound("Not found: Table %s", e.Ref)
	}

	return t, nil
}

func (b *InMemoryBackend) effectWrite(e bqsql.Effect) error {
	t, err := b.effectTable(e)
	if err != nil {
		return err
	}

	t.Rows, t.Schema = e.Rows, e.Fields
	t.Modified = nowMs()
	t.Seq = b.nextSeq()

	return nil
}

func (b *InMemoryBackend) effectCreate(e bqsql.Effect) error {
	d, ok := b.datasets[dsKey(e.Ref.Project, e.Ref.Dataset)]
	if !ok {
		return errNotFound("Not found: Dataset %s:%s", e.Ref.Project, e.Ref.Dataset)
	}

	now := nowMs()
	t := &Table{
		Meta:      optionsMeta(e.Options),
		Schema:    e.Fields,
		Rows:      e.Rows,
		ViewQuery: e.ViewQuery,
		Created:   now,
		Modified:  now,
		Seq:       b.nextSeq(),
	}

	if t.Rows == nil {
		t.Rows = [][]any{}
	}

	if old, exists := d.Tables[e.Ref.Table]; exists && e.OrReplace {
		t.Created = old.Created
	}

	d.Tables[e.Ref.Table] = t

	return nil
}

func (b *InMemoryBackend) effectCreateSchema(e bqsql.Effect) error {
	key := dsKey(e.Ref.Project, e.Ref.Dataset)
	if _, ok := b.datasets[key]; ok {
		if e.IfNotExist {
			return nil
		}

		return errDuplicate("Already Exists: Dataset %s:%s", e.Ref.Project, e.Ref.Dataset)
	}

	loc := defaultLocation
	if l := e.Options[litLocation]; l != "" {
		loc = l
	}

	now := nowMs()
	b.datasets[key] = &Dataset{
		Meta:     optionsMeta(e.Options),
		Tables:   map[string]*Table{},
		Location: loc,
		Created:  now,
		Modified: now,
	}

	return nil
}

func (b *InMemoryBackend) effectDropSchema(e bqsql.Effect) error {
	key := dsKey(e.Ref.Project, e.Ref.Dataset)

	d, ok := b.datasets[key]
	if !ok {
		if e.IfExists {
			return nil
		}

		return errNotFound("Not found: Dataset %s:%s", e.Ref.Project, e.Ref.Dataset)
	}

	if len(d.Tables) > 0 && !e.Cascade {
		return errInUse("Dataset %s:%s is still in use", e.Ref.Project, e.Ref.Dataset)
	}

	delete(b.datasets, key)

	return nil
}

func (b *InMemoryBackend) effectAlter(e bqsql.Effect) error {
	t, err := b.effectTable(e)
	if err != nil {
		return err
	}

	switch e.AlterOp {
	case "OPTIONS":
		mergePatch(t.Meta, optionsMeta(e.Options))
	default:
		t.Schema, t.Rows = e.Fields, e.Rows
	}

	t.Modified = nowMs()
	t.Seq = b.nextSeq()

	return nil
}

//nolint:gochecknoglobals // compiled once, read-only
var labelPairs = sync.OnceValue(func() *regexp.Regexp {
	return regexp.MustCompile(`\(\s*["']([^"']*)["']\s*,\s*["']([^"']*)["']\s*\)`)
})

// optionsMeta maps DDL OPTIONS(...) to the REST metadata fields they set.
func optionsMeta(opts map[string]string) map[string]any {
	meta := map[string]any{}

	if v, ok := opts["description"]; ok {
		meta["description"] = v
	}

	if v, ok := opts["friendly_name"]; ok {
		meta[litFriendlyname] = v
	}

	if v, ok := opts["labels"]; ok {
		labels := map[string]any{}
		for _, m := range labelPairs().FindAllStringSubmatch(v, -1) {
			labels[m[1]] = m[2]
		}

		meta["labels"] = labels
	}

	if v, ok := opts["expiration_timestamp"]; ok {
		s := strings.Trim(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(v), "TIMESTAMP")), `"'`)
		if t, err := bqsql.ParseTemporal(bqsql.KindTimestamp, s); err == nil {
			meta["expirationTime"] = msString(t.UnixMilli())
		}
	}

	return meta
}

func apiReason(err error) string {
	if ae, ok := errors.AsType[*APIError](err); ok {
		return ae.Reason
	}

	return "invalidQuery"
}

func apiMessage(err error) string {
	if ae, ok := errors.AsType[*APIError](err); ok {
		return ae.Message
	}

	return err.Error()
}
