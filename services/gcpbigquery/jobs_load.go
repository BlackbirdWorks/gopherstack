package gcpbigquery

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	optAddField   = "ALLOW_FIELD_ADDITION"
	optRelaxField = "ALLOW_FIELD_RELAXATION"

	maxReportedErrors = 5
	maxLoadFiles      = 1000
	maxLoadBytes      = 512 << 20

	formatCSV     = "CSV"
	formatJSON    = "NEWLINE_DELIMITED_JSON"
	formatParquet = "PARQUET"
)

type loadConfig struct {
	Dest        tableRef
	Format      string
	Create      string
	Write       string
	NullMarker  string
	Delimiter   string
	Quote       *string
	Encoding    string
	URIs        []string
	SchemaOpts  []string
	Schema      []bqsql.Field
	SkipRows    int
	MaxBad      int
	Autodetect  bool
	Jagged      bool
	IgnoreExtra bool
}

type loadInput struct {
	name string
	data []byte
}

var (
	errLoad   = errors.New("load failed")
	errBadRow = errors.New("bad row")
)

func decodeLoadConfig(cfg map[string]any) (loadConfig, error) {
	lc := loadConfig{Format: formatCSV, Delimiter: ",", Create: createIfNeeded, Write: writeAppend}

	dest := refIDs(cfg, "destinationTable")
	lc.Dest = tableRef{ProjectID: dest[litProjectid], DatasetID: dest["datasetId"], TableID: dest["tableId"]}

	if lc.Dest.DatasetID == "" || lc.Dest.TableID == "" {
		return lc, errInvalid("Required parameter is missing: load destinationTable")
	}

	str := func(k string) string {
		s, _ := cfg[k].(string)

		return s
	}
	if v := str("sourceFormat"); v != "" {
		lc.Format = strings.ToUpper(v)
	}

	if v := str("createDisposition"); v != "" {
		lc.Create = v
	}

	if v := str("writeDisposition"); v != "" {
		lc.Write = v
	}

	if v := str("fieldDelimiter"); v != "" {
		lc.Delimiter = v
	}

	lc.NullMarker, lc.Encoding = str("nullMarker"), strings.ToUpper(str("encoding"))
	lc.Autodetect, _ = cfg["autodetect"].(bool)
	lc.Jagged, _ = cfg["allowJaggedRows"].(bool)
	lc.IgnoreExtra, _ = cfg["ignoreUnknownValues"].(bool)

	if q, ok := cfg["quote"].(string); ok {
		lc.Quote = &q
	}

	lc.SkipRows, _ = intOf(cfg["skipLeadingRows"])
	lc.MaxBad, _ = intOf(cfg["maxBadRecords"])

	lc.URIs = append(lc.URIs, anyStrings(cfg["sourceUris"])...)
	lc.SchemaOpts = anyStrings(cfg["schemaUpdateOptions"])

	for _, o := range lc.SchemaOpts {
		if o != optAddField && o != optRelaxField {
			return lc, errInvalid("Invalid schemaUpdateOptions value %q", o)
		}
	}

	var err error
	if lc.Schema, err = decodeSchema(cfg["schema"]); err != nil {
		return lc, err
	}

	return lc, nil
}

func anyStrings(v any) []string {
	arr, _ := v.([]any)

	var out []string

	for _, e := range arr {
		if s, ok := e.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// loadPre holds the inputs of a load job, read from Cloud Storage before the write lock is taken.
type loadPre struct {
	err    *jobError
	inputs []loadInput
}

func (b *InMemoryBackend) validateLoad(cfg map[string]any) (loadConfig, error) {
	lc, err := decodeLoadConfig(cfg)
	if err != nil {
		return lc, err
	}

	if _, ok := b.datasets[dsKey(lc.Dest.ProjectID, lc.Dest.DatasetID)]; !ok {
		return lc, errNotFound("Not found: Dataset %s:%s", lc.Dest.ProjectID, lc.Dest.DatasetID)
	}

	return lc, nil
}

// execLoad validates a load job without running it (dryRun); the caller holds the lock.
func (b *InMemoryBackend) execLoad(cfg map[string]any) (*outcome, error) {
	if _, err := b.validateLoad(cfg); err != nil {
		return nil, err
	}

	return &outcome{Stats: map[string]any{}}, nil
}

// preload reads a load job's sources without holding the backend lock.
func (b *InMemoryBackend) preload(kind string, cfg map[string]any, media []byte) *loadPre {
	if kind != jobLoad {
		return nil
	}

	lc, err := decodeLoadConfig(objectOf(cfg, jobLoad))
	if err != nil {
		return &loadPre{err: &jobError{Reason: apiReason(err), Message: apiMessage(err)}}
	}

	b.mu.RLock("preload")
	objs, budget := b.objects, b.newLoadBudget()
	b.mu.RUnlock()

	inputs, jerr := loadInputsFrom(objs, lc, media, budget)

	return &loadPre{inputs: inputs, err: jerr}
}

// execLoadPre runs a load job over inputs read beforehand; the caller holds the write lock.
func (b *InMemoryBackend) execLoadPre(cfg map[string]any, pre *loadPre) (*outcome, error) {
	lc, err := b.validateLoad(cfg)
	if err != nil {
		return nil, err
	}

	if pre.err != nil {
		return &outcome{Err: pre.err, Stats: map[string]any{}}, nil
	}

	inputs := pre.inputs

	fields, replace, jerr := b.loadSchema(lc, inputs)
	if jerr != nil {
		return &outcome{Err: jerr, Stats: map[string]any{}}, nil
	}

	rows, bad, jerr := b.loadRows(lc, cfg, &fields, inputs)
	if jerr != nil {
		return &outcome{Err: jerr, Warnings: badWarnings(bad), Stats: map[string]any{}}, nil
	}

	if replace && lc.Write == writeAppend {
		if fields, rows, jerr = b.reconcileAppend(lc, fields, rows); jerr != nil {
			return &outcome{Err: jerr, Stats: map[string]any{}}, nil
		}

		replace = false
	}

	out := &outcome{Dest: &lc.Dest, Stats: loadStats(inputs, fields, rows, len(bad)), Warnings: badWarnings(bad)}
	if jerr = b.writeDestination(lc.Dest, fields, rows, lc.Create, lc.Write, replace); jerr != nil {
		out.Err, out.Dest = jerr, nil
	}

	return out, nil
}

func loadStats(in []loadInput, fields []bqsql.Field, rows [][]any, bad int) map[string]any {
	var size int64
	for _, i := range in {
		size += int64(len(i.data))
	}

	var outBytes int64

	for _, r := range rows {
		for i, f := range fields {
			outBytes += bqsql.LogicalBytes(f, r[i])
		}
	}

	return map[string]any{
		"inputFiles": strconv.Itoa(len(in)), "inputFileBytes": strconv.FormatInt(size, 10),
		"outputRows": strconv.Itoa(len(rows)), "outputBytes": strconv.FormatInt(outBytes, 10),
		"badRecords": strconv.Itoa(bad),
	}
}

// loadBudget bounds the files and decompressed bytes one load job may read.
type loadBudget struct {
	files    int
	maxFiles int
	bytes    int64
	maxBytes int64
}

func (lb *loadBudget) take(name string, data []byte) ([]byte, *jobError) {
	lb.files++
	if lb.files > lb.maxFiles {
		return nil, &jobError{
			Reason: litInvalid, Message: fmt.Sprintf("Too many source files: at most %d are allowed", lb.maxFiles),
		}
	}

	out, err := gunzipLimited(data, lb.bytes, lb.maxBytes)
	if err != nil {
		return nil, &jobError{Reason: litInvalid, Message: "Could not read " + name + ": " + err.Error()}
	}

	lb.bytes -= int64(len(out))

	return out, nil
}

func (b *InMemoryBackend) loadInputs(lc loadConfig, media []byte) ([]loadInput, *jobError) {
	return loadInputsFrom(b.objects, lc, media, b.newLoadBudget())
}

func loadInputsFrom(objs ObjectStore, lc loadConfig, media []byte, budget *loadBudget) ([]loadInput, *jobError) {
	if media != nil {
		data, jerr := budget.take("the uploaded data", media)
		if jerr != nil {
			return nil, jerr
		}

		return []loadInput{{name: "upload", data: data}}, nil
	}

	return loadURIInputs(objs, lc, budget)
}

func loadURIInputs(objs ObjectStore, lc loadConfig, budget *loadBudget) ([]loadInput, *jobError) {
	if len(lc.URIs) == 0 {
		return nil, &jobError{Reason: litInvalid, Message: "Required parameter is missing: sourceUris"}
	}

	if objs == nil {
		return nil, &jobError{Reason: litInvalid, Message: "Cloud Storage is not available to this BigQuery emulator"}
	}

	var out []loadInput

	for _, uri := range lc.URIs {
		got, jerr := readURI(objs, uri, budget)
		if jerr != nil {
			return nil, jerr
		}

		out = append(out, got...)
	}

	return out, nil
}

func readURI(objs ObjectStore, uri string, budget *loadBudget) ([]loadInput, *jobError) {
	u, ok := parseGCSURI(uri)
	if !ok {
		return nil, &jobError{Reason: litInvalid, Message: "Invalid Cloud Storage URI: " + uri}
	}

	names := []string{u.object}

	if strings.Contains(u.object, "*") {
		prefix, _, _ := strings.Cut(u.object, "*")

		all, err := objs.ListObjects(u.bucket, prefix)
		if err != nil {
			return nil, uriError(uri, err)
		}

		names = names[:0]

		for _, n := range all {
			if wildcardMatch(u.object, n) {
				names = append(names, n)
			}
		}

		if len(names) == 0 {
			return nil, &jobError{
				Reason:  litNotfound,
				Message: "Not found: Uris List of uris contains no matching objects for " + uri,
			}
		}
	}

	var out []loadInput

	for _, n := range names {
		data, err := objs.ReadObject(u.bucket, n)
		if err != nil {
			return nil, uriError("gs://"+u.bucket+"/"+n, err)
		}

		name := "gs://" + u.bucket + "/" + n

		data, jerr := budget.take(name, data)
		if jerr != nil {
			return nil, jerr
		}

		out = append(out, loadInput{name: name, data: data})
	}

	return out, nil
}

func uriError(uri string, err error) *jobError {
	if errors.Is(err, ErrObjectNotFound) {
		return &jobError{Reason: litNotfound, Message: "Not found: URI " + uri}
	}

	return &jobError{Reason: "backendError", Message: "Cannot read " + uri + ": " + err.Error()}
}

func gunzipLimited(data []byte, limit, total int64) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		if int64(len(data)) > limit {
			return nil, fmt.Errorf("%w: input exceeds the %d byte limit of this emulator", errLoad, total)
		}

		return data, nil
	}

	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer zr.Close()

	out, err := io.ReadAll(io.LimitReader(zr, limit+1))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	if int64(len(out)) > limit {
		return nil, fmt.Errorf("%w: decompressed input exceeds the %d byte limit of this emulator", errLoad, total)
	}

	return out, nil
}

// loadSchema picks the schema to parse into: an explicit one, the existing table's, or an autodetected one.
func (b *InMemoryBackend) loadSchema(lc loadConfig, in []loadInput) ([]bqsql.Field, bool, *jobError) {
	if len(lc.Schema) > 0 {
		return lc.Schema, true, nil
	}

	if d := b.datasets[dsKey(lc.Dest.ProjectID, lc.Dest.DatasetID)]; d != nil {
		if t, ok := d.live(lc.Dest.TableID); ok && len(t.Schema) > 0 {
			return t.Schema, false, nil
		}
	}

	if !lc.Autodetect && !selfDescribing(lc.Format) {
		return nil, false, &jobError{
			Reason:  litInvalid,
			Message: "Schema must be specified when creating a new table, or autodetect enabled",
		}
	}

	fields, err := detectSchema(lc, in)
	if err != nil {
		return nil, false, &jobError{Reason: litInvalid, Message: err.Error()}
	}

	return fields, true, nil
}

func parseInputs(lc loadConfig, fields []bqsql.Field, in []loadInput) ([][]any, []string, *jobError) {
	rows := [][]any{}

	var bad []string

	for _, input := range in {
		got, badRows, err := parseInput(lc, fields, input)
		if err != nil {
			return nil, bad, &jobError{
				Reason:  litInvalid,
				Message: "Error while reading data, error message: " + err.Error() + "; " + input.name,
			}
		}

		rows = append(rows, got...)

		for _, e := range badRows {
			bad = append(bad, e.Error()+"; "+input.name)
		}
	}

	if len(bad) > lc.MaxBad {
		return nil, bad, &jobError{
			Reason: litInvalid,
			Message: fmt.Sprintf(
				"Error while reading data, error message: processing encountered too many errors, giving up. "+
					"Rows: %d; errors: %d; max bad: %d; error percent: 0",
				len(rows)+len(bad), len(bad), lc.MaxBad,
			),
		}
	}

	return rows, bad, nil
}

func parseInput(lc loadConfig, fields []bqsql.Field, in loadInput) ([][]any, []error, error) {
	switch lc.Format {
	case formatCSV:
		return parseCSVInput(lc, fields, in.data)
	case formatJSON:
		return parseJSONInput(lc, fields, in.data)
	case formatParquet:
		return parseParquetInput(lc, fields, in.data)
	case formatAvro:
		return parseAvroInput(lc, fields, in.data)
	case formatORC:
		return parseORCInput(lc, fields, in.data)
	}

	return nil, nil, fmt.Errorf("%w: sourceFormat %s is not supported", errLoad, lc.Format)
}

func badWarnings(bad []string) []*jobError {
	n := min(len(bad), maxReportedErrors)
	out := make([]*jobError, n)

	for i := range n {
		out[i] = &jobError{Reason: litInvalid, Message: "Error while reading data, error message: " + bad[i]}
	}

	return out
}

// loadRows parses the inputs into rows of *fields; with hivePartitioningOptions the partition keys of each object
// become columns appended to *fields.
func (b *InMemoryBackend) loadRows(
	lc loadConfig, cfg map[string]any, fields *[]bqsql.Field, in []loadInput,
) ([][]any, []string, *jobError) {
	if _, ok := cfg[litHive].(map[string]any); !ok {
		return parseInputs(lc, *fields, in)
	}

	sp := externalSpec{lc: lc}
	if err := sp.hiveFromConfig(cfg); err != nil {
		return nil, nil, &jobError{Reason: litInvalid, Message: apiMessage(err)}
	}

	parts, err := sp.partitionFields(in)
	if err != nil {
		return nil, nil, &jobError{Reason: litInvalid, Message: err.Error()}
	}

	merged := mergePartitionFields(*fields, parts)
	if err = bqsql.NormalizeSchema(merged); err != nil {
		return nil, nil, &jobError{Reason: litInvalid, Message: "Invalid hive partition column: " + err.Error()}
	}

	*fields = merged
	rows := [][]any{}

	for _, input := range in {
		got, rerr := externalFileRows(sp, *fields, input)
		if rerr != nil {
			return nil, nil, &jobError{Reason: litInvalid, Message: rerr.Error()}
		}

		for _, r := range got {
			rows = append(rows, r[:len(r)-1])
		}
	}

	return rows, nil, nil
}

// reconcileAppend matches the loaded schema to the existing table by column name for WRITE_APPEND and applies
// schemaUpdateOptions (https://docs.cloud.google.com/bigquery/docs/managing-table-schemas); the table schema only
// changes once every check passed.
func (b *InMemoryBackend) reconcileAppend(
	lc loadConfig, fields []bqsql.Field, rows [][]any,
) ([]bqsql.Field, [][]any, *jobError) {
	d := b.datasets[dsKey(lc.Dest.ProjectID, lc.Dest.DatasetID)]
	if d == nil {
		return fields, rows, nil
	}

	t, ok := d.live(lc.Dest.TableID)
	if !ok || len(t.Schema) == 0 || t.ViewQuery != "" || t.isExternal() {
		return fields, rows, nil
	}

	merged, changed, jerr := mergeLoadSchema(lc, t.Schema, fields)
	if jerr != nil {
		return nil, nil, jerr
	}

	pos := make(map[string]int, len(fields))
	for i, f := range fields {
		pos[strs.Fold(f.Name)] = i
	}

	out := make([][]any, len(rows))

	for i, r := range rows {
		row := make([]any, len(merged))
		for j, f := range merged {
			if p, found := pos[strs.Fold(f.Name)]; found {
				row[j] = r[p]
			}
		}

		out[i] = row
	}

	if changed {
		t.record(nowMs())

		for i, r := range t.Rows {
			t.Rows[i] = append(slices.Clip(r), make([]any, len(merged)-len(r))...)
		}

		t.Schema = merged
	}

	return merged, out, nil
}

func mergeLoadSchema(lc loadConfig, existing, loaded []bqsql.Field) ([]bqsql.Field, bool, *jobError) {
	ref := lc.Dest.ProjectID + ":" + lc.Dest.DatasetID + "." + lc.Dest.TableID
	m := schemaMerger{lc: lc, ref: ref}

	merged, jerr := m.mergeFields(existing, loaded, "")
	if jerr != nil {
		return nil, false, jerr
	}

	return merged, m.changed, nil
}

type schemaMerger struct {
	ref     string
	lc      loadConfig
	changed bool
}

func (m *schemaMerger) mismatch(format string, a ...any) *jobError {
	return &jobError{
		Reason:  litInvalid,
		Message: "Provided Schema does not match Table " + m.ref + ". " + fmt.Sprintf(format, a...),
	}
}

func indexOfField(fs []bqsql.Field, name string) int {
	return slices.IndexFunc(fs, func(f bqsql.Field) bool { return strs.Equal(f.Name, name) })
}

// mergeFields matches loaded to existing by case-insensitive name, recursing into RECORD fields; existing is not
// modified.
func (m *schemaMerger) mergeFields(existing, loaded []bqsql.Field, path string) ([]bqsql.Field, *jobError) {
	merged := slices.Clone(existing)
	seen := make(map[string]bool, len(loaded))

	for _, p := range loaded {
		seen[strs.Fold(p.Name)] = true

		i := indexOfField(merged, p.Name)
		if i < 0 {
			if jerr := m.addField(path, p); jerr != nil {
				return nil, jerr
			}

			merged = append(merged, p)

			continue
		}

		if jerr := m.mergeField(&merged[i], p, path+p.Name); jerr != nil {
			return nil, jerr
		}
	}

	for i, e := range merged {
		if e.Mode != bqsql.ModeRequired || seen[strs.Fold(e.Name)] || i >= len(existing) {
			continue
		}

		if !slices.Contains(m.lc.SchemaOpts, optRelaxField) {
			return nil, m.mismatch("Field %s%s is missing in new schema", path, e.Name)
		}

		merged[i].Mode, m.changed = bqsql.ModeNullable, true
	}

	return merged, nil
}

func (m *schemaMerger) mergeField(cur *bqsql.Field, p bqsql.Field, name string) *jobError {
	if !strs.Equal(cur.Type, p.Type) {
		return m.mismatch("Field %s has changed type from %s to %s", name, cur.Type, p.Type)
	}

	if cur.Mode != p.Mode {
		if cur.Mode != bqsql.ModeRequired || p.Mode != bqsql.ModeNullable {
			return m.mismatch("Field %s has changed mode from %s to %s", name, cur.Mode, p.Mode)
		}

		if !slices.Contains(m.lc.SchemaOpts, optRelaxField) {
			return m.mismatch("Field %s has changed mode from REQUIRED to NULLABLE", name)
		}

		cur.Mode, m.changed = bqsql.ModeNullable, true
	}

	if len(p.Fields) == 0 && len(cur.Fields) == 0 {
		return nil
	}

	sub, jerr := m.mergeFields(cur.Fields, p.Fields, name+".")
	if jerr != nil {
		return jerr
	}

	cur.Fields = sub

	return nil
}

func (m *schemaMerger) addField(path string, p bqsql.Field) *jobError {
	if !slices.Contains(m.lc.SchemaOpts, optAddField) {
		return m.mismatch("Cannot add fields (field: %s%s)", path, p.Name)
	}

	if p.Mode == bqsql.ModeRequired {
		return m.mismatch("Cannot add required fields (field: %s%s)", path, p.Name)
	}

	m.changed = true

	return nil
}

func (b *InMemoryBackend) newLoadBudget() *loadBudget {
	lb := &loadBudget{maxFiles: maxLoadFiles, maxBytes: maxLoadBytes}
	if b.loadMaxFiles > 0 {
		lb.maxFiles = b.loadMaxFiles
	}

	if b.loadMaxBytes > 0 {
		lb.maxBytes = b.loadMaxBytes
	}

	lb.bytes = lb.maxBytes

	return lb
}

// SetLoadLimits sets the source file count and decompressed byte caps of a load job; zero keeps the default.
func (b *InMemoryBackend) SetLoadLimits(files int, bytes int64) {
	b.mu.Lock("SetLoadLimits")
	defer b.mu.Unlock()

	b.loadMaxFiles, b.loadMaxBytes = files, bytes
}
