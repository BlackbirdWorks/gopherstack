package gcpbigquery

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
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

// execLoad runs a load job; the caller holds the write lock.
func (b *InMemoryBackend) execLoad(_ string, cfg map[string]any, media []byte, dryRun bool) (*outcome, error) {
	lc, err := decodeLoadConfig(cfg)
	if err != nil {
		return nil, err
	}

	if _, ok := b.datasets[dsKey(lc.Dest.ProjectID, lc.Dest.DatasetID)]; !ok {
		return nil, errNotFound("Not found: Dataset %s:%s", lc.Dest.ProjectID, lc.Dest.DatasetID)
	}

	if dryRun {
		return &outcome{Stats: map[string]any{}}, nil
	}

	inputs, jerr := b.loadInputs(lc, media)
	if jerr != nil {
		return &outcome{Err: jerr, Stats: map[string]any{}}, nil
	}

	fields, replace, jerr := b.loadSchema(lc, inputs)
	if jerr != nil {
		return &outcome{Err: jerr, Stats: map[string]any{}}, nil
	}

	rows, jerr := parseInputs(lc, fields, inputs)
	if jerr != nil {
		return &outcome{Err: jerr, Stats: map[string]any{}}, nil
	}

	out := &outcome{Dest: &lc.Dest, Stats: loadStats(inputs, fields, rows)}
	if jerr = b.writeDestination(lc.Dest, fields, rows, lc.Create, lc.Write, replace); jerr != nil {
		out.Err, out.Dest = jerr, nil
	}

	return out, nil
}

func loadStats(in []loadInput, fields []bqsql.Field, rows [][]any) map[string]any {
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
		"badRecords": "0",
	}
}

func (b *InMemoryBackend) loadInputs(lc loadConfig, media []byte) ([]loadInput, *jobError) {
	if media != nil {
		data, err := gunzipIfNeeded(media)
		if err != nil {
			return nil, &jobError{Reason: litInvalid, Message: "Could not decompress the uploaded data: " + err.Error()}
		}

		return []loadInput{{name: "upload", data: data}}, nil
	}

	if len(lc.URIs) == 0 {
		return nil, &jobError{Reason: litInvalid, Message: "Required parameter is missing: sourceUris"}
	}

	if b.objects == nil {
		return nil, &jobError{Reason: litInvalid, Message: "Cloud Storage is not available to this BigQuery emulator"}
	}

	var out []loadInput

	for _, uri := range lc.URIs {
		got, jerr := b.readURI(uri)
		if jerr != nil {
			return nil, jerr
		}

		out = append(out, got...)
	}

	return out, nil
}

func (b *InMemoryBackend) readURI(uri string) ([]loadInput, *jobError) {
	u, ok := parseGCSURI(uri)
	if !ok {
		return nil, &jobError{Reason: litInvalid, Message: "Invalid Cloud Storage URI: " + uri}
	}

	names := []string{u.object}

	if strings.Contains(u.object, "*") {
		prefix, _, _ := strings.Cut(u.object, "*")

		all, err := b.objects.ListObjects(u.bucket, prefix)
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
		data, err := b.objects.ReadObject(u.bucket, n)
		if err != nil {
			return nil, uriError("gs://"+u.bucket+"/"+n, err)
		}

		if data, err = gunzipIfNeeded(data); err != nil {
			return nil, &jobError{
				Reason:  litInvalid,
				Message: "Could not decompress gs://" + u.bucket + "/" + n + ": " + err.Error(),
			}
		}

		out = append(out, loadInput{name: "gs://" + u.bucket + "/" + n, data: data})
	}

	return out, nil
}

func uriError(uri string, err error) *jobError {
	if errors.Is(err, ErrObjectNotFound) {
		return &jobError{Reason: litNotfound, Message: "Not found: URI " + uri}
	}

	return &jobError{Reason: "backendError", Message: "Cannot read " + uri + ": " + err.Error()}
}

func gunzipIfNeeded(data []byte) ([]byte, error) {
	if len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b {
		return data, nil
	}

	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	defer zr.Close()

	out, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	return out, nil
}

// loadSchema picks the schema to parse into: an explicit one, the existing table's, or an autodetected one.
func (b *InMemoryBackend) loadSchema(lc loadConfig, in []loadInput) ([]bqsql.Field, bool, *jobError) {
	if len(lc.Schema) > 0 {
		return lc.Schema, true, nil
	}

	if d := b.datasets[dsKey(lc.Dest.ProjectID, lc.Dest.DatasetID)]; d != nil {
		if t, ok := d.Tables[lc.Dest.TableID]; ok && len(t.Schema) > 0 {
			return t.Schema, false, nil
		}
	}

	if !lc.Autodetect && lc.Format != formatParquet {
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

func parseInputs(lc loadConfig, fields []bqsql.Field, in []loadInput) ([][]any, *jobError) {
	rows := [][]any{}
	bad := 0

	var firstErr error

	for _, input := range in {
		got, badRows, err := parseInput(lc, fields, input)
		if err != nil {
			return nil, &jobError{
				Reason:  litInvalid,
				Message: "Error while reading data, error message: " + err.Error() + "; " + input.name,
			}
		}

		rows = append(rows, got...)
		bad += len(badRows)

		if firstErr == nil && len(badRows) > 0 {
			firstErr = badRows[0]
		}
	}

	if bad > lc.MaxBad {
		return nil, &jobError{
			Reason: litInvalid,
			Message: fmt.Sprintf(
				"Error while reading data, error message: processing encountered too many errors, giving up. "+
					"Rows: %d; errors: %d; max bad: %d; error percent: 0 (%v)",
				len(rows)+bad,
				bad,
				lc.MaxBad,
				firstErr,
			),
		}
	}

	return rows, nil
}

func parseInput(lc loadConfig, fields []bqsql.Field, in loadInput) ([][]any, []error, error) {
	switch lc.Format {
	case formatCSV:
		return parseCSVInput(lc, fields, in.data)
	case formatJSON:
		return parseJSONInput(lc, fields, in.data)
	case formatParquet:
		return parseParquetInput(lc, fields, in.data)
	}

	return nil, nil, fmt.Errorf("%w: sourceFormat %s is not supported", errLoad, lc.Format)
}
