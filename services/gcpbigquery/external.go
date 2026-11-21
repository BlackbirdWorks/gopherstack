package gcpbigquery

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

const (
	litExternalCfg   = "externalDataConfiguration"
	litHive          = "hivePartitioningOptions"
	litCSVOptions    = "csvOptions"
	typeExternal     = "EXTERNAL"
	fileNameColumn   = "_FILE_NAME"
	hiveModeAuto     = "AUTO"
	hiveModeStrings  = "STRINGS"
	hiveModeCustom   = "CUSTOM"
	hiveNullPart     = "__HIVE_DEFAULT_PARTITION__"
	maxHiveKeys      = 10
	litSourceFormat  = "sourceFormat"
	litSourceURIs    = "sourceUris"
	litIgnoreUnknown = "ignoreUnknownValues"
	litQuote         = "quote"
	optSkipRows      = "skip_leading_rows"
	litEncoding      = "encoding"
)

var errExternal = errors.New("external table")

type externalSpec struct {
	hiveMode      string
	hivePrefix    string
	customKeys    []bqsql.Field
	partKeys      []string
	lc            loadConfig
	requireFilter bool
}

//nolint:gochecknoglobals // compiled once, read-only
var (
	quotedURI   = sync.OnceValue(func() *regexp.Regexp { return regexp.MustCompile(`'([^']*)'|"([^"]*)"`) })
	customKeyRe = sync.OnceValue(func() *regexp.Regexp {
		return regexp.MustCompile(`\{([A-Za-z0-9_]+):([A-Za-z0-9_]+)\}`)
	})
	externalOpts = map[string]bool{
		"format": true, "uris": true, optSkipRows: true, "field_delimiter": true, litQuote: true,
		litEncoding: true, "null_marker": true, "max_bad_records": true, "ignore_unknown_values": true,
		"allow_jagged_rows": true, "allow_quoted_newlines": true, "compression": true,
		"hive_partition_uri_prefix": true, "require_hive_partition_filter": true, "expiration_timestamp": true,
		"description": true,
	}
)

func (t *Table) isExternal() bool {
	_, ok := t.Meta[litExternalCfg].(map[string]any)

	return ok
}

func externalFormat(f string) (string, error) {
	switch f = strings.ToUpper(strings.TrimSpace(f)); f {
	case "JSON":
		return formatJSON, nil
	case formatCSV, formatJSON, formatParquet, formatAvro, formatORC:
		return f, nil
	case "":
		return "", errInvalid("externalDataConfiguration.sourceFormat is required")
	}

	return "", errInvalid("Source format %s is not supported for external tables; use CSV, "+
		"NEWLINE_DELIMITED_JSON, PARQUET, AVRO or ORC", f)
}

// externalConfigFromOptions maps CREATE EXTERNAL TABLE OPTIONS to the REST externalDataConfiguration.
func externalConfigFromOptions(opts map[string]string) (map[string]any, error) {
	for k := range opts {
		if !externalOpts[k] {
			return nil, errInvalid("Unknown option %q for an external table", k)
		}
	}

	f, err := externalFormat(opts["format"])
	if err != nil {
		return nil, err
	}

	var uris []any

	for _, m := range quotedURI().FindAllStringSubmatch(opts["uris"], -1) {
		uris = append(uris, m[1]+m[2])
	}

	if len(uris) == 0 {
		return nil, errInvalid("The uris option needs at least one Cloud Storage URI")
	}

	cfg := map[string]any{litSourceFormat: f, litSourceURIs: uris}
	csv := map[string]any{}

	for opt, key := range map[string]string{
		optSkipRows: "skipLeadingRows", "field_delimiter": "fieldDelimiter", litQuote: litQuote,
		litEncoding: litEncoding, "null_marker": "nullMarker", "allow_jagged_rows": "allowJaggedRows",
		"allow_quoted_newlines": "allowQuotedNewlines",
	} {
		if v, ok := opts[opt]; ok {
			csv[key] = optionValue(v)

			if opt == optSkipRows {
				csv[key] = v
			}
		}
	}

	if len(csv) > 0 {
		cfg[litCSVOptions] = csv
	}

	if v, ok := opts["max_bad_records"]; ok {
		cfg["maxBadRecords"] = optionValue(v)
	}

	if v, ok := opts["ignore_unknown_values"]; ok {
		cfg[litIgnoreUnknown] = optionValue(v)
	}

	if v, ok := opts["compression"]; ok {
		cfg["compression"] = strings.ToUpper(v)
	}

	if p, ok := opts["hive_partition_uri_prefix"]; ok {
		cfg[litHive] = map[string]any{
			"mode": hiveModeAuto, "sourceUriPrefix": p,
			"requirePartitionFilter": strings.EqualFold(opts["require_hive_partition_filter"], litTrue),
		}
	}

	return cfg, nil
}

func optionValue(v string) any {
	switch strings.ToLower(v) {
	case litTrue:
		return true
	case "false":
		return false
	}

	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}

	return v
}

func csvOption[T any](cfg map[string]any, key string) (T, bool) {
	opts, _ := cfg[litCSVOptions].(map[string]any)
	v, ok := opts[key].(T)

	return v, ok
}

func specFromConfig(cfg map[string]any, schema []bqsql.Field) (externalSpec, error) {
	var sp externalSpec

	f, _ := cfg[litSourceFormat].(string)

	format, err := externalFormat(f)
	if err != nil {
		return sp, err
	}

	lc := loadConfig{Format: format, Delimiter: ",", Schema: schema}
	lc.URIs = anyStrings(cfg[litSourceURIs])
	lc.Autodetect, _ = cfg["autodetect"].(bool)
	lc.IgnoreExtra, _ = cfg[litIgnoreUnknown].(bool)
	lc.MaxBad, _ = intOf(cfg["maxBadRecords"])

	if len(lc.URIs) == 0 {
		return sp, errInvalid("externalDataConfiguration.sourceUris is required")
	}

	if v, ok := csvOption[string](cfg, "fieldDelimiter"); ok && v != "" {
		lc.Delimiter = v
	}

	lc.NullMarker, _ = csvOption[string](cfg, "nullMarker")
	lc.Encoding, _ = csvOption[string](cfg, litEncoding)
	lc.Encoding = strings.ToUpper(lc.Encoding)
	lc.Jagged, _ = csvOption[bool](cfg, "allowJaggedRows")

	if q, ok := csvOption[string](cfg, litQuote); ok {
		lc.Quote = &q
	}

	if opts, _ := cfg[litCSVOptions].(map[string]any); opts != nil {
		lc.SkipRows, _ = intOf(opts["skipLeadingRows"])
	}

	sp.lc = lc

	return sp, sp.hiveFromConfig(cfg)
}

func (sp *externalSpec) hiveFromConfig(cfg map[string]any) error {
	h, _ := cfg[litHive].(map[string]any)
	if h == nil {
		return nil
	}

	sp.hivePrefix, _ = h["sourceUriPrefix"].(string)
	sp.requireFilter, _ = h["requirePartitionFilter"].(bool)
	sp.partKeys = anyStrings(h["fields"])

	sp.hiveMode, _ = h["mode"].(string)
	if sp.hiveMode == "" {
		sp.hiveMode = hiveModeAuto
	}

	sp.hiveMode = strings.ToUpper(sp.hiveMode)

	switch sp.hiveMode {
	case hiveModeAuto, hiveModeStrings:
	case hiveModeCustom:
		for _, m := range customKeyRe().FindAllStringSubmatch(sp.hivePrefix, -1) {
			sp.customKeys = append(sp.customKeys,
				bqsql.Field{Name: m[1], Type: strings.ToUpper(m[2]), Mode: bqsql.ModeNullable})
		}

		if len(sp.customKeys) == 0 {
			return errInvalid("hivePartitioningOptions.sourceUriPrefix needs {key:TYPE} segments in CUSTOM mode")
		}

		sp.hivePrefix, _, _ = strings.Cut(sp.hivePrefix, "{")
	default:
		return errInvalid("Invalid hive partitioning mode %q; use AUTO, STRINGS or CUSTOM", sp.hiveMode)
	}

	if sp.hivePrefix == "" {
		return errInvalid("hivePartitioningOptions.sourceUriPrefix is required")
	}

	return nil
}

// hiveKV splits the key=value directories of object (a full gs:// URI) below the spec's prefix.
func (sp *externalSpec) hiveKV(object string) ([][2]string, error) {
	rest, ok := strings.CutPrefix(object, strings.TrimSuffix(sp.hivePrefix, "/")+"/")
	if !ok {
		return nil, fmt.Errorf("%w: %s is not under the hive partitioning prefix %s",
			errExternal, object, sp.hivePrefix)
	}

	segs := strings.Split(rest, "/")
	segs = segs[:len(segs)-1]

	var out [][2]string

	if sp.hiveMode == hiveModeCustom {
		if len(segs) < len(sp.customKeys) {
			return nil, fmt.Errorf("%w: %s does not carry the partition keys of %s", errExternal, object, sp.hivePrefix)
		}

		for i, k := range sp.customKeys {
			v := segs[i]
			if _, after, found := strings.Cut(v, "="); found {
				v = after
			}

			out = append(out, [2]string{k.Name, v})
		}

		return out, nil
	}

	for _, s := range segs {
		k, v, found := strings.Cut(s, "=")
		if !found {
			return nil, fmt.Errorf("%w: %s has a directory without key=value", errExternal, object)
		}

		out = append(out, [2]string{k, v})
	}

	if len(out) > maxHiveKeys {
		return nil, fmt.Errorf("%w: at most %d partition keys are supported", errExternal, maxHiveKeys)
	}

	return out, nil
}

func unescapeHive(v string) any {
	if v == hiveNullPart {
		return nil
	}

	if u, err := url.PathUnescape(v); err == nil {
		return u
	}

	return v
}

// partitionFields derives the partition columns from the matched files.
func (sp *externalSpec) partitionFields(inputs []loadInput) ([]bqsql.Field, error) {
	if sp.hiveMode == "" {
		return nil, nil
	}

	if sp.hiveMode == hiveModeCustom {
		return sp.customKeys, nil
	}

	names, kinds, err := sp.scanPartitionKeys(inputs)
	if err != nil {
		return nil, err
	}

	out := make([]bqsql.Field, len(names))

	for i, n := range names {
		typ := bqsql.KindString.RESTName()

		if k := kinds[n]; sp.hiveMode == hiveModeAuto && inferablePartition(k) {
			typ = k.RESTName()
		}

		out[i] = bqsql.Field{Name: n, Type: typ, Mode: bqsql.ModeNullable}
	}

	return out, nil
}

// scanPartitionKeys checks every file carries the same keys in the same order and infers each key's kind.
func (sp *externalSpec) scanPartitionKeys(inputs []loadInput) ([]string, map[string]bqsql.Kind, error) {
	var names []string

	kinds := map[string]bqsql.Kind{}

	for _, in := range inputs {
		kv, err := sp.hiveKV(in.name)
		if err != nil {
			return nil, nil, err
		}

		if names == nil {
			for _, p := range kv {
				names = append(names, p[0])
			}
		}

		if len(kv) != len(names) {
			return nil, nil, fmt.Errorf("%w: partition keys differ between files", errExternal)
		}

		for i, p := range kv {
			if p[0] != names[i] {
				return nil, nil, fmt.Errorf("%w: partition keys must appear in the same order in every file",
					errExternal)
			}

			kinds[p[0]] = mergeKind(kinds[p[0]], partitionKind(p[1]))
		}
	}

	return names, kinds, nil
}

func partitionKind(v string) bqsql.Kind {
	if v == hiveNullPart {
		return bqsql.KindUnknown
	}

	u, _ := unescapeHive(v).(string)

	return csvCellKind(u)
}

func jobErr(j *jobError) error {
	return fmt.Errorf("%w: %s", errExternal, j.Message)
}

// externalSchema resolves the table schema when the table is created: the declared one, or the files' own.
func (b *InMemoryBackend) externalSchema(cfg map[string]any, declared []bqsql.Field) ([]bqsql.Field, error) {
	fields, keys, err := b.resolveExternal(cfg, declared)
	if err != nil {
		return nil, err
	}

	if h, ok := cfg[litHive].(map[string]any); ok {
		ks := make([]any, len(keys))
		for i, k := range keys {
			ks[i] = k
		}

		h["fields"] = ks
	}

	return fields, nil
}

func (b *InMemoryBackend) resolveExternal(
	cfg map[string]any, declared []bqsql.Field,
) ([]bqsql.Field, []string, error) {
	sp, err := specFromConfig(cfg, declared)
	if err != nil {
		return nil, nil, err
	}

	if b.objects == nil {
		return nil, nil, errInvalid("Cloud Storage is not available to this BigQuery emulator")
	}

	inputs, jerr := b.loadInputs(sp.lc, nil)
	if jerr != nil {
		return nil, nil, errInvalid("%s", jerr.Message)
	}

	parts, err := sp.partitionFields(inputs)
	if err != nil {
		return nil, nil, errInvalid("%v", err)
	}

	fileFields := declared

	if len(fileFields) == 0 {
		if !sp.lc.Autodetect && !selfDescribing(sp.lc.Format) {
			return nil, nil, errInvalid(
				"Schema must be specified when creating an external table, or autodetect enabled")
		}

		if fileFields, err = detectSchema(sp.lc, inputs); err != nil {
			return nil, nil, errInvalid("%v", err)
		}
	}

	keys := make([]string, len(parts))
	for i, p := range parts {
		keys[i] = p.Name
	}

	return mergePartitionFields(fileFields, parts), keys, nil
}

func mergePartitionFields(fileFields, parts []bqsql.Field) []bqsql.Field {
	out := append([]bqsql.Field{}, fileFields...)

	for _, p := range parts {
		found := false

		for _, f := range fileFields {
			if strs.Equal(f.Name, p.Name) {
				found = true

				break
			}
		}

		if !found {
			out = append(out, p)
		}
	}

	return out
}

// readExternalRows reads the table's objects; each row carries the schema columns, then the pseudo columns.
func (b *InMemoryBackend) readExternalRows(t *Table) ([][]any, error) {
	cfg, _ := t.Meta[litExternalCfg].(map[string]any)

	sp, err := specFromConfig(cfg, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", errExternal, apiMessage(err))
	}

	if b.objects == nil {
		return nil, fmt.Errorf("%w: Cloud Storage is not available to this BigQuery emulator", errExternal)
	}

	inputs, jerr := b.loadInputs(sp.lc, nil)
	if jerr != nil {
		return nil, jobErr(jerr)
	}

	rows := [][]any{}

	for _, in := range inputs {
		got, rerr := externalFileRows(sp, t.Schema, in)
		if rerr != nil {
			return nil, rerr
		}

		rows = append(rows, got...)
	}

	return rows, nil
}

func externalFileRows(sp externalSpec, schema []bqsql.Field, in loadInput) ([][]any, error) {
	var kv [][2]string

	if sp.hiveMode != "" {
		var err error
		if kv, err = sp.hiveKV(in.name); err != nil {
			return nil, err
		}
	}

	isPart := map[string]string{}
	for _, p := range kv {
		isPart[strs.Fold(p[0])] = p[1]
	}

	var fileFields []bqsql.Field

	for _, f := range schema {
		if _, part := isPart[strs.Fold(f.Name)]; !part {
			fileFields = append(fileFields, f)
		}
	}

	lc := sp.lc
	lc.Schema = fileFields

	parsed, _, jerr := parseInputs(lc, fileFields, []loadInput{in})
	if jerr != nil {
		return nil, jobErr(jerr)
	}

	out := make([][]any, len(parsed))

	for i, row := range parsed {
		out[i] = assembleExternalRow(schema, row, isPart, in.name)
	}

	return out, nil
}

func assembleExternalRow(
	schema []bqsql.Field, row []any, parts map[string]string, file string,
) []any {
	out := make([]any, 0, len(schema)+1)
	pos := 0

	for _, f := range schema {
		if raw, part := parts[strs.Fold(f.Name)]; part {
			v, err := bqsql.ParseValue(f, unescapeHive(raw), bqsql.ParseOptions{})
			if err != nil {
				v = nil
			}

			out = append(out, v)

			continue
		}

		out = append(out, row[pos])
		pos++
	}

	return append(out, file)
}

func externalTableData(b *InMemoryBackend, t *Table, td *bqsql.TableData) {
	td.External = true
	td.Pseudo = []bqsql.Field{{Name: fileNameColumn, Type: bqsql.KindString.RESTName(), Mode: bqsql.ModeNullable}}
	td.RowsFn = func() ([][]any, error) { return b.readExternalRows(t) }
	td.Rows = nil

	cfg, _ := t.Meta[litExternalCfg].(map[string]any)

	if sp, err := specFromConfig(cfg, nil); err == nil && sp.requireFilter {
		td.PartitionFilter = sp.partKeys
	}
}

// externalTableSchema resolves the schema of an external table created through tables.insert; the table schema wins
// over the one inside the configuration.
func (b *InMemoryBackend) externalTableSchema(cfg map[string]any, schema []bqsql.Field) ([]bqsql.Field, error) {
	if len(schema) == 0 {
		inner, err := decodeSchema(cfg["schema"])
		if err != nil {
			return nil, err
		}

		schema = inner
	}

	return b.externalSchema(cfg, schema)
}

// externalFromEffect turns CREATE EXTERNAL TABLE into the stored configuration and schema.
func (b *InMemoryBackend) externalFromEffect(e bqsql.Effect, t *Table) error {
	cfg, err := externalConfigFromOptions(e.Options)
	if err != nil {
		return err
	}

	if e.WithPartCols {
		if _, ok := e.Options["hive_partition_uri_prefix"]; !ok {
			return errInvalid("WITH PARTITION COLUMNS requires the hive_partition_uri_prefix option")
		}

		keys := make([]any, len(e.PartFields))
		for i, f := range e.PartFields {
			keys[i] = f.Name
		}

		if h, ok := cfg[litHive].(map[string]any); ok {
			h["fields"] = keys
		}
	}

	declared := append(append([]bqsql.Field{}, e.Fields...), e.PartFields...)

	schema, err := b.externalSchema(cfg, declared)
	if err != nil {
		return err
	}

	t.Schema = schema
	t.Meta[litExternalCfg] = cfg

	return nil
}

func inferablePartition(k bqsql.Kind) bool {
	return k == bqsql.KindInt || k == bqsql.KindDate || k == bqsql.KindTimestamp
}
