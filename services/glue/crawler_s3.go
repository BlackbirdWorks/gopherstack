package glue

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	typeBigint          = "bigint"
	paramFalse          = "false"
	paramTrue           = "true"
	crawlerManagedParam = "UPDATED_BY_CRAWLER"
	crawlerSampleBytes  = 1 << 20
	crawlerSampleRows   = 1000
	crawlerS3Timeout    = 30 * time.Second

	serdeLazySimple = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	serdeJSON       = "org.openx.data.jsonserde.JsonSerDe"
	serdeParquet    = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	serdeORC        = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	serdeAvro       = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
	fmtTextInput    = "org.apache.hadoop.mapred.TextInputFormat"
	fmtTextOutput   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
)

// CrawlerS3 is the S3 read side a crawler needs to infer table schemas.
type CrawlerS3 interface {
	ListObjectsV2(ctx context.Context, input *sdk_s3.ListObjectsV2Input) (*sdk_s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, input *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error)
	PutObject(ctx context.Context, input *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error)
}

// SetS3Backend wires S3 so crawlers infer table schemas and partitions from
// the objects under their S3 targets. Unwired, a crawl creates bare tables.
func (b *InMemoryBackend) SetS3Backend(s CrawlerS3) {
	b.mu.Lock("SetS3Backend")
	defer b.mu.Unlock()

	b.crawlerS3 = s
}

func (b *InMemoryBackend) currentCrawlerS3() CrawlerS3 {
	b.mu.RLock("currentCrawlerS3")
	defer b.mu.RUnlock()

	return b.crawlerS3
}

type crawlFile struct {
	key  string
	dirs []string
}

type crawlGroup struct {
	name  string
	root  string
	files []crawlFile
}

type crawlFormat struct {
	serde, input, output, classification string
	params, serdeParams                  map[string]string
	columns                              []Column
}

func splitCrawlS3URI(u string) (string, string, bool) {
	rest, ok := strings.CutPrefix(u, "s3://")
	if !ok {
		return "", "", false
	}

	bucket, prefix, _ := strings.Cut(rest, "/")
	if bucket == "" {
		return "", "", false
	}

	return bucket, strings.Trim(prefix, "/"), true
}

func crawlTableName(prefix, segment string) string {
	name := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + 'a' - 'A'
		default:
			return '_'
		}
	}, segment)

	return strings.ToLower(prefix) + name
}

func (b *InMemoryBackend) listCrawlFiles(
	ctx context.Context,
	bucket, prefix string,
	excl []string,
) ([]crawlFile, error) {
	listPrefix := prefix
	if listPrefix != "" {
		listPrefix += "/"
	}

	var files []crawlFile

	var token *string

	for {
		out, err := b.crawlerS3.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(listPrefix), ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}

		for _, o := range out.Contents {
			key := aws.ToString(o.Key)
			rel := strings.TrimPrefix(key, listPrefix)
			base := path.Base(rel)

			if rel == "" || strings.HasSuffix(key, "/") || strings.HasPrefix(base, "_") ||
				strings.HasPrefix(base, ".") ||
				crawlExcluded(rel, excl) {
				continue
			}

			segs := strings.Split(rel, "/")
			files = append(files, crawlFile{key: key, dirs: segs[:len(segs)-1]})
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return files, nil
		}

		token = out.NextContinuationToken
	}
}

func crawlExcluded(rel string, patterns []string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, rel); ok {
			return true
		}

		if ok, _ := path.Match(p, path.Base(rel)); ok {
			return true
		}
	}

	return false
}

func hiveSegment(s string) (string, string, bool) {
	k, v, ok := strings.Cut(s, "=")

	return k, v, ok && k != "" && v != ""
}

func allHive(files []crawlFile) bool {
	for _, f := range files {
		for _, d := range f.dirs {
			if _, _, ok := hiveSegment(d); !ok {
				return false
			}
		}
	}

	return true
}

func hasRootFile(files []crawlFile) bool {
	for _, f := range files {
		if len(f.dirs) == 0 {
			return true
		}
	}

	return false
}

func groupCrawlFiles(rootName, rootPrefix string, files []crawlFile) []crawlGroup {
	if len(files) == 0 {
		return nil
	}

	if hasRootFile(files) || allHive(files) {
		return []crawlGroup{{name: rootName, root: rootPrefix, files: files}}
	}

	byDir := map[string][]crawlFile{}
	for _, f := range files {
		byDir[f.dirs[0]] = append(byDir[f.dirs[0]], crawlFile{key: f.key, dirs: f.dirs[1:]})
	}

	groups := make([]crawlGroup, 0, len(byDir))

	for _, dir := range slices.Sorted(maps.Keys(byDir)) {
		root := dir
		if rootPrefix != "" {
			root = rootPrefix + "/" + dir
		}

		groups = append(groups, crawlGroup{name: dir, root: root, files: byDir[dir]})
	}

	return groups
}

func mostCommonDepth(files []crawlFile) []crawlFile {
	counts := map[int]int{}
	for _, f := range files {
		counts[len(f.dirs)]++
	}

	best, bestN := 0, -1

	for _, d := range slices.Sorted(maps.Keys(counts)) {
		if counts[d] > bestN {
			best, bestN = d, counts[d]
		}
	}

	out := make([]crawlFile, 0, bestN)

	for _, f := range files {
		if len(f.dirs) == best {
			out = append(out, f)
		}
	}

	return out
}

func partitionKeysFor(files []crawlFile) []Column {
	if len(files) == 0 || len(files[0].dirs) == 0 {
		return nil
	}

	keys := make([]Column, len(files[0].dirs))

	for i := range keys {
		keys[i] = Column{Name: partitionKeyName(files, i), Type: partitionKeyType(files, i)}
	}

	return keys
}

func partitionKeyName(files []crawlFile, level int) string {
	want, _, _ := hiveSegment(files[0].dirs[level])

	for _, f := range files {
		if k, _, ok := hiveSegment(f.dirs[level]); !ok || k != want {
			return "partition_" + strconv.Itoa(level)
		}
	}

	return strings.ToLower(want)
}

func partitionKeyType(files []crawlFile, level int) string {
	for _, f := range files {
		if _, err := strconv.ParseInt(partitionValue(f.dirs[level]), 10, 64); err != nil {
			return avroString
		}
	}

	return typeBigint
}

func partitionValue(seg string) string {
	if _, v, ok := hiveSegment(seg); ok {
		return v
	}

	return seg
}

func (b *InMemoryBackend) readSample(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := b.crawlerS3.GetObject(ctx, &sdk_s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	return io.ReadAll(io.LimitReader(out.Body, crawlerSampleBytes))
}

func inferFormat(key string, sample []byte) crawlFormat {
	lower := strings.ToLower(key)
	lower = strings.TrimSuffix(lower, ".gz")

	switch path.Ext(lower) {
	case ".json", ".jsonl", ".ndjson":
		return jsonFormat(sample)
	case ".parquet":
		return crawlFormat{
			serde:          serdeParquet,
			classification: "parquet",
			input:          "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			output:         "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
		}
	case ".orc":
		return crawlFormat{
			serde:          serdeORC,
			classification: "orc",
			input:          "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
			output:         "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
		}
	case ".avro":
		return crawlFormat{
			serde:          serdeAvro,
			classification: "avro",
			input:          "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
			output:         "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
		}
	case ".tsv":
		return delimitedFormat(sample, '\t', "tsv")
	default:
		return delimitedFormat(sample, ',', "csv")
	}
}

func delimitedFormat(sample []byte, delim rune, classification string) crawlFormat {
	r := csv.NewReader(bytes.NewReader(sample))
	r.Comma = delim
	r.FieldsPerRecord = -1
	r.LazyQuotes = true

	var rows [][]string

	for len(rows) < crawlerSampleRows {
		rec, err := r.Read()
		if err != nil {
			break
		}

		rows = append(rows, rec)
	}

	d := string(delim)
	f := crawlFormat{
		serde: serdeLazySimple, input: fmtTextInput, output: fmtTextOutput, classification: classification,
		serdeParams: map[string]string{"field.delim": d, "serialization.format": d},
		params:      map[string]string{"delimiter": d, "areColumnsQuoted": paramFalse, "columnsOrdered": paramTrue},
	}

	if len(rows) == 0 {
		return f
	}

	width := len(rows[0])
	header := len(rows) > 1 && csvHasHeader(rows)
	data := rows

	if header {
		data = rows[1:]
		f.params["skip.header.line.count"] = "1"
		f.params["hasHeader"] = "true"
	}

	f.columns = make([]Column, width)

	for i := range width {
		name := "col" + strconv.Itoa(i)
		if header {
			name = strings.ToLower(strings.TrimSpace(rows[0][i]))
		}

		vals := make([]string, 0, len(data))

		for _, row := range data {
			if i < len(row) {
				vals = append(vals, row[i])
			}
		}

		f.columns[i] = Column{Name: name, Type: scalarType(vals)}
	}

	return f
}

func csvHasHeader(rows [][]string) bool {
	first := rows[0]
	seen := map[string]bool{}

	for i, h := range first {
		h = strings.ToLower(strings.TrimSpace(h))
		if h == "" || seen[h] || scalarType([]string{first[i]}) != avroString {
			return false
		}

		seen[h] = true
	}

	for i := range first {
		vals := make([]string, 0, len(rows)-1)

		for _, row := range rows[1:] {
			if i < len(row) {
				vals = append(vals, row[i])
			}
		}

		if scalarType(vals) != avroString {
			return true
		}
	}

	return false
}

func scalarType(vals []string) string {
	isInt, isFloat, isBool, seen := true, true, true, false

	for _, v := range vals {
		if v == "" {
			continue
		}

		seen = true

		if _, err := strconv.ParseInt(v, 10, 64); err != nil {
			isInt = false
		}

		if _, err := strconv.ParseFloat(v, 64); err != nil {
			isFloat = false
		}

		if lv := strings.ToLower(v); lv != paramTrue && lv != paramFalse {
			isBool = false
		}
	}

	switch {
	case !seen:
		return avroString
	case isInt:
		return typeBigint
	case isFloat:
		return avroDouble
	case isBool:
		return avroBoolean
	default:
		return avroString
	}
}

func jsonFormat(sample []byte) crawlFormat {
	f := crawlFormat{
		serde: serdeJSON, input: fmtTextInput, output: fmtTextOutput, classification: "json",
		params: map[string]string{"compressionType": "none", "typeOfData": "file"},
	}

	types := map[string]string{}

	var order []string

	for line := range bytes.SplitSeq(sample, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		keys, vals, ok := decodeJSONObject(line)
		if !ok {
			continue
		}

		for i, k := range keys {
			t := jsonType(vals[i])
			prev, seen := types[k]

			switch {
			case !seen:
				order = append(order, k)

				types[k] = t
			case prev != t && t != "":
				types[k] = mergeJSONTypes(prev, t)
			}
		}
	}

	for _, k := range order {
		t := types[k]
		if t == "" {
			t = avroString
		}

		f.columns = append(f.columns, Column{Name: strings.ToLower(k), Type: t})
	}

	return f
}

func mergeJSONTypes(a, b string) string {
	switch {
	case a == "":
		return b
	case a == typeBigint && b == avroDouble, a == avroDouble && b == typeBigint:
		return avroDouble
	default:
		return avroString
	}
}

func decodeJSONObject(line []byte) ([]string, []json.RawMessage, bool) {
	dec := json.NewDecoder(bytes.NewReader(line))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return nil, nil, false
	}

	var keys []string

	var vals []json.RawMessage

	for dec.More() {
		kt, err := dec.Token()
		if err != nil {
			return nil, nil, false
		}

		var raw json.RawMessage
		if err = dec.Decode(&raw); err != nil {
			return nil, nil, false
		}

		keys = append(keys, fmt.Sprint(kt))
		vals = append(vals, raw)
	}

	return keys, vals, true
}

func jsonType(raw json.RawMessage) string {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return avroString
	}

	return jsonValueType(v)
}

func jsonValueType(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case bool:
		return avroBoolean
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return typeBigint
		}

		return avroDouble
	case []any:
		elem := ""

		for _, e := range x {
			if elem = jsonValueType(e); elem != "" {
				break
			}
		}

		if elem == "" {
			elem = avroString
		}

		return "array<" + elem + ">"
	case map[string]any:
		parts := make([]string, 0, len(x))

		for _, k := range slices.Sorted(maps.Keys(x)) {
			t := jsonValueType(x[k])
			if t == "" {
				t = avroString
			}

			parts = append(parts, strings.ToLower(k)+":"+t)
		}

		return "struct<" + strings.Join(parts, ",") + ">"
	default:
		return avroString
	}
}

func (r *crawlRun) crawlS3(t S3Target) error {
	b := r.b

	if b.crawlerS3 == nil {
		r.legacyS3(t)

		return nil
	}

	bucket, prefix, ok := splitCrawlS3URI(t.Path)
	if !ok {
		return fmt.Errorf("invalid S3 path %q", t.Path)
	}

	ctx, cancel := context.WithTimeout(context.Background(), crawlerS3Timeout)
	defer cancel()

	files, err := b.listCrawlFiles(ctx, bucket, prefix, t.Exclusions)
	if err != nil {
		return fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
	}

	rootName := bucket
	if prefix != "" {
		rootName = path.Base(prefix)
	}

	for _, g := range groupCrawlFiles(rootName, prefix, files) {
		if err = r.crawlGroup(ctx, bucket, g); err != nil {
			return err
		}
	}

	return nil
}

func (r *crawlRun) legacyS3(t S3Target) {
	prefix := ""
	if _, after, ok := strings.Cut(strings.TrimPrefix(t.Path, "s3://"), "/"); ok {
		prefix = strings.Trim(after, "/")
	}

	if prefix == "" {
		prefix = "default"
	}

	name := strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(prefix)
	if !r.b.tables.Has(tableKey(r.c.DatabaseName, name)) {
		r.b.tables.Put(&Table{
			Name: name, DatabaseName: r.c.DatabaseName, CreateTime: float64(time.Now().Unix()),
		})
		r.stats.added++
	}
}

func (r *crawlRun) crawlGroup(ctx context.Context, bucket string, g crawlGroup) error {
	c, b := r.c, r.b
	files := mostCommonDepth(g.files)
	sort.Slice(files, func(i, j int) bool { return files[i].key < files[j].key })

	name := crawlTableName(c.TablePrefix, g.name)
	key := tableKey(c.DatabaseName, name)

	if cur, exists := b.tables.Get(key); exists && r.newFoldersOnly && cur.Parameters[crawlerManagedParam] == c.Name {
		r.seen[key] = true
		r.crawlPartitions(cur, bucket, g.root, files)

		return nil
	}

	sample, err := b.readSample(ctx, bucket, files[0].key)
	if err != nil {
		return fmt.Errorf("read s3://%s/%s: %w", bucket, files[0].key, err)
	}

	f := inferFormat(files[0].key, sample)
	location := strings.TrimSuffix("s3://"+bucket+"/"+g.root, "/") + "/"

	params := map[string]string{"classification": f.classification, crawlerManagedParam: c.Name}
	maps.Copy(params, f.params)

	tbl := r.upsert(&Table{
		Name: name, DatabaseName: c.DatabaseName, TableType: tableTypeExternal,
		Parameters:    params,
		PartitionKeys: partitionKeysFor(files),
		StorageDescriptor: StorageDescriptor{
			Columns: f.columns, Location: location, InputFormat: f.input, OutputFormat: f.output,
			SerdeInfo:  &SerDeInfo{SerializationLibrary: f.serde, Parameters: maps.Clone(f.serdeParams)},
			Parameters: maps.Clone(params),
		},
	})
	if tbl != nil {
		r.crawlPartitions(tbl, bucket, g.root, files)
	}

	return nil
}

func (r *crawlRun) crawlPartitions(tbl *Table, bucket, root string, files []crawlFile) {
	if len(tbl.PartitionKeys) == 0 {
		return
	}

	b := r.b
	seen := map[string]bool{}
	now := float64(time.Now().Unix())

	for _, f := range files {
		vals := make([]string, len(f.dirs))
		for i, d := range f.dirs {
			vals[i] = partitionValue(d)
		}

		pk := partitionKey(tbl.DatabaseName, tbl.Name, vals)
		if seen[pk] || b.partitions.Has(pk) {
			continue
		}

		seen[pk] = true
		loc := "s3://" + bucket + "/" + strings.TrimSuffix(root+"/"+strings.Join(f.dirs, "/"), "/") + "/"
		psd := tbl.StorageDescriptor
		psd.Location = loc
		psd.Parameters = maps.Clone(tbl.StorageDescriptor.Parameters)

		b.partitions.Put(&Partition{
			DatabaseName: tbl.DatabaseName, TableName: tbl.Name, CatalogID: b.accountID,
			Values: vals, StorageDescriptor: psd, CreationTime: now,
		})
		r.stats.partitions++
	}
}
