package glue

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/iskorotkov/avro/v2/ocf"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	hudiDir         = ".hoodie"
	hudiProps       = "hoodie.properties"
	hudiTypeMOR     = "MERGE_ON_READ"
	hudiInput       = "org.apache.hudi.hadoop.HoodieParquetInputFormat"
	hudiRealtimeIn  = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat"
	hudiMetaPrefix  = "_hoodie_"
	hudiSchemaProp  = "hoodie.table.create.schema"
	hudiPartProp    = "hoodie.table.partition.fields"
	hudiNameProp    = "hoodie.table.name"
	hudiTypeProp    = "hoodie.table.type"
	hudiTimelineDir = ".hoodie/timeline"
)

var errHudiNoSchema = errors.New("no Hudi schema in hoodie.properties or a completed commit")

func isHudiCompletedCommit(file string) bool {
	return strings.HasSuffix(file, ".commit") || strings.HasSuffix(file, ".deltacommit") ||
		strings.HasSuffix(file, ".replacecommit")
}

// hudiCommitSchema reads extraMetadata.schema from a completed instant: JSON
// in 0.x timelines, an Avro container file in 1.x.
func hudiCommitSchema(body []byte) string {
	if !bytes.HasPrefix(body, []byte("Obj\x01")) {
		var commit struct {
			ExtraMetadata map[string]string `json:"extraMetadata"`
		}

		if decodeJSON(body, &commit) != nil {
			return ""
		}

		return commit.ExtraMetadata["schema"]
	}

	dec, err := ocf.NewDecoder(bytes.NewReader(body))
	if err != nil {
		return ""
	}

	for dec.HasNext() {
		var row map[string]any
		if dec.Decode(&row) != nil {
			return ""
		}

		switch m := row["extraMetadata"].(type) {
		case map[string]string:
			return m["schema"]
		case map[string]any:
			s, _ := m["schema"].(string)

			return s
		}
	}

	return ""
}

func hudiMetaColumns() []string {
	return []string{
		"_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
		"_hoodie_partition_path", "_hoodie_file_name",
	}
}

func parseProperties(data []byte) map[string]string {
	out := map[string]string{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, lakeScanBuf), lakeMetaLimit)

	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' || line[0] == '!' {
			continue
		}

		k, v, ok := strings.Cut(line, "=")
		if ok {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}

	return out
}

func isHudiProps(file string) bool { return file == hudiProps }

func (r *crawlRun) crawlHudi(t HudiTarget) error {
	ctx, cancel, err := r.lakeContext()
	if err != nil {
		return err
	}
	defer cancel()

	for _, p := range t.Paths {
		bucket, prefix, ok := splitCrawlS3URI(p)
		if !ok {
			return fmt.Errorf("%w %q", errInvalidCrawlPath, p)
		}

		keys, lerr := r.listKeys(ctx, bucket, prefix)
		if lerr != nil {
			return lerr
		}

		for _, root := range lakeRoots(keys, prefix, hudiDir, isHudiProps, int(t.MaximumTraversalDepth), t.Exclusions) {
			if err = r.crawlHudiTable(ctx, bucket, root, keys); err != nil {
				return fmt.Errorf("hudi table s3://%s/%s: %w", bucket, root, err)
			}
		}
	}

	return nil
}

func (r *crawlRun) hudiSchema(
	ctx context.Context, bucket, root string, props map[string]string, keys []string,
) (any, error) {
	if s := props[hudiSchemaProp]; s != "" {
		var schema any
		if err := decodeJSON([]byte(s), &schema); err != nil {
			return nil, fmt.Errorf("parse %s: %w", hudiSchemaProp, err)
		}

		return schema, nil
	}

	var commits []string

	for _, k := range keys {
		dir, file := splitKey(k)
		if (dir == joinKey(root, hudiDir) || dir == joinKey(root, hudiTimelineDir)) && isHudiCompletedCommit(file) {
			commits = append(commits, k)
		}
	}

	slices.SortFunc(commits, func(x, y string) int {
		_, fx := splitKey(x)
		_, fy := splitKey(y)

		return strings.Compare(fx, fy)
	})

	for _, key := range slices.Backward(commits) {
		body, err := r.readObject(ctx, bucket, key)
		if err != nil {
			return nil, err
		}

		def := hudiCommitSchema(body)
		if def == "" {
			continue
		}

		var schema any
		if err = decodeJSON([]byte(def), &schema); err != nil {
			return nil, fmt.Errorf("parse schema of s3://%s/%s: %w", bucket, key, err)
		}

		return schema, nil
	}

	return nil, errHudiNoSchema
}

func avroHiveType(t any) string {
	switch v := t.(type) {
	case string:
		return avroPrimitiveHive(v)
	case []any:
		for _, m := range v {
			if s, ok := m.(string); !ok || s != avroNull {
				return avroHiveType(m)
			}
		}
	case map[string]any:
		return avroComplexHive(v)
	}

	return avroString
}

func avroPrimitiveHive(t string) string {
	switch t {
	case avroLong:
		return typeBigint
	case avroInt, avroFloat, avroDouble, avroBoolean:
		return t
	case avroBytes, avroFixed:
		return typeBinary
	default:
		return avroString
	}
}

func avroComplexHive(v map[string]any) string {
	switch v["logicalType"] {
	case typeDate:
		return typeDate
	case "timestamp-millis", "timestamp-micros":
		return typeTimestamp
	case "decimal":
		p, _ := v["precision"].(float64)
		s, _ := v["scale"].(float64)

		return fmt.Sprintf("decimal(%d,%d)", int(p), int(s))
	}

	switch v["type"] {
	case "record":
		fields, _ := v["fields"].([]any)
		parts := make([]string, 0, len(fields))

		for _, f := range fields {
			fm, _ := f.(map[string]any)
			name, _ := fm["name"].(string)
			parts = append(parts, name+":"+avroHiveType(fm["type"]))
		}

		return "struct<" + strings.Join(parts, ",") + ">"
	case "array":
		return "array<" + avroHiveType(v["items"]) + ">"
	case avroMap:
		return "map<string," + avroHiveType(v["values"]) + ">"
	default:
		return avroHiveType(v["type"])
	}
}

func hasMetaPrefix(field any) bool {
	fm, _ := field.(map[string]any)
	name, _ := fm["name"].(string)

	return strings.HasPrefix(name, hudiMetaPrefix)
}

func hudiColumns(schema any) []Column {
	rec, _ := schema.(map[string]any)
	fields, _ := rec["fields"].([]any)

	var cols []Column

	if len(fields) == 0 || !hasMetaPrefix(fields[0]) {
		for _, n := range hudiMetaColumns() {
			cols = append(cols, Column{Name: n, Type: avroString})
		}
	}

	for _, f := range fields {
		fm, _ := f.(map[string]any)
		name, _ := fm["name"].(string)
		cols = append(cols, Column{Name: name, Type: avroHiveType(fm["type"])})
	}

	return cols
}

func hudiPartitionFields(props map[string]string) []string {
	var out []string

	for f := range strings.SplitSeq(props[hudiPartProp], ",") {
		f, _, _ = strings.Cut(strings.TrimSpace(f), ":")
		if f != "" {
			out = append(out, f)
		}
	}

	return out
}

func (r *crawlRun) crawlHudiTable(ctx context.Context, bucket, root string, keys []string) error {
	body, err := r.readObject(ctx, bucket, joinKey(joinKey(root, hudiDir), hudiProps))
	if err != nil {
		return err
	}

	props := parseProperties(body)

	schema, err := r.hudiSchema(ctx, bucket, root, props, keys)
	if err != nil {
		return err
	}

	cols, pkeys := moveToPartitionKeys(hudiColumns(schema), hudiPartitionFields(props))

	baseName := props[hudiNameProp]
	if baseName == "" {
		baseName = lastPathSegment(root)
	}

	location := "s3://" + bucket + "/" + root + "/"
	files := hudiDataFiles(keys, root, len(pkeys))

	variants := []struct{ suffix, input string }{{"", hudiInput}}
	if props[hudiTypeProp] == hudiTypeMOR {
		variants = []struct{ suffix, input string }{{"_ro", hudiInput}, {"_rt", hudiRealtimeIn}}
	}

	for _, v := range variants {
		params := map[string]string{paramClassification: classificationParq, crawlerManagedParam: r.c.Name}

		tbl := r.upsert(&Table{
			Name: crawlTableName(r.c.TablePrefix, baseName+v.suffix), DatabaseName: strs.Fold(r.c.DatabaseName),
			Parameters: params, PartitionKeys: pkeys,
			StorageDescriptor: StorageDescriptor{
				Columns: cols, Location: location, InputFormat: v.input, OutputFormat: parquetOutput,
				SerdeInfo:  &SerDeInfo{SerializationLibrary: serdeParquetHive},
				Parameters: maps.Clone(params),
			},
		})
		if tbl != nil {
			r.crawlPartitions(tbl, bucket, root, files)
		}
	}

	return nil
}

func hudiDataFiles(keys []string, root string, depth int) []crawlFile {
	if depth == 0 {
		return nil
	}

	var out []crawlFile

	prefix := ""
	if root != "" {
		prefix = root + "/"
	}

	for _, k := range keys {
		rel, ok := strings.CutPrefix(k, prefix)
		if !ok || strings.HasPrefix(rel, hudiDir+"/") || !strings.HasSuffix(rel, ".parquet") {
			continue
		}

		segs := strings.Split(rel, "/")
		if len(segs)-1 == depth {
			out = append(out, crawlFile{key: k, dirs: segs[:depth]})
		}
	}

	return out
}
