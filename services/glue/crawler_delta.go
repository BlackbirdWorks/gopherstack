package glue

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	deltaInteger       = "integer"
	deltaShort         = "short"
	deltaByte          = "byte"
	deltaTimestampNTZ  = "timestamp_ntz"
	deltaLogDir        = "_delta_log"
	symlinkDir         = "_symlink_format_manifest"
	symlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
	textOutputFormat   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	nativeDeltaInput   = "org.apache.hadoop.mapred.SequenceFileInputFormat"
	nativeDeltaOutput  = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"
	hiveNullPartition  = "__HIVE_DEFAULT_PARTITION__"
)

var (
	errDeltaNoLog      = errors.New("no Delta commits found")
	errDeltaNoMetadata = errors.New("delta log carries no metaData action in its JSON commits")
	errDeltaCheckpoint = errors.New("delta log starts at a checkpoint; manifest generation needs commit 0")

	deltaCommitRe = regexp.MustCompile(`^(\d{20})\.json$`)
)

type deltaMeta struct {
	schema     string
	partitions []string
}

type deltaFile struct {
	values map[string]string
	path   string
}

type deltaState struct {
	files    map[string]deltaFile
	meta     deltaMeta
	hasMeta  bool
	firstVer int64
}

func replayDeltaLog(commits map[int64][]byte) (*deltaState, error) {
	if len(commits) == 0 {
		return nil, errDeltaNoLog
	}

	vers := make([]int64, 0, len(commits))
	for v := range commits {
		vers = append(vers, v)
	}

	slices.Sort(vers)

	st := &deltaState{files: map[string]deltaFile{}, firstVer: vers[0]}

	for _, v := range vers {
		sc := bufio.NewScanner(bytes.NewReader(commits[v]))
		sc.Buffer(make([]byte, 0, lakeScanBuf), lakeMetaLimit)

		for sc.Scan() {
			if err := st.apply(sc.Bytes()); err != nil {
				return nil, fmt.Errorf("commit %020d: %w", v, err)
			}
		}
	}

	if !st.hasMeta {
		return nil, errDeltaNoMetadata
	}

	return st, nil
}

func (st *deltaState) apply(line []byte) error {
	if len(bytes.TrimSpace(line)) == 0 {
		return nil
	}

	var act struct {
		MetaData *struct {
			SchemaString     string   `json:"schemaString"`
			PartitionColumns []string `json:"partitionColumns"`
		} `json:"metaData"`
		Add *struct {
			PartitionValues map[string]*string `json:"partitionValues"`
			Path            string             `json:"path"`
		} `json:"add"`
		Remove *struct {
			Path string `json:"path"`
		} `json:"remove"`
	}

	if err := json.Unmarshal(line, &act); err != nil {
		return err
	}

	switch {
	case act.MetaData != nil:
		st.meta = deltaMeta{schema: act.MetaData.SchemaString, partitions: act.MetaData.PartitionColumns}
		st.hasMeta = true
	case act.Add != nil:
		vals := map[string]string{}

		for k, v := range act.Add.PartitionValues {
			if v == nil {
				vals[k] = hiveNullPartition
			} else {
				vals[k] = *v
			}
		}

		st.files[act.Add.Path] = deltaFile{path: act.Add.Path, values: vals}
	case act.Remove != nil:
		delete(st.files, act.Remove.Path)
	}

	return nil
}

func deltaColumns(schemaString string) ([]Column, error) {
	var schema struct {
		Fields []struct {
			Type any    `json:"type"`
			Name string `json:"name"`
		} `json:"fields"`
	}

	if err := json.Unmarshal([]byte(schemaString), &schema); err != nil {
		return nil, fmt.Errorf("parse Delta schemaString: %w", err)
	}

	cols := make([]Column, len(schema.Fields))
	for i, f := range schema.Fields {
		cols[i] = Column{Name: f.Name, Type: deltaType(f.Type)}
	}

	return cols, nil
}

func deltaType(t any) string {
	switch v := t.(type) {
	case string:
		return deltaPrimitive(v)
	case map[string]any:
		switch v["type"] {
		case "struct":
			fields, _ := v["fields"].([]any)
			parts := make([]string, 0, len(fields))

			for _, f := range fields {
				fm, _ := f.(map[string]any)
				name, _ := fm["name"].(string)
				parts = append(parts, name+":"+deltaType(fm["type"]))
			}

			return "struct<" + strings.Join(parts, ",") + ">"
		case "array":
			return "array<" + deltaType(v["elementType"]) + ">"
		case avroMap:
			return "map<" + deltaType(v["keyType"]) + "," + deltaType(v["valueType"]) + ">"
		}
	}

	return avroString
}

func deltaPrimitive(t string) string {
	switch t {
	case avroLong:
		return typeBigint
	case deltaInteger:
		return avroInt
	case deltaShort:
		return typeSmallint
	case deltaByte:
		return typeTinyint
	case deltaTimestampNTZ:
		return typeTimestamp
	case avroString, avroFloat, avroDouble, avroBoolean, typeBinary, typeDate, typeTimestamp:
		return t
	default:
		if strings.HasPrefix(t, "decimal") {
			return t
		}

		return avroString
	}
}

func (r *crawlRun) crawlDelta(t DeltaTarget) error {
	ctx, cancel, err := r.lakeContext()
	if err != nil {
		return err
	}
	defer cancel()

	for _, p := range t.DeltaTables {
		if err = r.crawlDeltaTable(ctx, t, p); err != nil {
			return fmt.Errorf("delta table %s: %w", p, err)
		}
	}

	return nil
}

func (r *crawlRun) loadDeltaLog(ctx context.Context, bucket, prefix string) (*deltaState, error) {
	keys, err := r.b.listKeys(ctx, bucket, joinKey(prefix, deltaLogDir))
	if err != nil {
		return nil, err
	}

	commits := map[int64][]byte{}

	for _, k := range keys {
		_, file := splitKey(k)

		m := deltaCommitRe.FindStringSubmatch(file)
		if m == nil || path.Dir(k) != joinKey(prefix, deltaLogDir) {
			continue
		}

		ver, _ := strconv.ParseInt(m[1], 10, 64)

		body, rerr := r.b.readObject(ctx, bucket, k)
		if rerr != nil {
			return nil, rerr
		}

		commits[ver] = body
	}

	return replayDeltaLog(commits)
}

func (r *crawlRun) crawlDeltaTable(ctx context.Context, t DeltaTarget, p string) error {
	bucket, prefix, ok := splitCrawlS3URI(p)
	if !ok {
		return fmt.Errorf("%w %q", errInvalidCrawlPath, p)
	}

	st, err := r.loadDeltaLog(ctx, bucket, prefix)
	if err != nil {
		return err
	}

	cols, err := deltaColumns(st.meta.schema)
	if err != nil {
		return err
	}

	cols, pkeys := moveToPartitionKeys(cols, st.meta.partitions)
	root := "s3://" + bucket + "/" + prefix + "/"
	name := crawlTableName(r.c.TablePrefix, lastPathSegment(prefix))
	params := map[string]string{paramClassification: classificationParq, crawlerManagedParam: r.c.Name}

	desired := &Table{Name: name, DatabaseName: r.c.DatabaseName, PartitionKeys: pkeys, Parameters: params}

	if t.CreateNativeDeltaTable {
		params["table_type"] = "DELTA"
		desired.StorageDescriptor = StorageDescriptor{
			Columns: cols, Location: root, InputFormat: nativeDeltaInput, OutputFormat: nativeDeltaOutput,
			SerdeInfo:  &SerDeInfo{SerializationLibrary: serdeParquetHive},
			Parameters: maps.Clone(params),
		}
		r.upsert(desired)

		return nil
	}

	params[paramTypeOfData] = "file"
	manifestRoot := root + symlinkDir + "/"
	desired.StorageDescriptor = StorageDescriptor{
		Columns: cols, Location: manifestRoot, InputFormat: symlinkInputFormat, OutputFormat: textOutputFormat,
		SerdeInfo:  &SerDeInfo{SerializationLibrary: serdeParquetHive},
		Parameters: maps.Clone(params),
	}

	if t.WriteManifest {
		if st.firstVer != 0 {
			return errDeltaCheckpoint
		}

		if err = r.writeDeltaManifests(ctx, bucket, prefix, st, pkeys); err != nil {
			return err
		}
	}

	tbl := r.upsert(desired)
	if tbl != nil && t.WriteManifest {
		r.addDeltaPartitions(tbl, manifestRoot, st, pkeys)
	}

	return nil
}

func deltaPartitionPath(f deltaFile, pkeys []Column) string {
	segs := make([]string, len(pkeys))
	for i, k := range pkeys {
		segs[i] = k.Name + "=" + f.values[k.Name]
	}

	return strings.Join(segs, "/")
}

func (r *crawlRun) writeDeltaManifests(
	ctx context.Context, bucket, prefix string, st *deltaState, pkeys []Column,
) error {
	byPart := map[string][]string{}

	for _, f := range st.files {
		loc := f.path
		if !strings.Contains(loc, "://") {
			loc = "s3://" + bucket + "/" + joinKey(prefix, f.path)
		}

		pp := deltaPartitionPath(f, pkeys)
		byPart[pp] = append(byPart[pp], loc)
	}

	for pp, locs := range byPart {
		sort.Strings(locs)

		key := joinKey(prefix, symlinkDir)
		if pp != "" {
			key = joinKey(key, pp)
		}

		if err := r.b.putObject(ctx, bucket, key+"/manifest", []byte(strings.Join(locs, "\n")+"\n")); err != nil {
			return err
		}
	}

	return nil
}

func (r *crawlRun) addDeltaPartitions(tbl *Table, manifestRoot string, st *deltaState, pkeys []Column) {
	if len(pkeys) == 0 {
		return
	}

	seen := map[string]bool{}
	now := float64(time.Now().Unix())

	for _, f := range st.files {
		vals := make([]string, len(pkeys))
		for i, k := range pkeys {
			vals[i] = f.values[k.Name]
		}

		pk := partitionKey(tbl.DatabaseName, tbl.Name, vals)
		if seen[pk] || r.b.partitions.Has(pk) {
			continue
		}

		seen[pk] = true
		psd := tbl.StorageDescriptor
		psd.Location = manifestRoot + deltaPartitionPath(f, pkeys) + "/"
		psd.Parameters = maps.Clone(tbl.StorageDescriptor.Parameters)

		r.b.partitions.Put(&Partition{
			DatabaseName: tbl.DatabaseName, TableName: tbl.Name, CatalogID: r.b.accountID,
			Values: vals, StorageDescriptor: psd, CreationTime: now,
		})
		r.stats.partitions++
	}
}
