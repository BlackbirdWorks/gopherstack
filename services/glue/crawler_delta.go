package glue

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/deltalog"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	symlinkDir         = "_symlink_format_manifest"
	symlinkInputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
	textOutputFormat   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	nativeDeltaInput   = "org.apache.hadoop.mapred.SequenceFileInputFormat"
	nativeDeltaOutput  = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"
	hiveNullPartition  = "__HIVE_DEFAULT_PARTITION__"
)

var errDeltaNoLog = errors.New("no Delta commits found")

type deltaFile struct {
	values map[string]string
	path   string
}

type deltaState struct {
	files []deltaFile
	cols  []Column
	parts []string
}

type deltaS3 struct {
	r      *crawlRun
	bucket string
	base   string
}

func (d deltaS3) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := d.r.listKeys(ctx, d.bucket, d.base+prefix)
	if err != nil {
		return nil, err
	}

	for i, k := range keys {
		keys[i] = strings.TrimPrefix(k, d.base)
	}

	return keys, nil
}

func (d deltaS3) Get(ctx context.Context, key string) ([]byte, error) {
	return d.r.readObject(ctx, d.bucket, d.base+key)
}

func (r *crawlRun) loadDeltaLog(ctx context.Context, bucket, prefix string) (*deltaState, error) {
	base := ""
	if prefix != "" {
		base = strings.TrimSuffix(prefix, "/") + "/"
	}

	snap, err := deltalog.Load(ctx, deltaS3{r: r, bucket: bucket, base: base})
	if err != nil {
		if errors.Is(err, deltalog.ErrNotDeltaTable) {
			return nil, errDeltaNoLog
		}

		return nil, err
	}

	st := &deltaState{parts: snap.PartitionColumns}

	for _, c := range snap.Columns {
		st.cols = append(st.cols, Column{Name: c.Name, Type: c.Type})
	}

	for _, f := range snap.Files {
		vals := make(map[string]string, len(f.PartitionValues))

		for k, v := range f.PartitionValues {
			if v == nil {
				vals[k] = hiveNullPartition
			} else {
				vals[k] = *v
			}
		}

		st.files = append(st.files, deltaFile{path: f.Path, values: vals})
	}

	return st, nil
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

func (r *crawlRun) crawlDeltaTable(ctx context.Context, t DeltaTarget, p string) error {
	bucket, prefix, ok := splitCrawlS3URI(p)
	if !ok {
		return fmt.Errorf("%w %q", errInvalidCrawlPath, p)
	}

	st, err := r.loadDeltaLog(ctx, bucket, prefix)
	if err != nil {
		return err
	}

	cols, pkeys := moveToPartitionKeys(st.cols, st.parts)
	root := "s3://" + bucket + "/" + prefix + "/"
	name := crawlTableName(r.c.TablePrefix, lastPathSegment(prefix))
	params := map[string]string{paramClassification: classificationParq, crawlerManagedParam: r.c.Name}

	desired := &Table{Name: name, DatabaseName: strs.Fold(r.c.DatabaseName), PartitionKeys: pkeys, Parameters: params}

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

		if err := r.putObject(ctx, bucket, key+"/manifest", []byte(strings.Join(locs, "\n")+"\n")); err != nil {
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
		if seen[pk] {
			continue
		}

		seen[pk] = true
		psd := tbl.StorageDescriptor
		psd.Location = manifestRoot + deltaPartitionPath(f, pkeys) + "/"
		psd.Parameters = maps.Clone(tbl.StorageDescriptor.Parameters)

		r.addPartition(&Partition{
			DatabaseName: tbl.DatabaseName, TableName: tbl.Name, CatalogID: r.accountID,
			Values: vals, StorageDescriptor: psd, CreationTime: now,
		})
	}
}
