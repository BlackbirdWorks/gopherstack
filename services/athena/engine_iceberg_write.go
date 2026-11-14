package athena

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	maxCommitAttempts = 4
	propParquetCodec  = "write.parquet.compression-codec"
	propDefaultFormat = "write.format.default"
	nullPartition     = "null"
)

func (p *writePlan) precheckIcebergInsert(ctx context.Context, def *GlueTableDef) statementOutcome {
	l := &tableLoader{get: p.get, list: p.list, limits: defaultEngineLimits()}

	tbl, err := l.openIceberg(ctx, def, p.db+"."+p.table)
	if err != nil {
		return engineFailure(err)
	}

	if _, ok := p.cat.(tableCommitter); !ok {
		return stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: the catalog cannot commit Iceberg metadata")
	}

	p.def, p.ice = def, tbl

	return stmtOK()
}

func writeHiveType(t string) string {
	if strings.HasPrefix(t, typeTimestamp) {
		return typeTimestamp
	}

	return t
}

func (p *writePlan) applyIcebergInsert(ctx context.Context, res *sqlResult, limits EngineLimits) statementOutcome {
	dest, err := icebergColumns(p.ice.meta.CurrentSchema(), p.display())
	if err != nil {
		return engineFailure(err)
	}

	src, out := p.insertSources(dest, len(res.columns))
	if out.failed {
		return out
	}

	files, out := p.writeIcebergFiles(ctx, res, src, limits)
	if out.failed || len(files) == 0 {
		return out
	}

	return p.commitIcebergChange(ctx, iceberg.Change{Operation: iceberg.OpAppend, AddData: files}, true)
}

// writeIcebergFiles writes res as partitioned data files of the table's default
// spec; src maps each schema column to a result column.
func (p *writePlan) writeIcebergFiles(
	ctx context.Context, res *sqlResult, src []int, limits EngineLimits,
) ([]iceberg.DataFile, statementOutcome) {
	meta := p.ice.meta
	schema := meta.CurrentSchema()

	dest, err := icebergColumns(schema, p.display())
	if err != nil {
		return nil, engineFailure(err)
	}

	cols := make([]outCol, len(dest))

	for i, c := range dest {
		if iceberg.PrimitiveName(schema.Fields[i].Type) == "" {
			return nil, stmtFail(athenaErrTypeSyntax,
				"NOT_SUPPORTED: writing nested column '%s' to an Iceberg table is not supported", c.Name)
		}

		cols[i] = outCol{name: c.Name, typ: writeHiveType(c.Type), src: src[i], id: schema.Fields[i].ID}
	}

	groups, err := p.icebergGroups(res, schema, cols)
	if err != nil {
		return nil, stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: %s", err.Error())
	}

	var (
		files []iceberg.DataFile
		total int64
	)

	ext, avroFmt := ".parquet", strings.EqualFold(meta.Properties[propDefaultFormat], "avro")
	if avroFmt {
		ext = ".avro"
	}

	for seq, g := range groups {
		if err = ctx.Err(); err != nil {
			return nil, engineFailure(err)
		}

		var body []byte
		if avroFmt {
			body, err = encodeAvroRows(res, g.rows, cols, avroCodecFor(meta.Properties[propParquetCodec]))
		} else {
			body, err = encodeParquetRows(res, g.rows, cols, meta.Properties[propParquetCodec])
		}

		if err != nil {
			return nil, engineFailure(err)
		}

		if total += int64(len(body)); total > limits.MaxResultBytes {
			return nil, engineFailure(fmt.Errorf("%w: more than %d bytes", errResultBytes, limits.MaxResultBytes))
		}

		path := fmt.Sprintf("%s/data/%s%s_%0*d%s", strings.TrimSuffix(meta.Location, "/"),
			icebergPartDir(meta.DefaultSpec(), g.partition), p.queryID, fileSeqWidth, seq, ext)

		if out := p.putObjectAt(ctx, path, body); out.failed {
			return nil, out
		}

		files = append(files, iceberg.DataFile{
			Path: path, Format: strings.ToUpper(ext[1:]), Partition: g.partition, RecordCount: int64(len(g.rows)),
			FileSize: int64(len(body)), ValueCounts: countsFor(res, g.rows, cols, false),
			NullCounts: countsFor(res, g.rows, cols, true),
		})
	}

	return files, stmtOK()
}

type icebergGroup struct {
	partition map[string]any
	rows      []int
}

func (p *writePlan) icebergGroups(res *sqlResult, schema iceberg.Schema, cols []outCol) ([]*icebergGroup, error) {
	spec := p.ice.meta.DefaultSpec()
	trs := make([]iceberg.Transform, len(spec.Fields))
	srcCol := make([]int, len(spec.Fields))
	srcType := make([]string, len(spec.Fields))

	for i, pf := range spec.Fields {
		tr, err := iceberg.ParseTransform(pf.Transform)
		if err != nil {
			return nil, err
		}

		trs[i] = tr

		for ci, c := range cols {
			if c.id == pf.SourceID {
				srcCol[i], srcType[i] = ci, iceberg.PrimitiveName(schema.Fields[ci].Type)
			}
		}
	}

	var order []*icebergGroup

	byKey := map[string]*icebergGroup{}

	for r := range res.rows {
		part := make(map[string]any, len(spec.Fields))
		keyParts := make([]string, len(spec.Fields))

		for i, pf := range spec.Fields {
			var raw *string

			if v, null := res.cell(r, cols[srcCol[i]].src); !null {
				raw = &v
			}

			val, err := trs[i].Apply(srcType[i], raw)
			if err != nil {
				return nil, err
			}

			part[pf.Name] = val
			keyParts[i] = fmt.Sprint(val)
		}

		key := strings.Join(keyParts, "\x00")

		g, ok := byKey[key]
		if !ok {
			g = &icebergGroup{partition: part}
			byKey[key] = g
			order = append(order, g)
		}

		g.rows = append(g.rows, r)
	}

	return order, nil
}

func icebergPartDir(spec iceberg.PartitionSpec, part map[string]any) string {
	var sb strings.Builder

	for _, pf := range spec.Fields {
		v := nullPartition
		if part[pf.Name] != nil {
			v = fmt.Sprint(part[pf.Name])
		}

		sb.WriteString(url.PathEscape(pf.Name) + "=" + url.PathEscape(v) + "/")
	}

	return sb.String()
}

func countsFor(res *sqlResult, rows []int, cols []outCol, nullsOnly bool) map[int]int64 {
	out := make(map[int]int64, len(cols))

	for _, c := range cols {
		var n int64

		for _, r := range rows {
			_, null := res.cell(r, c.src)
			if !nullsOnly || null {
				n++
			}
		}

		out[c.id] = n
	}

	return out
}

func (p *writePlan) putObjectAt(ctx context.Context, uri string, body []byte) statementOutcome {
	bucket, key, ok := splitS3URI(uri)
	if !ok {
		return stmtFail(athenaErrTypeSyntax, "line 1:1: invalid S3 location %q", uri)
	}

	_, err := p.put.PutObject(ctx, &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	if err != nil {
		return stmtFail(athenaErrTypeSyntax, "ICEBERG_FILESYSTEM_ERROR: cannot write s3://%s/%s: %s", bucket, key, err)
	}

	return stmtOK()
}

// currentManifests lists the manifests of the table's current snapshot.
func (l *tableLoader) currentManifests(ctx context.Context, tbl *icebergTable) ([]iceberg.ManifestFile, error) {
	snap, ok := tbl.meta.CurrentSnapshot()
	if !ok || snap.ManifestList == "" {
		return nil, nil
	}

	raw, err := l.readURI(ctx, snap.ManifestList)
	if err != nil {
		return nil, icebergFailf("ICEBERG_MISSING_METADATA", "Manifest list not found: %s", snap.ManifestList)
	}

	list, err := iceberg.ReadManifestList(raw)
	if err != nil {
		return nil, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid manifest list: %v", err)
	}

	return list, nil
}

// commitIcebergChange writes the manifests, manifest list and metadata of ch,
// then swaps the catalog pointer. With retry a lost race re-plans on the new
// head (safe for appends); otherwise it fails.
func (p *writePlan) commitIcebergChange(ctx context.Context, ch iceberg.Change, retry bool) statementOutcome {
	l := &tableLoader{get: p.get, list: p.list, limits: defaultEngineLimits()}
	tbl := p.ice

	for range maxCommitAttempts {
		conflict, out := p.tryCommit(ctx, l, tbl, ch)
		if !conflict {
			return out
		}

		if !retry {
			break
		}

		def, err := p.cat.GetTableDef(p.db, p.table)
		if err != nil {
			return stmtFail(
				athenaErrTypeEntityMiss,
				"TABLE_NOT_FOUND: line 1:1: Table '%s' does not exist",
				p.display(),
			)
		}

		if tbl, err = l.openIceberg(ctx, def, p.db+"."+p.table); err != nil {
			return engineFailure(err)
		}
	}

	return stmtFail(athenaErrTypeSyntax,
		"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s (concurrent modification)", p.display())
}

// tryCommit makes one commit attempt; conflict=true means the catalog pointer moved.
func (p *writePlan) tryCommit(
	ctx context.Context, l *tableLoader, tbl *icebergTable, ch iceberg.Change,
) (bool, statementOutcome) {
	prev, err := l.currentManifests(ctx, tbl)
	if err != nil {
		return false, engineFailure(err)
	}

	read := func(path string) ([]iceberg.ManifestEntry, error) {
		body, rerr := l.readURI(ctx, path)
		if rerr != nil {
			return nil, icebergFailf("ICEBERG_MISSING_METADATA", "Manifest not found: %s", path)
		}

		return iceberg.ReadManifest(body)
	}

	plan, err := tbl.meta.PlanCommit(tbl.location, prev, read, ch, p.now().UTC())
	if err != nil {
		return false, engineFailure(icebergFailf("ICEBERG_COMMIT_ERROR", "Cannot plan Iceberg update: %v", err))
	}

	return p.publishPlan(ctx, tbl, plan)
}

// publishPlan stores the plan's objects and swaps the catalog's metadata pointer.
func (p *writePlan) publishPlan(
	ctx context.Context, tbl *icebergTable, plan *iceberg.AppendPlan,
) (bool, statementOutcome) {
	for _, o := range plan.Objects {
		if out := p.putObjectAt(ctx, o.Location, o.Body); out.failed {
			return false, out
		}
	}

	committer, ok := p.cat.(tableCommitter)
	if !ok {
		return false, stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: the catalog cannot commit Iceberg metadata")
	}

	switch err := committer.CommitTableMetadata(p.db, p.table, tbl.location, plan.MetadataLocation); {
	case err == nil:
		return false, stmtOK()
	case errors.Is(err, ErrCatalogCommitConflict):
		return true, stmtOK()
	default:
		return false, stmtFail(athenaErrTypeSyntax,
			"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s: %s", p.display(), err)
	}
}

func avroCodecFor(v string) string {
	switch c := strings.ToUpper(v); c {
	case codecSnappy, codecDeflate:
		return c
	default:
		return ""
	}
}
