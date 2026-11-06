package athena

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

const (
	propMaxSnapshotAgeMS = "history.expire.max-snapshot-age-ms"
	propMinSnapshots     = "history.expire.min-snapshots-to-keep"
	propMaxMetadataFiles = "write.metadata.previous-versions-max"
	defaultSnapshotAgeMS = 432000 * millisPerSecondInt
	defaultMinSnapshots  = 1
	defaultMetadataFiles = 100
)

// S3Deleter is the optional delete side of the wired S3 backend, used by VACUUM.
type S3Deleter interface {
	DeleteObject(ctx context.Context, input *sdk_s3.DeleteObjectInput) (*sdk_s3.DeleteObjectOutput, error)
}

// tableColumnSetter is implemented by catalogs that keep a column list beside the Iceberg metadata.
type tableColumnSetter interface {
	SetTableColumns(db, name string, cols []Column) error
}

type icebergTarget struct {
	wp  *writePlan
	l   *tableLoader
	tbl *icebergTable
	del S3Deleter
}

// icebergTargetFor resolves an Iceberg table for a maintenance statement.
// handled=false means the table is not an Iceberg table of a Glue catalog.
func (b *InMemoryBackend) icebergTargetFor(
	parts []string, qctx QueryExecutionContext,
) (*icebergTarget, statementOutcome, bool) {
	catalog, db, table := refTarget(parts, qctx)

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	get, list, put, ok := b.ddlDeps()
	if !ok {
		return nil, stmtOK(), false
	}

	def, err := cat.GetTableDef(db, table)
	if err != nil {
		return nil, stmtFail(athenaErrTypeEntityMiss, "Table does not exist: %s", table), true
	}

	if tableTypeOf(def) != tableTypeIceberg {
		return nil, stmtOK(), false
	}

	l := &tableLoader{get: get, list: list, limits: b.limitsSnapshot()}

	tbl, err := l.openIceberg(context.Background(), def, db+"."+table)
	if err != nil {
		return nil, engineFailure(err), true
	}

	wp := &writePlan{
		cat:     cat,
		put:     put,
		get:     get,
		list:    list,
		now:     b.now,
		catalog: catalog,
		db:      db,
		table:   table,
		ice:     tbl,
	}
	del, _ := b.s3.(S3Deleter)

	return &icebergTarget{wp: wp, l: l, tbl: tbl, del: del}, stmtOK(), true
}

func (b *InMemoryBackend) execVacuum(query string, qctx QueryExecutionContext) (*sqlResult, statementOutcome, bool) {
	toks, err := tokenize(query)
	if err != nil {
		return nil, stmtOK(), false
	}

	c := &sigCursor{toks: significant(toks)}
	if !c.acceptWords("vacuum") {
		return nil, stmtOK(), false
	}

	parts := c.qualifiedName()
	if parts == nil || !c.done() {
		return nil, stmtFail(athenaErrTypeSyntax, "line 1:1: mismatched input in VACUUM statement"), true
	}

	catalog, db, table := refTarget(parts, qctx)

	cat, ok := b.glueCatalogFor(catalog)
	if !ok {
		return nil, stmtOK(), false
	}

	if def, gerr := cat.GetTableDef(db, table); gerr == nil && tableTypeOf(def) != tableTypeIceberg {
		return nil, stmtFail(athenaErrTypeSyntax, "NOT_SUPPORTED: VACUUM is supported only for Iceberg tables"), true
	}

	tg, out, handled := b.icebergTargetFor(parts, qctx)
	if !handled || out.failed {
		return nil, out, handled
	}

	return &sqlResult{}, tg.vacuum(context.Background()), true
}

func propInt64(props map[string]string, key string, def int64) int64 {
	return int64(intProp(props, key, int(min(def, int64(1<<31-1)))))
}

// protectedSnapshots picks the snapshots VACUUM keeps: the newest vacuum_min_snapshots_to_keep
// and anything newer than the cutoff.
func protectedSnapshots(meta *iceberg.TableMetadata, cutoffMS int64) map[int64]bool {
	minKeep := intProp(meta.Properties, propMinSnapshots, defaultMinSnapshots)
	newest := slices.Clone(meta.Snapshots)
	slices.SortFunc(newest, func(a, b iceberg.Snapshot) int { return int(b.TimestampMS - a.TimestampMS) })

	protected := map[int64]bool{}

	for i, s := range newest {
		if i < minKeep || s.TimestampMS >= cutoffMS {
			protected[s.SnapshotID] = true
		}
	}

	return protected
}

func (tg *icebergTarget) vacuum(ctx context.Context) statementOutcome {
	p := tg.wp
	meta := tg.tbl.meta
	now := p.now().UTC()
	ageMS := propInt64(meta.Properties, propMaxSnapshotAgeMS, defaultSnapshotAgeMS)
	maxMeta := intProp(meta.Properties, propMaxMetadataFiles, defaultMetadataFiles)
	cutoff := now.UnixMilli() - ageMS
	protected := protectedSnapshots(meta, cutoff)

	var gone []iceberg.Snapshot

	var dropped []iceberg.MetadataLogEntry

	plan, err := meta.PlanMetadataUpdate(tg.tbl.location, func(next *iceberg.TableMetadata) error {
		gone = next.ExpireSnapshots(func(s iceberg.Snapshot) bool { return protected[s.SnapshotID] })
		dropped = next.TrimMetadataLog(maxMeta - 1)

		return nil
	}, now)
	if err != nil {
		return engineFailure(err)
	}

	committed := len(gone) > 0 || len(dropped) > 0
	retained := meta

	if committed {
		retained = plan.Metadata

		if conflict, out := p.publishPlan(ctx, tg.tbl, plan); out.failed || conflict {
			if out.failed {
				return out
			}

			return stmtFail(
				athenaErrTypeSyntax,
				"ICEBERG_COMMIT_ERROR: Failed to commit Iceberg update to table: %s (concurrent modification)",
				p.display(),
			)
		}
	}

	keep, force, err := tg.cleanupSets(ctx, retained, gone, dropped)
	if err != nil {
		return engineFailure(err)
	}

	keep[tg.tbl.location] = true
	keep[plan.MetadataLocation] = committed

	return tg.removeUnreferenced(ctx, keep, force, cutoff)
}

// cleanupSets returns the files to keep and the files that must go even when young.
func (tg *icebergTarget) cleanupSets(
	ctx context.Context, retained *iceberg.TableMetadata, gone []iceberg.Snapshot, dropped []iceberg.MetadataLogEntry,
) (map[string]bool, map[string]bool, error) {
	keep, err := tg.reachable(ctx, retained.Snapshots)
	if err != nil {
		return nil, nil, err
	}

	force, err := tg.reachable(ctx, gone)
	if err != nil {
		return nil, nil, err
	}

	for k := range keep {
		delete(force, k)
	}

	for _, e := range retained.MetadataLog {
		keep[e.MetadataFile] = true
	}

	for _, e := range dropped {
		force[e.MetadataFile] = true
	}

	return keep, force, nil
}

// reachable lists the manifest lists, manifests and data/delete files of the snapshots.
func (tg *icebergTarget) reachable(ctx context.Context, snaps []iceberg.Snapshot) (map[string]bool, error) {
	keep := map[string]bool{}

	for _, s := range snaps {
		if s.ManifestList == "" {
			continue
		}

		keep[s.ManifestList] = true

		raw, err := tg.l.readURI(ctx, s.ManifestList)
		if err != nil {
			continue
		}

		list, err := iceberg.ReadManifestList(raw)
		if err != nil {
			return nil, icebergFailf("ICEBERG_INVALID_METADATA", "Invalid manifest list %s: %v", s.ManifestList, err)
		}

		for _, mf := range list {
			keep[mf.Path] = true
		}

		files, err := tg.l.snapshotFiles(ctx, s)
		if err != nil {
			return nil, err
		}

		for _, f := range append(files.data, files.deletes...) {
			keep[f.entry.DataFile.Path] = true
		}
	}

	return keep, nil
}

// removeUnreferenced deletes table files outside keep that are older than cutoffMS.
func (tg *icebergTarget) removeUnreferenced(
	ctx context.Context, keep, force map[string]bool, cutoffMS int64,
) statementOutcome {
	if tg.del == nil {
		return stmtOK()
	}

	bucket, prefix, ok := splitS3URI(strings.TrimSuffix(tg.tbl.meta.Location, "/") + "/")
	if !ok {
		return stmtOK()
	}

	var token *string

	for {
		out, err := tg.wp.list.ListObjectsV2(ctx, &sdk_s3.ListObjectsV2Input{
			Bucket: aws.String(bucket), Prefix: aws.String(prefix), ContinuationToken: token,
		})
		if err != nil {
			return stmtFail(
				athenaErrTypeSyntax,
				"ICEBERG_FILESYSTEM_ERROR: cannot list s3://%s/%s: %s",
				bucket,
				prefix,
				err,
			)
		}

		for _, o := range out.Contents {
			uri := "s3://" + bucket + "/" + aws.ToString(o.Key)
			young := o.LastModified != nil && o.LastModified.After(time.UnixMilli(cutoffMS))
			if keep[uri] || (young && !force[uri]) {
				continue
			}

			if _, derr := tg.del.DeleteObject(ctx, &sdk_s3.DeleteObjectInput{
				Bucket: aws.String(bucket), Key: o.Key,
			}); derr != nil && !errors.Is(derr, context.Canceled) {
				return stmtFail(athenaErrTypeSyntax, "ICEBERG_FILESYSTEM_ERROR: cannot delete %s: %s", uri, derr)
			}
		}

		if !aws.ToBool(out.IsTruncated) || out.NextContinuationToken == nil {
			return stmtOK()
		}

		token = out.NextContinuationToken
	}
}
