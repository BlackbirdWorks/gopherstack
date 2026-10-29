package glue

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

var (
	errCatalogNoTable     = errors.New("catalog table not found")
	errCatalogStore       = errors.New("catalog table is not backed by Amazon S3, Amazon DynamoDB or Delta Lake")
	errCatalogNoConn      = errors.New("catalog target connection not found")
	errCatalogMix         = errors.New("catalog targets cannot be combined with other target types")
	errCatalogTarget      = errors.New("invalid catalog target")
	errCatalogDeltaNative = errors.New("delta table location is not an S3 path")
)

func validateCrawlerTargets(t CrawlerTarget) error {
	if len(t.CatalogTargets) == 0 {
		return nil
	}

	if len(t.S3Targets)+len(t.JdbcTargets)+len(t.DynamoDBTargets)+len(t.DeltaTargets)+
		len(t.HudiTargets)+len(t.IcebergTargets)+len(t.MongoDBTargets) > 0 {
		return fmt.Errorf("%w: %w", ErrValidation, errCatalogMix)
	}

	for _, ct := range t.CatalogTargets {
		switch {
		case ct.DatabaseName == "" || len(ct.Tables) == 0:
			return fmt.Errorf(
				"%w: %w: DatabaseName and at least one table are required",
				ErrValidation,
				errCatalogTarget,
			)
		case ct.EventQueueArn != "" && !isSQSARN(ct.EventQueueArn):
			return fmt.Errorf(
				"%w: %w: EventQueueArn %q is not an SQS ARN",
				ErrValidation,
				errCatalogTarget,
				ct.EventQueueArn,
			)
		case ct.DlqEventQueueArn != "" && !isSQSARN(ct.DlqEventQueueArn):
			return fmt.Errorf(
				"%w: %w: DlqEventQueueArn %q is not an SQS ARN",
				ErrValidation,
				errCatalogTarget,
				ct.DlqEventQueueArn,
			)
		case ct.DlqEventQueueArn != "" && ct.EventQueueArn == "":
			return fmt.Errorf("%w: %w: DlqEventQueueArn requires EventQueueArn", ErrValidation, errCatalogTarget)
		}
	}

	return nil
}

func isSQSARN(s string) bool {
	parts := strings.Split(s, ":")

	return len(parts) == 6 && parts[0] == "arn" && parts[2] == "sqs" && parts[5] != ""
}

func (r *crawlRun) crawlCatalog(ct CatalogTarget) error {
	if ct.ConnectionName != "" {
		if _, ok := r.conns[ct.ConnectionName]; !ok {
			return fmt.Errorf("%w: %q", errCatalogNoConn, ct.ConnectionName)
		}
	}

	for _, name := range ct.Tables {
		cur, ok := r.tables.Get(tableKey(ct.DatabaseName, name))
		if !ok {
			return fmt.Errorf("%w: %s.%s", errCatalogNoTable, ct.DatabaseName, name)
		}

		var err error

		switch {
		case isDeltaCatalogTable(cur):
			err = r.recrawlDelta(cur)
		case isDDBCatalogTable(cur):
			err = r.recrawlDynamoDB(cur)
		case strings.HasPrefix(cur.StorageDescriptor.Location, "s3://"):
			err = r.recrawlS3(cur)
		default:
			err = fmt.Errorf("%w: %s.%s", errCatalogStore, ct.DatabaseName, name)
		}

		if err != nil {
			return fmt.Errorf("catalog table %s.%s: %w", ct.DatabaseName, name, err)
		}
	}

	return nil
}

func isDeltaCatalogTable(t *Table) bool {
	return strings.EqualFold(t.Parameters["table_type"], "delta") ||
		t.StorageDescriptor.InputFormat == symlinkInputFormat ||
		strings.Contains(t.StorageDescriptor.Location, symlinkDir)
}

func isDDBCatalogTable(t *Table) bool {
	return t.StorageDescriptor.InputFormat == ddbInputFormat ||
		strings.EqualFold(t.Parameters[paramClassification], ddbClassification)
}

// applyCatalogTable rewrites an existing catalog table's schema in place; the
// table's storage and format settings stay as the user defined them.
func (r *crawlRun) applyCatalogTable(cur *Table, cols, pkeys []Column) *Table {
	desired := cloneTable(cur)
	desired.StorageDescriptor.Columns = keepColumnMeta(cur.StorageDescriptor.Columns, cols)

	if pkeys != nil {
		desired.PartitionKeys = keepColumnMeta(cur.PartitionKeys, pkeys)
	}

	desired.Parameters = mapsWith(cur.Parameters, crawlerManagedParam, r.c.Name)

	return r.upsert(desired)
}

func keepColumnMeta(old, fresh []Column) []Column {
	out := make([]Column, len(fresh))

	for i, c := range fresh {
		out[i] = c

		for _, o := range old {
			if strs.Equal(o.Name, c.Name) {
				out[i].Comment, out[i].Parameters = o.Comment, o.Parameters

				break
			}
		}
	}

	return out
}

func (r *crawlRun) recrawlS3(cur *Table) error {
	if r.s3 == nil {
		return errLakeNoS3
	}

	bucket, prefix, ok := splitCrawlS3URI(cur.StorageDescriptor.Location)
	if !ok {
		return fmt.Errorf("%w %q", errInvalidCrawlPath, cur.StorageDescriptor.Location)
	}

	ctx, cancel := context.WithTimeout(context.Background(), crawlerS3Timeout)
	defer cancel()

	all, err := r.listCrawlFiles(ctx, bucket, prefix, nil)
	if err != nil {
		return fmt.Errorf("list s3://%s/%s: %w", bucket, prefix, err)
	}

	if len(all) == 0 {
		r.markSeen(tableKey(cur.DatabaseName, cur.Name))

		return nil
	}

	files := mostCommonDepth(all)

	sample, err := r.readSample(ctx, bucket, files[0].key)
	if err != nil {
		return fmt.Errorf("read s3://%s/%s: %w", bucket, files[0].key, err)
	}

	pkeys := partitionKeysFor(files)
	if len(cur.PartitionKeys) == len(pkeys) && len(pkeys) > 0 {
		pkeys = cur.PartitionKeys
	}

	if tbl := r.applyCatalogTable(cur, inferFormat(files[0].key, sample).columns, pkeys); tbl != nil {
		r.crawlPartitions(tbl, bucket, prefix, files)
	}

	return nil
}

func (r *crawlRun) recrawlDelta(cur *Table) error {
	ctx, cancel, err := r.lakeContext()
	if err != nil {
		return err
	}
	defer cancel()

	loc := strings.TrimSuffix(cur.StorageDescriptor.Location, "/")
	loc = strings.TrimSuffix(loc, "/"+symlinkDir)

	bucket, prefix, ok := splitCrawlS3URI(loc)
	if !ok {
		return fmt.Errorf("%w: %q", errCatalogDeltaNative, cur.StorageDescriptor.Location)
	}

	st, err := r.loadDeltaLog(ctx, bucket, prefix)
	if err != nil {
		return err
	}

	cols, pkeys := moveToPartitionKeys(st.cols, st.parts)
	tbl := r.applyCatalogTable(cur, cols, pkeys)

	if tbl != nil && strings.Contains(cur.StorageDescriptor.Location, symlinkDir) {
		r.addDeltaPartitions(tbl, strings.TrimSuffix(cur.StorageDescriptor.Location, "/")+"/", st, pkeys)
	}

	return nil
}

func (r *crawlRun) recrawlDynamoDB(cur *Table) error {
	if r.ddb == nil {
		return fmt.Errorf("%w: %q", errDDBUnavailable, cur.StorageDescriptor.Location)
	}

	ctx, cancel := context.WithTimeout(awsmeta.WithRegion(context.Background(), r.region), crawlerS3Timeout)
	defer cancel()

	cols, params, err := r.describeDDBTable(ctx, DynamoDBTarget{Path: cur.StorageDescriptor.Location, ScanAll: true})
	if err != nil {
		return err
	}

	merged := mapsWith(cur.Parameters, crawlerManagedParam, r.c.Name)
	maps.Copy(merged, params)

	desired := cloneTable(cur)
	desired.StorageDescriptor.Columns = keepColumnMeta(cur.StorageDescriptor.Columns, cols)
	desired.Parameters = merged
	r.upsert(desired)

	return nil
}
