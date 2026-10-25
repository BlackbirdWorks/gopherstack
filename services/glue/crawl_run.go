package glue

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"
)

const (
	updateBehaviorLog        = "LOG"
	deleteBehaviorDelete     = "DELETE_FROM_DATABASE"
	deleteBehaviorLog        = "LOG"
	recrawlNewFoldersOnly    = "CRAWL_NEW_FOLDERS_ONLY"
	mergeNewColumns          = "MergeNewColumns"
	crawlerDeprecatedParam   = "DEPRECATED_BY_CRAWLER"
	crawlerSummaryTemplate   = `{"TABLES_ADDED":%d,"TABLES_UPDATED":%d,"TABLES_DELETED":%d,"PARTITIONS_ADDED":%d}`
	tableTypeExternal        = "EXTERNAL_TABLE"
	crawlerStateCompleted    = "COMPLETED"
	crawlerStateFailed       = "FAILED"
	crawlerStateStoppedCrawl = "STOPPED"
)

type crawlStats struct {
	added, updated, deleted, partitions int
}

// crawlRun is one execution of a crawler over all of its targets. Every
// table write goes through upsert so SchemaChangePolicy and the Configuration
// AddOrUpdateBehavior apply uniformly to every target kind.
type crawlRun struct {
	b              *InMemoryBackend
	c              *Crawler
	seen           map[string]bool
	stats          crawlStats
	logOnlyUpdates bool
	mergeColumns   bool
	newFoldersOnly bool
}

func newCrawlRun(b *InMemoryBackend, c *Crawler) *crawlRun {
	r := &crawlRun{b: b, c: c, seen: map[string]bool{}}

	if c.SchemaChangePolicy != nil {
		r.logOnlyUpdates = c.SchemaChangePolicy.UpdateBehavior == updateBehaviorLog
	}

	if c.RecrawlPolicy != nil {
		r.newFoldersOnly = c.RecrawlPolicy.RecrawlBehavior == recrawlNewFoldersOnly
	}

	r.mergeColumns = configuredMergeNewColumns(c.Configuration)

	return r
}

func configuredMergeNewColumns(cfg string) bool {
	if cfg == "" {
		return false
	}

	var doc struct {
		CrawlerOutput struct {
			Tables struct {
				AddOrUpdateBehavior string `json:"AddOrUpdateBehavior"`
			} `json:"Tables"`
		} `json:"CrawlerOutput"`
	}

	if err := json.Unmarshal([]byte(cfg), &doc); err != nil {
		return false
	}

	return doc.CrawlerOutput.Tables.AddOrUpdateBehavior == mergeNewColumns
}

// runCrawlLocked crawls every target of c. b.mu must be held.
func (b *InMemoryBackend) runCrawlLocked(c *Crawler) (crawlStats, error) {
	r := newCrawlRun(b, c)

	if err := r.crawlAllTargets(); err != nil {
		return r.stats, err
	}

	r.sweepGoneTables()

	return r.stats, nil
}

func (r *crawlRun) crawlAllTargets() error {
	t := r.c.Targets

	for _, s3t := range t.S3Targets {
		if err := r.crawlS3(s3t); err != nil {
			return err
		}
	}

	for _, dt := range t.DynamoDBTargets {
		if err := r.crawlDynamoDB(dt); err != nil {
			return err
		}
	}

	for _, dt := range t.DeltaTargets {
		if err := r.crawlDelta(dt); err != nil {
			return err
		}
	}

	for _, it := range t.IcebergTargets {
		if err := r.crawlIceberg(it); err != nil {
			return err
		}
	}

	for _, ht := range t.HudiTargets {
		if err := r.crawlHudi(ht); err != nil {
			return err
		}
	}

	for _, jt := range t.JdbcTargets {
		if err := r.crawlJDBC(jt); err != nil {
			return err
		}
	}

	return nil
}

// upsert writes desired into the catalog and returns the stored table, or nil
// when a table of that name belongs to someone other than this crawler.
func (r *crawlRun) upsert(desired *Table) *Table {
	b := r.b
	key := tableKey(desired.DatabaseName, desired.Name)
	now := float64(time.Now().Unix())

	cur, exists := b.tables.Get(key)

	switch {
	case !exists:
		desired.CatalogID = b.accountID
		desired.CreateTime, desired.UpdateTime = now, now

		if desired.TableType == "" {
			desired.TableType = tableTypeExternal
		}

		b.tables.Put(desired)
		b.addTableVersionLocked(desired)
		r.seen[key] = true
		r.stats.added++

		return desired
	case cur.Parameters[crawlerManagedParam] != r.c.Name:
		return nil
	}

	r.seen[key] = true

	if r.logOnlyUpdates {
		return cur
	}

	if r.mergeColumns {
		desired.StorageDescriptor.Columns = mergeColumns(cur.StorageDescriptor.Columns, desired.StorageDescriptor.Columns)
	}

	if tableUnchanged(cur, desired) {
		return cur
	}

	cur.Parameters = desired.Parameters
	cur.StorageDescriptor = desired.StorageDescriptor
	cur.PartitionKeys = desired.PartitionKeys
	cur.TableType = desired.TableType
	cur.UpdateTime = now
	b.tables.Put(cur)
	b.addTableVersionLocked(cur)
	r.stats.updated++

	return cur
}

func mergeColumns(existing, discovered []Column) []Column {
	out := slices.Clone(existing)

	for _, col := range discovered {
		if !slices.ContainsFunc(out, func(c Column) bool { return c.Name == col.Name }) {
			out = append(out, col)
		}
	}

	return out
}

func tableUnchanged(cur, desired *Table) bool {
	return cur.TableType == desired.TableType &&
		reflect.DeepEqual(cur.Parameters, desired.Parameters) &&
		reflect.DeepEqual(cur.StorageDescriptor, desired.StorageDescriptor) &&
		reflect.DeepEqual(cur.PartitionKeys, desired.PartitionKeys)
}

// sweepGoneTables applies DeleteBehavior to tables this crawler created in an
// earlier run that its targets no longer produce.
func (r *crawlRun) sweepGoneTables() {
	behavior := ""
	if r.c.SchemaChangePolicy != nil {
		behavior = r.c.SchemaChangePolicy.DeleteBehavior
	}

	if behavior == deleteBehaviorLog {
		return
	}

	now := fmt.Sprintf("%d", time.Now().Unix())

	for _, tbl := range r.b.tables.All() {
		key := tableKey(tbl.DatabaseName, tbl.Name)

		if tbl.DatabaseName != r.c.DatabaseName || tbl.Parameters[crawlerManagedParam] != r.c.Name || r.seen[key] {
			continue
		}

		if behavior == deleteBehaviorDelete {
			r.b.tables.Delete(key)
			r.b.deleteTablePartitionsLocked(tbl.DatabaseName, tbl.Name)
			r.stats.deleted++

			continue
		}

		if _, done := tbl.Parameters[crawlerDeprecatedParam]; done {
			continue
		}

		tbl.Parameters = mapsWith(tbl.Parameters, crawlerDeprecatedParam, now)
		tbl.UpdateTime = float64(time.Now().Unix())
		r.b.tables.Put(tbl)
		r.b.addTableVersionLocked(tbl)
		r.stats.updated++
	}
}

func mapsWith(m map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(m)+1)
	for mk, mv := range m {
		out[mk] = mv
	}

	out[k] = v

	return out
}

func crawlSummary(s crawlStats) string {
	return fmt.Sprintf(crawlerSummaryTemplate, s.added, s.updated, s.deleted, s.partitions)
}

func lastPathSegment(p string) string {
	p = strings.Trim(p, "/")
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}

	return p
}
