package glue

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
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

	paramClassification = "classification"
	paramTypeOfData     = "typeOfData"
	typeOfDataTable     = "table"
	paramConnectionName = "connectionName"
	typeSmallint        = "smallint"
	typeTinyint         = "tinyint"
	typeTimestamp       = "timestamp"
	typeDate            = "date"
)

var errInvalidCrawlPath = errors.New("invalid S3 path")

type crawlStats struct {
	added, updated, deleted, partitions int
}

// crawlRun is one execution of a crawler over all of its targets. Target I/O
// runs unlocked against a snapshot; catalog writes are recorded as ops and
// replayed under the backend lock by commit. Every table write goes through
// upsertCore so SchemaChangePolicy and AddOrUpdateBehavior apply uniformly.
type crawlRun struct {
	s3             CrawlerS3
	ddb            CrawlerDynamoDB
	secrets        CrawlerSecrets
	b              *InMemoryBackend
	c              *Crawler
	conns          map[string]*Connection
	tables         tableOverlay
	seen           map[string]bool
	accountID      string
	region         string
	ops            []func(*crawlRun)
	stats          crawlStats
	logOnlyUpdates bool
	catalogMode    bool
	mergeColumns   bool
	newFoldersOnly bool
}

type tableStore interface {
	Get(key string) (*Table, bool)
	Put(t *Table)
}

type tableOverlay map[string]*Table

func (o tableOverlay) Get(key string) (*Table, bool) {
	t, ok := o[key]

	return t, ok
}

func (o tableOverlay) Put(t *Table) { o[tableKey(t.DatabaseName, t.Name)] = t }

func newCrawlRun(c *Crawler) *crawlRun {
	r := &crawlRun{c: c, seen: map[string]bool{}}

	if c.SchemaChangePolicy != nil {
		r.logOnlyUpdates = c.SchemaChangePolicy.UpdateBehavior == updateBehaviorLog
	}

	if c.RecrawlPolicy != nil {
		r.newFoldersOnly = c.RecrawlPolicy.RecrawlBehavior == recrawlNewFoldersOnly
	}

	r.mergeColumns = configuredMergeNewColumns(c.Configuration)
	r.catalogMode = len(c.Targets.CatalogTargets) > 0

	return r
}

// snapshotCrawlLocked captures everything a crawl reads from the backend so
// its I/O can run unlocked. b.mu must be held.
func (b *InMemoryBackend) snapshotCrawlLocked(c *Crawler) *crawlRun {
	r := newCrawlRun(cloneCrawler(c))
	r.s3, r.ddb, r.secrets = b.crawlerS3, b.crawlerDDB, b.crawlerSecrets
	r.accountID, r.region = b.accountID, b.region
	r.conns = map[string]*Connection{}
	r.tables = tableOverlay{}

	for _, ct := range c.Targets.CatalogTargets {
		if conn, ok := b.connections.Get(ct.ConnectionName); ok && ct.ConnectionName != "" {
			r.conns[ct.ConnectionName] = cloneConnection(conn)
		}

		for _, name := range ct.Tables {
			if t, ok := b.tables.Get(tableKey(ct.DatabaseName, name)); ok {
				r.tables.Put(cloneTable(t))
			}
		}
	}

	for _, jt := range c.Targets.JdbcTargets {
		if conn, ok := b.connections.Get(jt.ConnectionName); ok {
			r.conns[jt.ConnectionName] = cloneConnection(conn)
		}
	}

	for _, mt := range c.Targets.MongoDBTargets {
		if conn, ok := b.connections.Get(mt.ConnectionName); ok {
			r.conns[mt.ConnectionName] = cloneConnection(conn)
		}
	}

	for _, t := range b.tables.All() {
		if strs.Equal(t.DatabaseName, c.DatabaseName) {
			r.tables.Put(cloneTable(t))
		}
	}

	return r
}

func (r *crawlRun) markSeen(key string) {
	r.seen[key] = true
	r.ops = append(r.ops, func(a *crawlRun) { a.seen[key] = true })
}

func (r *crawlRun) addPartition(p *Partition) {
	r.ops = append(r.ops, func(a *crawlRun) {
		if a.b.partitions.Has(partitionKey(p.DatabaseName, p.TableName, p.Values)) {
			return
		}

		a.b.partitions.Put(p)
		a.stats.partitions++
	})
}

// commit replays the recorded catalog writes against the live backend and, when
// the crawl succeeded, sweeps tables its targets no longer produce. b.mu must be held.
func (r *crawlRun) commit(b *InMemoryBackend, sweep bool) crawlStats {
	a := newCrawlRun(r.c)
	a.b, a.accountID = b, b.accountID

	for _, op := range r.ops {
		op(a)
	}

	if sweep {
		a.sweepGoneTables()
	}

	return a.stats
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

func crawlEach[T any](targets []T, crawl func(T) error) error {
	for _, t := range targets {
		if err := crawl(t); err != nil {
			return err
		}
	}

	return nil
}

func (r *crawlRun) crawlAllTargets() error {
	t := r.c.Targets

	steps := []func() error{
		func() error { return crawlEach(t.S3Targets, r.crawlS3) },
		func() error { return crawlEach(t.CatalogTargets, r.crawlCatalog) },
		func() error { return crawlEach(t.DynamoDBTargets, r.crawlDynamoDB) },
		func() error { return crawlEach(t.DeltaTargets, r.crawlDelta) },
		func() error { return crawlEach(t.IcebergTargets, r.crawlIceberg) },
		func() error { return crawlEach(t.HudiTargets, r.crawlHudi) },
		func() error { return crawlEach(t.JdbcTargets, r.crawlJDBC) },
		func() error { return crawlEach(t.MongoDBTargets, r.crawlMongoDB) },
	}

	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}

	return nil
}

// upsert writes desired into the catalog and returns the stored table, or nil
// when a table of that name belongs to someone other than this crawler.
func (r *crawlRun) upsert(desired *Table) *Table {
	rec := cloneTable(desired)
	r.ops = append(r.ops, func(a *crawlRun) {
		a.upsertCore(a.b.tables, a.b.addTableVersionLocked, rec)
	})

	return r.upsertCore(r.tables, func(*Table) {}, desired)
}

func (r *crawlRun) upsertCore(st tableStore, version func(*Table), desired *Table) *Table {
	key := tableKey(desired.DatabaseName, desired.Name)
	now := float64(time.Now().Unix())

	cur, exists := st.Get(key)

	switch {
	case !exists:
		desired.CatalogID = r.accountID
		desired.CreateTime, desired.UpdateTime = now, now

		if desired.TableType == "" {
			desired.TableType = tableTypeExternal
		}

		st.Put(desired)
		version(desired)
		r.seen[key] = true
		r.stats.added++

		return desired
	case !r.catalogMode && cur.Parameters[crawlerManagedParam] != r.c.Name:
		return nil
	}

	r.seen[key] = true

	if r.logOnlyUpdates {
		return cur
	}

	if r.mergeColumns {
		desired.StorageDescriptor.Columns = mergeColumns(
			cur.StorageDescriptor.Columns,
			desired.StorageDescriptor.Columns,
		)
	}

	if tableUnchanged(cur, desired) {
		return cur
	}

	cur.Parameters = desired.Parameters
	cur.StorageDescriptor = desired.StorageDescriptor
	cur.PartitionKeys = desired.PartitionKeys
	cur.TableType = desired.TableType
	cur.UpdateTime = now
	st.Put(cur)
	version(cur)
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

	if behavior == deleteBehaviorLog || r.catalogMode {
		return
	}

	now := strconv.FormatInt(time.Now().Unix(), 10)

	for _, tbl := range r.b.tables.All() {
		key := tableKey(tbl.DatabaseName, tbl.Name)

		if !strs.Equal(tbl.DatabaseName, r.c.DatabaseName) || tbl.Parameters[crawlerManagedParam] != r.c.Name ||
			r.seen[key] {
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
	out := maps.Clone(m)
	if out == nil {
		out = map[string]string{}
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
