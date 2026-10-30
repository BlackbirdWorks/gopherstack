package cosmosdb

import (
	"encoding/json"
	"sort"
	"strconv"
)

// maxParsedQueries bounds the parsed-statement cache.
const maxParsedQueries = 256

// docSource identifies the container a lazily-resolved sqlRow belongs to.
type docSource struct {
	b           *InMemoryBackend
	computed    map[string]sqlExpr
	dbID        string
	containerID string
}

// sqlRow is a query-time view of a document. With src == nil, m is the
// complete row (system properties included); otherwise m is the stored
// body and system properties resolve lazily from doc.
type sqlRow struct {
	m   map[string]any
	doc *storedDocument
	src *docSource
}

func (r sqlRow) lookup(seg string) (any, bool) {
	if r.src != nil {
		switch seg {
		case "id":
			return r.doc.ID, true
		case sysPropTS:
			return json.Number(strconv.FormatInt(r.doc.Timestamp.Unix(), 10)), true
		case sysPropETag:
			return r.src.b.etagFunc(r.doc.Timestamp), true
		case sysPropAttachments:
			return "attachments/", true
		case sysPropRID, sysPropSelf:
			info := r.src.b.documentInfoShared(r.src.dbID, r.src.containerID, r.doc)
			if seg == sysPropRID {
				return info.RID, true
			}

			return info.Self, true
		}
	}

	if r.src != nil {
		if expr, isComputed := r.src.computed[seg]; isComputed {
			return r.computedValue(expr), true
		}
	}

	v, ok := r.m[seg]

	if i, isInt := v.(int64); isInt {
		return json.Number(strconv.FormatInt(i, 10)), true
	}

	return v, ok
}

// computedValue evaluates a computed property's expression against the stored document.
func (r sqlRow) computedValue(expr sqlExpr) any {
	plain := *r.src
	plain.computed = nil

	return newEvalCtx(nil).eval(expr, []any{&sqlRow{m: r.m, doc: r.doc, src: &plain}})
}

func (r sqlRow) full() map[string]any {
	if r.src == nil {
		return r.m
	}

	return documentAsMap(r.src.b.documentInfoShared(r.src.dbID, r.src.containerID, r.doc))
}

type storedRows struct {
	src  *docSource
	docs []*storedDocument
}

func (r storedRows) count() int { return len(r.docs) }

func (r storedRows) row(i int) sqlRow {
	return sqlRow{m: r.docs[i].Body, doc: r.docs[i], src: r.src}
}

// sortedDocs returns the container's documents ordered by (ID,
// PartitionKeyJSON); the slice is cached and read-only until the next write.
// Callers must hold the backend lock (read or write).
func (c *storedContainer) sortedDocs() []*storedDocument {
	if p := c.sorted.Load(); p != nil {
		return *p
	}

	docs := make([]*storedDocument, 0, len(c.Documents))
	for _, d := range c.Documents {
		docs = append(docs, d)
	}

	sort.Slice(docs, func(i, j int) bool {
		if docs[i].ID != docs[j].ID {
			return docs[i].ID < docs[j].ID
		}

		return docs[i].PartitionKeyJSON < docs[j].PartitionKeyJSON
	})

	c.sorted.Store(&docs)

	return docs
}

func (b *InMemoryBackend) parseCached(query string) (*sqlSelectStmt, error) {
	if stmt, ok := b.parsed.Get(query); ok {
		return stmt, nil
	}

	stmt, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	if b.parsed.Len() >= maxParsedQueries {
		b.parsed.Clear()
	}

	b.parsed.Set(query, stmt)

	return stmt, nil
}

// QueryDocuments runs a Cosmos SQL query against containerID. Result values share nested data with stored
// documents and must be treated as read-only.
func (b *InMemoryBackend) QueryDocuments(
	dbID, containerID, query string, params []QueryParameter,
) ([]any, error) {
	res, err := b.RunQuery(dbID, containerID, query, params, "")

	return res.Rows, err
}

// RunQuery is QueryDocuments scoped to partitionKey (canonical JSON; empty means cross-partition), also
// reporting how many rows were scanned.
func (b *InMemoryBackend) RunQuery(
	dbID, containerID, query string, params []QueryParameter, partitionKey string,
) (QueryResult, error) {
	b.mu.RLock("QueryDocuments")
	defer b.mu.RUnlock()

	c, err := b.findDataContainer(dbID, containerID, true)
	if err != nil {
		return QueryResult{}, err
	}

	return b.queryLocked(c, dbID, containerID, query, params, partitionKey)
}

// RunQueryOpts is RunQuery with the indexing policy enforced: opts carries the request's scan switch.
func (b *InMemoryBackend) RunQueryOpts(
	dbID, containerID, query string, params []QueryParameter, partitionKey string, opts QueryOptions,
) (QueryResult, error) {
	b.mu.RLock("QueryDocuments")
	defer b.mu.RUnlock()

	c, err := b.findDataContainer(dbID, containerID, true)
	if err != nil {
		return QueryResult{}, err
	}

	stmt, err := b.parseCached(query)
	if err != nil {
		return QueryResult{}, err
	}

	if idxErr := checkQueryIndexing(stmt, c.IndexingPolicy, opts, b.computedExprs(c)); idxErr != nil {
		return QueryResult{}, idxErr
	}

	return b.queryLocked(c, dbID, containerID, query, params, partitionKey)
}

// queryLocked runs a query over c; the caller holds the backend lock (read or write).
func (b *InMemoryBackend) queryLocked(
	c *storedContainer, dbID, containerID, query string, params []QueryParameter, partitionKey string,
) (QueryResult, error) {
	stmt, err := b.parseCached(query)
	if err != nil {
		return QueryResult{}, err
	}

	docs := c.liveDocs(b.now())
	if partitionKey != "" {
		scoped := make([]*storedDocument, 0, len(docs))

		for _, d := range docs {
			if partitionKeyMatches(d.PartitionKeyJSON, partitionKey) {
				scoped = append(scoped, d)
			}
		}

		docs = scoped
	}

	rows := storedRows{docs: docs, src: &docSource{
		b: b, dbID: dbID, containerID: containerID, computed: b.computedExprs(c),
	}}

	udf := b.newUDFRuntime(c.UDFs)
	defer udf.close()

	res, err := executeStatementWith(stmt, params, rows, udf, b.newQueryColl(c))
	res.Loaded = loadedCharge(stmt, c.IndexingPolicy, res)

	return res, err
}

// loadedCharge sums the read cost of the documents the query had to retrieve: those its filter kept when the
// indexing policy can serve the filter, every examined document otherwise (or when there is no filter).
func loadedCharge(stmt *sqlSelectStmt, indexing json.RawMessage, res QueryResult) float64 {
	docs := res.Matched
	if stmt.where == nil || parseIndexingPolicy(indexing).needsScan(stmt.where) {
		docs = res.Visited
	}

	var total float64

	for _, d := range docs {
		total += readCharge(int(docBytes(d)))
	}

	return total
}
