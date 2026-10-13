package cosmosdb

import "sort"

// maxParsedQueries bounds the parsed-statement cache.
const maxParsedQueries = 256

// docSource identifies the container a lazily-resolved sqlRow belongs to.
type docSource struct {
	b           *InMemoryBackend
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
			return r.doc.Timestamp.Unix(), true
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

	v, ok := r.m[seg]

	return v, ok
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

func (r storedRows) full(i int) map[string]any { return r.row(i).full() }

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

// QueryDocuments runs a Cosmos SQL query against containerID without
// materializing every document. Rows share nested values with stored
// documents and must be treated as read-only.
func (b *InMemoryBackend) QueryDocuments(
	dbID, containerID, query string, params []QueryParameter,
) ([]map[string]any, error) {
	b.mu.RLock("QueryDocuments")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, containerID)
	if err != nil {
		return nil, err
	}

	stmt, err := b.parseCached(query)
	if err != nil {
		return nil, err
	}

	rows := storedRows{docs: c.sortedDocs(), src: &docSource{b: b, dbID: dbID, containerID: containerID}}

	return executeStatement(stmt, params, rows), nil
}
