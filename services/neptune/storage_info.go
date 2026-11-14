package neptune

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const (
	storageInfoPath   = "/storage/info"
	actionStorageInfo = "neptune-db:GetStorageInfo"
	indexEntryBytes   = 32
)

type storageMetrics struct {
	SizeInBytes int64 `json:"sizeInBytes"`
	Count       int64 `json:"count"`
}

type quadsInfo struct {
	SPOG  storageMetrics `json:"SPOG"`
	POGS  storageMetrics `json:"POGS"`
	GPSO  storageMetrics `json:"GPSO"`
	OSGP  storageMetrics `json:"OSGP"`
	PZSOG storageMetrics `json:"PZSOG"`
}

func indexInfo(entries int) quadsInfo {
	m := storageMetrics{Count: int64(entries), SizeInBytes: int64(entries) * indexEntryBytes}

	return quadsInfo{SPOG: m, POGS: m, GPSO: m, OSGP: m, PZSOG: m}
}

func (q quadsInfo) bytes() int64 {
	return q.SPOG.SizeInBytes + q.POGS.SizeInBytes + q.GPSO.SizeInBytes + q.OSGP.SizeInBytes + q.PZSOG.SizeInBytes
}

func (f *cypherFront) serveStorageInfo(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeCypherError(w, http.StatusMethodNotAllowed, "MethodNotAllowedException", "Method Not Allowed.")

		return
	}

	if !authorizeActions(w, r, langSparql, actionStorageInfo) {
		return
	}

	rdf, pg := f.rdf.store.Storage(), f.graph.Storage()
	rq, pq := indexInfo(rdf.Entries), indexInfo(pg.Entries)

	total := dictMetrics(rdf, pg)
	blobs := storageMetrics{
		Count: int64(rdf.Blobs + pg.Blobs), SizeInBytes: int64(rdf.BlobBytes + pg.BlobBytes),
	}

	recs, recBytes := f.rdf.store.Stream().Size()
	pgRecs, pgRecBytes := f.graph.Stream().Size()
	streams := storageMetrics{Count: int64(recs + pgRecs), SizeInBytes: int64(recBytes + pgRecBytes)}

	rp, rb := f.rdf.rdfStats.metrics()
	pp, pb := f.rdf.pgStats.metrics()
	dfe := storageMetrics{Count: int64(rp + pp), SizeInBytes: int64(rb + pb)}

	writeCypherJSON(w, http.StatusOK, map[string]any{
		"totalStorageBytesUsed": rq.bytes() + pq.bytes() + total.SizeInBytes + streams.SizeInBytes + dfe.SizeInBytes,
		"quads":                 map[string]quadsInfo{"RDF": rq, "PG": pq},
		"dictionaries":          map[string]storageMetrics{"total": total, "blobs": blobs},
		"undoLogs":              map[string]int64{"sizeInBytes": 0, "listSize": 0},
		"streams":               streams,
		"dfeStatistics":         dfe,
		"schema":                storageMetrics{},
		"internals":             storageMetrics{},
	})
}

func dictMetrics(rdf sparql.Storage, pg opencypher.Storage) storageMetrics {
	return storageMetrics{
		Count:       int64(rdf.Terms + rdf.Blobs + pg.Terms + pg.Blobs),
		SizeInBytes: int64(rdf.TermBytes + rdf.BlobBytes + pg.TermBytes + pg.BlobBytes),
	}
}
