package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// createIndexRealRequest and updateIndexRealRequest mirror
// CreateIndexInput/UpdateIndexInput in the pinned SDK (api_op_CreateIndex.go:37-60,
// api_op_UpdateIndex.go:32-52): IndexSchema is a smithy document.Interface, an
// arbitrary JSON value with no fixed shape, decoded here as `any` and stored
// verbatim rather than parsed into Mappings/Settings/Aliases.
type createIndexRealRequest struct {
	IndexSchema any    `json:"IndexSchema"`
	IndexName   string `json:"IndexName"`
}

type updateIndexRealRequest struct {
	IndexSchema any `json:"IndexSchema"`
}

// indexStatusResponseJSON is the response for the real CreateIndex/UpdateIndex/
// DeleteIndex ops: Status is their only field (api_op_CreateIndex.go:62-73,
// api_op_UpdateIndex.go:54-65, api_op_DeleteIndex.go:44-53).
type indexStatusResponseJSON struct {
	Status string `json:"Status"`
}

// handleCreateIndexRealRoute serves the real CreateIndex op: POST
// {domainName}/index, with IndexName carried in the body (not the URL) --
// see the dispatchDomainPostRoutesExtended doc comment. Returns true always
// (this path is always "handled", success or error).
func (h *Handler) handleCreateIndexRealRoute(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	domainName, ok := strings.CutSuffix(trimmed, "/index")
	if !ok {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return true
	}

	body, _ := httputils.ReadBody(r)

	var req createIndexRealRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if req.IndexName == "" {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "IndexName is required")

		return true
	}

	_, err := h.Backend.CreateIndex(domainName, req.IndexName, nil, nil, nil, req.IndexSchema)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return true
	}

	h.writeJSON(r, w, indexStatusResponseJSON{Status: indexStatusCreated})

	return true
}

// handleUpdateIndexRoute handles PUT {domainName}/index/{indexName}.
func (h *Handler) handleUpdateIndexRoute(w http.ResponseWriter, r *http.Request, trimmed string) {
	parts := strings.SplitN(trimmed, "/index/", 2) //nolint:mnd // path split count
	if len(parts) != 2 {                           //nolint:mnd // path split count
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return
	}

	body, _ := httputils.ReadBody(r)
	var req updateIndexRealRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	_, err := h.Backend.UpdateIndex(parts[0], parts[1], nil, nil, req.IndexSchema)
	if err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}
	h.writeJSON(r, w, indexStatusResponseJSON{Status: indexStatusUpdated})
}

// handleIndexGetRoute handles GET routes under {domainName}/index/{indexName}:
// index metadata, document fetch (_doc/{id}) and document count (_count).
func (h *Handler) handleIndexGetRoute(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	sp, ok := parseIndexSubPath(trimmed)
	if !ok {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return true
	}

	switch sp.op {
	case indexOpCount:
		count, err := h.Backend.CountDocuments(sp.domain, sp.index)
		if err != nil {
			h.writeIndexError(r, w, err)

			return true
		}
		h.writeJSON(r, w, map[string]any{"count": count, jsonKeyDocShards: readOpShards()})
	case indexOpDoc:
		doc, meta, err := h.Backend.GetDocument(sp.domain, sp.index, sp.docID)
		if err != nil {
			h.writeIndexError(r, w, err)

			return true
		}
		h.writeJSON(r, w, map[string]any{
			jsonKeyDocIndex:    sp.index,
			jsonKeyDocID:       sp.docID,
			jsonKeyDocVersion:  meta.Version,
			jsonKeyDocSeqNo:    meta.SeqNo,
			jsonKeyDocPrimTerm: docPrimaryTerm,
			"found":            true,
			"_source":          doc,
		})
	case "":
		idx, err := h.Backend.GetIndex(sp.domain, sp.index)
		if err != nil {
			h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

			return true
		}
		h.writeJSON(r, w, getIndexResponseJSON{IndexSchema: toIndexSchema(idx)})
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "unsupported index operation")
	}

	return true
}

// docPrimaryTerm is the real OpenSearch REST document API's _primary_term
// value. This backend models a single-node, single-primary index for its
// entire lifetime, so the primary term never advances past its initial 1 --
// an honest constant, not a fabricated one (there is no primary-failover
// concept here to make it change).
const docPrimaryTerm = 1

const (
	jsonKeyShardsTotal      = "total"
	jsonKeyShardsSuccessful = "successful"
	jsonKeyShardsFailed     = "failed"
	jsonKeyShardsSkipped    = "skipped"
	// singleShardOK is this single-node emulator's constant shard
	// count/success value -- there is no replica or shard-failure model.
	singleShardOK = 1
)

// writeOpShards is the _shards envelope real OpenSearch document writes
// (index/delete) return (https://docs.opensearch.org/latest/api-reference/
// document-apis/index-document/, https://docs.opensearch.org/latest/
// api-reference/document-apis/delete-document/): total/successful/failed,
// no "skipped" (that member is read-op-only, see readOpShards). This backend
// models one node with no replicas, so total/successful are always 1.
func writeOpShards() map[string]any {
	return map[string]any{
		jsonKeyShardsTotal:      singleShardOK,
		jsonKeyShardsSuccessful: singleShardOK,
		jsonKeyShardsFailed:     0,
	}
}

// readOpShards is the _shards envelope real OpenSearch _count/_search
// responses return (https://docs.opensearch.org/latest/api-reference/
// search-apis/count/): total/successful/skipped/failed.
func readOpShards() map[string]any {
	return map[string]any{
		jsonKeyShardsTotal:      singleShardOK,
		jsonKeyShardsSuccessful: singleShardOK,
		jsonKeyShardsSkipped:    0,
		jsonKeyShardsFailed:     0,
	}
}

// getIndexResponseJSON is the response for the real GetIndex op: IndexSchema
// is its only field (api_op_GetIndex.go: "The JSON schema of the index
// including mappings, settings, and semantic enrichment configuration.
// This member is required."), an opaque smithy document.Interface value --
// NOT an IndexName/IndexStatus/DocumentCount metadata shape (that shape
// belongs to no real op; it predates this fix and was reused here by
// mistake, leaving the real client's required IndexSchema permanently nil).
type getIndexResponseJSON struct {
	IndexSchema any `json:"IndexSchema"`
}

// toIndexSchema builds GetIndexOutput.IndexSchema from a backend index. When
// the index was created via the real CreateIndex/UpdateIndex path, the raw
// schema document is stored verbatim on DomainIndex.IndexSchema and echoed
// back unchanged. Indices created via the classic mappings/settings/aliases
// path (handleCreateIndex) have no such raw document, so an equivalent one
// is synthesized from the same real backend state using the wire field
// names CreateIndex/UpdateIndex use for their own IndexSchema body.
func toIndexSchema(idx *DomainIndex) any {
	if idx.IndexSchema != nil {
		return idx.IndexSchema
	}

	return map[string]any{
		"Mappings": idx.Mappings,
		"Settings": idx.Settings,
		"Aliases":  idx.Aliases,
	}
}

// indexSubPath describes a parsed {domain}/index/{index}[/{op}[/{id}]] path.
type indexSubPath struct {
	domain string
	index  string
	op     string // "", "_doc", "_search", "_count"
	docID  string
}

// parseIndexSubPath splits a domain-scoped index path into its components.
func parseIndexSubPath(trimmed string) (indexSubPath, bool) {
	domain, rest, found := strings.Cut(trimmed, "/index/")
	if !found || rest == "" {
		return indexSubPath{}, false
	}

	// Segments: [0]=index, [1]=op (_doc/_search/_count), [2]=docID.
	const (
		segOp  = 1
		segDoc = 2
	)

	segs := strings.Split(rest, "/")
	sp := indexSubPath{domain: domain, index: segs[0]}

	if sp.index == "" {
		return indexSubPath{}, false
	}

	if len(segs) > segOp {
		sp.op = segs[segOp]
	}

	if len(segs) > segDoc {
		sp.docID = segs[segDoc]
	}

	return sp, true
}

// handleCreateIndexRoute handles POST routes under {domainName}/index/{indexName}:
// index creation, document indexing (_doc) and bounded search (_search).
func (h *Handler) handleCreateIndexRoute(
	w http.ResponseWriter,
	r *http.Request,
	trimmed string,
) bool {
	sp, ok := parseIndexSubPath(trimmed)
	if !ok {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return true
	}

	switch sp.op {
	case indexOpDoc:
		h.handleIndexDocument(w, r, sp)
	case indexOpSearch:
		h.handleSearchIndex(w, r, sp)
	case "":
		h.handleCreateIndex(w, r, sp)
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "unsupported index operation")
	}

	return true
}

// handleCreateIndex serves this backend's raw OpenSearch-REST-style create
// index route ({domainName}/index/{indexName}, not an AWS SDK op -- see the
// package doc on the data-plane surface). Real OpenSearch's Create Index API
// (https://docs.opensearch.org/latest/api-reference/index-apis/create-index/)
// takes lowercase "settings"/"mappings"/"aliases" and responds
// {"acknowledged":true,"shards_acknowledged":true,"index":"<name>"} -- this
// used to accept the AWS-control-plane-style PascalCase keys instead (a
// different, unrelated op's shape) and echo back GetIndex's metadata
// envelope, silently dropping every real client's request body and never
// answering with the real response shape.
func (h *Handler) handleCreateIndex(w http.ResponseWriter, r *http.Request, sp indexSubPath) {
	body, _ := httputils.ReadBody(r)

	var req struct {
		Mappings map[string]any `json:"mappings"`
		Settings map[string]any `json:"settings"`
		Aliases  map[string]any `json:"aliases"`
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	if _, err := h.Backend.CreateIndex(sp.domain, sp.index, req.Mappings, req.Settings, req.Aliases, nil); err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return
	}

	h.writeJSON(r, w, map[string]any{
		"acknowledged":        true,
		"shards_acknowledged": true,
		"index":               sp.index,
	})
}

// handleIndexDocument stores a document in an index.
func (h *Handler) handleIndexDocument(w http.ResponseWriter, r *http.Request, sp indexSubPath) {
	body, _ := httputils.ReadBody(r)
	var doc map[string]any
	if len(body) > 0 {
		if err := json.Unmarshal(body, &doc); err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid document body")

			return
		}
	}

	id, created, meta, err := h.Backend.IndexDocument(sp.domain, sp.index, sp.docID, doc)
	if err != nil {
		h.writeIndexError(r, w, err)

		return
	}

	result := docResultUpdated
	if created {
		result = docResultCreated
	}

	h.writeJSON(r, w, map[string]any{
		jsonKeyDocIndex:    sp.index,
		jsonKeyDocID:       id,
		jsonKeyDocVersion:  meta.Version,
		jsonKeyDocSeqNo:    meta.SeqNo,
		jsonKeyDocPrimTerm: docPrimaryTerm,
		jsonKeyDocResult:   result,
		jsonKeyDocShards:   writeOpShards(),
	})
}

// handleSearchIndex runs a bounded search over an index's stored documents.
func (h *Handler) handleSearchIndex(w http.ResponseWriter, r *http.Request, sp indexSubPath) {
	body, _ := httputils.ReadBody(r)
	var req struct {
		Query map[string]any `json:"query"`
		Size  *int           `json:"size"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid search body")

			return
		}
	}

	size := 0
	if req.Size != nil {
		size = *req.Size
	}

	res, err := h.Backend.SearchIndex(sp.domain, sp.index, req.Query, size)
	if err != nil {
		h.writeIndexError(r, w, err)

		return
	}

	// matchScore is the constant relevance score this backend reports: it has
	// no scoring engine (see documents.go's compileQuery), so every hit that
	// passes a predicate is an equally exact match, matching real
	// OpenSearch's behavior for a bare {"match_all":{}} query (max_score 1.0
	// for every hit) rather than fabricating a distribution this backend
	// cannot compute.
	const matchScore = 1.0

	hits := make([]map[string]any, 0, len(res.Hits))
	for _, hit := range res.Hits {
		hits = append(hits, map[string]any{
			jsonKeyDocIndex: hit.Index,
			jsonKeyDocID:    hit.ID,
			"_score":        matchScore,
			"_source":       hit.Source,
		})
	}

	var maxScore any = matchScore
	if res.Total == 0 {
		maxScore = nil
	}

	h.writeJSON(r, w, map[string]any{
		"took":           0,
		"timed_out":      false,
		jsonKeyDocShards: readOpShards(),
		"hits": map[string]any{
			"total":     map[string]any{"value": res.Total, "relation": "eq"},
			"max_score": maxScore,
			"hits":      hits,
		},
	})
}

// writeIndexError maps backend index/document errors to the AWS-accurate HTTP
// status and error code.
func (h *Handler) writeIndexError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrDomainNotFound), errors.Is(err, ErrConnectionNotFound):
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, ErrValidation):
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	case errors.Is(err, ErrAccessDenied):
		h.writeError(r, w, http.StatusForbidden, "AccessDeniedException", err.Error())
	default:
		h.writeError(r, w, http.StatusInternalServerError, "InternalException", err.Error())
	}
}

// handleIndexDeleteRoute handles DELETE routes under {domainName}/index/{indexName}:
// document deletion (_doc/{id}) and index deletion.
func (h *Handler) handleIndexDeleteRoute(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	sp, ok := parseIndexSubPath(trimmed)
	if !ok {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "invalid index path")

		return true
	}

	if sp.op == indexOpDoc {
		meta, err := h.Backend.DeleteDocument(sp.domain, sp.index, sp.docID)
		if err != nil {
			h.writeIndexError(r, w, err)

			return true
		}
		h.writeJSON(r, w, map[string]any{
			jsonKeyDocIndex:    sp.index,
			jsonKeyDocID:       sp.docID,
			jsonKeyDocVersion:  meta.Version,
			jsonKeyDocSeqNo:    meta.SeqNo,
			jsonKeyDocPrimTerm: docPrimaryTerm,
			jsonKeyDocResult:   docResultDeleted,
			jsonKeyDocShards:   writeOpShards(),
		})

		return true
	}

	// Real DeleteIndexOutput has exactly one field, Status (types.IndexStatus,
	// api_op_DeleteIndex.go:44-53) -- NOT the full index-metadata shape GetIndex
	// returns, which toIndexResponseJSON was previously reused for here. A real
	// client's out.Status was always empty, and the wrong keys (IndexName/
	// Mappings/Settings/...) were sent instead.
	if _, err := h.Backend.DeleteIndex(sp.domain, sp.index); err != nil {
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", err.Error())

		return true
	}
	h.writeJSON(r, w, indexStatusResponseJSON{Status: indexStatusDeleted})

	return true
}
