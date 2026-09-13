package opensearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// bulkActionMeta is the per-action metadata object documented for every
// _bulk action line (https://opensearch.org/docs/latest/api-reference/
// document-apis/bulk/). Only _index/_id are modeled: routing/version/
// version_type/if_seq_no/if_primary_term/retry_on_conflict are real,
// documented metadata fields too, but this backend has no shard-routing or
// optimistic-concurrency-by-version concept anywhere else in the data
// plane, so they are accepted on the wire (simply not read) rather than
// rejected.
type bulkActionMeta struct {
	Index string `json:"_index,omitempty"`
	ID    string `json:"_id,omitempty"`
}

// bulkActionEnvelope is one action line: exactly one of Index/Create/Update/
// Delete must be set, per the documented action-line shape.
type bulkActionEnvelope struct {
	Index  *bulkActionMeta `json:"index,omitempty"`
	Create *bulkActionMeta `json:"create,omitempty"`
	Update *bulkActionMeta `json:"update,omitempty"`
	Delete *bulkActionMeta `json:"delete,omitempty"`
}

// bulkUpdateBody is the source line for an "update" action: either a "doc"
// (optionally with "doc_as_upsert") or a "script" -- this backend has no
// scripting engine, so a non-empty Script is rejected (see
// handleBulkUpdate).
type bulkUpdateBody struct {
	Doc         map[string]any  `json:"doc,omitempty"`
	Script      json.RawMessage `json:"script,omitempty"`
	DocAsUpsert bool            `json:"doc_as_upsert,omitempty"`
}

// bulkAction is one parsed action line plus its source (nil for "delete").
type bulkAction struct {
	Op     string
	Index  string
	ID     string
	Source json.RawMessage
}

const (
	bulkOpIndex  = "index"
	bulkOpCreate = "create"
	bulkOpUpdate = "update"
	bulkOpDelete = "delete"
)

var (
	errBulkActionAmbiguous = errors.New("action line must specify exactly one of index, create, update, or delete")
	errBulkMissingSource   = errors.New("action requires a following source line")
)

// parseBulkPath splits a domain-scoped bulk path into its domain and default
// index. Real OpenSearch serves both POST /_bulk (no default index -- every
// action line must carry its own _index) and POST /{index}/_bulk (that index
// is used whenever an action line omits _index) -- see the doc cited above.
// This backend's data plane prefixes both under {domainName}/..., matching
// the existing {domainName}/index/{indexName}/_search convention
// (handler_indices.go).
func parseBulkPath(trimmed string) (string, string) {
	base := strings.TrimSuffix(trimmed, "/_bulk")
	if dom, idx, ok := strings.Cut(base, "/index/"); ok {
		return dom, idx
	}

	return base, ""
}

// handleBulkRoute serves POST {domainName}/_bulk and
// POST {domainName}/index/{indexName}/_bulk. Content-Type is not inspected:
// both application/x-ndjson and application/json bodies are accepted, and
// ?refresh/?routing are accepted without error but have no effect -- this
// backend has no replica/refresh-interval model (writes are visible
// immediately) and no shard-routing model.
func (h *Handler) handleBulkRoute(w http.ResponseWriter, r *http.Request, trimmed string) bool {
	domainName, defaultIndex := parseBulkPath(trimmed)

	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return true
	}

	actions, perr := parseBulkActions(body)
	if perr != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", perr.Error())

		return true
	}

	items := make([]map[string]any, 0, len(actions))
	hasErrors := false

	for _, act := range actions {
		item, isErr := h.processBulkAction(domainName, defaultIndex, act)
		items = append(items, item)

		if isErr {
			hasErrors = true
		}
	}

	h.writeJSON(r, w, map[string]any{
		"took":   0,
		"errors": hasErrors,
		"items":  items,
	})

	return true
}

// splitNDJSONLines splits a bulk request body into its non-blank lines.
func splitNDJSONLines(body []byte) [][]byte {
	var lines [][]byte

	for raw := range bytes.SplitSeq(body, []byte("\n")) {
		line := bytes.TrimSpace(raw)
		if len(line) == 0 {
			continue
		}

		lines = append(lines, line)
	}

	return lines
}

// resolveBulkAction extracts the single set action (index/create/update/
// delete) and its metadata from a parsed envelope.
func resolveBulkAction(env bulkActionEnvelope) (string, bulkActionMeta, error) {
	count := 0

	var (
		op   string
		meta bulkActionMeta
	)

	if env.Index != nil {
		count++
		op, meta = bulkOpIndex, *env.Index
	}

	if env.Create != nil {
		count++
		op, meta = bulkOpCreate, *env.Create
	}

	if env.Update != nil {
		count++
		op, meta = bulkOpUpdate, *env.Update
	}

	if env.Delete != nil {
		count++
		op, meta = bulkOpDelete, *env.Delete
	}

	if count != 1 {
		return "", bulkActionMeta{}, errBulkActionAmbiguous
	}

	return op, meta, nil
}

// parseBulkActions parses a bulk request body into its action/source pairs.
// Every action line except "delete" is documented to be followed by a
// source line.
func parseBulkActions(body []byte) ([]bulkAction, error) {
	lines := splitNDJSONLines(body)

	actions := make([]bulkAction, 0, len(lines))

	for i := 0; i < len(lines); i++ {
		var env bulkActionEnvelope
		if err := json.Unmarshal(lines[i], &env); err != nil {
			return nil, fmt.Errorf("malformed action line %d: %w", i+1, err)
		}

		op, meta, err := resolveBulkAction(env)
		if err != nil {
			return nil, fmt.Errorf("action line %d: %w", i+1, err)
		}

		act := bulkAction{Op: op, Index: meta.Index, ID: meta.ID}

		if op != bulkOpDelete {
			i++
			if i >= len(lines) {
				return nil, fmt.Errorf("action line %d: %s %w", i, op, errBulkMissingSource)
			}

			act.Source = lines[i]
		}

		actions = append(actions, act)
	}

	return actions, nil
}

// processBulkAction dispatches one parsed action against the backend and
// returns its wire item (wrapped under its action name) plus whether it
// counts toward the response's top-level "errors" flag.
func (h *Handler) processBulkAction(domainName, defaultIndex string, act bulkAction) (map[string]any, bool) {
	index := act.Index
	if index == "" {
		index = defaultIndex
	}

	if index == "" {
		return bulkErrorItem(act.Op, index, act.ID, http.StatusBadRequest,
			"illegal_argument_exception", "_index is required"), true
	}

	switch act.Op {
	case bulkOpDelete:
		return h.processBulkDelete(domainName, index, act.ID)
	case bulkOpCreate:
		return h.processBulkCreate(domainName, index, act.ID, act.Source)
	case bulkOpUpdate:
		return h.processBulkUpdate(domainName, index, act.ID, act.Source)
	default:
		return h.processBulkIndex(domainName, index, act.ID, act.Source)
	}
}

func (h *Handler) processBulkIndex(domainName, index, id string, source json.RawMessage) (map[string]any, bool) {
	doc, err := unmarshalBulkSource(source)
	if err != nil {
		return bulkErrorItem(bulkOpIndex, index, id, http.StatusBadRequest,
			"illegal_argument_exception", "invalid source document"), true
	}

	newID, created, meta, idxErr := h.Backend.IndexDocument(domainName, index, id, doc)
	if idxErr != nil {
		return bulkBackendErrorItem(bulkOpIndex, index, id, idxErr), true
	}

	result := docResultUpdated
	if created {
		result = docResultCreated
	}

	return bulkSuccessItem(bulkOpIndex, index, newID, result, meta), false
}

func (h *Handler) processBulkCreate(domainName, index, id string, source json.RawMessage) (map[string]any, bool) {
	doc, err := unmarshalBulkSource(source)
	if err != nil {
		return bulkErrorItem(bulkOpCreate, index, id, http.StatusBadRequest,
			"illegal_argument_exception", "invalid source document"), true
	}

	newID, meta, createErr := h.Backend.CreateDocument(domainName, index, id, doc)
	if createErr != nil {
		return bulkBackendErrorItem(bulkOpCreate, index, id, createErr), true
	}

	return bulkSuccessItem(bulkOpCreate, index, newID, docResultCreated, meta), false
}

func (h *Handler) processBulkUpdate(domainName, index, id string, source json.RawMessage) (map[string]any, bool) {
	if id == "" {
		return bulkErrorItem(bulkOpUpdate, index, id, http.StatusBadRequest,
			"illegal_argument_exception", "_id is required for the update action"), true
	}

	var body bulkUpdateBody
	if len(source) > 0 {
		if err := json.Unmarshal(source, &body); err != nil {
			return bulkErrorItem(bulkOpUpdate, index, id, http.StatusBadRequest,
				"illegal_argument_exception", "invalid update body"), true
		}
	}

	if len(body.Script) > 0 {
		return bulkErrorItem(bulkOpUpdate, index, id, http.StatusBadRequest,
			"illegal_argument_exception", "scripted updates are not supported by this emulator"), true
	}

	// Pre-check the index so UpdateDocument's ErrConnectionNotFound (which
	// it also returns for a missing document, docAsUpsert=false) can be
	// classified unambiguously below as a genuinely missing document rather
	// than mislabeled as a missing index.
	if _, err := h.Backend.CountDocuments(domainName, index); err != nil {
		return bulkBackendErrorItem(bulkOpUpdate, index, id, err), true
	}

	created, meta, err := h.Backend.UpdateDocument(domainName, index, id, body.Doc, body.DocAsUpsert)
	if err != nil {
		if errors.Is(err, ErrConnectionNotFound) {
			return bulkErrorItem(bulkOpUpdate, index, id, http.StatusNotFound,
				"document_missing_exception", err.Error()), true
		}

		return bulkBackendErrorItem(bulkOpUpdate, index, id, err), true
	}

	result := docResultUpdated
	if created {
		result = docResultCreated
	}

	return bulkSuccessItem(bulkOpUpdate, index, id, result, meta), false
}

func (h *Handler) processBulkDelete(domainName, index, id string) (map[string]any, bool) {
	if id == "" {
		return bulkErrorItem(bulkOpDelete, index, id, http.StatusBadRequest,
			"illegal_argument_exception", "_id is required for the delete action"), true
	}

	found, meta, err := h.Backend.BulkDeleteDocument(domainName, index, id)
	if err != nil {
		return bulkBackendErrorItem(bulkOpDelete, index, id, err), true
	}

	if !found {
		return bulkResultItem(bulkOpDelete, index, id, docResultNotFound, http.StatusNotFound, meta), true
	}

	return bulkSuccessItem(bulkOpDelete, index, id, docResultDeleted, meta), false
}

// unmarshalBulkSource decodes a bulk action's source line, tolerating an
// absent one (an empty document, matching IndexDocument's own handling of a
// missing body).
func unmarshalBulkSource(source json.RawMessage) (map[string]any, error) {
	if len(source) == 0 {
		return map[string]any{}, nil
	}

	doc := map[string]any{}
	if err := json.Unmarshal(source, &doc); err != nil {
		return nil, err
	}

	return doc, nil
}

// bulkStatusForResult maps a successful result to its documented HTTP
// status: 201 for a fresh create, 200 otherwise.
func bulkStatusForResult(result string) int {
	if result == docResultCreated {
		return http.StatusCreated
	}

	return http.StatusOK
}

// bulkSuccessItem builds a successful per-item result: _index/_id/_version/
// result/_shards/_seq_no/_primary_term/status, matching the real Index/
// Create/Update/Delete Document API response shapes this backend already
// serves at handler_indices.go's single-document routes.
func bulkSuccessItem(action, index, id, result string, meta DocumentMeta) map[string]any {
	return bulkResultItem(action, index, id, result, bulkStatusForResult(result), meta)
}

// bulkResultItem builds a non-error per-item result at an explicit status
// (bulkSuccessItem's shared body, also used by the delete "not_found" case,
// which reports status 404 with no error{} object -- see BulkDeleteDocument).
func bulkResultItem(action, index, id, result string, status int, meta DocumentMeta) map[string]any {
	return map[string]any{
		action: map[string]any{
			jsonKeyDocIndex:    index,
			jsonKeyDocID:       id,
			jsonKeyDocVersion:  meta.Version,
			jsonKeyDocResult:   result,
			jsonKeyDocShards:   writeOpShards(),
			jsonKeyDocSeqNo:    meta.SeqNo,
			jsonKeyDocPrimTerm: docPrimaryTerm,
			"status":           status,
		},
	}
}

// bulkErrorItem builds a failed per-item result: _index/_id/status/error{type,reason}.
func bulkErrorItem(action, index, id string, status int, errType, reason string) map[string]any {
	return map[string]any{
		action: map[string]any{
			jsonKeyDocIndex: index,
			jsonKeyDocID:    id,
			"status":        status,
			"error": map[string]any{
				jsonKeyPolicyTypeJR: errType,
				"reason":            reason,
			},
		},
	}
}

// bulkBackendErrorItem maps a backend error to its documented bulk item
// error type and HTTP status.
func bulkBackendErrorItem(action, index, id string, err error) map[string]any {
	switch {
	case errors.Is(err, ErrDomainNotFound), errors.Is(err, ErrConnectionNotFound):
		return bulkErrorItem(action, index, id, http.StatusNotFound, "index_not_found_exception", err.Error())
	case errors.Is(err, ErrDocumentVersionConflict):
		return bulkErrorItem(action, index, id, http.StatusConflict, "version_conflict_engine_exception", err.Error())
	case errors.Is(err, ErrAccessDenied):
		return bulkErrorItem(action, index, id, http.StatusForbidden, "security_exception", err.Error())
	case errors.Is(err, ErrValidation):
		return bulkErrorItem(action, index, id, http.StatusBadRequest, "illegal_argument_exception", err.Error())
	default:
		return bulkErrorItem(action, index, id, http.StatusInternalServerError, "exception", err.Error())
	}
}
