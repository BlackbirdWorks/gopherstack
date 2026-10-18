package cosmosdb

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func (h *Handler) handleDocuments(c *echo.Context, dbID, collID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.listDocuments(c, dbID, collID)
	case http.MethodPost:
		if isQueryRequest(c.Request()) {
			return h.queryDocuments(c, dbID, collID)
		}

		return h.createDocument(c, dbID, collID)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

func (h *Handler) handleDocumentItem(c *echo.Context, dbID, collID, docID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getDocument(c, dbID, collID, docID)
	case http.MethodPut:
		return h.replaceDocument(c, dbID, collID, docID)
	case http.MethodDelete:
		return h.deleteDocument(c, dbID, collID, docID)
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

// partitionKeyFromHeader parses the x-ms-documentdb-partitionkey header (a JSON array of one to three scalars)
// into the canonical partition key JSON: a scalar for one component, an array for hierarchical keys. Whether the
// component count matches the container is checked by the backend.
func partitionKeyFromHeader(r *http.Request) (string, error) {
	raw := r.Header.Get(headerPartitionKey)
	if raw == "" {
		return "", fmt.Errorf("%w: %s header is required", ErrInvalidDocument, headerPartitionKey)
	}

	return parsePartitionKeyHeader(raw)
}

func parsePartitionKeyHeader(raw string) (string, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()

	var arr []any
	if err := dec.Decode(&arr); err != nil {
		return "", fmt.Errorf("%w: malformed %s header: %w", ErrInvalidDocument, headerPartitionKey, err)
	}

	if trailingErr := rejectTrailingJSON(dec); trailingErr != nil {
		return "", fmt.Errorf(
			"%w: trailing content after %s header value: %w", ErrInvalidDocument, headerPartitionKey, trailingErr,
		)
	}

	if len(arr) < 1 || len(arr) > maxPartitionKeyPaths {
		return "", fmt.Errorf(
			"%w: %s header must carry between one and %d partition key values, got %d",
			ErrInvalidDocument, headerPartitionKey, maxPartitionKeyPaths, len(arr),
		)
	}

	for _, v := range arr {
		if _, err := canonicalPartitionKeyJSON(v); err != nil {
			return "", err
		}
	}

	if len(arr) == 1 {
		return canonicalPartitionKeyJSON(arr[0])
	}

	data, err := json.Marshal(arr)
	if err != nil {
		return "", fmt.Errorf("cosmosdb: encode partition key: %w", err)
	}

	return string(data), nil
}

// writeDocumentError maps a backend error to its Cosmos status and body.
func (h *Handler) writeDocumentError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(err, ErrDocumentNotFound):
		return h.writeDocumentNotFoundError(c)
	case errors.Is(err, ErrDocumentAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "Document with the specified id already exists.")
	case errors.Is(err, ErrETagMismatch):
		return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"Operation cannot be performed because one of the specified precondition is not met.")
	case errors.Is(err, ErrInvalidDocument), errors.Is(err, ErrPartitionKeyMismatch):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	case errors.Is(err, ErrQueryParse), errors.Is(err, ErrQueryTooDeep):
		return h.writeError(c, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("The SQL query text is invalid or has one or more syntax errors: %s", err.Error()))
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func (h *Handler) createDocument(c *echo.Context, dbID, collID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	doc, decErr := decodeJSONObject(body)
	if decErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	upsert := strings.EqualFold(c.Request().Header.Get(headerIsUpsert), "true")

	info, createErr := h.Backend.CreateDocument(dbID, collID, doc, upsert)
	if createErr != nil {
		return h.writeDocumentError(c, createErr)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusCreated, h.encodeDocument(info), createCharge)
}

func (h *Handler) getDocument(c *echo.Context, dbID, collID, docID string) error {
	pk, pkErr := partitionKeyFromHeader(c.Request())
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	info, err := h.Backend.GetDocument(dbID, collID, pk, docID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusOK, h.encodeDocument(info), readCharge)
}

func (h *Handler) replaceDocument(c *echo.Context, dbID, collID, docID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	doc, decErr := decodeJSONObject(body)
	if decErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	info, replaceErr := h.Backend.ReplaceDocument(dbID, collID, pk, docID, doc, r.Header.Get("If-Match"))
	if replaceErr != nil {
		return h.writeDocumentError(c, replaceErr)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusOK, h.encodeDocument(info), replaceCharge)
}

func (h *Handler) deleteDocument(c *echo.Context, dbID, collID, docID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	if err := h.Backend.DeleteDocument(dbID, collID, pk, docID, r.Header.Get("If-Match")); err != nil {
		return h.writeDocumentError(c, err)
	}

	setCharge(c, ruDelete)

	return c.NoContent(http.StatusNoContent)
}

// pagedDocumentLister is implemented by backends that can page the read feed without copying every document.
type pagedDocumentLister interface {
	ListDocumentsPaged(dbID, containerID, token string, limit int) (page.Page[DocumentInfo], error)
}

func (h *Handler) listDocuments(c *echo.Context, dbID, collID string) error {
	token, limit, reqErr := pageRequest(c.Request())
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	var pg page.Page[DocumentInfo]

	if lister, ok := h.Backend.(pagedDocumentLister); ok {
		var err error
		if pg, err = lister.ListDocumentsPaged(dbID, collID, token, limit); err != nil {
			return h.writeDocumentError(c, err)
		}
	} else {
		infos, err := h.Backend.ListDocuments(dbID, collID)
		if err != nil {
			return h.writeDocumentError(c, err)
		}

		pg = paginate(infos, token, limit)
	}

	values := make([]map[string]any, 0, len(pg.Data))
	for _, info := range pg.Data {
		values = append(values, h.encodeDocument(info))
	}

	setPageHeaders(c, len(values), pg.Next)

	return h.writeJSONCharged(c, http.StatusOK,
		map[string]any{sysPropRID: "", "Documents": values, sysPropCount: len(values)}, readCharge)
}

// queryRequestBody is the request body shape for a SQL query POST:
// {"query":"SELECT * FROM c WHERE c.x = @p","parameters":[{"name":"@p","value":...}]}.
type queryRequestBody struct {
	Query      string           `json:"query"`
	Parameters []QueryParameter `json:"parameters"`
}

func (h *Handler) queryDocuments(c *echo.Context, dbID, collID string) error {
	r := c.Request()

	token, limit, reqErr := pageRequest(r)
	if reqErr != nil {
		return h.writeContinuationError(c)
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req queryRequestBody

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	if decErr := dec.Decode(&req); decErr != nil || rejectTrailingJSON(dec) != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	scope, scopeErr := queryPartitionScope(r)
	if scopeErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", scopeErr.Error())
	}

	res, execErr := h.runQuery(dbID, collID, req, scope)
	if execErr != nil {
		return h.writeDocumentError(c, execErr)
	}

	pg := paginate(res.Rows, token, limit)
	setPageHeaders(c, len(pg.Data), pg.Next)

	payload := map[string]any{sysPropRID: "", "Documents": pg.Data, sysPropCount: len(pg.Data)}

	return h.writeJSONCharged(c, http.StatusOK, payload, func(size int) float64 {
		return queryCharge(res.Scanned, size)
	})
}

// queryPartitionScope returns the partition key a query is scoped to ("" for cross-partition). An empty key
// array means cross-partition.
func queryPartitionScope(r *http.Request) (string, error) {
	raw := strings.TrimSpace(r.Header.Get(headerPartitionKey))
	if raw == "" || raw == "[]" {
		return "", nil
	}

	return parsePartitionKeyHeader(raw)
}

// encodeDocument builds a document's JSON response body.
func (h *Handler) encodeDocument(info DocumentInfo) map[string]any {
	return documentAsMap(info)
}

// writeDocumentNotFoundError maps a missing-document StorageBackend error to the Cosmos error.
func (h *Handler) writeDocumentNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "NotFound", "Resource Not Found")
}

// partitionQuerier is implemented by backends that can run a query scoped to a partition.
type partitionQuerier interface {
	RunQuery(dbID, containerID, query string, params []QueryParameter, partitionKey string) (QueryResult, error)
}

// runQuery uses the backend's direct query path when it has one, otherwise ListDocuments plus ExecuteQuery.
func (h *Handler) runQuery(dbID, collID string, req queryRequestBody, partitionKey string) (QueryResult, error) {
	if q, ok := h.Backend.(partitionQuerier); ok {
		return q.RunQuery(dbID, collID, req.Query, req.Parameters, partitionKey)
	}

	infos, err := h.Backend.ListDocuments(dbID, collID)
	if err != nil {
		return QueryResult{}, err
	}

	if partitionKey != "" {
		scoped := infos[:0]

		for _, info := range infos {
			if partitionKeyMatches(info.PartitionKeyJSON, partitionKey) {
				scoped = append(scoped, info)
			}
		}

		infos = scoped
	}

	rows, execErr := ExecuteQuery(req.Query, req.Parameters, infos)

	return QueryResult{Rows: rows, Scanned: len(infos)}, execErr
}
