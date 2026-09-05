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

// partitionKeyFromHeader parses the mandatory x-ms-documentdb-partitionkey
// header (a JSON array carrying exactly the partition key's single scalar
// value, e.g. ["foo"], [123], [true], or [null]) into its canonical JSON
// encoding. Required on every point operation (Get/Replace/Delete
// Document); Create derives the partition key from the document body
// instead (see store.go's prepareDocumentBody), matching real Cosmos.
//
// The array MUST contain exactly one element: an empty array ([]) is
// rejected rather than silently treated as a null partition key ([null] is
// the correct way to express that), and an array with more than one
// element is rejected too rather than silently truncated to its first
// element -- an earlier version of this function did exactly that
// truncation, which could route a request against the wrong document
// without any error. The element itself must be a scalar (string, number,
// bool, or null) -- canonicalPartitionKeyJSON enforces that.
func partitionKeyFromHeader(r *http.Request) (string, error) {
	raw := r.Header.Get(headerPartitionKey)
	if raw == "" {
		return "", fmt.Errorf("%w: %s header is required", ErrInvalidDocument, headerPartitionKey)
	}

	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()

	var arr []any
	if err := dec.Decode(&arr); err != nil {
		return "", fmt.Errorf("%w: malformed %s header: %w", ErrInvalidDocument, headerPartitionKey, err)
	}

	if len(arr) != 1 {
		return "", fmt.Errorf(
			"%w: %s header must carry exactly one partition key value, got %d",
			ErrInvalidDocument, headerPartitionKey, len(arr),
		)
	}

	return canonicalPartitionKeyJSON(arr[0])
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

	switch {
	case createErr == nil:
	case errors.Is(createErr, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(createErr, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(createErr, ErrDocumentAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "Document with the specified id already exists.")
	case errors.Is(createErr, ErrInvalidDocument):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", createErr.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", createErr.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSON(c, http.StatusCreated, h.encodeDocument(info))
}

func (h *Handler) getDocument(c *echo.Context, dbID, collID, docID string) error {
	pk, pkErr := partitionKeyFromHeader(c.Request())
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	info, err := h.Backend.GetDocument(dbID, collID, pk, docID)

	switch {
	case err == nil:
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(err, ErrDocumentNotFound):
		return h.writeDocumentNotFoundError(c)
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSON(c, http.StatusOK, h.encodeDocument(info))
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

	ifMatch := r.Header.Get("If-Match")

	info, replaceErr := h.Backend.ReplaceDocument(dbID, collID, pk, docID, doc, ifMatch)

	switch {
	case replaceErr == nil:
	case errors.Is(replaceErr, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(replaceErr, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(replaceErr, ErrDocumentNotFound):
		return h.writeDocumentNotFoundError(c)
	case errors.Is(replaceErr, ErrETagMismatch):
		return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"Operation cannot be performed because one of the specified precondition is not met.")
	case errors.Is(replaceErr, ErrInvalidDocument):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", replaceErr.Error())
	case errors.Is(replaceErr, ErrPartitionKeyMismatch):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", replaceErr.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", replaceErr.Error())
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSON(c, http.StatusOK, h.encodeDocument(info))
}

func (h *Handler) deleteDocument(c *echo.Context, dbID, collID, docID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	ifMatch := r.Header.Get("If-Match")

	err := h.Backend.DeleteDocument(dbID, collID, pk, docID, ifMatch)

	switch {
	case err == nil:
		return c.NoContent(http.StatusNoContent)
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(err, ErrDocumentNotFound):
		return h.writeDocumentNotFoundError(c)
	case errors.Is(err, ErrETagMismatch):
		return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"Operation cannot be performed because one of the specified precondition is not met.")
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}
}

func (h *Handler) listDocuments(c *echo.Context, dbID, collID string) error {
	infos, err := h.Backend.ListDocuments(dbID, collID)

	switch {
	case err == nil:
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	values := make([]map[string]any, 0, len(infos))
	for _, info := range infos {
		values = append(values, h.encodeDocument(info))
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{sysPropRID: "", "Documents": values, sysPropCount: len(values)})
}

// queryRequestBody is the request body shape for a SQL query POST:
// {"query":"SELECT * FROM c WHERE c.x = @p","parameters":[{"name":"@p","value":...}]}.
type queryRequestBody struct {
	Query      string           `json:"query"`
	Parameters []QueryParameter `json:"parameters"`
}

func (h *Handler) queryDocuments(c *echo.Context, dbID, collID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	var req queryRequestBody

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	if decErr := dec.Decode(&req); decErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
	}

	infos, listErr := h.Backend.ListDocuments(dbID, collID)

	switch {
	case listErr == nil:
	case errors.Is(listErr, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(listErr, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	default:
		return h.writeError(c, http.StatusInternalServerError, "InternalError", listErr.Error())
	}

	results, execErr := ExecuteQuery(req.Query, req.Parameters, infos)
	if execErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest",
			fmt.Sprintf("The SQL query text is invalid or has one or more syntax errors: %s", execErr.Error()))
	}

	return h.writeJSON(c, http.StatusOK,
		map[string]any{sysPropRID: "", "Documents": results, sysPropCount: len(results)})
}

// encodeDocument builds a document's Cosmos DB JSON response body: its
// user-defined fields (already a deep-cloned, alias-safe copy -- see
// StorageBackend's documentInfo/deepCopyBody) overlaid with the
// server-managed system properties.
func (h *Handler) encodeDocument(info DocumentInfo) map[string]any {
	return documentAsMap(info)
}

// writeDocumentNotFoundError maps a missing-document StorageBackend error to
// the corresponding Cosmos error code/status.
func (h *Handler) writeDocumentNotFoundError(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "NotFound", "Resource Not Found")
}
