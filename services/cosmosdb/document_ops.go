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
		if isChangeFeedRequest(c.Request()) {
			return h.readChangeFeed(c, dbID, collID)
		}

		return h.listDocuments(c, dbID, collID)
	case http.MethodPost:
		if isBatchRequest(c.Request()) {
			return h.executeBatch(c, dbID, collID)
		}

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
	case http.MethodPatch:
		return h.patchDocument(c, dbID, collID, docID)
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
	var indexErr *queryIndexError

	switch {
	case errors.Is(err, ErrDatabaseNotFound):
		return h.writeDatabaseNotFoundError(c)
	case errors.Is(err, ErrContainerNotFound):
		return h.writeContainerNotFoundError(c)
	case errors.Is(err, ErrDocumentNotFound):
		return h.writeDocumentNotFoundError(c)
	case errors.Is(err, ErrDocumentAlreadyExists):
		return h.writeError(c, http.StatusConflict, "Conflict", "Document with the specified id already exists.")
	case errors.Is(err, ErrUniqueKeyViolation):
		return h.writeError(c, http.StatusConflict, "Conflict", "Unique index constraint violation.")
	case errors.Is(err, ErrETagMismatch), errors.Is(err, ErrPatchConditionFailed):
		return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"Operation cannot be performed because one of the specified precondition is not met.")
	case errors.Is(err, ErrInvalidDocument), errors.Is(err, ErrPartitionKeyMismatch), errors.Is(err, ErrInvalidPatch),
		errors.Is(err, ErrBatchTooLarge), errors.Is(err, ErrInvalidTrigger), errors.Is(err, ErrInvalidPKRange),
		errors.Is(err, ErrChangeFeedStart), errors.Is(err, ErrChangeFeedBackup), errors.Is(err, ErrChangeFeedRetention):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	case isRoutingError(err):
		return h.writeRoutingError(c, err)
	case errors.Is(err, ErrScriptNotFound):
		return h.writeError(c, http.StatusNotFound, "NotFound", "Resource Not Found")
	case errors.Is(err, ErrScriptTimeout):
		return h.writeError(c, http.StatusRequestTimeout, "RequestTimeout", err.Error())
	case errors.Is(err, ErrScriptFailed):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", err.Error())
	case errors.As(err, &indexErr):
		return h.writeError(c, http.StatusBadRequest, "BadRequest", indexErr.msg)
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

	if trig := triggerRequest(c.Request()); trig.requested() {
		kind := batchCreate
		if upsert {
			kind = batchUpsert
		}

		return h.runWithTriggers(c, dbID, collID, docOp{kind: kind, body: doc, pre: trig.pre, post: trig.post})
	}

	info, createErr := h.be(c).CreateDocument(dbID, collID, doc, upsert)
	if createErr != nil {
		return h.writeDocumentError(c, createErr)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusCreated, h.encodeDocument(info),
		h.writeCharger(dbID, collID, info.Body, false))
}

func (h *Handler) getDocument(c *echo.Context, dbID, collID, docID string) error {
	pk, pkErr := partitionKeyFromHeader(c.Request())
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	info, err := h.be(c).GetDocument(dbID, collID, pk, docID)
	if err != nil {
		return h.writeDocumentError(c, err)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusOK, h.encodeDocument(info), h.readCharger(c))
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

	if trig := triggerRequest(r); trig.requested() {
		return h.runWithTriggers(c, dbID, collID, docOp{
			kind: batchReplace, pk: pk, id: docID, body: doc, ifMatch: r.Header.Get("If-Match"),
			pre: trig.pre, post: trig.post,
		})
	}

	info, replaceErr := h.be(c).ReplaceDocument(dbID, collID, pk, docID, doc, r.Header.Get("If-Match"))
	if replaceErr != nil {
		return h.writeDocumentError(c, replaceErr)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusOK, h.encodeDocument(info),
		h.writeCharger(dbID, collID, info.Body, true))
}

func (h *Handler) patchDocument(c *echo.Context, dbID, collID, docID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	body, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	req, parseErr := parsePatchRequest(body)
	if parseErr != nil {
		return h.writeDocumentError(c, parseErr)
	}

	if trig := triggerRequest(r); trig.requested() {
		patchBody, decErr := decodeJSONObject(body)
		if decErr != nil {
			return h.writeError(c, http.StatusBadRequest, "BadRequest", "The input is not valid JSON.")
		}

		return h.runWithTriggers(c, dbID, collID, docOp{
			kind: batchPatch, body: patchBody, pk: pk, id: docID, ifMatch: r.Header.Get("If-Match"),
			pre: trig.pre, post: trig.post,
		})
	}

	patcher, ok := h.be(c).(interface {
		PatchDocument(dbID, containerID, partitionKey, id string, req patchRequest, ifMatch string) (DocumentInfo, error)
	})
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "The backend cannot patch documents.")
	}

	info, patchErr := patcher.PatchDocument(dbID, collID, pk, docID, req, r.Header.Get("If-Match"))
	if patchErr != nil {
		return h.writeDocumentError(c, patchErr)
	}

	c.Response().Header().Set("ETag", info.ETag)

	return h.writeJSONCharged(c, http.StatusOK, h.encodeDocument(info),
		h.writeCharger(dbID, collID, info.Body, true))
}

func (h *Handler) deleteDocument(c *echo.Context, dbID, collID, docID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	if trig := triggerRequest(r); trig.requested() {
		return h.runWithTriggers(c, dbID, collID, docOp{
			kind: batchDelete, pk: pk, id: docID, ifMatch: r.Header.Get("If-Match"), pre: trig.pre, post: trig.post,
		})
	}

	if err := h.be(c).DeleteDocument(dbID, collID, pk, docID, r.Header.Get("If-Match")); err != nil {
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

	if lister, ok := h.be(c).(pagedDocumentLister); ok {
		var err error
		if pg, err = lister.ListDocumentsPaged(dbID, collID, token, limit); err != nil {
			return h.writeDocumentError(c, err)
		}
	} else {
		infos, err := h.be(c).ListDocuments(dbID, collID)
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
		map[string]any{sysPropRID: "", listDocumentsKey: values, sysPropCount: len(values)}, h.feedCharger(c, values))
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

	res, execErr := h.runQuery(h.be(c), dbID, collID, req, scope, queryOptions(r))
	if execErr != nil {
		return h.writeDocumentError(c, execErr)
	}

	pg := paginate(res.Rows, token, limit)
	setPageHeaders(c, len(pg.Data), pg.Next)

	payload := map[string]any{sysPropRID: "", listDocumentsKey: pg.Data, sysPropCount: len(pg.Data)}

	share := 1.0
	if len(res.Rows) > 0 {
		share = float64(len(pg.Data)) / float64(len(res.Rows))
	}

	charge := h.readFactor(c) * queryCharge(res.Loaded*share, h.queryPartitions(dbID, collID, scope))

	return h.writeJSONCharged(c, http.StatusOK, payload, func(int) float64 { return charge })
}

// queryOptions reads the scan switch; azcosmos sends x-ms-documentdb-force-query-scan, the REST docs
// x-ms-documentdb-query-enable-scan.
func queryOptions(r *http.Request) QueryOptions {
	for _, name := range []string{"x-ms-documentdb-query-enable-scan", "x-ms-documentdb-force-query-scan"} {
		if strings.EqualFold(strings.TrimSpace(r.Header.Get(name)), trueText) {
			return QueryOptions{AllowScan: true}
		}
	}

	return QueryOptions{}
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

// indexedQuerier is implemented by backends that enforce the container's indexing policy.
type indexedQuerier interface {
	RunQueryOpts(
		dbID, containerID, query string, params []QueryParameter, partitionKey string, opts QueryOptions,
	) (QueryResult, error)
}

// runQuery uses the backend's direct query path when it has one, otherwise ListDocuments plus ExecuteQuery.
func (h *Handler) runQuery(
	be StorageBackend, dbID, collID string, req queryRequestBody, partitionKey string, opts QueryOptions,
) (QueryResult, error) {
	if q, ok := be.(indexedQuerier); ok {
		return q.RunQueryOpts(dbID, collID, req.Query, req.Parameters, partitionKey, opts)
	}

	if q, ok := be.(partitionQuerier); ok {
		return q.RunQuery(dbID, collID, req.Query, req.Parameters, partitionKey)
	}

	infos, err := be.ListDocuments(dbID, collID)
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

	res := QueryResult{Rows: rows, Scanned: len(infos)}
	for _, info := range infos {
		raw, _ := jsonMarshal(info.Body)
		res.Loaded += readCharge(len(raw))
	}

	return res, execErr
}

// isRoutingError reports an error that depends on where the request ran: a split partition, a lagging region.
func isRoutingError(err error) bool {
	return errors.Is(err, ErrReadSessionNotAvailable) || errors.Is(err, ErrConflictNotFound) ||
		errors.Is(err, ErrPKRangeGone)
}

func (h *Handler) writeRoutingError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrReadSessionNotAvailable):
		c.Response().Header().Set(headerSubStatus, subStatusSessionGone)

		return h.writeError(c, http.StatusNotFound, "NotFound", err.Error())
	case errors.Is(err, ErrPKRangeGone):
		c.Response().Header().Set(headerSubStatus, subStatusPKRangeGone)

		return h.writeError(c, http.StatusGone, "Gone", err.Error())
	}

	return h.writeError(c, http.StatusNotFound, "NotFound", "Resource Not Found")
}
