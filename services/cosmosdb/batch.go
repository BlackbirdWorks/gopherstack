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

// maxBatchOperations is the Cosmos limit on operations in one transactional batch.
const maxBatchOperations = 100

const (
	headerIsBatch = "X-Ms-Cosmos-Is-Batch-Request"
	headerPrefer  = "Prefer"

	batchCreate  = "create"
	batchUpsert  = "upsert"
	batchRead    = "read"
	batchReplace = "replace"
	batchDelete  = "delete"
	batchPatch   = "patch"
)

func isBatchRequest(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get(headerIsBatch), "true")
}

type batchOpWire struct {
	OperationType string          `json:"operationType"`
	ID            string          `json:"id"`
	IfMatch       string          `json:"ifMatch"`
	ResourceBody  json.RawMessage `json:"resourceBody"`
}

// batchOp is a decoded batch operation.
type batchOp struct {
	body    map[string]any
	kind    string
	id      string
	ifMatch string
	patch   patchRequest
}

// BatchResult is one operation's outcome in the batch response array.
type BatchResult struct {
	Body          map[string]any `json:"resourceBody,omitempty"`
	ETag          string         `json:"eTag,omitempty"`
	StatusCode    int            `json:"statusCode"`
	RequestCharge float64        `json:"requestCharge"`
}

func parseBatchOps(raw []byte) ([]batchOp, error) {
	var wire []batchOpWire

	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&wire); err != nil || rejectTrailingJSON(dec) != nil {
		return nil, fmt.Errorf("%w: the batch body is not a JSON array of operations", ErrInvalidDocument)
	}

	if len(wire) == 0 {
		return nil, fmt.Errorf("%w: a batch needs at least one operation", ErrInvalidDocument)
	}

	if len(wire) > maxBatchOperations {
		return nil, ErrBatchTooLarge
	}

	ops := make([]batchOp, len(wire))

	for i, w := range wire {
		op, err := decodeBatchOp(w)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i, err)
		}

		ops[i] = op
	}

	return ops, nil
}

func decodeBatchOp(w batchOpWire) (batchOp, error) {
	op := batchOp{kind: strings.ToLower(w.OperationType), id: w.ID, ifMatch: w.IfMatch}

	switch op.kind {
	case batchCreate, batchUpsert, batchReplace:
		body, err := decodeJSONObject(w.ResourceBody)
		op.body = body

		return op, err
	case batchPatch:
		req, err := parsePatchRequest(w.ResourceBody)
		op.patch = req

		return op, err
	case batchRead, batchDelete:
		return op, nil
	default:
		return op, fmt.Errorf("%w: unknown operationType %q", ErrInvalidDocument, w.OperationType)
	}
}

// ExecuteBatch runs ops atomically in one partition. When an operation fails the batch is rolled back, the
// failing result carries its status and every other result is 424. The bool reports whether it committed.
func (b *InMemoryBackend) ExecuteBatch(dbID, collID, partitionKey string, ops []batchOp) ([]BatchResult, bool, error) {
	b.mu.Lock("ExecuteBatch")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, collID)
	if err != nil {
		return nil, false, err
	}

	if pkErr := tx.c.checkPartitionKey(partitionKey); pkErr != nil {
		return nil, false, pkErr
	}

	results := make([]BatchResult, len(ops))

	for i, op := range ops {
		res, opErr := tx.runBatchOp(partitionKey, op)
		if opErr != nil {
			tx.rollback()

			return failedBatch(len(ops), i, opErr), false, nil
		}

		results[i] = res
	}

	return results, true, nil
}

func failedBatch(n, failed int, err error) []BatchResult {
	results := make([]BatchResult, n)
	for i := range results {
		results[i].StatusCode = http.StatusFailedDependency
	}

	results[failed].StatusCode = statusForError(err)

	return results
}

func (tx *containerTx) runBatchOp(pk string, op batchOp) (BatchResult, error) {
	var (
		info   DocumentInfo
		err    error
		status = http.StatusOK
		charge = replaceCharge
	)

	readPrice := func(size, _ int) float64 { return readCharge(size) }

	switch op.kind {
	case batchCreate:
		info, _, err = tx.createDoc(pk, op.body, false)
		status, charge = http.StatusCreated, createCharge
	case batchUpsert:
		var existed bool

		info, existed, err = tx.createDoc(pk, op.body, true)
		charge = createCharge

		if !existed {
			status = http.StatusCreated
		}
	case batchRead:
		info, err = tx.readDoc(pk, op.id)
		charge = readPrice
	case batchReplace:
		info, err = tx.replaceDoc(pk, op.id, op.body, op.ifMatch)
	case batchPatch:
		info, err = tx.patchDoc(pk, op.id, op.patch, op.ifMatch)
	case batchDelete:
		err = tx.deleteDoc(pk, op.id, op.ifMatch)

		return BatchResult{StatusCode: http.StatusNoContent, RequestCharge: ruDelete}, err
	}

	if err != nil {
		return BatchResult{}, err
	}

	body := documentAsMap(info)
	size, _ := json.Marshal(body)

	return BatchResult{
		StatusCode: status, ETag: info.ETag, Body: body,
		RequestCharge: charge(len(size), countIndexedPaths(parseIndexingPolicy(tx.c.IndexingPolicy), info.Body)),
	}, nil
}

// statusForError maps a backend error to the HTTP status a batch result reports.
func statusForError(err error) int {
	var indexErr *queryIndexError

	switch {
	case errors.As(err, &indexErr):
		return http.StatusBadRequest
	case errors.Is(err, ErrDocumentNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrDocumentAlreadyExists), errors.Is(err, ErrUniqueKeyViolation):
		return http.StatusConflict
	case errors.Is(err, ErrETagMismatch), errors.Is(err, ErrPatchConditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrInvalidDocument), errors.Is(err, ErrPartitionKeyMismatch), errors.Is(err, ErrInvalidPatch),
		errors.Is(err, ErrQueryParse), errors.Is(err, ErrQueryTooDeep):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}

type batchExecutor interface {
	ExecuteBatch(dbID, collID, partitionKey string, ops []batchOp) ([]BatchResult, bool, error)
}

func (h *Handler) executeBatch(c *echo.Context, dbID, collID string) error {
	r := c.Request()

	pk, pkErr := partitionKeyFromHeader(r)
	if pkErr != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequest", pkErr.Error())
	}

	raw, err := httputils.ReadBody(r)
	if err != nil {
		return h.writeError(c, http.StatusInternalServerError, "InternalError", "Failed to read request body.")
	}

	ops, parseErr := parseBatchOps(raw)
	if parseErr != nil {
		return h.writeDocumentError(c, parseErr)
	}

	exec, ok := h.be(c).(batchExecutor)
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "The backend cannot run batches.")
	}

	results, committed, execErr := exec.ExecuteBatch(dbID, collID, pk, ops)
	if execErr != nil {
		return h.writeDocumentError(c, execErr)
	}

	minimal := strings.Contains(strings.ToLower(r.Header.Get(headerPrefer)), "return=minimal")

	var total float64

	for i := range results {
		total += results[i].RequestCharge

		if minimal && ops[i].kind != batchRead {
			results[i].Body = nil
		}
	}

	setCharge(c, total)

	status := http.StatusOK
	if !committed {
		status = http.StatusMultiStatus
	}

	return h.writeJSON(c, status, results)
}
