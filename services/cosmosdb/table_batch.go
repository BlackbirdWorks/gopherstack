package cosmosdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
)

const (
	maxBatchOps       = 100
	maxBatchBodyBytes = 4 << 20
	mimeMultipartMix  = "multipart/mixed"
	crlf              = "\r\n"
	headerContentID   = "Content-ID"
)

var (
	errBatchEmpty      = errors.New("the batch request contains no operations")
	errBatchTooMany    = errors.New("the batch request contains more than 100 operations")
	errBatchNotChange  = errors.New("only a single changeset is supported")
	errBatchOpMalform  = errors.New("a batch operation is malformed")
	errBatchOpMissing  = errors.New("an If-Match header is required for delete")
	errBatchDifferent  = errors.New("all commands in a batch must operate on same entity group")
	errBatchDuplicates = errors.New("the batch request contains multiple changes with same row key")
)

// tableBatchOp is one parsed operation of a $batch changeset.
type tableBatchOp struct {
	headers http.Header
	table   string
	op      odatatable.BatchOp
}

// tableBatchFailure is the HTTP status, code and message of a failing batch operation.
type tableBatchFailure struct {
	code    string
	message string
	status  int
}

// handleTableBatch implements POST /$batch: one multipart/mixed changeset of insert, update, merge, delete and
// upsert operations on a single partition, applied atomically.
func (h *Handler) handleTableBatch(c *echo.Context) error {
	r := c.Request()
	if r.Method != http.MethodPost {
		return h.writeTableError(c, http.StatusMethodNotAllowed, "UnsupportedHttpVerb",
			"The resource doesn't support the specified HTTP verb.")
	}

	ops, parseErr := readTableBatch(r)
	if parseErr != nil {
		return h.writeTableError(c, http.StatusBadRequest, "InvalidInput", "99:"+parseErr.Error())
	}

	if idx, failure := validateTableBatch(ops); failure != nil {
		return h.writeBatchFailure(c, idx, *failure)
	}

	batchOps := make([]odatatable.BatchOp, len(ops))
	for i, o := range ops {
		batchOps[i] = o.op
	}

	infos, err := h.TableBackend.ExecuteBatch(ops[0].table, batchOps)
	if err != nil {
		if be, ok := errors.AsType[*odatatable.BatchError](err); ok {
			return h.writeBatchFailure(c, be.Index, tableBatchFailureFor(be.Err))
		}

		return h.writeBatchFailure(c, 0, batchFail(http.StatusInternalServerError, "InternalError", err.Error()))
	}

	return h.writeBatchSuccess(c, ops, infos)
}

func tableBatchFailureFor(err error) tableBatchFailure {
	switch {
	case errors.Is(err, odatatable.ErrTableNotFound):
		return batchFail(http.StatusNotFound, "TableNotFound", "The table specified does not exist.")
	case errors.Is(err, odatatable.ErrEntityNotFound):
		return batchFail(http.StatusNotFound, "ResourceNotFound", "The specified resource does not exist.")
	case errors.Is(err, odatatable.ErrEntityAlreadyExists):
		return batchFail(http.StatusConflict, "EntityAlreadyExists", "The specified entity already exists.")
	case errors.Is(err, odatatable.ErrETagMismatch):
		return batchFail(http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return batchFail(http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// readTableBatch parses the request into its changeset operations.
func readTableBatch(r *http.Request) ([]tableBatchOp, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != mimeMultipartMix || params["boundary"] == "" {
		return nil, errBatchOpMalform
	}

	body, readErr := io.ReadAll(io.LimitReader(r.Body, maxBatchBodyBytes))
	if readErr != nil {
		return nil, errBatchOpMalform
	}

	outer := multipart.NewReader(bytes.NewReader(body), params["boundary"])

	part, err := outer.NextPart()
	if err != nil {
		return nil, errBatchEmpty
	}

	innerType, innerParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
	if err != nil || innerType != mimeMultipartMix || innerParams["boundary"] == "" {
		return nil, errBatchNotChange
	}

	raw, readAllErr := io.ReadAll(part)
	if readAllErr != nil && !errors.Is(readAllErr, io.ErrUnexpectedEOF) {
		return nil, errBatchOpMalform
	}

	if _, more := outer.NextPart(); more == nil {
		return nil, errBatchNotChange
	}

	return readChangeset(raw, innerParams["boundary"])
}

// readChangeset reads the changeset parts of raw.
func readChangeset(raw []byte, boundary string) ([]tableBatchOp, error) {
	mr := multipart.NewReader(bytes.NewReader(raw), boundary)

	var ops []tableBatchOp

	for {
		p, nextErr := mr.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}

		if nextErr != nil {
			return nil, errBatchOpMalform
		}

		pb, readErr := io.ReadAll(p)
		if readErr != nil {
			return nil, errBatchOpMalform
		}

		op, parseErr := parseTableBatchOp(pb)
		if parseErr != nil {
			return nil, parseErr
		}

		if id := p.Header.Get(headerContentID); id != "" {
			op.headers.Set(headerContentID, id)
		}

		ops = append(ops, op)
	}

	switch {
	case len(ops) == 0:
		return nil, errBatchEmpty
	case len(ops) > maxBatchOps:
		return nil, errBatchTooMany
	}

	return ops, nil
}

// splitHTTPMessage separates an embedded HTTP message into its head lines and body.
func splitHTTPMessage(msg []byte) ([]string, []byte) {
	text := string(msg)
	sep := crlf + crlf

	idx := strings.Index(text, sep)
	if idx < 0 {
		sep = "\n\n"
		idx = strings.Index(text, sep)
	}

	head, body := text, ""
	if idx >= 0 {
		head, body = text[:idx], text[idx+len(sep):]
	}

	return strings.Split(strings.ReplaceAll(strings.TrimLeft(head, crlf), crlf, "\n"), "\n"), []byte(body)
}

// parseTableBatchOp decodes one embedded HTTP request ("POST <url> HTTP/1.1", headers, body).
func parseTableBatchOp(msg []byte) (tableBatchOp, error) {
	lines, body := splitHTTPMessage(msg)

	fields := strings.Fields(lines[0])
	if len(fields) < arityTriple {
		return tableBatchOp{}, errBatchOpMalform
	}

	headers := http.Header{}

	for _, ln := range lines[1:] {
		if k, v, ok := strings.Cut(ln, ":"); ok {
			headers.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}

	u, err := url.Parse(fields[1])
	if err != nil {
		return tableBatchOp{}, errBatchOpMalform
	}

	method := fields[0]
	if method == http.MethodPost && strings.EqualFold(headers.Get(xHTTPMethodOverrideHeader), mergeMethod) {
		method = mergeMethod
	}

	kind, table, inner := parseTableResource(strings.Trim(u.Path, "/"))

	op, err := buildBatchOperation(method, kind, inner, headers, body)
	if err != nil {
		return tableBatchOp{}, err
	}

	return tableBatchOp{table: table, op: op, headers: headers}, nil
}

func buildBatchOperation(
	method string, kind tableResourceKind, inner string, headers http.Header, body []byte,
) (odatatable.BatchOp, error) {
	if method == http.MethodPost && kind == tableResourceEntityCollection {
		pk, rk, hasPK, hasRK, props, err := odatatable.DecodeEntityBody(body)
		if err != nil || !hasPK || !hasRK {
			return odatatable.BatchOp{}, errBatchOpMalform
		}

		return odatatable.BatchOp{Kind: odatatable.BatchInsert, PartitionKey: pk, RowKey: rk, Props: props}, nil
	}

	pk, rk, ok := odatatable.ParseEntityKeyPredicate(inner)
	if kind != tableResourceEntityItem || !ok {
		return odatatable.BatchOp{}, errBatchOpMalform
	}

	op := odatatable.BatchOp{PartitionKey: pk, RowKey: rk, IfMatch: headers.Get("If-Match")}

	switch method {
	case http.MethodDelete:
		if op.IfMatch == "" {
			return odatatable.BatchOp{}, errBatchOpMissing
		}

		op.Kind = odatatable.BatchDelete

		return op, nil
	case http.MethodPut:
		op.Kind = odatatable.BatchReplace
	case http.MethodPatch, mergeMethod:
		op.Kind = odatatable.BatchMerge
	default:
		return odatatable.BatchOp{}, errBatchOpMalform
	}

	_, _, _, _, props, err := odatatable.DecodeEntityBody(body)
	if err != nil {
		return odatatable.BatchOp{}, errBatchOpMalform
	}

	op.Props = props

	return op, nil
}

// validateTableBatch enforces one table, one partition and one operation per entity; it returns the failing index.
func validateTableBatch(ops []tableBatchOp) (int, *tableBatchFailure) {
	type rowID struct{ pk, rk string }

	seen := make(map[rowID]struct{}, len(ops))

	for i, o := range ops {
		if o.table != ops[0].table || o.op.PartitionKey != ops[0].op.PartitionKey {
			f := batchFail(
				http.StatusBadRequest,
				"CommandsInBatchActOnDifferentPartitions",
				errBatchDifferent.Error()+".",
			)

			return i, &f
		}

		id := rowID{o.op.PartitionKey, o.op.RowKey}
		if _, dup := seen[id]; dup {
			f := batchFail(http.StatusBadRequest, "InvalidDuplicateRow",
				errBatchDuplicates.Error()+". An entity can appear only once in a batch request.")

			return i, &f
		}

		seen[id] = struct{}{}
	}

	return 0, nil
}

func batchFail(status int, code, message string) tableBatchFailure {
	return tableBatchFailure{status: status, code: code, message: message}
}

func newBatchBoundary(prefix string) string {
	return prefix + newDocumentID()
}

// writeBatchFailure answers 202 with a changeset holding only the failing operation's response.
func (h *Handler) writeBatchFailure(c *echo.Context, index int, f tableBatchFailure) error {
	reqID := newDocumentID()
	value := fmt.Sprintf(
		"%d:%s\nRequestId:%s\nTime:%s",
		index,
		f.message,
		reqID,
		time.Now().UTC().Format(time.RFC3339Nano),
	)

	body, err := jsonMarshal(tableODataErrorEnvelope{Error: tableODataErrorDetail{
		Code: f.code, Message: tableODataErrorMessage{Lang: tableErrorLang, Value: value},
	}})
	if err != nil {
		return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
	}

	part := batchResponsePart{
		status:  f.status,
		headers: [][2]string{{"Content-Type", "application/json;odata=minimalmetadata;streaming=true;charset=utf-8"}},
		body:    body,
	}

	return h.writeBatchResponse(c, []batchResponsePart{part})
}

func (h *Handler) writeBatchSuccess(c *echo.Context, ops []tableBatchOp, infos []odatatable.EntityInfo) error {
	r := c.Request()
	parts := make([]batchResponsePart, len(ops))

	for i, o := range ops {
		part := batchResponsePart{status: http.StatusNoContent, contentID: o.headers.Get(headerContentID)}

		if o.op.Kind != odatatable.BatchDelete {
			part.headers = append(part.headers, [2]string{"ETag", infos[i].ETag})
		}

		if o.op.Kind == odatatable.BatchInsert && o.headers.Get("Prefer") != preferReturnNoContent {
			level := tableODataLevelFromAccept(o.headers.Get("Accept"))

			payload, err := jsonMarshal(h.encodeTableEntity(infos[i], o.table, level, "", r.Host))
			if err != nil {
				return h.writeTableError(c, http.StatusInternalServerError, "InternalError", err.Error())
			}

			part.status, part.body = http.StatusCreated, payload
			part.headers = append(part.headers, [2]string{
				"Content-Type", "application/json;odata=" + level + ";streaming=true;charset=utf-8",
			})
		}

		parts[i] = part
	}

	return h.writeBatchResponse(c, parts)
}

type batchResponsePart struct {
	contentID string
	headers   [][2]string
	body      []byte
	status    int
}

func (p batchResponsePart) render(changeset string) string {
	var sb strings.Builder

	sb.WriteString("--" + changeset + crlf + "Content-Type: application/http" + crlf +
		"Content-Transfer-Encoding: binary" + crlf)

	if p.contentID != "" {
		sb.WriteString("Content-ID: " + p.contentID + crlf)
	}

	sb.WriteString(crlf + "HTTP/1.1 " + strconv.Itoa(p.status) + " " + http.StatusText(p.status) + crlf +
		"X-Content-Type-Options: nosniff" + crlf + "Cache-Control: no-cache" + crlf + "DataServiceVersion: 1.0;" + crlf)

	for _, kv := range p.headers {
		sb.WriteString(kv[0] + ": " + kv[1] + crlf)
	}

	sb.WriteString(crlf)

	sb.Write(p.body)
	sb.WriteString(crlf)

	return sb.String()
}

func (h *Handler) writeBatchResponse(c *echo.Context, parts []batchResponsePart) error {
	batch, changeset := newBatchBoundary("batchresponse_"), newBatchBoundary("changesetresponse_")

	var sb strings.Builder

	sb.WriteString("--" + batch + crlf + "Content-Type: multipart/mixed; boundary=" + changeset + crlf + crlf)

	for _, p := range parts {
		sb.WriteString(p.render(changeset))
	}

	sb.WriteString("--" + changeset + "--" + crlf + "--" + batch + "--" + crlf)

	return c.Blob(http.StatusAccepted, "multipart/mixed; boundary="+batch, []byte(sb.String()))
}
