package odatatable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

const (
	maxBatchOps       = 100
	maxBatchBodyBytes = 4 << 20
	mimeMultipartMix  = "multipart/mixed"
	crlf              = "\r\n"
	batchRequestParts = 3
	batchBatchResName = "$batch"
	batchTablesName   = "Tables"
	// HeaderContentID is the per-operation Content-ID header echoed in batch responses.
	HeaderContentID = "Content-ID"
	headerMethodOvr = "X-Http-Method"
	methodMerge     = "MERGE"
)

// Batch request parse errors.
var (
	ErrBatchEmpty      = errors.New("the batch request contains no operations")
	ErrBatchTooMany    = errors.New("the batch request contains more than 100 operations")
	ErrBatchNotChange  = errors.New("only a single changeset is supported")
	ErrBatchOpMalform  = errors.New("a batch operation is malformed")
	ErrBatchOpMissing  = errors.New("an If-Match header is required for delete")
	ErrBatchDifferent  = errors.New("all commands in a batch must operate on same entity group")
	ErrBatchDuplicates = errors.New("the batch request contains multiple changes with same row key")
)

// WireBatchOp is one parsed operation of a $batch changeset.
type WireBatchOp struct {
	Headers http.Header
	Table   string
	Op      BatchOp
}

// BatchFailure is the HTTP status, code and message of a failing batch operation.
type BatchFailure struct {
	Code    string
	Message string
	Status  int
}

// NewBatchFailure builds a BatchFailure.
func NewBatchFailure(status int, code, message string) BatchFailure {
	return BatchFailure{Status: status, Code: code, Message: message}
}

// BatchFailureFor maps a backend sentinel error to its batch failure.
func BatchFailureFor(err error) BatchFailure {
	switch {
	case errors.Is(err, ErrTableNotFound):
		return NewBatchFailure(http.StatusNotFound, "TableNotFound", "The table specified does not exist.")
	case errors.Is(err, ErrEntityNotFound):
		return NewBatchFailure(http.StatusNotFound, "ResourceNotFound", "The specified resource does not exist.")
	case errors.Is(err, ErrEntityAlreadyExists):
		return NewBatchFailure(http.StatusConflict, "EntityAlreadyExists", "The specified entity already exists.")
	case errors.Is(err, ErrETagMismatch):
		return NewBatchFailure(http.StatusPreconditionFailed, "UpdateConditionNotSatisfied",
			"The update condition specified in the request was not satisfied.")
	default:
		return NewBatchFailure(http.StatusInternalServerError, "InternalError", err.Error())
	}
}

// ReadBatch parses a $batch request into its changeset operations. resourceOf maps an embedded request's URL path
// to the resource segment (for example, stripping a leading account name).
func ReadBatch(r *http.Request, resourceOf func(path string) string) ([]WireBatchOp, error) {
	mediaType, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != mimeMultipartMix || params["boundary"] == "" {
		return nil, ErrBatchOpMalform
	}

	body, readErr := io.ReadAll(io.LimitReader(r.Body, maxBatchBodyBytes))
	if readErr != nil {
		return nil, ErrBatchOpMalform
	}

	outer := multipart.NewReader(bytes.NewReader(body), params["boundary"])

	part, err := outer.NextPart()
	if err != nil {
		return nil, ErrBatchEmpty
	}

	innerType, innerParams, err := mime.ParseMediaType(part.Header.Get("Content-Type"))
	if err != nil || innerType != mimeMultipartMix || innerParams["boundary"] == "" {
		return nil, ErrBatchNotChange
	}

	raw, readAllErr := io.ReadAll(part)
	if readAllErr != nil && !errors.Is(readAllErr, io.ErrUnexpectedEOF) {
		return nil, ErrBatchOpMalform
	}

	if _, more := outer.NextPart(); more == nil {
		return nil, ErrBatchNotChange
	}

	return readChangeset(raw, innerParams["boundary"], resourceOf)
}

func readChangeset(raw []byte, boundary string, resourceOf func(string) string) ([]WireBatchOp, error) {
	mr := multipart.NewReader(bytes.NewReader(raw), boundary)

	var ops []WireBatchOp

	for {
		p, nextErr := mr.NextPart()
		if errors.Is(nextErr, io.EOF) {
			break
		}

		if nextErr != nil {
			return nil, ErrBatchOpMalform
		}

		pb, readErr := io.ReadAll(p)
		if readErr != nil {
			return nil, ErrBatchOpMalform
		}

		op, parseErr := parseWireBatchOp(pb, resourceOf)
		if parseErr != nil {
			return nil, parseErr
		}

		if id := p.Header.Get(HeaderContentID); id != "" {
			op.Headers.Set(HeaderContentID, id)
		}

		ops = append(ops, op)
	}

	switch {
	case len(ops) == 0:
		return nil, ErrBatchEmpty
	case len(ops) > maxBatchOps:
		return nil, ErrBatchTooMany
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

// parseWireBatchOp decodes one embedded HTTP request ("POST <url> HTTP/1.1", headers, body).
func parseWireBatchOp(msg []byte, resourceOf func(string) string) (WireBatchOp, error) {
	lines, body := splitHTTPMessage(msg)

	fields := strings.Fields(lines[0])
	if len(fields) < batchRequestParts {
		return WireBatchOp{}, ErrBatchOpMalform
	}

	headers := http.Header{}

	for _, ln := range lines[1:] {
		if k, v, ok := strings.Cut(ln, ":"); ok {
			headers.Set(strings.TrimSpace(k), strings.TrimSpace(v))
		}
	}

	u, err := url.Parse(fields[1])
	if err != nil {
		return WireBatchOp{}, ErrBatchOpMalform
	}

	method := fields[0]
	if method == http.MethodPost && strings.EqualFold(headers.Get(headerMethodOvr), methodMerge) {
		method = methodMerge
	}

	table, inner, isItem, isCollection := classifyBatchResource(resourceOf(u.Path))

	op, err := buildBatchOperation(method, isItem, isCollection, inner, headers, body)
	if err != nil {
		return WireBatchOp{}, err
	}

	return WireBatchOp{Table: table, Op: op, Headers: headers}, nil
}

// classifyBatchResource splits a resource segment into table name, key predicate, and whether it addresses a single
// entity or an entity collection.
func classifyBatchResource(resource string) (string, string, bool, bool) {
	if resource == batchBatchResName {
		return "", "", false, false
	}

	idx := strings.IndexByte(resource, '(')
	if idx == -1 {
		return resource, "", false, resource != batchTablesName
	}

	if !strings.HasSuffix(resource, ")") {
		return "", "", false, false
	}

	name, inner := resource[:idx], resource[idx+1:len(resource)-1]

	switch {
	case name == batchTablesName:
		return name, inner, false, false
	case inner == "":
		return name, "", false, true
	default:
		return name, inner, true, false
	}
}

func buildBatchOperation(
	method string, isItem, isCollection bool, inner string, headers http.Header, body []byte,
) (BatchOp, error) {
	if method == http.MethodPost && isCollection {
		pk, rk, hasPK, hasRK, props, err := DecodeEntityBody(body)
		if err != nil || !hasPK || !hasRK {
			return BatchOp{}, ErrBatchOpMalform
		}

		return BatchOp{Kind: BatchInsert, PartitionKey: pk, RowKey: rk, Props: props}, nil
	}

	pk, rk, ok := ParseEntityKeyPredicate(inner)
	if !isItem || !ok {
		return BatchOp{}, ErrBatchOpMalform
	}

	op := BatchOp{PartitionKey: pk, RowKey: rk, IfMatch: headers.Get("If-Match")}

	switch method {
	case http.MethodDelete:
		if op.IfMatch == "" {
			return BatchOp{}, ErrBatchOpMissing
		}

		op.Kind = BatchDelete

		return op, nil
	case http.MethodPut:
		op.Kind = BatchReplace
	case http.MethodPatch, methodMerge:
		op.Kind = BatchMerge
	default:
		return BatchOp{}, ErrBatchOpMalform
	}

	_, _, _, _, props, err := DecodeEntityBody(body)
	if err != nil {
		return BatchOp{}, ErrBatchOpMalform
	}

	op.Props = props

	return op, nil
}

// ValidateBatch enforces one table, one partition and one operation per entity; it returns the failing index.
func ValidateBatch(ops []WireBatchOp) (int, *BatchFailure) {
	type rowID struct{ pk, rk string }

	seen := make(map[rowID]struct{}, len(ops))

	for i, o := range ops {
		if o.Table != ops[0].Table || o.Op.PartitionKey != ops[0].Op.PartitionKey {
			f := NewBatchFailure(http.StatusBadRequest, "CommandsInBatchActOnDifferentPartitions",
				ErrBatchDifferent.Error()+".")

			return i, &f
		}

		id := rowID{o.Op.PartitionKey, o.Op.RowKey}
		if _, dup := seen[id]; dup {
			f := NewBatchFailure(http.StatusBadRequest, "InvalidDuplicateRow",
				ErrBatchDuplicates.Error()+". An entity can appear only once in a batch request.")

			return i, &f
		}

		seen[id] = struct{}{}
	}

	return 0, nil
}

// BatchResponsePart is one operation response inside a changeset response.
type BatchResponsePart struct {
	ContentID string
	Headers   [][2]string
	Body      []byte
	Status    int
}

func (p BatchResponsePart) render(changeset string) string {
	var sb strings.Builder

	sb.WriteString("--" + changeset + crlf + "Content-Type: application/http" + crlf +
		"Content-Transfer-Encoding: binary" + crlf)

	if p.ContentID != "" {
		sb.WriteString("Content-ID: " + p.ContentID + crlf)
	}

	sb.WriteString(crlf + "HTTP/1.1 " + strconv.Itoa(p.Status) + " " + http.StatusText(p.Status) + crlf +
		"X-Content-Type-Options: nosniff" + crlf + "Cache-Control: no-cache" + crlf + "DataServiceVersion: 1.0;" + crlf)

	for _, kv := range p.Headers {
		sb.WriteString(kv[0] + ": " + kv[1] + crlf)
	}

	sb.WriteString(crlf)

	sb.Write(p.Body)
	sb.WriteString(crlf)

	return sb.String()
}

// RenderBatchResponse builds the multipart/mixed 202 body and its Content-Type. newID supplies boundary suffixes.
func RenderBatchResponse(parts []BatchResponsePart, newID func() string) (string, []byte) {
	batch, changeset := "batchresponse_"+newID(), "changesetresponse_"+newID()

	var sb strings.Builder

	sb.WriteString("--" + batch + crlf + "Content-Type: multipart/mixed; boundary=" + changeset + crlf + crlf)

	for _, p := range parts {
		sb.WriteString(p.render(changeset))
	}

	sb.WriteString("--" + changeset + "--" + crlf + "--" + batch + "--" + crlf)

	return "multipart/mixed; boundary=" + batch, []byte(sb.String())
}

type failureEnvelope struct {
	Error struct {
		Message struct {
			Lang  string `json:"lang"`
			Value string `json:"value"`
		} `json:"message"`
		Code string `json:"code"`
	} `json:"odata.error"`
}

// FailureBody builds the OData error JSON for a failing batch operation at index, in Azure's
// "<index>:<message>\nRequestId:...\nTime:..." form.
func FailureBody(index int, f BatchFailure, requestID, timestamp string) ([]byte, error) {
	var env failureEnvelope

	env.Error.Code = f.Code
	env.Error.Message.Lang = "en-US"
	env.Error.Message.Value = fmt.Sprintf("%d:%s\nRequestId:%s\nTime:%s", index, f.Message, requestID, timestamp)

	return json.Marshal(env)
}
