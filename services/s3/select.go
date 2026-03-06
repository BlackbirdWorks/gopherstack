package s3

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/xml"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// event stream constants for the binary wire format.
const (
	// eventStreamHeaderTypeString is the header value type for string (7).
	eventStreamHeaderTypeString = byte(7)
	// eventStreamPreludeLen is the total prelude size: 4 bytes total-length + 4 bytes headers-length.
	eventStreamPreludeLen = 8
	// eventStreamPreludeCRCLen is the CRC field appended after the prelude.
	eventStreamPreludeCRCLen = 4
	// eventStreamMsgCRCLen is the trailing message CRC length.
	eventStreamMsgCRCLen = 4
	// eventStreamMinMsgLen is the smallest possible message (no headers, no payload).
	eventStreamMinMsgLen = eventStreamPreludeLen + eventStreamPreludeCRCLen + eventStreamMsgCRCLen
	// eventStreamHeaderValueLenBytes is the number of bytes used to encode a string header value length.
	eventStreamHeaderValueLenBytes = 2
)

// selectRequest is the XML body for SelectObjectContent.
type selectRequest struct {
	OutputSerialization selectOutputSerialization `xml:"OutputSerialization"`
	XMLName             xml.Name                  `xml:"SelectObjectContentRequest"`
	InputSerialization  selectInputSerialization  `xml:"InputSerialization"`
	Expression          string                    `xml:"Expression"`
	ExpressionType      string                    `xml:"ExpressionType"`
}

// selectInputSerialization describes how the source object is formatted.
type selectInputSerialization struct {
	CSV             *selectCSVInput  `xml:"CSV"`
	JSON            *selectJSONInput `xml:"JSON"`
	CompressionType string           `xml:"CompressionType"`
}

// selectCSVInput holds CSV input settings.
type selectCSVInput struct {
	FileHeaderInfo             string `xml:"FileHeaderInfo"`
	FieldDelimiter             string `xml:"FieldDelimiter"`
	RecordDelimiter            string `xml:"RecordDelimiter"`
	QuoteCharacter             string `xml:"QuoteCharacter"`
	QuoteEscapeCharacter       string `xml:"QuoteEscapeCharacter"`
	Comments                   string `xml:"Comments"`
	AllowQuotedRecordDelimiter string `xml:"AllowQuotedRecordDelimiter"`
}

// selectJSONInput holds JSON input settings.
type selectJSONInput struct {
	Type string `xml:"Type"` // DOCUMENT or LINES
}

// selectOutputSerialization describes how results are formatted.
type selectOutputSerialization struct {
	CSV  *selectCSVOutput  `xml:"CSV"`
	JSON *selectJSONOutput `xml:"JSON"`
}

// selectCSVOutput holds CSV output settings.
type selectCSVOutput struct {
	FieldDelimiter       string `xml:"FieldDelimiter"`
	RecordDelimiter      string `xml:"RecordDelimiter"`
	QuoteCharacter       string `xml:"QuoteCharacter"`
	QuoteEscapeCharacter string `xml:"QuoteEscapeCharacter"`
	QuoteFields          string `xml:"QuoteFields"`
}

// selectJSONOutput holds JSON output settings.
type selectJSONOutput struct {
	RecordDelimiter string `xml:"RecordDelimiter"`
}

// selectStatsXML is the XML body of a Stats event.
type selectStatsXML struct {
	XMLName        xml.Name `xml:"Stats"`
	BytesScanned   int64    `xml:"BytesScanned"`
	BytesProcessed int64    `xml:"BytesProcessed"`
	BytesReturned  int64    `xml:"BytesReturned"`
}

// selectObjectContent handles the POST /<bucket>/<key>?select&select-type=2 request.
func (h *S3Handler) selectObjectContent(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "SelectObjectContent")
	logger.Load(ctx).DebugContext(ctx, "S3 selectObjectContent", "bucket", bucketName, "key", key)

	req, objectData, bytesScanned, ok := h.readSelectRequest(ctx, w, r, bucketName, key)
	if !ok {
		return
	}

	query, ok := parseSelectQuery(ctx, w, r, req.Expression)
	if !ok {
		return
	}

	resultData, bytesReturned, evalErr := h.evaluateQuery(ctx, query, objectData, req)
	if evalErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "InternalError",
			Message: fmt.Sprintf("error evaluating query: %v", evalErr),
		}, http.StatusInternalServerError)

		return
	}

	h.writeSelectResponse(w, resultData, bytesScanned, bytesReturned)
}

// readSelectRequest parses the incoming HTTP request into a selectRequest and fetches the object.
func (h *S3Handler) readSelectRequest(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) (*selectRequest, []byte, int64, bool) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return nil, nil, 0, false
	}

	var req selectRequest

	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MalformedXML",
			Message: "The XML you provided was not well-formed or did not validate against our published schema.",
		}, http.StatusBadRequest)

		return nil, nil, 0, false
	}

	if !strings.EqualFold(req.ExpressionType, "SQL") {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "InvalidExpressionType",
			Message: "The ExpressionType is invalid for this request.",
		}, http.StatusBadRequest)

		return nil, nil, 0, false
	}

	if req.Expression == "" {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "MissingRequiredParameter",
			Message: "The SQL expression is required.",
		}, http.StatusBadRequest)

		return nil, nil, 0, false
	}

	getOut, getErr := h.Backend.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if getErr != nil {
		WriteError(ctx, w, r, getErr)

		return nil, nil, 0, false
	}

	defer getOut.Body.Close()

	objectData, readErr := io.ReadAll(getOut.Body)
	if readErr != nil {
		WriteError(ctx, w, r, readErr)

		return nil, nil, 0, false
	}

	return &req, objectData, int64(len(objectData)), true
}

// parseSelectQuery parses the SQL expression and writes an error response on failure.
func parseSelectQuery(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	expression string,
) (*sqlQuery, bool) {
	query, err := parseSQL(expression)
	if err != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    "ParseException",
			Message: fmt.Sprintf("SQL expression is invalid: %v", err),
		}, http.StatusBadRequest)

		return nil, false
	}

	return query, true
}

// writeSelectResponse sends the event-stream response with Records, Stats, and End events.
func (h *S3Handler) writeSelectResponse(w http.ResponseWriter, resultData []byte, bytesScanned, bytesReturned int64) {
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.WriteHeader(http.StatusOK)

	if len(resultData) > 0 {
		if err := writeSelectEvent(w, "Records", "application/octet-stream", resultData); err != nil {
			return
		}
	}

	statsPayload, _ := xml.Marshal(selectStatsXML{
		BytesScanned:   bytesScanned,
		BytesProcessed: bytesScanned,
		BytesReturned:  bytesReturned,
	})

	if err := writeSelectEvent(w, "Stats", "text/xml", statsPayload); err != nil {
		return
	}

	_ = writeSelectEvent(w, "End", "", nil)

	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// evaluateQuery processes the object data with the given SQL query.
// Returns the serialized result bytes and the count of returned bytes.
func (h *S3Handler) evaluateQuery(
	_ context.Context,
	query *sqlQuery,
	data []byte,
	req *selectRequest,
) ([]byte, int64, error) {
	switch {
	case req.InputSerialization.CSV != nil:
		return evaluateCSVQuery(query, data, req)

	case req.InputSerialization.JSON != nil:
		return evaluateJSONQuery(query, data, req)

	default:
		// Default to CSV if nothing specified.
		return evaluateCSVQuery(query, data, req)
	}
}

// ---- Event stream encoding ----

// writeSelectEvent encodes and writes a single event-stream message.
// eventType is one of "Records", "Stats", or "End".
// contentType is the payload content type (empty for End event).
// payload is the event payload bytes (nil for End).
func writeSelectEvent(w io.Writer, eventType, contentType string, payload []byte) error {
	msg, err := buildEventStreamMessage(eventType, contentType, payload)
	if err != nil {
		return err
	}

	_, err = w.Write(msg)

	return err
}

// buildEventStreamMessage builds the binary event-stream frame.
//
// Frame layout: [4 total-len][4 headers-len][4 prelude-crc][headers][payload][4 msg-crc].
func buildEventStreamMessage(eventType, contentType string, payload []byte) ([]byte, error) {
	headers := encodeSelectHeaders(eventType, contentType)

	headersLen := uint32(len(headers)) //nolint:gosec // headers are small; no overflow possible
	payloadLen := uint32(len(payload)) //nolint:gosec // payload bounded by S3 object size
	totalLen := eventStreamMinMsgLen + headersLen + payloadLen

	crcHash := crc32.New(crc32.IEEETable)

	var buf bytes.Buffer

	// writeChunk writes data to both the buffer and the running CRC hash.
	writeChunk := func(data []byte) error {
		if _, err := buf.Write(data); err != nil {
			return err
		}

		_, err := crcHash.Write(data)

		return err
	}

	prelude := make([]byte, eventStreamPreludeLen)
	binary.BigEndian.PutUint32(prelude[0:], totalLen)
	binary.BigEndian.PutUint32(prelude[4:], headersLen)

	if err := writeChunk(prelude); err != nil {
		return nil, err
	}

	preludeCRC := make([]byte, eventStreamPreludeCRCLen)
	binary.BigEndian.PutUint32(preludeCRC, crcHash.Sum32())

	if err := writeChunk(preludeCRC); err != nil {
		return nil, err
	}

	if len(headers) > 0 {
		if err := writeChunk(headers); err != nil {
			return nil, err
		}
	}

	if len(payload) > 0 {
		if err := writeChunk(payload); err != nil {
			return nil, err
		}
	}

	msgCRC := make([]byte, eventStreamMsgCRCLen)
	binary.BigEndian.PutUint32(msgCRC, crcHash.Sum32())
	buf.Write(msgCRC) // bytes.Buffer.Write never returns an error

	return buf.Bytes(), nil
}

// encodeSelectHeaders encodes event-stream headers for SelectObjectContent events.
// Headers: :message-type, :event-type, and optionally :content-type.
func encodeSelectHeaders(eventType, contentType string) []byte {
	var buf bytes.Buffer

	writeEventStringHeader(&buf, ":message-type", "event")
	writeEventStringHeader(&buf, ":event-type", eventType)

	if contentType != "" {
		writeEventStringHeader(&buf, ":content-type", contentType)
	}

	return buf.Bytes()
}

// writeEventStringHeader writes a single event-stream string-typed header.
// Format: [1 name-len][name][1 type=7][2 value-len][value].
func writeEventStringHeader(w *bytes.Buffer, name, value string) {
	nameBuf := []byte(name)
	valBuf := []byte(value)

	w.WriteByte(byte(len(nameBuf))) //nolint:gosec // header names are always < 256 bytes
	w.Write(nameBuf)
	w.WriteByte(eventStreamHeaderTypeString)

	vlen := make([]byte, eventStreamHeaderValueLenBytes)
	binary.BigEndian.PutUint16(vlen, uint16(len(valBuf))) //nolint:gosec // header values are always < 65536 bytes
	w.Write(vlen)
	w.Write(valBuf)
}
