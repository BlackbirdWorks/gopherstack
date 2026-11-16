package neptune

import (
	"compress/gzip"
	"encoding/json/v2"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/neptune/streamlog"
)

const (
	streamPGPath          = "/propertygraph/stream"
	streamPGShortPath     = "/pg/stream"
	streamGremlinPath     = "/gremlin/stream"
	streamSparqlPath      = "/sparql/stream"
	actionStreamRecords   = "neptune-db:GetStreamRecords"
	formatPGJSON          = "PG_JSON"
	formatNQuads          = "NQUADS"
	streamDefaultLimit    = 10
	streamMaxLimit        = 100000
	streamMaxResponseSize = 10 << 20
	codeStreamsDisabled   = "UnsupportedOperationException"
	codeStreamNotFound    = "StreamRecordsNotFoundException"
	codeStreamExpired     = "ExpiredStreamException"
)

type streamEventID struct {
	CommitNum int64 `json:"commitNum,string"`
	OpNum     int64 `json:"opNum,string"`
}

type streamRecordOut struct {
	Op          string         `json:"op"`
	Data        streamlog.Data `json:"data"`
	EventID     streamEventID  `json:"eventId"`
	TimestampMs int64          `json:"commitTimestamp"`
	IsLastOp    bool           `json:"isLastOp,omitempty"`
}

type streamResponse struct {
	Format       string            `json:"format"`
	Records      []streamRecordOut `json:"records"`
	LastEventID  streamEventID     `json:"lastEventId"`
	LastTrxMs    int64             `json:"lastTrxTimestamp"`
	TotalRecords int               `json:"totalRecords"`
}

func (f *cypherFront) serveStreams(w http.ResponseWriter, r *http.Request, path string) bool {
	switch path {
	case streamPGPath, streamPGShortPath, streamGremlinPath:
		f.serveStream(w, r, langGremlin, f.graph.Stream(), formatPGJSON)
	case streamSparqlPath:
		f.serveStream(w, r, langSparql, f.rdf.store.Stream(), formatNQuads)
	default:
		return false
	}

	return true
}

func (f *cypherFront) serveStream(
	w http.ResponseWriter, r *http.Request, lang string, log *streamlog.Log, format string,
) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Unsupported method for this path")

		return
	}

	if !authorizeActions(w, r, lang, actionStreamRecords) {
		return
	}

	if !log.Enabled() {
		writeCypherError(w, http.StatusBadRequest, codeStreamsDisabled, "Streams is not enabled for this cluster.")

		return
	}

	q, perr := parseStreamQuery(r)
	if perr != nil {
		writeCypherError(w, perr.status, perr.code, perr.msg)

		return
	}

	page, err := log.Read(q)
	if err != nil {
		writeStreamError(w, err)

		return
	}

	writeStreamResponse(w, r, buildStreamResponse(format, page))
}

func badStream(code, msg string) *streamParamError {
	return &streamParamError{code: code, msg: msg, status: http.StatusBadRequest}
}

type streamParamError struct {
	code, msg string
	status    int
}

func parseStreamQuery(r *http.Request) (streamlog.Query, *streamParamError) {
	v := r.URL.Query()
	q := streamlog.Query{Iterator: streamlog.AtSequenceNumber, Limit: streamDefaultLimit, OpNum: 1}

	if it := v.Get("iteratorType"); it != "" {
		q.Iterator = it
	}

	switch q.Iterator {
	case streamlog.AtSequenceNumber, streamlog.AfterSequenceNumber, streamlog.TrimHorizon, streamlog.Latest:
	default:
		return q, badStream(codeInvalidParam,
			"Invalid iteratorType '"+q.Iterator+"'; must be one of AT_SEQUENCE_NUMBER, "+
				"AFTER_SEQUENCE_NUMBER, TRIM_HORIZON, LATEST")
	}

	if s := v.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > streamMaxLimit {
			return q, badStream(codeInvalidParam, "limit must be between 1 and 100000")
		}

		q.Limit = n
	}

	if perr := parseSequence(v, &q); perr != nil {
		return q, perr
	}

	return q, nil
}

func parseSequence(v url.Values, q *streamlog.Query) *streamParamError {
	if s := v.Get("opNum"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n < 1 {
			return badStream(codeInvalidParam, "opNum must be a positive integer")
		}

		q.OpNum = n
	}

	if q.Iterator != streamlog.AtSequenceNumber && q.Iterator != streamlog.AfterSequenceNumber {
		return nil
	}

	s := v.Get("commitNum")
	if s == "" {
		return badStream(codeMissingParam, "commitNum is required when iteratorType is "+q.Iterator)
	}

	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 1 {
		return badStream(codeInvalidParam, "commitNum must be a positive integer")
	}

	q.CommitNum = n

	return nil
}

func writeStreamError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, streamlog.ErrNotFound):
		writeCypherError(w, http.StatusNotFound, codeStreamNotFound, err.Error())
	case errors.Is(err, streamlog.ErrExpired):
		writeCypherError(w, http.StatusBadRequest, codeStreamExpired, err.Error())
	default:
		writeCypherError(w, http.StatusBadRequest, codeInvalidParam, err.Error())
	}
}

func buildStreamResponse(format string, page streamlog.Page) streamResponse {
	resp := streamResponse{
		Format: format, Records: []streamRecordOut{},
		LastEventID: streamEventID{CommitNum: page.LastCommit, OpNum: page.LastOp}, LastTrxMs: page.LastTimeMs,
	}

	size := 0

	for i := range page.Records {
		rec := page.Records[i]
		out := streamRecordOut{
			Op: rec.Op, Data: rec.Data, TimestampMs: rec.TimestampMs, IsLastOp: rec.IsLastOp,
			EventID: streamEventID{CommitNum: rec.CommitNum, OpNum: rec.OpNum},
		}

		raw, err := json.Marshal(out, jsonx.V1())
		if err == nil {
			size += len(raw)
		}

		resp.Records = append(resp.Records, out)
		resp.LastEventID, resp.LastTrxMs = out.EventID, rec.TimestampMs

		if size >= streamMaxResponseSize {
			break
		}
	}

	resp.TotalRecords = len(resp.Records)

	return resp
}

func writeStreamResponse(w http.ResponseWriter, r *http.Request, resp streamResponse) {
	body, err := json.Marshal(resp, jsonx.V1())
	if err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	w.Header().Set("Content-Type", "application/json;charset=UTF-8")

	if strings.Contains(strings.ToLower(r.Header.Get("Accept-Encoding")), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.WriteHeader(http.StatusOK)

		zw := gzip.NewWriter(w)
		_, _ = zw.Write(body)
		_ = zw.Close()

		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
