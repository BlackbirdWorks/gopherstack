package gcprest

import (
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

type codeInfo struct {
	name string
	http int
}

//nolint:gochecknoglobals // immutable lookup table
var codeTable = map[codes.Code]codeInfo{
	codes.OK:                 {"OK", http.StatusOK},
	codes.Canceled:           {"CANCELLED", 499},
	codes.Unknown:            {"UNKNOWN", http.StatusInternalServerError},
	codes.InvalidArgument:    {"INVALID_ARGUMENT", http.StatusBadRequest},
	codes.DeadlineExceeded:   {"DEADLINE_EXCEEDED", http.StatusGatewayTimeout},
	codes.NotFound:           {"NOT_FOUND", http.StatusNotFound},
	codes.AlreadyExists:      {"ALREADY_EXISTS", http.StatusConflict},
	codes.PermissionDenied:   {"PERMISSION_DENIED", http.StatusForbidden},
	codes.ResourceExhausted:  {"RESOURCE_EXHAUSTED", http.StatusTooManyRequests},
	codes.FailedPrecondition: {"FAILED_PRECONDITION", http.StatusBadRequest},
	codes.Aborted:            {"ABORTED", http.StatusConflict},
	codes.OutOfRange:         {"OUT_OF_RANGE", http.StatusBadRequest},
	codes.Unimplemented:      {"UNIMPLEMENTED", http.StatusNotImplemented},
	codes.Internal:           {"INTERNAL", http.StatusInternalServerError},
	codes.Unavailable:        {"UNAVAILABLE", http.StatusServiceUnavailable},
	codes.DataLoss:           {"DATA_LOSS", http.StatusInternalServerError},
	codes.Unauthenticated:    {"UNAUTHENTICATED", http.StatusUnauthorized},
}

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Message string            `json:"message"`
	Status  string            `json:"status"`
	Details []json.RawMessage `json:"details"`
	Code    int               `json:"code"`
}

// HTTPStatus maps a gRPC code to the HTTP status Google APIs use for it.
func HTTPStatus(c codes.Code) int {
	if info, ok := codeTable[c]; ok {
		return info.http
	}

	return http.StatusInternalServerError
}

func writeError(w http.ResponseWriter, err error) {
	st, _ := status.FromError(err)
	info, ok := codeTable[st.Code()]

	if !ok {
		info = codeInfo{"UNKNOWN", http.StatusInternalServerError}
	}

	details := make([]json.RawMessage, 0, len(st.Proto().GetDetails()))

	for _, d := range st.Proto().GetDetails() {
		if b, mErr := protojson.Marshal(d); mErr == nil {
			details = append(details, b)
		}
	}

	body := errorBody{Error: errorDetail{Code: info.http, Message: st.Message(), Status: info.name, Details: details}}

	writeJSON(w, info.http, body)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	writeRaw(w, code, b)
}

func writeRaw(w http.ResponseWriter, code int, b []byte) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(code)
	_, _ = w.Write(b) //nolint:gosec // b is JSON from encoding/json or protojson, not HTML
}

// WriteError writes err as the Google JSON error envelope, with its status details.
func WriteError(w http.ResponseWriter, err error) { writeError(w, err) }
