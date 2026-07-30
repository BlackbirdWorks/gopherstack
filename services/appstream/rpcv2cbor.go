package appstream

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errExpectedCBORMap is returned when a decoded rpc-v2-cbor request body's
// top-level value isn't a map (an operation's input is always a struct,
// which CBOR encodes as a map).
var errExpectedCBORMap = errors.New("expected CBOR map")

// cborServicePath is the URL path prefix used by the Smithy RPCv2 CBOR
// protocol for AppStream, confirmed against the installed SDK at
// aws-sdk-go-v2/service/appstream@v1.64.0/serializers.go: every operation
// serializer sets req.URL.Path to this prefix plus the operation name and
// req.Header["smithy-protocol"] = "rpc-v2-cbor", with a CBOR-encoded body
// (as opposed to the pre-v1.64.0 awsjson1.1 protocol, which dispatched via
// an "X-Amz-Target: PhotonAdminProxyService.<Operation>" header and a JSON
// body -- see appstreamTargetPrefix in handler.go, which is still served
// for backward compatibility).
const cborServicePath = "/service/PhotonAdminProxyService/operation/"

// maxCBORBodyBytes caps AppStream rpc-v2-cbor request bodies. AppStream
// request payloads (fleet/stack/image descriptors) are well below 1 MiB;
// cap conservatively to prevent unbounded io.ReadAll memory use.
const maxCBORBodyBytes = 1 << 20

// timestampKeys is the set of AppStream response field names the real SDK
// models as Timestamp shapes (Go *time.Time in
// aws-sdk-go-v2/service/appstream/types@v1.64.0/types.go, verified by
// grepping that file for "*time.Time" and cross-checking which of those
// fields gopherstack's handlers actually populate). The rpc-v2-cbor
// protocol requires these to be encoded as a CBOR Tag(1, epoch-seconds);
// smithycbor.AsTime on the client rejects a bare number. If a future
// operation starts emitting a new timestamp-shaped field, its key must be
// added here or the SDK will fail to parse that field.
var timestampKeys = map[string]bool{ //nolint:gochecknoglobals // static lookup table, not mutated
	"CreatedTime":      true, //nolint:goconst // existing issue: see handler.go/handler_*.go's own "CreatedTime" literals.
	"CreatedDate":      true,
	"LastModifiedTime": true,
	"StartTime":        true,
	"Expires":          true,
}

// isCBORRequest returns true when the request uses the rpc-v2-cbor (Smithy
// RPCv2) protocol. Delegates to pkgs/service, shared with CloudWatch (the
// only other rpc-v2-cbor service).
func isCBORRequest(r *http.Request) bool {
	return service.IsRPCv2CBORRequest(r, cborServicePath)
}

// extractCBOROperation returns the operation name from an rpc-v2-cbor
// request path.
func extractCBOROperation(path string) string {
	return service.ExtractRPCv2CBOROperation(path, cborServicePath)
}

// handleCBOR dispatches an rpc-v2-cbor request.
//
// AppStream's operation table (h.ops, in handler.go) already operates on
// decoded/generic JSON bodies -- every op handler unmarshals into a small
// per-op struct and returns a plain map[string]any that gets json.Marshal'd
// -- unlike CloudWatch, whose pre-CBOR protocol was query/XML and therefore
// needed a wholesale hand-written CBOR implementation (see
// services/cloudwatch/rpcv2cbor.go). Because both of AppStream's wire
// protocols are structurally the same generic-map shape (they differ only
// in whether that map is text-encoded as JSON or binary-encoded as CBOR),
// this handler bridges rpc-v2-cbor through the existing JSON operation
// table instead of duplicating all ~90 operations as CBOR-native handlers:
// decode CBOR -> generic Go value -> JSON bytes -> h.dispatch (unchanged)
// -> JSON bytes -> generic Go value -> CBOR. It still uses the same CBOR
// library, request/response headers, and error shaping as CloudWatch's
// implementation -- see dual_protocol_decision and reuse_opportunity in the
// task receipt for the tradeoffs of this approach.
func (h *Handler) handleCBOR(c *echo.Context) error {
	r := c.Request()
	op := extractCBOROperation(r.URL.Path)

	body, err := io.ReadAll(http.MaxBytesReader(c.Response(), r.Body, maxCBORBodyBytes))
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "SerializationException", "cannot read body")
	}

	jsonBody, err := cborBodyToJSON(body)
	if err != nil {
		return h.cborError(c, http.StatusBadRequest, "SerializationException", "invalid CBOR body")
	}

	respBody, err := h.dispatch(r.Context(), op, jsonBody)
	if err != nil {
		return h.handleCBORError(c, err)
	}

	cv, err := jsonToCBOR(respBody)
	if err != nil {
		return h.cborError(c, http.StatusInternalServerError, "InternalServiceError", "cannot encode response")
	}

	return writeCBOR(c, cv)
}

// handleCBORError shapes a backend error as a CBOR response, using the same
// code/status mapping as the JSON path's handleError.
func (h *Handler) handleCBORError(c *echo.Context, err error) error {
	code, status := h.errorCodeStatus(err)

	return h.cborError(c, status, code, err.Error())
}

// cborError writes a CBOR error response. See
// [service.WriteRPCv2CBORError] for why the CBOR body itself (not just the
// X-Amzn-Errortype header) must carry the exception name.
func (h *Handler) cborError(c *echo.Context, status int, code, message string) error {
	return service.WriteRPCv2CBORError(c, status, code, message)
}

// writeCBOR writes a CBOR-encoded response with the Smithy-Protocol header.
// Delegates to pkgs/service, shared with CloudWatch (the only other
// rpc-v2-cbor service).
func writeCBOR(c *echo.Context, v cbor.Value) error {
	return service.WriteRPCv2CBORResponse(c, v)
}

// cborBodyToJSON decodes a raw rpc-v2-cbor request body into the JSON bytes
// expected by the existing op handlers' json.Unmarshal calls. An empty body
// (no input members) decodes as an empty JSON object, matching how the
// legacy JSON handlers already treat a nil/empty body. The top-level value
// must be a CBOR map (an operation's input structure), mirroring
// CloudWatch's handleCBOR -- this rejects malformed bodies (e.g. a bare
// scalar) at the transport level with SerializationException instead of
// letting them reach an op handler's json.Unmarshal and surface a
// misleading validation error.
func cborBodyToJSON(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return []byte("{}"), nil
	}

	v, err := cbor.Decode(body)
	if err != nil {
		return nil, err
	}

	m, ok := v.(cbor.Map)
	if !ok {
		return nil, fmt.Errorf("%w: got %T", errExpectedCBORMap, v)
	}

	return json.Marshal(cborToGo(m))
}

// cborToGo converts a decoded CBOR value tree into the generic Go value
// tree (map[string]any / []any / string / bool / float64 / nil) that
// encoding/json can marshal, so it can be fed through the existing JSON-body
// op handlers unchanged.
func cborToGo(v cbor.Value) any {
	switch vv := v.(type) {
	case cbor.Map:
		m := make(map[string]any, len(vv))
		for k, val := range vv {
			m[k] = cborToGo(val)
		}

		return m
	case cbor.List:
		l := make([]any, len(vv))
		for i, val := range vv {
			l[i] = cborToGo(val)
		}

		return l
	case cbor.String:
		return string(vv)
	case cbor.Slice:
		// AppStream has no blob-shaped fields today; preserved for
		// completeness rather than silently dropping bytes.
		return base64.StdEncoding.EncodeToString(vv)
	case cbor.Bool:
		return bool(vv)
	case cbor.Uint:
		return uint64(vv)
	case cbor.NegInt:
		return -int64(vv) //nolint:gosec // AppStream integer fields never approach int64 bounds
	case cbor.Float32:
		return float64(vv)
	case cbor.Float64:
		return float64(vv)
	case *cbor.Tag:
		if t, terr := cbor.AsTime(vv); terr == nil {
			return awstimeSeconds(t.Unix(), t.Nanosecond())
		}

		return cborToGo(vv.Value)
	default:
		// *cbor.Nil, *cbor.Undefined, or anything unrecognized.
		return nil
	}
}

// awstimeSeconds converts Unix seconds/nanoseconds into the fractional
// epoch-seconds float64 the JSON op handlers already expect for timestamp
// input (see pkgs/awstime.Epoch, which the response side uses the same way).
func awstimeSeconds(sec int64, nsec int) float64 {
	const nanosPerSecond = 1e9

	return float64(sec) + float64(nsec)/nanosPerSecond
}

// jsonToCBOR decodes a JSON response body (as produced by h.dispatch) into
// a CBOR value tree suitable for the wire.
func jsonToCBOR(body []byte) (cbor.Value, error) {
	var v any
	if err := json.Unmarshal(body, &v); err != nil {
		return nil, err
	}

	return goToCBOR(v, ""), nil
}

// goToCBOR converts a generic Go value (as produced by json.Unmarshal into
// `any`) into a CBOR value tree. key is the map key this value was stored
// under, if any; it drives the timestampKeys special case below.
func goToCBOR(v any, key string) cbor.Value {
	switch vv := v.(type) {
	case nil:
		return &cbor.Nil{}
	case map[string]any:
		m := make(cbor.Map, len(vv))
		for k, val := range vv {
			m[k] = goToCBOR(val, k)
		}

		return m
	case []any:
		l := make(cbor.List, len(vv))
		for i, val := range vv {
			l[i] = goToCBOR(val, "")
		}

		return l
	case string:
		return cbor.String(vv)
	case bool:
		return cbor.Bool(vv)
	case float64:
		return numberToCBOR(vv, key)
	default:
		return &cbor.Nil{}
	}
}

// maxSafeCBORInt is the largest magnitude float64 that round-trips exactly
// through uint64 (2^53, the float64 mantissa limit). AppStream's integer
// fields (capacities, durations, counts) never approach this.
const maxSafeCBORInt = 1 << 53

// numberToCBOR encodes a JSON-decoded float64 as the most specific CBOR
// type the SDK's typed deserializer will accept for that field:
//   - timestampKeys fields must be a Tag(1, epoch-seconds) (smithycbor.AsTime
//     rejects a bare number);
//   - whole numbers are encoded as Uint/NegInt rather than Float64, because
//     the SDK's AsInt32/AsInt64 coercers (used for integer-shaped fields
//     like DesiredInstances or MaxUserDurationInSeconds) only accept
//     Uint/NegInt, not Float32/Float64;
//   - anything else falls back to Float64.
func numberToCBOR(f float64, key string) cbor.Value {
	if timestampKeys[key] {
		return cbor.Tag{ID: 1, Value: cbor.Float64(f)}
	}

	if f == math.Trunc(f) {
		switch {
		case f >= 0 && f <= maxSafeCBORInt:
			return cbor.Uint(uint64(f))
		case f < 0 && -f <= maxSafeCBORInt:
			return cbor.NegInt(uint64(-f))
		}
	}

	return cbor.Float64(f)
}
