package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	rpcv2PathPrefix  = "/service/"
	rpcv2PathInfix   = "/operation/"
	rpcv2ContentType = "application/cbor"
	jsonContentType  = "application/x-amz-json-1.0"
)

// RPCv2CBORBridge serves a JSON-target service's operations to SDK clients
// that speak Smithy RPCv2 CBOR (POST /service/<Name>/operation/<Op>), by
// translating each request into its X-Amz-Target JSON form and re-encoding
// the JSON response as CBOR.
type RPCv2CBORBridge struct {
	// BinaryKeys names JSON members modelled as blobs: byte strings in CBOR.
	BinaryKeys map[string]bool
	// TimeKeys names JSON members modelled as timestamps: CBOR tag 1.
	TimeKeys map[string]bool
	// Name is the Smithy service shape name, e.g. "CodeConnections_20231201".
	Name string
}

// Matches reports whether r is an RPCv2 CBOR call to this service.
func (b RPCv2CBORBridge) Matches(r *http.Request) bool {
	return IsRPCv2CBORRequest(r, rpcv2PathPrefix+b.Name+rpcv2PathInfix)
}

// Operation returns the operation name of a request for which Matches is true.
func (b RPCv2CBORBridge) Operation(r *http.Request) string {
	return ExtractRPCv2CBOROperation(r.URL.Path, rpcv2PathPrefix+b.Name+rpcv2PathInfix)
}

// Wrap returns a handler that serves RPCv2 CBOR requests through next, the
// service's X-Amz-Target JSON handler, and passes every other request on.
func (b RPCv2CBORBridge) Wrap(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		if !b.Matches(c.Request()) {
			return next(c)
		}

		return b.serve(c, next)
	}
}

func (b RPCv2CBORBridge) serve(c *echo.Context, next echo.HandlerFunc) error {
	req := c.Request()
	op := b.Operation(req)

	raw, err := httputils.ReadBody(req)
	if err != nil {
		return WriteRPCv2CBORError(c, http.StatusInternalServerError, "InternalFailure", "internal server error")
	}

	jsonBody, err := rpcv2CBORToJSON(raw)
	if err != nil {
		return WriteRPCv2CBORError(c, http.StatusBadRequest, "SerializationException", err.Error())
	}

	req.Header.Set("X-Amz-Target", b.Name+"."+op)
	req.Header.Set("Content-Type", jsonContentType)
	req.Header.Del("Content-Length")
	req.Body = io.NopCloser(bytes.NewReader(jsonBody))
	req.ContentLength = int64(len(jsonBody))

	orig := c.Response()
	buf := &bufferedResponse{header: http.Header{}}
	c.SetResponse(buf)

	nextErr := next(c)

	c.SetResponse(orig)

	if nextErr != nil {
		return nextErr
	}

	return b.writeBuffered(c, buf)
}

func (b RPCv2CBORBridge) writeBuffered(c *echo.Context, buf *bufferedResponse) error {
	status := buf.status
	if status == 0 {
		status = http.StatusOK
	}

	body, err := b.jsonToRPCv2CBOR(buf.body.Bytes())
	if err != nil {
		return WriteRPCv2CBORError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h := c.Response().Header()
	h.Set("Content-Type", rpcv2ContentType)
	h.Set("Smithy-Protocol", "rpc-v2-cbor")

	if et := buf.header.Get("X-Amzn-Errortype"); et != "" {
		h.Set("X-Amzn-Errortype", et)
	}

	c.Response().WriteHeader(status)
	_, err = c.Response().Write(body)

	return err
}

func (b RPCv2CBORBridge) jsonToRPCv2CBOR(body []byte) ([]byte, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return cbor.Encode(cbor.Map{}), nil
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}

	return cbor.Encode(b.anyToCBOR(v, "")), nil
}

func (b RPCv2CBORBridge) anyToCBOR(v any, key string) cbor.Value {
	switch t := v.(type) {
	case map[string]any:
		out := make(cbor.Map, len(t))
		for k, e := range t {
			out[k] = b.anyToCBOR(e, k)
		}

		return out
	case []any:
		out := make(cbor.List, len(t))
		for i, e := range t {
			out[i] = b.anyToCBOR(e, key)
		}

		return out
	case string:
		if b.BinaryKeys[key] {
			if raw, err := base64.StdEncoding.DecodeString(t); err == nil {
				return cbor.Slice(raw)
			}
		}

		return cbor.String(t)
	case bool:
		return cbor.Bool(t)
	case json.Number:
		return jsonNumberToCBOR(t, b.TimeKeys[key])
	default:
		return &cbor.Nil{}
	}
}

func jsonNumberToCBOR(n json.Number, timestamp bool) cbor.Value {
	if !timestamp && !strings.ContainsAny(string(n), ".eE") {
		if u, err := strconv.ParseUint(string(n), 10, 64); err == nil {
			return cbor.Uint(u)
		}

		if i, err := n.Int64(); err == nil {
			return cbor.NegInt(uint64(-1 - i)) //nolint:gosec // i < 0 here, so -1-i is non-negative
		}
	}

	f, _ := n.Float64()
	if timestamp {
		return cbor.Tag{ID: 1, Value: cbor.Float64(f)}
	}

	return cbor.Float64(f)
}

func rpcv2CBORToJSON(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return []byte("{}"), nil
	}

	return CBORToJSON(body)
}

type bufferedResponse struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (b *bufferedResponse) Header() http.Header { return b.header }

func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}

	return b.body.Write(p)
}

func (b *bufferedResponse) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}
