package cloudwatch

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"net/http"
	"strings"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

const jsonContentTypePrefix = "application/x-amz-json"

func isJSONTargetRequest(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Content-Type"), jsonContentTypePrefix)
}

// bufferedResponse captures a handler's response so it can be re-encoded.
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

// dispatchJSONTarget serves awsJson1_0 requests (botocore, AWS CLI v1) by
// bridging through the CBOR dispatcher and re-encoding the response as JSON.
func (h *Handler) dispatchJSONTarget(op string, body []byte, c *echo.Context) error {
	input := cbor.Map{}

	if len(bytes.TrimSpace(body)) > 0 {
		dec := json.NewDecoder(bytes.NewReader(body))
		dec.UseNumber()

		var raw any
		if err := dec.Decode(&raw); err != nil {
			return writeJSONError(c, http.StatusBadRequest, "SerializationException", err.Error())
		}

		if m, ok := jsonToCBOR(raw).(cbor.Map); ok {
			input = m
		}
	}

	orig := c.Response()
	buf := &bufferedResponse{header: http.Header{}}
	c.SetResponse(buf)

	err := h.dispatchCBOR(op, input, c)

	c.SetResponse(orig)

	if err != nil {
		return err
	}

	return writeBufferedAsJSON(c, buf)
}

func writeBufferedAsJSON(c *echo.Context, buf *bufferedResponse) error {
	status := buf.status
	if status == 0 {
		status = http.StatusOK
	}

	var out any = map[string]any{}

	if buf.body.Len() > 0 {
		val, err := cbor.Decode(buf.body.Bytes())
		if err != nil {
			return writeJSONError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
		}

		out = cborToJSON(val)
	}

	if et := buf.header.Get("X-Amzn-Errortype"); et != "" {
		c.Response().Header().Set("X-Amzn-Errortype", et)
	}

	return writeJSON(c, status, out)
}

func writeJSON(c *echo.Context, status int, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")
	c.Response().WriteHeader(status)
	_, err = c.Response().Write(data)

	return err
}

func writeJSONError(c *echo.Context, status int, code, msg string) error {
	c.Response().Header().Set("X-Amzn-Errortype", code)

	return writeJSON(c, status, map[string]any{"__type": code, "message": msg})
}

func jsonToCBOR(v any) cbor.Value {
	switch t := v.(type) {
	case map[string]any:
		m := make(cbor.Map, len(t))
		for k, e := range t {
			m[k] = jsonToCBOR(e)
		}

		return m
	case []any:
		l := make(cbor.List, 0, len(t))
		for _, e := range t {
			l = append(l, jsonToCBOR(e))
		}

		return l
	case string:
		return cbor.String(t)
	case bool:
		return cbor.Bool(t)
	case json.Number:
		return jsonNumberToCBOR(t)
	default:
		return &cbor.Nil{}
	}
}

func jsonNumberToCBOR(n json.Number) cbor.Value {
	if i, err := n.Int64(); err == nil {
		if i >= 0 {
			return cbor.Uint(i)
		}

		return cbor.NegInt(-1 - i)
	}

	f, _ := n.Float64()

	return cbor.Float64(f)
}

func cborToJSON(v cbor.Value) any {
	switch t := v.(type) {
	case cbor.Map:
		m := make(map[string]any, len(t))
		for k, e := range t {
			m[k] = cborToJSON(e)
		}

		return m
	case cbor.List:
		l := make([]any, 0, len(t))
		for _, e := range t {
			l = append(l, cborToJSON(e))
		}

		return l
	case cbor.String:
		return string(t)
	case cbor.Bool:
		return bool(t)
	case cbor.Uint:
		return uint64(t)
	case cbor.NegInt:
		return -1 - int64(t) //nolint:gosec // response ints are small
	case cbor.Float64:
		return jsonFloat(float64(t))
	case cbor.Float32:
		return jsonFloat(float64(t))
	case cbor.Slice:
		return base64.StdEncoding.EncodeToString(t)
	case cbor.Tag:
		return cborToJSON(t.Value)
	case *cbor.Tag:
		return cborToJSON(t.Value)
	default:
		return nil
	}
}

func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nil
	}

	return f
}
