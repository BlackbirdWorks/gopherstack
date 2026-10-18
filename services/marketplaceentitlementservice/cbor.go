package marketplaceentitlementservice

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"github.com/aws/smithy-go/encoding/cbor"
)

// The pinned SDK speaks Smithy RPCv2 CBOR (POST /service/AWSMPEntitlementService/operation/<Op>);
// the legacy awsjson1.1 X-Amz-Target form is served too. Both share the JSON op functions.
const cborServicePath = "/service/AWSMPEntitlementService/operation/"

// cborTimeKeys are the output members modelled as timestamps (CBOR tag 1).
var cborTimeKeys = map[string]bool{"ExpirationDate": true} //nolint:gochecknoglobals // immutable lookup

func cborToJSONBytes(body []byte) ([]byte, error) {
	if len(body) == 0 {
		return nil, nil
	}

	v, err := cbor.Decode(body)
	if err != nil {
		return nil, fail(ErrInvalidParameter, "invalid CBOR body: %v", err)
	}

	m, ok := v.(cbor.Map)
	if !ok {
		return nil, fail(ErrInvalidParameter, "expected a CBOR map")
	}

	return json.Marshal(cborToAny(m))
}

func cborToAny(v cbor.Value) any {
	switch t := v.(type) {
	case cbor.Map:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = cborToAny(e)
		}

		return out
	case cbor.List:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = cborToAny(e)
		}

		return out
	case cbor.String:
		return string(t)
	case cbor.Uint:
		return json.Number(strconv.FormatUint(uint64(t), 10))
	case cbor.NegInt:
		if uint64(t) > math.MaxInt64 {
			return float64(-1) - float64(t)
		}

		return json.Number(strconv.FormatInt(-1-int64(t), 10)) //nolint:gosec // bounded by the MaxInt64 check above
	case cbor.Float64:
		return float64(t)
	case cbor.Float32:
		return float64(t)
	case cbor.Bool:
		return bool(t)
	case cbor.Tag:
		return cborToAny(t.Value)
	case cbor.Slice:
		return base64.StdEncoding.EncodeToString(t)
	default:
		return nil
	}
}

func jsonToCBORBytes(body []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fail(ErrInternalServiceError, "encode response: %v", err)
	}

	return cbor.Encode(anyToCBOR(v, "")), nil
}

func anyToCBOR(v any, key string) cbor.Value {
	switch t := v.(type) {
	case map[string]any:
		out := make(cbor.Map, len(t))
		for k, e := range t {
			out[k] = anyToCBOR(e, k)
		}

		return out
	case []any:
		out := make(cbor.List, len(t))
		for i, e := range t {
			out[i] = anyToCBOR(e, key)
		}

		return out
	case string:
		return cbor.String(t)
	case bool:
		return cbor.Bool(t)
	case json.Number:
		return numberToCBOR(t, cborTimeKeys[key])
	default:
		return &cbor.Nil{}
	}
}

func numberToCBOR(n json.Number, timestamp bool) cbor.Value {
	if !timestamp && !strings.ContainsAny(string(n), ".eE") {
		if i, err := n.Int64(); err == nil {
			if i >= 0 {
				return cbor.Uint(i)
			}

			return cbor.NegInt(-1 - i)
		}
	}

	f, _ := n.Float64()
	if timestamp {
		return cbor.Tag{ID: 1, Value: cbor.Float64(f)}
	}

	return cbor.Float64(f)
}
