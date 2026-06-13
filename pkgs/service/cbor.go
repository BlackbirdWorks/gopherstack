package service

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strings"

	"github.com/aws/smithy-go/encoding/cbor"
)

// ContentTypeCBOR is the AWS CBOR wire protocol content type used by DynamoDB, Kinesis, and Timestream.
const ContentTypeCBOR = "application/x-amz-cbor-1.1"

// IsCBORRequest returns true when the request carries an AWS CBOR-encoded body.
func IsCBORRequest(r *http.Request) bool {
	ct := r.Header.Get("Content-Type")
	return strings.HasPrefix(ct, ContentTypeCBOR)
}

// CBORToJSON decodes AWS CBOR-1.1 wire bytes to equivalent JSON bytes.
//
// CBOR byte strings (major type 2) are base64-encoded in the JSON output,
// matching the JSON wire format that aws-sdk-go-v2 uses for binary attribute values.
func CBORToJSON(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return []byte("{}"), nil
	}

	val, err := cbor.Decode(data)
	if err != nil {
		return nil, fmt.Errorf("cbor decode: %w", err)
	}

	generic := cborToInterface(val)

	out, err := json.Marshal(generic)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	return out, nil
}

// JSONToCBOR encodes JSON bytes to AWS CBOR-1.1 wire bytes.
//
// binaryKeys is an optional set of JSON object keys whose string values should
// be treated as standard base64-encoded binary data and encoded as CBOR byte
// strings (major type 2). Keys present in the set match any occurrence in the
// document regardless of nesting depth. A nil map means no binary fields.
//
// For array values the parent key is inherited, so {"BS": ["abc","def"]}
// produces a CBOR list of byte strings when "BS" is in binaryKeys.
func JSONToCBOR(data []byte, binaryKeys map[string]bool) ([]byte, error) {
	var generic interface{}
	if err := json.Unmarshal(data, &generic); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w", err)
	}

	val := interfaceToCBOR(generic, "", binaryKeys)

	return cbor.Encode(val), nil
}

// cborToInterface converts a cbor.Value to a Go interface{} value suitable for
// json.Marshal. CBOR byte strings are converted to []byte, which json.Marshal
// encodes as standard base64 — matching the JSON wire format for binary DynamoDB
// attribute values.
func cborToInterface(v cbor.Value) interface{} {
	if v == nil {
		return nil
	}

	switch vv := v.(type) {
	case cbor.Map:
		m := make(map[string]interface{}, len(vv))
		for k, e := range vv {
			m[k] = cborToInterface(e)
		}
		return m

	case cbor.List:
		l := make([]interface{}, len(vv))
		for i, e := range vv {
			l[i] = cborToInterface(e)
		}
		return l

	case cbor.String:
		return string(vv)

	case cbor.Slice:
		// CBOR byte string → []byte; json.Marshal will base64-encode this.
		return []byte(vv)

	case cbor.Uint:
		return uint64(vv)

	case cbor.NegInt:
		// NegInt stores the magnitude; the true value is -(magnitude).
		if vv == 0 {
			// Represents -2^64, which overflows int64. Use the largest negative float64.
			return -math.MaxFloat64
		}
		return -int64(vv)

	case cbor.Float32:
		return float32(vv)

	case cbor.Float64:
		return float64(vv)

	case cbor.Bool:
		return bool(vv)

	case *cbor.Nil:
		return nil

	case *cbor.Undefined:
		return nil

	case *cbor.Tag:
		// Unwrap tagged values (e.g. epoch-time tag 1). Pass through the inner
		// value so it round-trips correctly via JSON.
		return cborToInterface(vv.Value)

	default:
		return nil
	}
}

// interfaceToCBOR converts a Go interface{} (as produced by json.Unmarshal) to a
// cbor.Value. key is the parent object key for the current value; it is used to
// look up binaryKeys so that base64 string values are encoded as CBOR byte strings.
func interfaceToCBOR(v interface{}, key string, binaryKeys map[string]bool) cbor.Value {
	if v == nil {
		return (*cbor.Nil)(nil)
	}

	switch vv := v.(type) {
	case map[string]interface{}:
		m := make(cbor.Map, len(vv))
		for k, e := range vv {
			m[k] = interfaceToCBOR(e, k, binaryKeys)
		}
		return m

	case []interface{}:
		l := make(cbor.List, len(vv))
		for i, e := range vv {
			// Array elements inherit the parent key for binary detection.
			l[i] = interfaceToCBOR(e, key, binaryKeys)
		}
		return l

	case string:
		if binaryKeys[key] {
			b, err := base64.StdEncoding.DecodeString(vv)
			if err == nil {
				return cbor.Slice(b)
			}
			// Fall through if the string isn't valid base64.
		}
		return cbor.String(vv)

	case float64:
		// json.Unmarshal always uses float64 for numbers.
		if isExactUint64(vv) {
			return cbor.Uint(uint64(vv))
		}
		if isExactNegInt(vv) {
			return cbor.NegInt(uint64(-vv))
		}
		return cbor.Float64(vv)

	case bool:
		return cbor.Bool(vv)

	default:
		return (*cbor.Nil)(nil)
	}
}

// isExactUint64 reports whether f can be represented exactly as a uint64.
func isExactUint64(f float64) bool {
	return f >= 0 && f == math.Trunc(f) && f <= math.MaxUint64
}

// isExactNegInt reports whether f can be represented exactly as a cbor.NegInt.
// NegInt stores the positive magnitude: true value = -(magnitude).
func isExactNegInt(f float64) bool {
	return f < 0 && f == math.Trunc(f) && -f <= math.MaxUint64
}
