package dataplane

import (
	"bufio"
	"bytes"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// keyAttr describes one component of a primary key (hash or range).
type keyAttr struct {
	name string
	typ  types.ScalarAttributeType // S, N, or B
}

// keySchema is the ordered list of primary-key attributes: [hash] or [hash, range].
type keySchema []keyAttr

var (
	errMissingKey      = errors.New("dax: required key attribute missing")
	errBadKeySchema    = errors.New("dax: invalid key schema")
	errBadKeyType      = errors.New("dax: unsupported key attribute type")
	errNumericRangeKey = errors.New(
		"dax: numeric range keys in compound primary keys are not yet supported by the data plane",
	)
)

// encodeItemKey writes the DAX wire encoding of an item's primary key, given the
// table's key schema. This is the inverse of decodeItemKey and mirrors
// cbor.GetEncodedItemKey in the DAX client.
func encodeItemKey(w *Writer, item map[string]types.AttributeValue, ks keySchema) error {
	if len(ks) == 0 {
		return errBadKeySchema
	}

	var buf bytes.Buffer
	bw := bufio.NewWriter(&buf)
	inner := NewWriter(bw)

	if len(ks) == 1 {
		if err := encodeSingleKey(inner, bw, item, ks[0]); err != nil {
			return err
		}
	} else {
		if err := encodeCompoundKey(inner, bw, item, ks); err != nil {
			return err
		}
	}

	if err := inner.Flush(); err != nil {
		return err
	}

	if err := bw.Flush(); err != nil {
		return err
	}

	return w.WriteBytes(buf.Bytes())
}

// encodeSingleKey encodes a hash-only key. S/B are written as raw bytes (no CBOR
// header); N is written as a CBOR number.
func encodeSingleKey(inner *Writer, bw *bufio.Writer, item map[string]types.AttributeValue, hk keyAttr) error {
	hv, present := item[hk.name]
	if !present {
		return errMissingKey
	}

	switch hk.typ {
	case types.ScalarAttributeTypeS:
		s, isStr := hv.(*types.AttributeValueMemberS)
		if !isStr {
			return errMissingKey
		}

		_, err := bw.WriteString(s.Value)

		return err
	case types.ScalarAttributeTypeN:
		return encodeAttributeValue(inner, hv)
	case types.ScalarAttributeTypeB:
		b, isBin := hv.(*types.AttributeValueMemberB)
		if !isBin {
			return errMissingKey
		}

		_, err := bw.Write(b.Value)

		return err
	default:
		return errBadKeyType
	}
}

// encodeCompoundKey encodes a hash+range key. The hash uses CBOR
// string/number/bytes; the range S/B are raw bytes and N is lexdecimal.
func encodeCompoundKey(inner *Writer, bw *bufio.Writer, item map[string]types.AttributeValue, ks keySchema) error {
	if err := encodeCompoundHash(inner, item, ks[0]); err != nil {
		return err
	}

	if err := inner.Flush(); err != nil {
		return err
	}

	return encodeCompoundRange(bw, item, ks[1])
}

func encodeCompoundHash(inner *Writer, item map[string]types.AttributeValue, hk keyAttr) error {
	hv, present := item[hk.name]
	if !present {
		return errMissingKey
	}

	switch hk.typ {
	case types.ScalarAttributeTypeS:
		s, isStr := hv.(*types.AttributeValueMemberS)
		if !isStr {
			return errMissingKey
		}

		return inner.WriteString(s.Value)
	case types.ScalarAttributeTypeN:
		return encodeAttributeValue(inner, hv)
	case types.ScalarAttributeTypeB:
		b, isBin := hv.(*types.AttributeValueMemberB)
		if !isBin {
			return errMissingKey
		}

		return inner.WriteBytes(b.Value)
	default:
		return errBadKeyType
	}
}

func encodeCompoundRange(bw *bufio.Writer, item map[string]types.AttributeValue, rk keyAttr) error {
	rv, present := item[rk.name]
	if !present {
		return errMissingKey
	}

	switch rk.typ {
	case types.ScalarAttributeTypeS:
		s, isStr := rv.(*types.AttributeValueMemberS)
		if !isStr {
			return errMissingKey
		}

		_, err := bw.WriteString(s.Value)

		return err
	case types.ScalarAttributeTypeN:
		return errNumericRangeKey
	case types.ScalarAttributeTypeB:
		b, isBin := rv.(*types.AttributeValueMemberB)
		if !isBin {
			return errMissingKey
		}

		_, err := bw.Write(b.Value)

		return err
	default:
		return errBadKeyType
	}
}

// decodeItemKey reverses encodeItemKey, reconstructing the key attribute map
// from the DAX wire bytes and the table's key schema.
func decodeItemKey(r *Reader, ks keySchema) (map[string]types.AttributeValue, error) {
	if len(ks) == 0 {
		return nil, errBadKeySchema
	}

	keyBytes, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}

	keys := make(map[string]types.AttributeValue, len(ks))

	if len(ks) == 1 {
		return decodeSingleKey(keyBytes, ks[0], keys)
	}

	return decodeCompoundKey(keyBytes, ks, keys)
}

func decodeSingleKey(
	keyBytes []byte, hk keyAttr, keys map[string]types.AttributeValue,
) (map[string]types.AttributeValue, error) {
	switch hk.typ {
	case types.ScalarAttributeTypeS:
		keys[hk.name] = &types.AttributeValueMemberS{Value: string(keyBytes)}
	case types.ScalarAttributeTypeB:
		keys[hk.name] = &types.AttributeValueMemberB{Value: keyBytes}
	case types.ScalarAttributeTypeN:
		av, err := decodeAttributeValue(NewReader(bytes.NewReader(keyBytes)))
		if err != nil {
			return nil, err
		}

		keys[hk.name] = av
	default:
		return nil, errBadKeyType
	}

	return keys, nil
}

func decodeCompoundKey(
	keyBytes []byte, ks keySchema, keys map[string]types.AttributeValue,
) (map[string]types.AttributeValue, error) {
	br := bufio.NewReader(bytes.NewReader(keyBytes))
	r := NewReader(br)

	if err := decodeCompoundHash(r, ks[0], keys); err != nil {
		return nil, err
	}

	if err := decodeCompoundRange(br, ks[1], keys); err != nil {
		return nil, err
	}

	return keys, nil
}

func decodeCompoundHash(r *Reader, hk keyAttr, keys map[string]types.AttributeValue) error {
	switch hk.typ {
	case types.ScalarAttributeTypeS:
		s, err := r.ReadString()
		if err != nil {
			return err
		}

		keys[hk.name] = &types.AttributeValueMemberS{Value: s}
	case types.ScalarAttributeTypeN:
		av, err := decodeAttributeValue(r)
		if err != nil {
			return err
		}

		keys[hk.name] = av
	case types.ScalarAttributeTypeB:
		b, err := r.ReadBytes()
		if err != nil {
			return err
		}

		keys[hk.name] = &types.AttributeValueMemberB{Value: b}
	default:
		return errBadKeyType
	}

	return nil
}

func decodeCompoundRange(br *bufio.Reader, rk keyAttr, keys map[string]types.AttributeValue) error {
	switch rk.typ {
	case types.ScalarAttributeTypeS:
		rest, err := readAll(br)
		if err != nil {
			return err
		}

		keys[rk.name] = &types.AttributeValueMemberS{Value: string(rest)}
	case types.ScalarAttributeTypeN:
		return errNumericRangeKey
	case types.ScalarAttributeTypeB:
		rest, err := readAll(br)
		if err != nil {
			return err
		}

		keys[rk.name] = &types.AttributeValueMemberB{Value: rest}
	default:
		return errBadKeyType
	}

	return nil
}

func readAll(br *bufio.Reader) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(br); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
