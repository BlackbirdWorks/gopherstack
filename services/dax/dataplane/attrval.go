package dataplane

import (
	"errors"
	"math/big"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file converts between DAX's CBOR attribute-value encoding and the AWS
// SDK v2 dynamodb types used by the gopherstack DynamoDB backend. The wire
// format matches github.com/aws/aws-dax-go/dax/internal/cbor/attrval.go.

// errInvalidAttr indicates a malformed attribute value on the wire.
var errInvalidAttr = errors.New("dax: invalid attribute value")

const (
	// base10 is the radix for DynamoDB number literals.
	base10 = 10
	// maxInt64Digits is the largest decimal-digit count that always fits in an
	// int64; longer literals use big.Int.
	maxInt64Digits = 18
	// decimalTupleLen is the element count of a CBOR tag-4 decimal: [scale, unscaled].
	decimalTupleLen = 2
)

// encodeAttributeValue writes a single DynamoDB AttributeValue in DAX CBOR form.
func encodeAttributeValue(w *Writer, v types.AttributeValue) error {
	switch av := v.(type) {
	case *types.AttributeValueMemberS:
		return w.WriteString(av.Value)
	case *types.AttributeValueMemberN:
		return writeStringNumber(w, av.Value)
	case *types.AttributeValueMemberB:
		return w.WriteBytes(av.Value)
	case *types.AttributeValueMemberBOOL:
		return w.WriteBool(av.Value)
	case *types.AttributeValueMemberNULL:
		return w.WriteNull()
	case *types.AttributeValueMemberSS:
		return encodeStringSet(w, av.Value)
	case *types.AttributeValueMemberNS:
		return encodeNumberSet(w, av.Value)
	case *types.AttributeValueMemberBS:
		return encodeBinarySet(w, av.Value)
	case *types.AttributeValueMemberL:
		return encodeList(w, av.Value)
	case *types.AttributeValueMemberM:
		return encodeMap(w, av.Value)
	default:
		return errInvalidAttr
	}
}

func encodeStringSet(w *Writer, ss []string) error {
	if err := w.WriteTag(tagStringSet); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(len(ss)); err != nil {
		return err
	}

	for _, s := range ss {
		if err := w.WriteString(s); err != nil {
			return err
		}
	}

	return nil
}

func encodeNumberSet(w *Writer, ns []string) error {
	if err := w.WriteTag(tagNumberSet); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(len(ns)); err != nil {
		return err
	}

	for _, n := range ns {
		if err := writeStringNumber(w, n); err != nil {
			return err
		}
	}

	return nil
}

func encodeBinarySet(w *Writer, bs [][]byte) error {
	if err := w.WriteTag(tagBinarySet); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(len(bs)); err != nil {
		return err
	}

	for _, b := range bs {
		if err := w.WriteBytes(b); err != nil {
			return err
		}
	}

	return nil
}

func encodeList(w *Writer, l []types.AttributeValue) error {
	if err := w.WriteArrayHeader(len(l)); err != nil {
		return err
	}

	for _, item := range l {
		if err := encodeAttributeValue(w, item); err != nil {
			return err
		}
	}

	return nil
}

func encodeMap(w *Writer, m map[string]types.AttributeValue) error {
	if err := w.WriteMapHeader(len(m)); err != nil {
		return err
	}

	for k, item := range m {
		if err := w.WriteString(k); err != nil {
			return err
		}

		if err := encodeAttributeValue(w, item); err != nil {
			return err
		}
	}

	return nil
}

// writeStringNumber encodes a DynamoDB number literal using the most compact
// CBOR representation that preserves the value, matching the DAX client.
func writeStringNumber(w *Writer, val string) error {
	if strings.ContainsAny(val, ".eE") {
		d := &decimal{}
		if !d.setString(val) {
			return errInvalidAttr
		}

		return writeDecimal(w, d)
	}

	if len(val) > maxInt64Digits {
		b := new(big.Int)
		if _, ok := b.SetString(val, base10); !ok {
			return errInvalidAttr
		}

		return w.WriteBigInt(b)
	}

	i, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return errInvalidAttr
	}

	return w.WriteInt64(i)
}

// writeDecimal encodes a decimal using the CBOR tag-4 representation.
func writeDecimal(w *Writer, d *decimal) error {
	if err := w.WriteTag(tagDecimal); err != nil {
		return err
	}

	if err := w.WriteArrayHeader(decimalTupleLen); err != nil {
		return err
	}

	if err := w.WriteInt(-d.scale); err != nil {
		return err
	}

	return w.WriteBigInt(d.unscaled())
}

// decodeAttributeValue reads a single DynamoDB AttributeValue from DAX CBOR.
func decodeAttributeValue(r *Reader) (types.AttributeValue, error) {
	hdr, err := r.PeekHeader()
	if err != nil {
		return nil, err
	}

	switch int(hdr) & majorTypeMask {
	case majorUtf:
		s, e := r.ReadString()

		return &types.AttributeValueMemberS{Value: s}, e
	case majorBytes:
		b, e := r.ReadBytes()

		return &types.AttributeValueMemberB{Value: b}, e
	case majorPosInt, majorNegInt:
		s, e := r.readIntegerToString()

		return &types.AttributeValueMemberN{Value: s}, e
	case majorArray:
		return decodeList(r)
	case majorMap:
		return decodeMap(r)
	case majorSimple:
		return decodeSimple(r, hdr)
	case majorTag:
		return decodeTagged(r)
	default:
		return nil, errInvalidAttr
	}
}

func decodeList(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	l := make([]types.AttributeValue, n)

	for i := range n {
		if l[i], err = decodeAttributeValue(r); err != nil {
			return nil, err
		}
	}

	return &types.AttributeValueMemberL{Value: l}, nil
}

func decodeMap(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadMapLength()
	if err != nil {
		return nil, err
	}

	m := make(map[string]types.AttributeValue, n)

	for range n {
		k, e := r.ReadString()
		if e != nil {
			return nil, e
		}

		if m[k], err = decodeAttributeValue(r); err != nil {
			return nil, err
		}
	}

	return &types.AttributeValueMemberM{Value: m}, nil
}

func decodeSimple(r *Reader, hdr byte) (types.AttributeValue, error) {
	if _, err := r.r.ReadByte(); err != nil {
		return nil, err
	}

	switch hdr {
	case simpleFalse:
		return &types.AttributeValueMemberBOOL{Value: false}, nil
	case simpleTrue:
		return &types.AttributeValueMemberBOOL{Value: true}, nil
	case simpleNil:
		return &types.AttributeValueMemberNULL{Value: true}, nil
	default:
		return nil, errInvalidAttr
	}
}

func decodeTagged(r *Reader) (types.AttributeValue, error) {
	_, tag, err := r.readHeader()
	if err != nil {
		return nil, err
	}

	switch tag {
	case tagPosBigInt:
		return decodeBigIntAttr(r, false)
	case tagNegBigInt:
		return decodeBigIntAttr(r, true)
	case tagDecimal:
		return decodeDecimalAttr(r)
	case tagStringSet:
		return decodeStringSet(r)
	case tagNumberSet:
		return decodeNumberSet(r)
	case tagBinarySet:
		return decodeBinarySet(r)
	default:
		return nil, errInvalidAttr
	}
}

func decodeBigIntAttr(r *Reader, neg bool) (types.AttributeValue, error) {
	b, err := r.ReadBytes()
	if err != nil {
		return nil, err
	}

	v := new(big.Int).SetBytes(b)
	if neg {
		v.Not(v)
	}

	return &types.AttributeValueMemberN{Value: v.String()}, nil
}

func decodeDecimalAttr(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	if n != decimalTupleLen {
		return nil, errInvalidAttr
	}

	scale, err := r.ReadInt()
	if err != nil {
		return nil, err
	}

	unscaled, err := r.readBigInt()
	if err != nil {
		return nil, err
	}

	d := newDecimal(unscaled, -scale)

	return &types.AttributeValueMemberN{Value: d.String()}, nil
}

func decodeStringSet(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	ss := make([]string, n)

	for i := range n {
		if ss[i], err = r.ReadString(); err != nil {
			return nil, err
		}
	}

	return &types.AttributeValueMemberSS{Value: ss}, nil
}

func decodeNumberSet(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	ns := make([]string, n)

	for i := range n {
		av, e := decodeAttributeValue(r)
		if e != nil {
			return nil, e
		}

		num, ok := av.(*types.AttributeValueMemberN)
		if !ok {
			return nil, errInvalidAttr
		}

		ns[i] = num.Value
	}

	return &types.AttributeValueMemberNS{Value: ns}, nil
}

func decodeBinarySet(r *Reader) (types.AttributeValue, error) {
	n, err := r.ReadArrayLength()
	if err != nil {
		return nil, err
	}

	bs := make([][]byte, n)

	for i := range n {
		if bs[i], err = r.ReadBytes(); err != nil {
			return nil, err
		}
	}

	return &types.AttributeValueMemberBS{Value: bs}, nil
}

// readIntegerToString reads a CBOR integer into a decimal string, using big.Int
// for magnitudes that exceed int64.
func (r *Reader) readIntegerToString() (string, error) {
	hdr, value, err := r.readHeader()
	if err != nil {
		return "", err
	}

	switch hdr & majorTypeMask {
	case majorPosInt:
		return strconv.FormatUint(value, 10), nil
	case majorNegInt:
		if value <= 1<<63-1 {
			return strconv.FormatInt(^int64(value), 10), nil
		}

		return new(big.Int).Not(new(big.Int).SetUint64(value)).String(), nil
	default:
		return "", ErrNaN
	}
}

// readBigInt reads a CBOR integer (possibly big-int tagged) into a big.Int.
func (r *Reader) readBigInt() (*big.Int, error) {
	hdr, err := r.PeekHeader()
	if err != nil {
		return nil, err
	}

	if int(hdr)&majorTypeMask == majorTag {
		_, tag, e := r.readHeader()
		if e != nil {
			return nil, e
		}

		b, e := r.ReadBytes()
		if e != nil {
			return nil, e
		}

		v := new(big.Int).SetBytes(b)
		if tag == tagNegBigInt {
			v.Not(v)
		}

		return v, nil
	}

	h, value, e := r.readHeader()
	if e != nil {
		return nil, e
	}

	switch h & majorTypeMask {
	case majorPosInt:
		return new(big.Int).SetUint64(value), nil
	case majorNegInt:
		return new(big.Int).Not(new(big.Int).SetUint64(value)), nil
	default:
		return nil, ErrNaN
	}
}
