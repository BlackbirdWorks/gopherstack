package dataplane

import (
	"io"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// This file exports internal codec entry points for the external test package
// (dataplane_test) so that protocol-level encoding can be exercised without
// loosening the package's public API.

// TestWriter is a CBOR writer exposed for tests.
type TestWriter = Writer

// TestReader is a CBOR reader exposed for tests.
type TestReader = Reader

// NewTestWriter wraps w in a CBOR writer for tests.
func NewTestWriter(w io.Writer) *TestWriter { return NewWriter(w) }

// NewTestReader wraps r in a CBOR reader for tests.
func NewTestReader(r io.Reader) *TestReader { return NewReader(r) }

// WriteInt64ForTest writes a signed 64-bit integer in CBOR form.
func WriteInt64ForTest(w *TestWriter, v int64) error { return w.WriteInt64(v) }

// ReadInt64ForTest reads a signed 64-bit integer from CBOR.
func ReadInt64ForTest(r *TestReader) (int64, error) { return r.ReadInt64() }

// EncodeAttributeValueForTest encodes a single AttributeValue in DAX CBOR form.
func EncodeAttributeValueForTest(w *TestWriter, v types.AttributeValue) error {
	return encodeAttributeValue(w, v)
}

// DecodeAttributeValueForTest decodes a single AttributeValue from DAX CBOR.
func DecodeAttributeValueForTest(r *TestReader) (types.AttributeValue, error) {
	return decodeAttributeValue(r)
}

// KeyPart describes one primary-key component for tests.
type KeyPart struct {
	Name string
	Type types.ScalarAttributeType
}

// EncodeItemKeyForTest encodes an item's primary key against a key schema.
func EncodeItemKeyForTest(w *TestWriter, item map[string]types.AttributeValue, parts []KeyPart) error {
	return encodeItemKey(w, item, toKeySchema(parts))
}

// DecodeItemKeyForTest decodes a primary key against a key schema.
func DecodeItemKeyForTest(r *TestReader, parts []KeyPart) (map[string]types.AttributeValue, error) {
	return decodeItemKey(r, toKeySchema(parts))
}

func toKeySchema(parts []KeyPart) keySchema {
	ks := make(keySchema, len(parts))
	for i, p := range parts {
		ks[i] = keyAttr{name: p.Name, typ: p.Type}
	}

	return ks
}
