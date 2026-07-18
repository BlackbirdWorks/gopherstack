package sqs

import (
	"crypto/md5" //nolint:gosec // MD5 used for SQS wire protocol compatibility, not security
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// computeBodyChecksumMD5 returns the hex-encoded MD5 digest of a message body for the
// MD5OfMessageBody / MD5OfMessageAttributes fields SQS returns on SendMessage,
// SendMessageBatch, and ReceiveMessage responses. This is NOT a security hash: it is the
// real AWS SQS wire-protocol content-integrity checksum (documented by AWS as MD5, computed
// identically by every AWS SDK to let callers verify their message wasn't corrupted in
// transit), analogous to S3's MD5-based ETag. The algorithm is dictated entirely by the AWS
// API contract — switching it to SHA-256 would produce a checksum no real SQS client
// recognizes or can verify against, breaking emulator parity.
func computeBodyChecksumMD5(body string) string {
	hash := md5.Sum([]byte(body)) //nolint:gosec // wire-protocol checksum, not a security hash

	return hex.EncodeToString(hash[:])
}

// computeSHA256 returns the hex-encoded SHA-256 hash of the given string.
// AWS SQS uses SHA-256 (not MD5) when generating the MessageDeduplicationId
// for content-based deduplication on FIFO queues.
func computeSHA256(body string) string {
	hash := sha256.Sum256([]byte(body))

	return hex.EncodeToString(hash[:])
}

// computeMD5OfMessageAttributes computes the MD5 of message attributes per the AWS SQS algorithm.
// Attributes are sorted alphabetically, then each is encoded as:
// 4-byte big-endian name length, name, 4-byte big-endian data-type length, data type,
// 1-byte transport type (1=String/Number, 2=Binary), 4-byte big-endian value length, value bytes.
func computeMD5OfMessageAttributes(attrs map[string]MessageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}

	names := collections.SortedKeys(attrs)

	var buf []byte
	for _, name := range names {
		attr := attrs[name]
		buf = appendWithLength(buf, []byte(name))
		buf = appendWithLength(buf, []byte(attr.DataType))

		if strings.HasPrefix(attr.DataType, "Binary") {
			buf = append(buf, msgAttrTransportTypeBinary)
			buf = appendWithLength(buf, attr.BinaryValue)
		} else {
			buf = append(buf, msgAttrTransportTypeString)
			buf = appendWithLength(buf, []byte(attr.StringValue))
		}
	}

	//nolint:gosec // MD5 required by SQS wire protocol
	hash := md5.Sum(buf)

	return hex.EncodeToString(hash[:])
}

// appendWithLength appends a 4-byte big-endian length prefix followed by data to buf.
func appendWithLength(buf, data []byte) []byte {
	var lenBuf [4]byte
	n := uint32(len(data)) //nolint:gosec // G115: bounded by SQS MaximumMessageSize (256 KB)
	binary.BigEndian.PutUint32(lenBuf[:], n)

	buf = append(buf, lenBuf[:]...)
	buf = append(buf, data...)

	return buf
}

// encodeMessageAttribute encodes a single attribute per SQS rules.
func encodeMessageAttribute(name string, attr MessageAttributeValue) []byte {
	var buf []byte
	buf = appendWithLength(buf, []byte(name))
	buf = appendWithLength(buf, []byte(attr.DataType))

	if strings.HasPrefix(attr.DataType, "Binary") {
		buf = append(buf, msgAttrTransportTypeBinary)
		buf = appendWithLength(buf, attr.BinaryValue)
	} else {
		buf = append(buf, msgAttrTransportTypeString)
		buf = appendWithLength(buf, []byte(attr.StringValue))
	}

	return buf
}

// computeMD5OfSubset uses pre-encoded attributes from msg to efficiently hash a subset.
func computeMD5OfSubset(msg *Message, returnedAttrs map[string]MessageAttributeValue) string {
	if len(returnedAttrs) == 0 {
		return ""
	}

	if msg.encodedAttrs == nil {
		if len(msg.MessageAttributes) == 0 {
			return ""
		}
		names := collections.SortedKeys(msg.MessageAttributes)
		encoded := make([]encodedMessageAttribute, 0, len(names))
		for _, name := range names {
			encoded = append(encoded, encodedMessageAttribute{
				Name:  name,
				Bytes: encodeMessageAttribute(name, msg.MessageAttributes[name]),
			})
		}
		msg.encodedAttrs = encoded
	}

	var buf []byte
	for _, ea := range msg.encodedAttrs {
		if _, ok := returnedAttrs[ea.Name]; ok {
			buf = append(buf, ea.Bytes...)
		}
	}

	//nolint:gosec // MD5 required by SQS wire protocol
	hash := md5.Sum(buf)

	return hex.EncodeToString(hash[:])
}
