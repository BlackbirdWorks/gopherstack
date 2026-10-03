package s3

import (
	"bytes"
	"context"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// Hand-encodes the List(Objects|ObjectsV2) XML body -- encoding/xml reflection
// was ~45% of ListObjectsV2's CPU. Must stay byte-identical to xml.Encoder.Encode.

// writeXMLElem writes tag containing s, always present (no omitempty).
func writeXMLElem(buf *bytes.Buffer, tag, s string) {
	buf.WriteByte('<')
	buf.WriteString(tag)
	buf.WriteByte('>')
	escapeXMLString(buf, s)
	buf.WriteString("</")
	buf.WriteString(tag)
	buf.WriteByte('>')
}

// writeXMLElemOmitEmpty writes tag only when s is non-empty, mirroring an
// `omitempty` struct tag on a string field.
func writeXMLElemOmitEmpty(buf *bytes.Buffer, tag, s string) {
	if s == "" {
		return
	}
	writeXMLElem(buf, tag, s)
}

func writeXMLInt(buf *bytes.Buffer, tag string, n int) {
	buf.WriteByte('<')
	buf.WriteString(tag)
	buf.WriteByte('>')
	buf.WriteString(strconv.Itoa(n))
	buf.WriteString("</")
	buf.WriteString(tag)
	buf.WriteByte('>')
}

func writeXMLInt64(buf *bytes.Buffer, tag string, n int64) {
	buf.WriteByte('<')
	buf.WriteString(tag)
	buf.WriteByte('>')
	buf.WriteString(strconv.FormatInt(n, 10))
	buf.WriteString("</")
	buf.WriteString(tag)
	buf.WriteByte('>')
}

func writeXMLBool(buf *bytes.Buffer, tag string, v bool) {
	buf.WriteByte('<')
	buf.WriteString(tag)
	buf.WriteByte('>')
	if v {
		buf.WriteString("true")
	} else {
		buf.WriteString("false")
	}
	buf.WriteString("</")
	buf.WriteString(tag)
	buf.WriteByte('>')
}

// escapeXMLString mirrors encoding/xml printer.EscapeString's escaping table,
// so hand-written output stays byte-identical to reflection-based marshaling.
func escapeXMLString(buf *bytes.Buffer, s string) {
	last := 0
	for i := 0; i < len(s); {
		r, width := utf8.DecodeRuneInString(s[i:])
		i += width

		var esc string
		switch r {
		case '"':
			esc = "&#34;"
		case '\'':
			esc = "&#39;"
		case '&':
			esc = "&amp;"
		case '<':
			esc = "&lt;"
		case '>':
			esc = "&gt;"
		case '\t':
			esc = "&#x9;"
		case '\n':
			esc = "&#xA;"
		case '\r':
			esc = "&#xD;"
		default:
			if !isValidXMLChar(r) || (r == 0xFFFD && width == 1) {
				esc = "�"

				break
			}

			continue
		}

		buf.WriteString(s[last : i-width])
		buf.WriteString(esc)
		last = i
	}
	buf.WriteString(s[last:])
}

// isValidXMLChar mirrors encoding/xml's isInCharacterRange.
func isValidXMLChar(r rune) bool {
	return r == 0x09 || r == 0x0A || r == 0x0D ||
		r >= 0x20 && r <= 0xD7FF ||
		r >= 0xE000 && r <= 0xFFFD ||
		r >= 0x10000 && r <= 0x10FFFF
}

func writeOwnerXML(buf *bytes.Buffer, o *Owner) {
	if o == nil {
		return
	}
	buf.WriteString("<Owner>")
	writeXMLElem(buf, "ID", o.ID)
	writeXMLElem(buf, "DisplayName", o.DisplayName)
	buf.WriteString("</Owner>")
}

func writeObjectXML(buf *bytes.Buffer, o *ObjectXML) {
	buf.WriteString("<Contents>")
	writeOwnerXML(buf, o.Owner)
	writeXMLElem(buf, "Key", o.Key)
	writeXMLElem(buf, "LastModified", o.LastModified)
	writeXMLElem(buf, "ETag", o.ETag)
	writeXMLElem(buf, "StorageClass", o.StorageClass)
	writeXMLElemOmitEmpty(buf, "ChecksumAlgorithm", o.ChecksumAlgorithm)
	writeXMLInt64(buf, "Size", o.Size)
	buf.WriteString("</Contents>")
}

func writeCommonPrefixXML(buf *bytes.Buffer, cp *CommonPrefixXML) {
	buf.WriteString("<CommonPrefixes>")
	writeXMLElem(buf, "Prefix", cp.Prefix)
	buf.WriteString("</CommonPrefixes>")
}

// writeListBucketV2XML appends the ListObjectsV2 response body to buf. Field
// order and omitempty behavior mirror ListBucketV2Result's xml tags exactly.
func writeListBucketV2XML(buf *bytes.Buffer, r *ListBucketV2Result) {
	buf.WriteString("<ListBucketResult>")
	writeXMLElemOmitEmpty(buf, "StartAfter", r.StartAfter)
	writeXMLElem(buf, "Prefix", r.Prefix)
	writeXMLElemOmitEmpty(buf, "Delimiter", r.Delimiter)
	writeXMLElemOmitEmpty(buf, "ContinuationToken", r.ContinuationToken)
	writeXMLElemOmitEmpty(buf, "NextContinuationToken", r.NextContinuationToken)
	writeXMLElem(buf, "Name", r.Name)
	writeXMLElemOmitEmpty(buf, "EncodingType", r.EncodingType)
	for i := range r.Contents {
		writeObjectXML(buf, &r.Contents[i])
	}
	for i := range r.CommonPrefixes {
		writeCommonPrefixXML(buf, &r.CommonPrefixes[i])
	}
	writeXMLInt(buf, "KeyCount", r.KeyCount)
	writeXMLInt(buf, "MaxKeys", r.MaxKeys)
	writeXMLBool(buf, "IsTruncated", r.IsTruncated)
	buf.WriteString("</ListBucketResult>")
}

// writeListBucketXML appends the ListObjects (v1) response body to buf. Field
// order and omitempty behavior mirror ListBucketResult's xml tags exactly.
func writeListBucketXML(buf *bytes.Buffer, r *ListBucketResult) {
	buf.WriteString("<ListBucketResult>")
	writeXMLElem(buf, "Name", r.Name)
	writeXMLElem(buf, "Prefix", r.Prefix)
	writeXMLElemOmitEmpty(buf, "Delimiter", r.Delimiter)
	writeXMLElemOmitEmpty(buf, "Marker", r.Marker)
	writeXMLElemOmitEmpty(buf, "NextMarker", r.NextMarker)
	writeXMLElemOmitEmpty(buf, "EncodingType", r.EncodingType)
	for i := range r.Contents {
		writeObjectXML(buf, &r.Contents[i])
	}
	for i := range r.CommonPrefixes {
		writeCommonPrefixXML(buf, &r.CommonPrefixes[i])
	}
	writeXMLInt(buf, "MaxKeys", r.MaxKeys)
	writeXMLBool(buf, "IsTruncated", r.IsTruncated)
	buf.WriteString("</ListBucketResult>")
}

// writeListXMLResponse writes an already-encoded XML body (header + root
// element) with the same headers httputils.WriteXML sets.
func writeListXMLResponse(ctx context.Context, w http.ResponseWriter, code int, buf *bytes.Buffer) {
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	if _, err := buf.WriteTo(w); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write XML response", "error", err)
	}
}
