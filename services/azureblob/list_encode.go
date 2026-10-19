package azureblob

import (
	"maps"
	"net/http"
	"slices"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/xmlappend"
)

const (
	listEnvelopeSize = 192
	blobEntrySize    = 320
	decimalBase      = 10
)

// listEcho holds the request parameters echoed back in a List Blobs response.
type listEcho struct {
	Prefix     string
	Marker     string
	Delimiter  string
	MaxResults int
}

func appendOptElem(buf []byte, name, val string) []byte {
	if val == "" {
		return buf
	}

	return xmlappend.Elem(buf, name, val)
}

func appendMetadata(buf []byte, md map[string]string) []byte {
	if len(md) == 0 {
		return buf
	}

	buf = append(buf, `<Metadata>`...)
	for _, k := range slices.Sorted(maps.Keys(md)) {
		buf = xmlappend.Elem(buf, k, md[k])
	}

	return append(buf, `</Metadata>`...)
}

func appendBlobEntry(buf []byte, bi *BlobInfo) []byte {
	buf = append(buf, `<Blob>`...)
	buf = appendMetadata(buf, bi.Metadata)
	buf = xmlappend.Elem(buf, "Name", bi.Name)
	buf = appendOptElem(buf, "Snapshot", bi.Snapshot)
	buf = append(buf, `<Properties><Last-Modified>`...)
	buf = bi.LastModified.AppendFormat(buf, http.TimeFormat)
	buf = append(buf, `</Last-Modified>`...)
	buf = xmlappend.Elem(buf, "Etag", bi.ETag)
	buf = xmlappend.Elem(buf, "Content-Type", bi.ContentType)
	buf = appendOptElem(buf, "Content-Encoding", bi.ContentEncoding)
	buf = appendOptElem(buf, "Content-Language", bi.ContentLanguage)
	buf = appendOptElem(buf, "Content-MD5", bi.ContentMD5)
	buf = appendOptElem(buf, "Cache-Control", bi.CacheControl)
	buf = appendOptElem(buf, "Content-Disposition", bi.ContentDisposition)
	buf = xmlappend.Elem(buf, "BlobType", blockBlobType)
	buf = appendOptElem(buf, "AccessTier", bi.AccessTier)

	if bi.AccessTierInferred {
		buf = xmlappend.Elem(buf, "AccessTierInferred", "true")
	}

	if !bi.AccessTierChanged.IsZero() {
		buf = xmlappend.Elem(buf, "AccessTierChangeTime", bi.AccessTierChanged.Format(http.TimeFormat))
	}

	buf = append(buf, `<Content-Length>`...)
	buf = strconv.AppendInt(buf, bi.ContentLength, decimalBase)

	return append(buf, `</Content-Length></Properties></Blob>`...)
}

// encodeBlobList renders the List Blobs response; output is byte-identical to
// xml.Marshal of the equivalent enumerationResults.
func encodeBlobList(endpoint, container string, echo listEcho, page BlobPage) []byte {
	buf := make([]byte, 0, listEnvelopeSize+len(page.Entries)*blobEntrySize)
	buf = append(buf, `<EnumerationResults ServiceEndpoint="`...)
	buf = xmlappend.Escaped(buf, endpoint)
	buf = append(buf, '"')

	if container != "" {
		buf = append(buf, ` ContainerName="`...)
		buf = xmlappend.Escaped(buf, container)
		buf = append(buf, '"')
	}

	buf = append(buf, '>')
	buf = appendOptElem(buf, "Prefix", echo.Prefix)
	buf = appendOptElem(buf, "Marker", echo.Marker)

	if echo.MaxResults > 0 {
		buf = xmlappend.Elem(buf, "MaxResults", strconv.Itoa(echo.MaxResults))
	}

	buf = appendOptElem(buf, "Delimiter", echo.Delimiter)
	buf = append(buf, `<Blobs>`...)

	for i := range page.Entries {
		e := &page.Entries[i]
		if e.IsPrefix {
			buf = append(buf, `<BlobPrefix>`...)
			buf = xmlappend.Elem(buf, "Name", e.Prefix)
			buf = append(buf, `</BlobPrefix>`...)

			continue
		}

		buf = appendBlobEntry(buf, &e.Blob)
	}

	buf = append(buf, `</Blobs>`...)
	buf = xmlappend.Elem(buf, "NextMarker", page.NextMarker)

	return append(buf, `</EnumerationResults>`...)
}

func httpTime(t time.Time) string { return t.Format(http.TimeFormat) }
