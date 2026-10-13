package azureblob

import (
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/xmlappend"
)

const (
	listEnvelopeSize = 192
	blobEntrySize    = 320
	decimalBase      = 10
)

// encodeBlobList renders the List Blobs response; output is byte-identical to
// xml.Marshal of the equivalent enumerationResults.
func encodeBlobList(endpoint, container string, blobs []BlobInfo) []byte {
	buf := make([]byte, 0, listEnvelopeSize+len(blobs)*blobEntrySize)
	buf = append(buf, `<EnumerationResults ServiceEndpoint="`...)
	buf = xmlappend.Escaped(buf, endpoint)
	buf = append(buf, '"')

	if container != "" {
		buf = append(buf, ` ContainerName="`...)
		buf = xmlappend.Escaped(buf, container)
		buf = append(buf, '"')
	}

	buf = append(buf, `><Blobs>`...)

	for i := range blobs {
		bi := &blobs[i]
		buf = append(buf, `<Blob>`...)
		buf = xmlappend.Elem(buf, "Name", bi.Name)
		buf = append(buf, `<Properties><Last-Modified>`...)
		buf = bi.LastModified.AppendFormat(buf, http.TimeFormat)
		buf = append(buf, `</Last-Modified>`...)
		buf = xmlappend.Elem(buf, "Etag", bi.ETag)
		buf = xmlappend.Elem(buf, "Content-Type", bi.ContentType)
		buf = xmlappend.Elem(buf, "BlobType", blockBlobType)
		buf = append(buf, `<Content-Length>`...)
		buf = strconv.AppendInt(buf, bi.ContentLength, decimalBase)
		buf = append(buf, `</Content-Length></Properties></Blob>`...)
	}

	return append(buf, `</Blobs><NextMarker></NextMarker></EnumerationResults>`...)
}

func httpTime(t time.Time) string { return t.Format(http.TimeFormat) }
