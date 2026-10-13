package azureblob //nolint:testpackage // exercises the unexported encoder against encoding/xml.

import (
	"encoding/xml"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeBlobList_MatchesXMLMarshal(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	tests := []struct {
		name      string
		container string
		endpoint  string
		blobs     []BlobInfo
	}{
		{name: "empty", container: "c", endpoint: "http://x:1"},
		{name: "no_container", endpoint: "http://x:1", blobs: []BlobInfo{{Name: "a", LastModified: ts}}},
		{name: "plain", container: "c", endpoint: "http://x:1", blobs: []BlobInfo{
			{Name: "a/b", ContentType: "text/plain", ETag: `"abc"`, LastModified: ts, ContentLength: 42},
			{Name: "z", ContentType: "", ETag: `"d"`, LastModified: ts},
		}},
		{name: "escapes", container: `c"<&>'`, endpoint: "http://x:1/?a=b&c", blobs: []BlobInfo{
			{Name: "a<b>&\"'\t\n\ré\x01\xff", ContentType: "a&b", ETag: `"q"`, LastModified: ts, ContentLength: -1},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := enumerationResults{
				ServiceEndpoint: tt.endpoint,
				ContainerName:   tt.container,
				Blobs:           &blobsList{Blob: make([]blobEntry, 0, len(tt.blobs))},
			}

			for _, bi := range tt.blobs {
				res.Blobs.Blob = append(res.Blobs.Blob, blobEntry{
					Name: bi.Name,
					Properties: blobProperties{
						LastModified:  httpTime(bi.LastModified),
						Etag:          bi.ETag,
						ContentLength: bi.ContentLength,
						ContentType:   bi.ContentType,
						BlobType:      blockBlobType,
					},
				})
			}

			want, err := xml.Marshal(res)
			require.NoError(t, err)
			assert.Equal(t, string(want), string(encodeBlobList(tt.endpoint, tt.container, tt.blobs)))
		})
	}
}
