package azureblob //nolint:testpackage // exercises the unexported encoder against encoding/xml.

import (
	"encoding/xml"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeBlobList_MatchesXMLMarshal(t *testing.T) {
	t.Parallel()

	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	blob := func(bi BlobInfo) ListEntry { return ListEntry{Blob: bi} }
	prefix := func(p string) ListEntry { return ListEntry{IsPrefix: true, Prefix: p} }

	tests := []struct {
		name      string
		container string
		endpoint  string
		echo      listEcho
		page      BlobPage
	}{
		{name: "empty", container: "c", endpoint: "http://x:1"},
		{name: "no_container", endpoint: "http://x:1", page: BlobPage{Entries: []ListEntry{
			blob(BlobInfo{Name: "a", LastModified: ts}),
		}}},
		{name: "plain", container: "c", endpoint: "http://x:1", page: BlobPage{Entries: []ListEntry{
			blob(BlobInfo{Name: "a/b", ContentType: "text/plain", ETag: `"abc"`, LastModified: ts, ContentLength: 42}),
			blob(BlobInfo{Name: "z", ETag: `"d"`, LastModified: ts}),
		}}},
		{name: "escapes", container: `c"<&>'`, endpoint: "http://x:1/?a=b&c", page: BlobPage{Entries: []ListEntry{
			blob(BlobInfo{
				Name: "a<b>&\"'\t\n\ré\x01\xff", ContentType: "a&b", ETag: `"q"`, LastModified: ts, ContentLength: -1,
			}),
		}}},
		{
			name: "full", container: "c", endpoint: "http://x:1",
			echo: listEcho{Prefix: "d/", Marker: "m", Delimiter: "/", MaxResults: 3},
			page: BlobPage{NextMarker: "next", Entries: []ListEntry{
				prefix("d/sub/"),
				blob(BlobInfo{
					Name: "d/f", Snapshot: "2026-01-02T03:04:05.0000000Z", ContentType: "text/plain", ETag: `"e"`,
					LastModified: ts, ContentLength: 3, ContentEncoding: "identity", ContentLanguage: "en",
					ContentMD5: "AAAAAAAAAAAAAAAAAAAAAA==", CacheControl: "no-cache", ContentDisposition: "inline",
					AccessTier: TierCool, AccessTierChanged: ts,
					Metadata: map[string]string{"b": "2&", "a": "1"},
				}),
				blob(BlobInfo{Name: "d/g", LastModified: ts, AccessTier: TierHot, AccessTierInferred: true}),
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res := enumerationResults{
				ServiceEndpoint: tt.endpoint,
				ContainerName:   tt.container,
				Prefix:          tt.echo.Prefix,
				Marker:          tt.echo.Marker,
				Delimiter:       tt.echo.Delimiter,
				NextMarker:      tt.page.NextMarker,
				Blobs:           &blobsList{Items: make([]any, 0, len(tt.page.Entries))},
			}

			if tt.echo.MaxResults > 0 {
				res.MaxResults = strconv.Itoa(tt.echo.MaxResults)
			}

			for _, e := range tt.page.Entries {
				if e.IsPrefix {
					res.Blobs.Items = append(res.Blobs.Items, blobPrefixEntry{Name: e.Prefix})

					continue
				}

				bi := e.Blob
				props := blobProperties{
					LastModified:       httpTime(bi.LastModified),
					Etag:               bi.ETag,
					ContentLength:      bi.ContentLength,
					ContentType:        bi.ContentType,
					ContentEncoding:    bi.ContentEncoding,
					ContentLanguage:    bi.ContentLanguage,
					ContentMD5:         bi.ContentMD5,
					CacheControl:       bi.CacheControl,
					ContentDisposition: bi.ContentDisposition,
					BlobType:           blockBlobType,
					AccessTier:         bi.AccessTier,
				}

				if bi.AccessTierInferred {
					props.AccessTierInferred = "true"
				}

				if !bi.AccessTierChanged.IsZero() {
					props.AccessTierChange = httpTime(bi.AccessTierChanged)
				}

				res.Blobs.Items = append(res.Blobs.Items, blobEntry{
					Name: bi.Name, Snapshot: bi.Snapshot, Properties: props, Metadata: metadataElem(bi.Metadata),
				})
			}

			want, err := xml.Marshal(res)
			require.NoError(t, err)
			assert.Equal(t, string(want), string(encodeBlobList(tt.endpoint, tt.container, tt.echo, tt.page)))
		})
	}
}
