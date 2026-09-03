package azureblob

import (
	"encoding/xml"
	"time"
)

// ContainerInfo is a read-only snapshot of a container's metadata, returned
// by StorageBackend.ListContainers. It intentionally excludes the container's
// blob map so callers cannot mutate backend state through it.
type ContainerInfo struct {
	CreatedAt time.Time
	Name      string
}

// BlobInfo is a read-only snapshot of a blob's metadata, returned by the
// StorageBackend blob accessors. Like ContainerInfo, it carries no reference
// to the backend's internal storage.
type BlobInfo struct {
	LastModified  time.Time
	Name          string
	ContentType   string
	ETag          string
	ContentLength int64
}

// storedBlob is the backend's internal representation of a blob. Only
// BlockBlob is modeled (MVP scope, see AZURE.md/PARITY.md): Data holds the
// full object body written by a single Put Blob call, there is no
// block-list/multipart state.
type storedBlob struct {
	LastModified time.Time
	Data         []byte
	Name         string
	ContentType  string
	ETag         string
}

func (b *storedBlob) info() BlobInfo {
	return BlobInfo{
		Name:          b.Name,
		ContentType:   b.ContentType,
		ETag:          b.ETag,
		LastModified:  b.LastModified,
		ContentLength: int64(len(b.Data)),
	}
}

// storedContainer is the backend's internal representation of a container.
type storedContainer struct {
	CreatedAt time.Time
	Name      string
	Blobs     map[string]*storedBlob
}

// --- Azure Blob REST XML response shapes ---
//
// These mirror the wire shape of Azure Storage's "EnumerationResults" and
// "Error" bodies closely enough for azure-sdk-for-go (and Azurite-targeting
// SDKs generally) to parse successfully. Field ordering generally matches
// the real service's documented schema, except where govet's fieldalignment
// check required reordering a smaller field (e.g. blobProperties'
// ContentLength moved after ContentType/BlobType) after larger ones --
// encoding/xml.Marshal does emit elements in struct field order, so this
// does change the response's element order, but XML unmarshaling (by every
// conformant client, including azure-sdk-for-go) matches by tag name, not
// position, so this has no effect on parsing.

// azureError is the standard Azure Storage REST error body.
type azureError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// enumerationResults is the top-level shape returned by List Containers and
// List Blobs. Exactly one of Containers/Blobs is populated depending on
// which operation produced it.
type enumerationResults struct {
	XMLName         xml.Name        `xml:"EnumerationResults"`
	ServiceEndpoint string          `xml:"ServiceEndpoint,attr"`
	ContainerName   string          `xml:"ContainerName,attr,omitempty"`
	Containers      *containersList `xml:"Containers"`
	Blobs           *blobsList      `xml:"Blobs"`
	NextMarker      string          `xml:"NextMarker"`
}

type containersList struct {
	Container []containerEntry `xml:"Container"`
}

type containerEntry struct {
	Name       string              `xml:"Name"`
	Properties containerProperties `xml:"Properties"`
}

type containerProperties struct {
	LastModified string `xml:"Last-Modified"`
	Etag         string `xml:"Etag"`
}

type blobsList struct {
	Blob []blobEntry `xml:"Blob"`
}

type blobEntry struct {
	Name       string         `xml:"Name"`
	Properties blobProperties `xml:"Properties"`
}

type blobProperties struct {
	LastModified  string `xml:"Last-Modified"`
	Etag          string `xml:"Etag"`
	ContentType   string `xml:"Content-Type"`
	BlobType      string `xml:"BlobType"`
	ContentLength int64  `xml:"Content-Length"`
}
