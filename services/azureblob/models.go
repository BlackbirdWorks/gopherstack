package azureblob

import (
	"encoding/xml"
	"maps"
	"slices"
	"time"
)

// Access tiers and public-access levels accepted by the REST API.
const (
	TierHot     = "Hot"
	TierCool    = "Cool"
	TierCold    = "Cold"
	TierArchive = "Archive"

	AccessBlob      = "blob"
	AccessContainer = "container"
)

// SignedIdentifier is one stored access policy of a container ACL.
type SignedIdentifier struct {
	ID         string
	Start      string
	Expiry     string
	Permission string
}

// ContainerInfo is a read-only snapshot of a container, returned by the
// StorageBackend container accessors. It carries no reference to backend
// storage.
type ContainerInfo struct {
	CreatedAt    time.Time
	LastModified time.Time
	Metadata     map[string]string
	Name         string
	ETag         string
	PublicAccess string
	ACL          []SignedIdentifier
	Lease        LeaseProps
}

// ContainerOptions are the create-time settings of a container.
type ContainerOptions struct {
	Metadata     map[string]string
	PublicAccess string
}

// BlobAttrs are the client-settable properties of a blob.
type BlobAttrs struct {
	Metadata           map[string]string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	ContentMD5         string
	AccessTier         string
}

// BlobInfo is a read-only snapshot of a blob (or of one of its snapshots),
// returned by the StorageBackend blob accessors.
type BlobInfo struct {
	LastModified       time.Time
	AccessTierChanged  time.Time
	Metadata           map[string]string
	Name               string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	ContentMD5         string
	ETag               string
	AccessTier         string
	Snapshot           string
	Lease              LeaseProps
	ContentLength      int64
	AccessTierInferred bool
}

// storedBlob is the backend's internal representation of a block blob.
// Snapshots holds immutable copies keyed by their snapshot timestamp.
type storedBlob struct {
	LastModified       time.Time
	AccessTierChanged  time.Time
	Metadata           map[string]string
	Snapshots          map[string]*storedBlob
	Name               string
	ContentType        string
	ContentEncoding    string
	ContentLanguage    string
	ContentDisposition string
	CacheControl       string
	ContentMD5         string
	ETag               string
	AccessTier         string
	Snapshot           string
	Data               []byte
	Blocks             []committedBlock
	Lease              *leaseRecord
}

func (b *storedBlob) info(now time.Time) BlobInfo {
	tier, inferred := b.AccessTier, false
	if tier == "" {
		tier, inferred = TierHot, true
	}

	return BlobInfo{
		Name:               b.Name,
		ContentType:        b.ContentType,
		ContentEncoding:    b.ContentEncoding,
		ContentLanguage:    b.ContentLanguage,
		ContentDisposition: b.ContentDisposition,
		CacheControl:       b.CacheControl,
		ContentMD5:         b.ContentMD5,
		ETag:               b.ETag,
		LastModified:       b.LastModified,
		ContentLength:      int64(len(b.Data)),
		Metadata:           maps.Clone(b.Metadata),
		AccessTier:         tier,
		AccessTierInferred: inferred,
		AccessTierChanged:  b.AccessTierChanged,
		Snapshot:           b.Snapshot,
		Lease:              b.Lease.props(now),
	}
}

// storedContainer is the backend's internal representation of a container.
type storedContainer struct {
	CreatedAt    time.Time
	LastModified time.Time
	Blobs        map[string]*storedBlob
	Metadata     map[string]string
	Name         string
	ETag         string
	PublicAccess string
	ACL          []SignedIdentifier
	Staged       map[string]map[string]*stagedBlock
	Lease        *leaseRecord
}

func (c *storedContainer) info(now time.Time) ContainerInfo {
	modified, etag := c.LastModified, c.ETag
	if modified.IsZero() {
		modified = c.CreatedAt
	}

	if etag == "" {
		etag = computeContainerETag(c.Name, c.CreatedAt)
	}

	return ContainerInfo{
		Name:         c.Name,
		CreatedAt:    c.CreatedAt,
		LastModified: modified,
		ETag:         etag,
		Metadata:     maps.Clone(c.Metadata),
		PublicAccess: c.PublicAccess,
		ACL:          slices.Clone(c.ACL),
		Lease:        c.Lease.props(now),
	}
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
	Prefix          string          `xml:"Prefix,omitempty"`
	Marker          string          `xml:"Marker,omitempty"`
	MaxResults      string          `xml:"MaxResults,omitempty"`
	Delimiter       string          `xml:"Delimiter,omitempty"`
	Containers      *containersList `xml:"Containers"`
	Blobs           *blobsList      `xml:"Blobs"`
	NextMarker      string          `xml:"NextMarker"`
}

// storageServiceProperties is the minimal (all-empty) response body for Get
// Blob Service Properties (GET /<account>?restype=service&comp=properties).
// Every field in the real schema (Cors/DefaultServiceVersion/
// DeleteRetentionPolicy/Logging/HourMetrics/MinuteMetrics/StaticWebsite) is
// optional, so an empty element round-trips through every SDK's XML decoder
// (they match by tag name, not presence). This endpoint exists solely to
// satisfy terraform-provider-azurerm v4.81+'s post-create data-plane
// readiness poll (AZURE.md section 10.8) -- it has no configurable state.
type storageServiceProperties struct {
	XMLName xml.Name `xml:"StorageServiceProperties"`
}

type containersList struct {
	Container []containerEntry `xml:"Container"`
}

type containerEntry struct {
	Metadata   *metadataXML        `xml:"Metadata,omitempty"`
	Name       string              `xml:"Name"`
	Properties containerProperties `xml:"Properties"`
}

type containerProperties struct {
	LastModified string `xml:"Last-Modified"`
	Etag         string `xml:"Etag"`
	PublicAccess string `xml:"PublicAccess,omitempty"`
	LeaseStatus  string `xml:"LeaseStatus"`
	LeaseState   string `xml:"LeaseState"`
	LeaseDur     string `xml:"LeaseDuration,omitempty"`
}

// blobsList holds blobEntry and blobPrefixEntry items in listing order.
type blobsList struct {
	Items []any `xml:",any"`
}

type blobEntry struct {
	XMLName    xml.Name       `xml:"Blob"`
	Metadata   *metadataXML   `xml:"Metadata,omitempty"`
	Name       string         `xml:"Name"`
	Snapshot   string         `xml:"Snapshot,omitempty"`
	Properties blobProperties `xml:"Properties"`
}

type blobPrefixEntry struct {
	XMLName xml.Name `xml:"BlobPrefix"`
	Name    string   `xml:"Name"`
}

type blobProperties struct {
	LastModified       string `xml:"Last-Modified"`
	Etag               string `xml:"Etag"`
	ContentType        string `xml:"Content-Type"`
	ContentEncoding    string `xml:"Content-Encoding,omitempty"`
	ContentLanguage    string `xml:"Content-Language,omitempty"`
	ContentMD5         string `xml:"Content-MD5,omitempty"`
	CacheControl       string `xml:"Cache-Control,omitempty"`
	ContentDisposition string `xml:"Content-Disposition,omitempty"`
	BlobType           string `xml:"BlobType"`
	AccessTier         string `xml:"AccessTier,omitempty"`
	AccessTierInferred string `xml:"AccessTierInferred,omitempty"`
	AccessTierChange   string `xml:"AccessTierChangeTime,omitempty"`
	ContentLength      int64  `xml:"Content-Length"`
	LeaseStatus        string `xml:"LeaseStatus,omitempty"`
	LeaseState         string `xml:"LeaseState,omitempty"`
	LeaseDur           string `xml:"LeaseDuration,omitempty"`
}

// metadataXML marshals as <Metadata><key>value</key>...</Metadata> with keys sorted.
type metadataXML map[string]string

// MarshalXML implements xml.Marshaler.
func (m metadataXML) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if err := e.EncodeToken(start); err != nil {
		return err
	}

	for _, k := range slices.Sorted(maps.Keys(m)) {
		if err := e.EncodeElement(m[k], xml.StartElement{Name: xml.Name{Local: k}}); err != nil {
			return err
		}
	}

	return e.EncodeToken(start.End())
}

func metadataElem(md map[string]string) *metadataXML {
	if len(md) == 0 {
		return nil
	}

	m := metadataXML(md)

	return &m
}

// signedIdentifiers is the Get/Set Container ACL body.
type signedIdentifiers struct {
	XMLName    xml.Name           `xml:"SignedIdentifiers"`
	Identifier []signedIdentifier `xml:"SignedIdentifier"`
}

type signedIdentifier struct {
	ID           string       `xml:"Id"`
	AccessPolicy accessPolicy `xml:"AccessPolicy"`
}

type accessPolicy struct {
	Start      string `xml:"Start,omitempty"`
	Expiry     string `xml:"Expiry,omitempty"`
	Permission string `xml:"Permission,omitempty"`
}
