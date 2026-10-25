package azurefile

import (
	"encoding/xml"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/sparse"
)

const (
	isoLayout       = "2006-01-02T15:04:05.0000000Z"
	defaultQuotaGiB = 5120
	maxQuotaGiB     = 102400
	maxSnapshots    = 200
	attrNone        = "None"
	attrArchive     = "Archive"
	attrDirectory   = "Directory"
	attrReadOnly    = "ReadOnly"
	defaultTier     = "TransactionOptimized"
	protoSMB        = "SMB"
	protoNFS        = "NFS"

	defaultMetricsRetentionDays = 7
	snapshotTick                = 100 * time.Nanosecond
)

func fmtISO(t time.Time) string { return t.UTC().Format(isoLayout) }

func fmtHTTP(t time.Time) string { return t.UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT") }

// leaseInfo is the persisted lease record of a share or file.
type leaseInfo struct {
	Until    time.Time `json:"until"`
	ID       string    `json:"id,omitempty"`
	State    string    `json:"state,omitempty"`
	Dur      int       `json:"dur,omitempty"`
	Infinite bool      `json:"infinite,omitempty"`
}

// node is a directory or a file inside a share.
type node struct {
	Created      time.Time         `json:"created"`
	LastWrite    time.Time         `json:"lastWrite"`
	Changed      time.Time         `json:"changed"`
	LastModified time.Time         `json:"lastModified"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	Children     map[string]*node  `json:"children,omitempty"`
	Copy         *copyInfo         `json:"copy,omitempty"`
	Name         string            `json:"name"`
	ID           string            `json:"id"`
	ParentID     string            `json:"parentId"`
	Attributes   string            `json:"attributes"`
	PermKey      string            `json:"permKey"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"contentType,omitempty"`
	Encoding     string            `json:"encoding,omitempty"`
	Language     string            `json:"language,omitempty"`
	CacheControl string            `json:"cacheControl,omitempty"`
	Disposition  string            `json:"disposition,omitempty"`
	MD5          []byte            `json:"md5,omitempty"`
	Data         sparse.Extents    `json:"data,omitempty"`
	Lease        leaseInfo         `json:"lease"`
	Size         int64             `json:"size"`
	Dir          bool              `json:"dir"`
}

// copyInfo records the most recent Copy File into a file.
type copyInfo struct {
	Completed time.Time `json:"completed"`
	ID        string    `json:"id"`
	Source    string    `json:"source"`
	Status    string    `json:"status"`
}

type accessPolicy struct {
	Start      string `json:"start,omitempty"`
	Expiry     string `json:"expiry,omitempty"`
	Permission string `json:"permission,omitempty"`
}

type signedIdentifier struct {
	ID     string       `json:"id"`
	Policy accessPolicy `json:"policy"`
}

// share is a file share, a share snapshot (SnapshotID set), or a soft-deleted share.
type share struct {
	Extra        shareExtra         `json:"extra"`
	Created      time.Time          `json:"created"`
	LastModified time.Time          `json:"lastModified"`
	TierChanged  time.Time          `json:"tierChanged"`
	DeletedAt    time.Time          `json:"deletedAt"`
	Metadata     map[string]string  `json:"metadata,omitempty"`
	Snapshots    map[string]*share  `json:"snapshots,omitempty"`
	Permissions  map[string]string  `json:"permissions,omitempty"`
	Root         *node              `json:"root"`
	ETag         string             `json:"etag"`
	Tier         string             `json:"tier"`
	Protocols    string             `json:"protocols"`
	RootSquash   string             `json:"rootSquash,omitempty"`
	SnapshotID   string             `json:"snapshotId,omitempty"`
	Version      string             `json:"version,omitempty"`
	Name         string             `json:"name"`
	ACL          []signedIdentifier `json:"acl,omitempty"`
	Lease        leaseInfo          `json:"lease"`
	Quota        int32              `json:"quota"`
	Deleted      bool               `json:"deleted,omitempty"`
}

// shareExtra holds optional provisioning knobs echoed back when set.
type shareExtra struct {
	SnapshotVDirAccess *bool  `json:"snapshotVDirAccess,omitempty"`
	SMBDirLease        *bool  `json:"smbDirLease,omitempty"`
	PaidBursting       *bool  `json:"paidBursting,omitempty"`
	BurstBandwidth     *int64 `json:"burstBandwidth,omitempty"`
	BurstIops          *int64 `json:"burstIops,omitempty"`
	Iops               *int64 `json:"iops,omitempty"`
	Bandwidth          *int64 `json:"bandwidth,omitempty"`
}

type account struct {
	Shares  map[string]*share `json:"shares"`
	Props   *serviceProps     `json:"props,omitempty"`
	Deleted []*share          `json:"deleted,omitempty"`
}

// --- service properties (XML wire shape; also the persisted form) ---

type corsRule struct {
	AllowedOrigins  string `xml:"AllowedOrigins"`
	AllowedMethods  string `xml:"AllowedMethods"`
	AllowedHeaders  string `xml:"AllowedHeaders"`
	ExposedHeaders  string `xml:"ExposedHeaders"`
	MaxAgeInSeconds int32  `xml:"MaxAgeInSeconds"`
}

type corsWrap struct {
	Rules []corsRule `xml:"CorsRule"`
}

type retentionPolicy struct {
	Days    *int32 `xml:"Days,omitempty"`
	Enabled bool   `xml:"Enabled"`
}

type metrics struct {
	Retention   *retentionPolicy `xml:"RetentionPolicy,omitempty"`
	IncludeAPIs *bool            `xml:"IncludeAPIs,omitempty"`
	Version     string           `xml:"Version"`
	Enabled     bool             `xml:"Enabled"`
}

type encryptionInTransit struct {
	Required *bool `xml:"Required,omitempty"`
}

type smbSettings struct {
	Multichannel *struct {
		Enabled bool `xml:"Enabled"`
	} `xml:"Multichannel,omitempty"`
	Encryption               *encryptionInTransit `xml:"EncryptionInTransit,omitempty"`
	Versions                 string               `xml:"Versions,omitempty"`
	AuthenticationMethods    string               `xml:"AuthenticationMethods,omitempty"`
	KerberosTicketEncryption string               `xml:"KerberosTicketEncryption,omitempty"`
	ChannelEncryption        string               `xml:"ChannelEncryption,omitempty"`
}

type protocolSettings struct {
	SMB *smbSettings `xml:"SMB,omitempty"`
	NFS *struct {
		Encryption *encryptionInTransit `xml:"EncryptionInTransit,omitempty"`
	} `xml:"NFS,omitempty"`
}

// serviceProps is the StorageServiceProperties document. Pointer fields
// distinguish "absent" (leave unchanged) from "present and empty".
type serviceProps struct {
	HourMetrics    *metrics          `json:"hour,omitempty"           xml:"HourMetrics,omitempty"`
	MinuteMetrics  *metrics          `json:"minute,omitempty"         xml:"MinuteMetrics,omitempty"`
	CORS           *corsWrap         `json:"cors,omitempty"           xml:"Cors,omitempty"`
	Protocol       *protocolSettings `json:"protocol,omitempty"       xml:"ProtocolSettings,omitempty"`
	ShareRetention *retentionPolicy  `json:"shareRetention,omitempty" xml:"ShareDeleteRetentionPolicy,omitempty"`
	XMLName        xml.Name          `json:"-"                        xml:"StorageServiceProperties"`
}

func defaultServiceProps() *serviceProps {
	days := int32(defaultMetricsRetentionDays)
	apis := true

	return &serviceProps{
		HourMetrics: &metrics{
			Version: metricsVersion, Enabled: true, IncludeAPIs: &apis,
			Retention: &retentionPolicy{Enabled: true, Days: &days},
		},
		MinuteMetrics: &metrics{Version: metricsVersion, Retention: &retentionPolicy{}},
		CORS:          &corsWrap{},
		Protocol: &protocolSettings{SMB: &smbSettings{Multichannel: &struct {
			Enabled bool `xml:"Enabled"`
		}{}}},
	}
}

// --- cloning (share snapshots) ---

func (n *node) clone() *node {
	c := *n
	c.Metadata = maps.Clone(n.Metadata)
	c.Data = n.Data.Clone()
	c.MD5 = append([]byte(nil), n.MD5...)

	if n.Copy != nil {
		cp := *n.Copy
		c.Copy = &cp
	}

	if n.Children != nil {
		c.Children = make(map[string]*node, len(n.Children))
		for k, ch := range n.Children {
			c.Children[k] = ch.clone()
		}
	}

	return &c
}

func (s *share) snapshotCopy() *share {
	c := *s
	c.Metadata = maps.Clone(s.Metadata)
	c.Permissions = maps.Clone(s.Permissions)
	c.ACL = append([]signedIdentifier(nil), s.ACL...)
	c.Root = s.Root.clone()
	c.Snapshots = nil
	c.Lease = leaseInfo{}

	return &c
}
