package mediapackage

import "context"

// StorageBackend is the interface for MediaPackage storage operations.
type StorageBackend interface {
	CreateChannel(id, description string, tags map[string]string) (*Channel, error)
	DescribeChannel(id string) (*Channel, error)
	UpdateChannel(id, description string) (*Channel, error)
	DeleteChannel(id string) (*Channel, error)
	ListChannels(maxResults int, nextToken string) ([]*Channel, string, error)
	ConfigureLogs(id string, egressLogGroup, ingressLogGroup *string) (*Channel, error)
	RotateChannelCredentials(id string) (*Channel, error)
	RotateIngestEndpointCredentials(channelID, ingestEndpointID string) (*Channel, error)

	CreateOriginEndpoint(
		channelID, id, description, manifestName string,
		startoverWindowSeconds, timeDelaySeconds int,
		origination string,
		whitelist []string,
		tags map[string]string,
		pkg PackagingConfig,
	) (*OriginEndpoint, error)
	DescribeOriginEndpoint(id string) (*OriginEndpoint, error)
	UpdateOriginEndpoint(
		id, description, manifestName string,
		startoverWindowSeconds, timeDelaySeconds int,
		origination string,
		whitelist []string,
		pkg PackagingConfig,
	) (*OriginEndpoint, error)
	DeleteOriginEndpoint(id string) (*OriginEndpoint, error)
	ListOriginEndpoints(channelID string, maxResults int, nextToken string) ([]*OriginEndpoint, string, error)

	CreateHarvestJob(id, originEndpointID, startTime, endTime string, s3Dest S3Destination) (*HarvestJob, error)
	DescribeHarvestJob(id string) (*HarvestJob, error)
	ListHarvestJobs(
		includeChannelID, includeStatus string,
		maxResults int,
		nextToken string,
	) ([]*HarvestJob, string, error)

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, keys []string) error
	ListTagsForResource(resourceARN string) (map[string]string, error)

	AccountID() string
	Region() string
	Reset()
	Snapshot(ctx context.Context) []byte
	Restore(ctx context.Context, data []byte) error
}

// IngestEndpoint holds ingest URL and credentials for a channel.
type IngestEndpoint struct {
	ID       string
	URL      string
	Username string
	Password string
}

// HlsIngest holds a list of ingest endpoints for a channel.
type HlsIngest struct {
	IngestEndpoints []*IngestEndpoint
}

// Channel represents a MediaPackage channel.
type Channel struct {
	HlsIngest           *HlsIngest
	EgressLogGroupName  *string
	IngressLogGroupName *string
	Tags                map[string]string
	ARN                 string
	ID                  string
	Description         string
	CreatedAt           string
}

// Authorization holds CDN-authorization credentials on an OriginEndpoint.
// Both fields are required together (types.Authorization,
// aws-sdk-go-v2/service/mediapackage@v1.42.4, types/types.go:10-26).
type Authorization struct {
	CdnIdentifierSecret string `json:"cdnIdentifierSecret"`
	SecretsRoleArn      string `json:"secretsRoleArn"`
}

// EncryptionContractConfiguration selects SPEKE 2.0 audio/video encryption
// presets (types.EncryptionContractConfiguration, SDK types/types.go:246-259).
type EncryptionContractConfiguration struct {
	PresetSpeke20Audio string `json:"presetSpeke20Audio"`
	PresetSpeke20Video string `json:"presetSpeke20Video"`
}

// SpekeKeyProvider configures an external SPEKE key provider. ResourceID,
// RoleArn, SystemIDs, and URL are required together (types.SpekeKeyProvider,
// SDK types/types.go:681-721).
type SpekeKeyProvider struct {
	EncryptionContractConfiguration *EncryptionContractConfiguration `json:"encryptionContractConfiguration,omitempty"`
	ResourceID                      string                           `json:"resourceId"`
	RoleArn                         string                           `json:"roleArn"`
	URL                             string                           `json:"url"`
	CertificateArn                  string                           `json:"certificateArn,omitempty"`
	SystemIDs                       []string                         `json:"systemIds"`
}

// MssEncryption is the Microsoft Smooth Streaming encryption configuration
// (types.MssEncryption, SDK types/types.go:563-572).
type MssEncryption struct {
	SpekeKeyProvider *SpekeKeyProvider `json:"spekeKeyProvider,omitempty"`
}

// StreamSelection filters/orders streams in packaged output
// (types.StreamSelection, SDK types/types.go:724-736).
type StreamSelection struct {
	StreamOrder           string `json:"streamOrder,omitempty"`
	MaxVideoBitsPerSecond int    `json:"maxVideoBitsPerSecond,omitempty"`
	MinVideoBitsPerSecond int    `json:"minVideoBitsPerSecond,omitempty"`
}

// MssPackage is a Microsoft Smooth Streaming packaging configuration
// (types.MssPackage, SDK types/types.go:575-590).
type MssPackage struct {
	Encryption             *MssEncryption   `json:"encryption,omitempty"`
	StreamSelection        *StreamSelection `json:"streamSelection,omitempty"`
	ManifestWindowSeconds  int              `json:"manifestWindowSeconds,omitempty"`
	SegmentDurationSeconds int              `json:"segmentDurationSeconds,omitempty"`
}

// PackagingConfig holds the CDN-authorization credentials and per-protocol
// packaging blocks (HlsPackage/DashPackage/CmafPackage/MssPackage) accepted
// by CreateOriginEndpoint/UpdateOriginEndpoint. Authorization and MssPackage
// are modeled to their full real SDK shape (small enough: 2 and ~11 leaf
// fields respectively, no repeated sub-resources). HlsPackage/DashPackage/
// CmafPackage remain an opaque passthrough -- each carries 12-16 leaf fields
// across several enum types, and CmafPackage additionally nests a *list* of
// HlsManifest sub-resources (~10 fields each); modeling those to the same
// full depth is a materially larger, separate unit of work (see PARITY.md).
// Every block is stored verbatim as the caller supplied it -- Describe/List
// echo back exactly what was configured.
type PackagingConfig struct {
	Authorization *Authorization
	CmafPackage   map[string]any
	DashPackage   map[string]any
	HlsPackage    map[string]any
	MssPackage    *MssPackage
}

// OriginEndpoint represents a MediaPackage origin endpoint.
type OriginEndpoint struct {
	Authorization          *Authorization
	MssPackage             *MssPackage
	CmafPackage            map[string]any
	DashPackage            map[string]any
	HlsPackage             map[string]any
	Tags                   map[string]string
	ARN                    string
	ChannelID              string
	ID                     string
	Description            string
	ManifestName           string
	URL                    string
	Origination            string
	CreatedAt              string
	Whitelist              []string
	StartoverWindowSeconds int
	TimeDelaySeconds       int
}

// S3Destination describes where harvested content is exported.
type S3Destination struct {
	BucketName  string
	ManifestKey string
	RoleArn     string
}

// HarvestJob represents a MediaPackage harvest job.
type HarvestJob struct {
	S3Destination    *S3Destination
	ARN              string
	ChannelID        string
	CreatedAt        string
	EndTime          string
	ID               string
	OriginEndpointID string
	StartTime        string
	Status           string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
