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

// PackagingConfig holds the CDN-authorization credentials and per-protocol
// packaging blocks (HlsPackage/DashPackage/CmafPackage/MssPackage) accepted
// by CreateOriginEndpoint/UpdateOriginEndpoint. Each block is stored verbatim
// as the caller supplied it -- encryption/ad-marker semantics are not
// interpreted -- so Describe/List echo back exactly what was configured.
// Previously these fields were silently discarded on create/update, so a
// Terraform/CDK OriginEndpoint configured with e.g. hlsPackage never
// round-tripped through gopherstack.
type PackagingConfig struct {
	Authorization map[string]any
	CmafPackage   map[string]any
	DashPackage   map[string]any
	HlsPackage    map[string]any
	MssPackage    map[string]any
}

// OriginEndpoint represents a MediaPackage origin endpoint.
type OriginEndpoint struct {
	Authorization          map[string]any
	CmafPackage            map[string]any
	DashPackage            map[string]any
	HlsPackage             map[string]any
	MssPackage             map[string]any
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
