package mediapackage

import "context"

// StorageBackend is the interface for MediaPackage storage operations.
type StorageBackend interface {
	CreateChannel(id, description string, tags map[string]string) (*Channel, error)
	DescribeChannel(id string) (*Channel, error)
	UpdateChannel(id, description string) (*Channel, error)
	DeleteChannel(id string) (*Channel, error)
	ListChannels(maxResults int, nextToken string) ([]*Channel, string, error)
	ConfigureLogs(id string, egressLogGroup, ingressLogGroup string) (*Channel, error)
	RotateChannelCredentials(id string) (*Channel, error)
	RotateIngestEndpointCredentials(channelID, ingestEndpointID string) (*Channel, error)

	CreateOriginEndpoint(
		channelID, id, description, manifestName string,
		startoverWindowSeconds, timeDelaySeconds int,
		origination string,
		whitelist []string,
		tags map[string]string,
	) (*OriginEndpoint, error)
	DescribeOriginEndpoint(id string) (*OriginEndpoint, error)
	UpdateOriginEndpoint(
		id, description, manifestName string,
		startoverWindowSeconds, timeDelaySeconds int,
		origination string,
		whitelist []string,
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

	// PackagingConfiguration CRUD
	CreatePackagingConfiguration(
		id, packagingGroupID, description string,
		tags map[string]string,
	) (*PackagingConfiguration, error)
	DescribePackagingConfiguration(id string) (*PackagingConfiguration, error)
	DeletePackagingConfiguration(id string) error
	ListPackagingConfigurations(maxResults int, nextToken string) ([]*PackagingConfiguration, string, error)

	// Channel lifecycle policy
	PutChannelLifecyclePolicy(channelID, policy string) error
	GetChannelLifecyclePolicy(channelID string) (string, error)

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
	HlsIngest   *HlsIngest
	Tags        map[string]string
	ARN         string
	ID          string
	Description string
}

// OriginEndpoint represents a MediaPackage origin endpoint.
type OriginEndpoint struct {
	Tags                   map[string]string
	ARN                    string
	ChannelID              string
	ID                     string
	Description            string
	ManifestName           string
	URL                    string
	Origination            string
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

// PackagingConfiguration represents a MediaPackage VOD packaging configuration.
type PackagingConfiguration struct {
	Tags             map[string]string `json:"tags,omitempty"`
	ARN              string
	ID               string
	PackagingGroupID string
	Description      string
	CreatedAt        string
}

var _ StorageBackend = (*InMemoryBackend)(nil)
