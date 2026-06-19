package mediapackage

import (
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultMaxResults = 20

	originationAllow = "ALLOW"

	harvestJobStatusSucceeded = "SUCCEEDED"

	resourceTypeChannel        = "channels"
	resourceTypeOriginEndpoint = "origin_endpoints"
	resourceTypeHarvestJob     = "harvest_jobs"
)

// ErrNotFound is returned when a resource does not exist.
var ErrNotFound = awserr.New("NotFoundException", awserr.ErrNotFound)

// ErrConflict is returned for duplicate resource IDs.
var ErrConflict = awserr.New("UnprocessableEntityException", awserr.ErrAlreadyExists)

// ErrInvalidParameter is returned for invalid input.
var ErrInvalidParameter = awserr.New("UnprocessableEntityException", awserr.ErrInvalidParameter)

type storedIngestEndpoint struct {
	ID       string `json:"id"`
	URL      string `json:"url"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type storedChannel struct {
	Tags            map[string]string      `json:"tags"`
	ARN             string                 `json:"arn"`
	ID              string                 `json:"id"`
	Description     string                 `json:"description"`
	IngestEndpoints []storedIngestEndpoint `json:"ingestEndpoints"`
}

func (c *storedChannel) toChannel() *Channel {
	tags := make(map[string]string, len(c.Tags))
	maps.Copy(tags, c.Tags)

	endpoints := make([]*IngestEndpoint, 0, len(c.IngestEndpoints))
	for _, ep := range c.IngestEndpoints {
		endpoints = append(endpoints, &IngestEndpoint{
			ID:       ep.ID,
			URL:      ep.URL,
			Username: ep.Username,
			Password: ep.Password,
		})
	}

	return &Channel{
		ARN:         c.ARN,
		ID:          c.ID,
		Description: c.Description,
		HlsIngest:   &HlsIngest{IngestEndpoints: endpoints},
		Tags:        tags,
	}
}

type storedOriginEndpoint struct {
	Tags                   map[string]string `json:"tags"`
	ARN                    string            `json:"arn"`
	ChannelID              string            `json:"channelId"`
	ID                     string            `json:"id"`
	Description            string            `json:"description"`
	ManifestName           string            `json:"manifestName"`
	URL                    string            `json:"url"`
	Origination            string            `json:"origination"`
	Whitelist              []string          `json:"whitelist"`
	StartoverWindowSeconds int               `json:"startoverWindowSeconds"`
	TimeDelaySeconds       int               `json:"timeDelaySeconds"`
}

func (e *storedOriginEndpoint) toOriginEndpoint() *OriginEndpoint {
	tags := make(map[string]string, len(e.Tags))
	maps.Copy(tags, e.Tags)

	whitelist := make([]string, len(e.Whitelist))
	copy(whitelist, e.Whitelist)

	return &OriginEndpoint{
		ARN:                    e.ARN,
		ChannelID:              e.ChannelID,
		ID:                     e.ID,
		Description:            e.Description,
		ManifestName:           e.ManifestName,
		URL:                    e.URL,
		Origination:            e.Origination,
		StartoverWindowSeconds: e.StartoverWindowSeconds,
		TimeDelaySeconds:       e.TimeDelaySeconds,
		Whitelist:              whitelist,
		Tags:                   tags,
	}
}

type storedS3Destination struct {
	BucketName  string `json:"bucketName"`
	ManifestKey string `json:"manifestKey"`
	RoleArn     string `json:"roleArn"`
}

type storedHarvestJob struct {
	S3Destination    *storedS3Destination `json:"s3Destination"`
	ARN              string               `json:"arn"`
	ChannelID        string               `json:"channelId"`
	CreatedAt        string               `json:"createdAt"`
	EndTime          string               `json:"endTime"`
	ID               string               `json:"id"`
	OriginEndpointID string               `json:"originEndpointId"`
	StartTime        string               `json:"startTime"`
	Status           string               `json:"status"`
}

func (j *storedHarvestJob) toHarvestJob() *HarvestJob {
	var dest *S3Destination
	if j.S3Destination != nil {
		dest = &S3Destination{
			BucketName:  j.S3Destination.BucketName,
			ManifestKey: j.S3Destination.ManifestKey,
			RoleArn:     j.S3Destination.RoleArn,
		}
	}

	return &HarvestJob{
		ARN:              j.ARN,
		ChannelID:        j.ChannelID,
		CreatedAt:        j.CreatedAt,
		EndTime:          j.EndTime,
		ID:               j.ID,
		OriginEndpointID: j.OriginEndpointID,
		S3Destination:    dest,
		StartTime:        j.StartTime,
		Status:           j.Status,
	}
}

type snapshot struct {
	Channels        map[string]*storedChannel        `json:"channels"`
	OriginEndpoints map[string]*storedOriginEndpoint `json:"originEndpoints"`
	HarvestJobs     map[string]*storedHarvestJob     `json:"harvestJobs"`
	Tags            map[string]map[string]string     `json:"tags"`
	AccountID       string                           `json:"accountId"`
	Region          string                           `json:"region"`
}

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	mu              *lockmetrics.RWMutex
	channels        map[string]*storedChannel
	originEndpoints map[string]*storedOriginEndpoint
	harvestJobs     map[string]*storedHarvestJob
	tags            map[string]map[string]string
	accountID       string
	region          string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		mu:              lockmetrics.New("mediapackage"),
		channels:        make(map[string]*storedChannel),
		originEndpoints: make(map[string]*storedOriginEndpoint),
		harvestJobs:     make(map[string]*storedHarvestJob),
		tags:            make(map[string]map[string]string),
		accountID:       accountID,
		region:          region,
	}
}

// AccountID returns the configured AWS account ID.
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// Region returns the configured AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// Reset clears all stored data.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.channels = make(map[string]*storedChannel)
	b.originEndpoints = make(map[string]*storedOriginEndpoint)
	b.harvestJobs = make(map[string]*storedHarvestJob)
	b.tags = make(map[string]map[string]string)
}

// Snapshot returns a JSON-encoded snapshot of the backend state.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := snapshot{
		AccountID:       b.accountID,
		Region:          b.region,
		Channels:        b.channels,
		OriginEndpoints: b.originEndpoints,
		HarvestJobs:     b.harvestJobs,
		Tags:            b.tags,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap snapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.channels = snap.Channels
	b.originEndpoints = snap.OriginEndpoints
	b.harvestJobs = snap.HarvestJobs
	b.tags = snap.Tags

	return nil
}

func (b *InMemoryBackend) buildChannelARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeChannel+"/"+id)
}

func (b *InMemoryBackend) buildOriginEndpointARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeOriginEndpoint+"/"+id)
}

func (b *InMemoryBackend) buildHarvestJobARN(id string) string {
	return arn.Build("mediapackage", b.region, b.accountID, resourceTypeHarvestJob+"/"+id)
}

func newIngestEndpoints(region, channelID string) []storedIngestEndpoint {
	return []storedIngestEndpoint{
		{
			ID:       uuid.NewString(),
			URL:      fmt.Sprintf("https://ingest1.mediapackage.%s.amazonaws.com/in/v2/%s/channel", region, channelID),
			Username: uuid.NewString(),
			Password: uuid.NewString(),
		},
		{
			ID:       uuid.NewString(),
			URL:      fmt.Sprintf("https://ingest2.mediapackage.%s.amazonaws.com/in/v2/%s/channel", region, channelID),
			Username: uuid.NewString(),
			Password: uuid.NewString(),
		},
	}
}

// CreateChannel creates a new MediaPackage channel.
func (b *InMemoryBackend) CreateChannel(id, description string, tags map[string]string) (*Channel, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if _, exists := b.channels[id]; exists {
		return nil, fmt.Errorf("%w: channel %q already exists", ErrConflict, id)
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	ch := &storedChannel{
		ARN:             b.buildChannelARN(id),
		ID:              id,
		Description:     description,
		Tags:            tagsCopy,
		IngestEndpoints: newIngestEndpoints(b.region, id),
	}

	b.channels[id] = ch

	return ch.toChannel(), nil
}

// DescribeChannel returns a channel by ID.
func (b *InMemoryBackend) DescribeChannel(id string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, id)
	}

	return ch.toChannel(), nil
}

// UpdateChannel updates a channel's description.
func (b *InMemoryBackend) UpdateChannel(id, description string) (*Channel, error) {
	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, id)
	}

	ch.Description = description

	return ch.toChannel(), nil
}

// DeleteChannel deletes a channel and all its origin endpoints.
func (b *InMemoryBackend) DeleteChannel(id string) (*Channel, error) {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, id)
	}

	// Delete associated origin endpoints.
	for epID, ep := range b.originEndpoints {
		if ep.ChannelID == id {
			delete(b.originEndpoints, epID)
		}
	}

	delete(b.channels, id)

	return ch.toChannel(), nil
}

// ListChannels returns a paginated list of channels sorted by ID.
func (b *InMemoryBackend) ListChannels(maxResults int, nextToken string) ([]*Channel, string, error) {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.channels))
	for id := range b.channels {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	all := make([]*storedChannel, 0, len(ids))
	for _, id := range ids {
		all = append(all, b.channels[id])
	}

	p := page.New(all, nextToken, maxResults, defaultMaxResults)

	channels := make([]*Channel, 0, len(p.Data))
	for _, ch := range p.Data {
		channels = append(channels, ch.toChannel())
	}

	return channels, p.Next, nil
}

// ConfigureLogs updates the log group configuration for a channel.
func (b *InMemoryBackend) ConfigureLogs(id, egressLogGroup, ingressLogGroup string) (*Channel, error) {
	b.mu.Lock("ConfigureLogs")
	defer b.mu.Unlock()

	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, id)
	}

	// Log groups are stored as metadata only; return the updated channel.
	_ = egressLogGroup
	_ = ingressLogGroup

	return ch.toChannel(), nil
}

// RotateChannelCredentials rotates the ingest endpoint credentials for a channel.
func (b *InMemoryBackend) RotateChannelCredentials(id string) (*Channel, error) {
	b.mu.Lock("RotateChannelCredentials")
	defer b.mu.Unlock()

	ch, ok := b.channels[id]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, id)
	}

	ch.IngestEndpoints = newIngestEndpoints(b.region, id)

	return ch.toChannel(), nil
}

// CreateOriginEndpoint creates a new origin endpoint.
func (b *InMemoryBackend) CreateOriginEndpoint(
	channelID, id, description, manifestName string,
	startoverWindowSeconds, timeDelaySeconds int,
	origination string,
	whitelist []string,
	tags map[string]string,
) (*OriginEndpoint, error) {
	if channelID == "" {
		return nil, fmt.Errorf("%w: ChannelId is required", ErrInvalidParameter)
	}

	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateOriginEndpoint")
	defer b.mu.Unlock()

	if _, exists := b.channels[channelID]; !exists {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, channelID)
	}

	if _, exists := b.originEndpoints[id]; exists {
		return nil, fmt.Errorf("%w: origin endpoint %q already exists", ErrConflict, id)
	}

	if origination == "" {
		origination = originationAllow
	}

	if manifestName == "" {
		manifestName = id
	}

	tagsCopy := make(map[string]string, len(tags))
	maps.Copy(tagsCopy, tags)

	wl := make([]string, len(whitelist))
	copy(wl, whitelist)

	ep := &storedOriginEndpoint{
		ARN:          b.buildOriginEndpointARN(id),
		ChannelID:    channelID,
		ID:           id,
		Description:  description,
		ManifestName: manifestName,
		URL: fmt.Sprintf(
			"https://cf.mediapackage.%s.amazonaws.com/out/v1/%s/%s.m3u8",
			b.region,
			id,
			manifestName,
		),
		Origination:            origination,
		StartoverWindowSeconds: startoverWindowSeconds,
		TimeDelaySeconds:       timeDelaySeconds,
		Whitelist:              wl,
		Tags:                   tagsCopy,
	}

	b.originEndpoints[id] = ep

	return ep.toOriginEndpoint(), nil
}

// DescribeOriginEndpoint returns an origin endpoint by ID.
func (b *InMemoryBackend) DescribeOriginEndpoint(id string) (*OriginEndpoint, error) {
	b.mu.RLock("DescribeOriginEndpoint")
	defer b.mu.RUnlock()

	ep, ok := b.originEndpoints[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin endpoint %q not found", ErrNotFound, id)
	}

	return ep.toOriginEndpoint(), nil
}

// UpdateOriginEndpoint updates an origin endpoint's configuration.
func (b *InMemoryBackend) UpdateOriginEndpoint(
	id, description, manifestName string,
	startoverWindowSeconds, timeDelaySeconds int,
	origination string,
	whitelist []string,
) (*OriginEndpoint, error) {
	b.mu.Lock("UpdateOriginEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.originEndpoints[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin endpoint %q not found", ErrNotFound, id)
	}

	if description != "" {
		ep.Description = description
	}

	if manifestName != "" {
		ep.ManifestName = manifestName
	}

	if origination != "" {
		ep.Origination = origination
	}

	if startoverWindowSeconds >= 0 {
		ep.StartoverWindowSeconds = startoverWindowSeconds
	}

	if timeDelaySeconds >= 0 {
		ep.TimeDelaySeconds = timeDelaySeconds
	}

	if whitelist != nil {
		wl := make([]string, len(whitelist))
		copy(wl, whitelist)
		ep.Whitelist = wl
	}

	return ep.toOriginEndpoint(), nil
}

// DeleteOriginEndpoint deletes an origin endpoint.
func (b *InMemoryBackend) DeleteOriginEndpoint(id string) (*OriginEndpoint, error) {
	b.mu.Lock("DeleteOriginEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.originEndpoints[id]
	if !ok {
		return nil, fmt.Errorf("%w: origin endpoint %q not found", ErrNotFound, id)
	}

	delete(b.originEndpoints, id)

	return ep.toOriginEndpoint(), nil
}

// ListOriginEndpoints returns a paginated list of origin endpoints.
func (b *InMemoryBackend) ListOriginEndpoints(
	channelID string,
	maxResults int,
	nextToken string,
) ([]*OriginEndpoint, string, error) {
	b.mu.RLock("ListOriginEndpoints")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.originEndpoints))
	for id, ep := range b.originEndpoints {
		if channelID == "" || ep.ChannelID == channelID {
			ids = append(ids, id)
		}
	}

	sort.Strings(ids)

	all := make([]*storedOriginEndpoint, 0, len(ids))
	for _, id := range ids {
		all = append(all, b.originEndpoints[id])
	}

	p := page.New(all, nextToken, maxResults, defaultMaxResults)

	endpoints := make([]*OriginEndpoint, 0, len(p.Data))
	for _, ep := range p.Data {
		endpoints = append(endpoints, ep.toOriginEndpoint())
	}

	return endpoints, p.Next, nil
}

// RotateIngestEndpointCredentials rotates credentials for a specific ingest endpoint.
func (b *InMemoryBackend) RotateIngestEndpointCredentials(channelID, ingestEndpointID string) (*Channel, error) {
	b.mu.Lock("RotateIngestEndpointCredentials")
	defer b.mu.Unlock()

	ch, ok := b.channels[channelID]
	if !ok {
		return nil, fmt.Errorf("%w: channel %q not found", ErrNotFound, channelID)
	}

	found := false

	for i, ep := range ch.IngestEndpoints {
		if ep.ID == ingestEndpointID {
			ch.IngestEndpoints[i].Username = uuid.NewString()
			ch.IngestEndpoints[i].Password = uuid.NewString()
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf(
			"%w: ingest endpoint %q not found in channel %q",
			ErrNotFound,
			ingestEndpointID,
			channelID,
		)
	}

	return ch.toChannel(), nil
}

// CreateHarvestJob creates a new harvest job record.
func (b *InMemoryBackend) CreateHarvestJob(
	id, originEndpointID, startTime, endTime string,
	s3Dest S3Destination,
) (*HarvestJob, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	if originEndpointID == "" {
		return nil, fmt.Errorf("%w: OriginEndpointId is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateHarvestJob")
	defer b.mu.Unlock()

	if _, exists := b.harvestJobs[id]; exists {
		return nil, fmt.Errorf("%w: harvest job %q already exists", ErrConflict, id)
	}

	ep, ok := b.originEndpoints[originEndpointID]
	if !ok {
		return nil, fmt.Errorf("%w: origin endpoint %q not found", ErrNotFound, originEndpointID)
	}

	job := &storedHarvestJob{
		ARN:              b.buildHarvestJobARN(id),
		ChannelID:        ep.ChannelID,
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		EndTime:          endTime,
		ID:               id,
		OriginEndpointID: originEndpointID,
		S3Destination: &storedS3Destination{
			BucketName:  s3Dest.BucketName,
			ManifestKey: s3Dest.ManifestKey,
			RoleArn:     s3Dest.RoleArn,
		},
		StartTime: startTime,
		Status:    harvestJobStatusSucceeded,
	}

	b.harvestJobs[id] = job

	return job.toHarvestJob(), nil
}

// DescribeHarvestJob returns a harvest job by ID.
func (b *InMemoryBackend) DescribeHarvestJob(id string) (*HarvestJob, error) {
	b.mu.RLock("DescribeHarvestJob")
	defer b.mu.RUnlock()

	job, ok := b.harvestJobs[id]
	if !ok {
		return nil, fmt.Errorf("%w: harvest job %q not found", ErrNotFound, id)
	}

	return job.toHarvestJob(), nil
}

// ListHarvestJobs returns a paginated list of harvest jobs, optionally filtered by channel or status.
func (b *InMemoryBackend) ListHarvestJobs(
	includeChannelID, includeStatus string,
	maxResults int,
	nextToken string,
) ([]*HarvestJob, string, error) {
	b.mu.RLock("ListHarvestJobs")
	defer b.mu.RUnlock()

	ids := make([]string, 0, len(b.harvestJobs))
	for id, job := range b.harvestJobs {
		if includeChannelID != "" && job.ChannelID != includeChannelID {
			continue
		}

		if includeStatus != "" && job.Status != includeStatus {
			continue
		}

		ids = append(ids, id)
	}

	sort.Strings(ids)

	all := make([]*storedHarvestJob, 0, len(ids))
	for _, id := range ids {
		all = append(all, b.harvestJobs[id])
	}

	p := page.New(all, nextToken, maxResults, defaultMaxResults)

	jobs := make([]*HarvestJob, 0, len(p.Data))
	for _, job := range p.Data {
		jobs = append(jobs, job.toHarvestJob())
	}

	return jobs, p.Next, nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if _, ok := b.tags[resourceARN]; !ok {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tags[resourceARN]; ok {
		for _, k := range keys {
			delete(existing, k)
		}
	}

	return nil
}

// ListTagsForResource returns all tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)

	if existing, ok := b.tags[resourceARN]; ok {
		maps.Copy(result, existing)
	}

	return result, nil
}
