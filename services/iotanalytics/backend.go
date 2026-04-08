package iotanalytics

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
)

// StorageBackend is the interface for the IoT Analytics backend.
type StorageBackend interface {
	CreateChannel(name string, tags map[string]string) (*Channel, error)
	DescribeChannel(name string) (*Channel, error)
	UpdateChannel(name string) error
	DeleteChannel(name string) error
	ListChannels() []*Channel

	CreateDatastore(name string, tags map[string]string) (*Datastore, error)
	DescribeDatastore(name string) (*Datastore, error)
	UpdateDatastore(name string) error
	DeleteDatastore(name string) error
	ListDatastores() []*Datastore

	CreateDataset(name string, tags map[string]string) (*Dataset, error)
	DescribeDataset(name string) (*Dataset, error)
	UpdateDataset(name string) error
	DeleteDataset(name string) error
	ListDatasets() []*Dataset

	CreatePipeline(name string, tags map[string]string) (*Pipeline, error)
	DescribePipeline(name string) (*Pipeline, error)
	UpdatePipeline(name string) error
	DeletePipeline(name string) error
	ListPipelines() []*Pipeline

	ListTagsForResource(resourceARN string) ([]TagDTO, error)
	TagResource(resourceARN string, tags []TagDTO) error
	UntagResource(resourceARN string, tagKeys []string) error

	BatchPutMessage(channelName string, messages []messageInput) ([]BatchPutMessageErrorEntry, error)
	SampleChannelData(channelName string) ([][]byte, error)

	StartPipelineReprocessing(pipelineName string) (string, error)
	CancelPipelineReprocessing(pipelineName, reprocessingID string) error

	CreateDatasetContent(datasetName string) (*DatasetContent, error)
	GetDatasetContent(datasetName, versionID string) (*DatasetContent, error)
	ListDatasetContents(datasetName string) ([]*DatasetContent, error)
	DeleteDatasetContent(datasetName, versionID string) error

	DescribeLoggingOptions() (*LoggingOptions, error)
	PutLoggingOptions(options *LoggingOptions) error

	RunPipelineActivity(payloads [][]byte) ([][]byte, error)

	Reset()
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// maxChannelMessages caps the number of messages stored per channel to prevent unbounded memory growth.
const maxChannelMessages = 1000

// InMemoryBackend is the in-memory backend for IoT Analytics.
type InMemoryBackend struct {
	loggingOptions  *LoggingOptions
	channelMessages map[string][][]byte
	datasetContents map[string][]*DatasetContent
	channels        map[string]*Channel
	datastores      map[string]*Datastore
	datasets        map[string]*Dataset
	pipelines       map[string]*Pipeline
	tags            map[string]map[string]string
	mu              sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory IoT Analytics backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		channels:        make(map[string]*Channel),
		datastores:      make(map[string]*Datastore),
		datasets:        make(map[string]*Dataset),
		pipelines:       make(map[string]*Pipeline),
		tags:            make(map[string]map[string]string),
		channelMessages: make(map[string][][]byte),
		datasetContents: make(map[string][]*DatasetContent),
	}
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.channels = make(map[string]*Channel)
	b.datastores = make(map[string]*Datastore)
	b.datasets = make(map[string]*Dataset)
	b.pipelines = make(map[string]*Pipeline)
	b.tags = make(map[string]map[string]string)
	b.channelMessages = make(map[string][][]byte)
	b.datasetContents = make(map[string][]*DatasetContent)
	b.loggingOptions = nil
}

// channelARN returns the ARN for an IoT Analytics channel.
func channelARN(name string) string {
	return fmt.Sprintf("arn:aws:iotanalytics:us-east-1:000000000000:channel/%s", name)
}

// datastoreARN returns the ARN for an IoT Analytics datastore.
func datastoreARN(name string) string {
	return fmt.Sprintf("arn:aws:iotanalytics:us-east-1:000000000000:datastore/%s", name)
}

// datasetARN returns the ARN for an IoT Analytics dataset.
func datasetARN(name string) string {
	return fmt.Sprintf("arn:aws:iotanalytics:us-east-1:000000000000:dataset/%s", name)
}

// pipelineARN returns the ARN for an IoT Analytics pipeline.
func pipelineARN(name string) string {
	return fmt.Sprintf("arn:aws:iotanalytics:us-east-1:000000000000:pipeline/%s", name)
}

// sortedKeys returns the keys of map m in sorted order.
func sortedKeys[K cmp.Ordered, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

// tagsToMap converts a slice of tagDTO to a map.
func tagsToMap(tags []TagDTO) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTagsSorted converts a map to a slice of tagDTO sorted by key.
func mapToTagsSorted(m map[string]string) []tagDTO {
	keys := sortedKeys(m)
	result := make([]tagDTO, 0, len(m))

	for _, k := range keys {
		result = append(result, tagDTO{Key: k, Value: m[k]})
	}

	return result
}

// cloneChannel returns a deep copy of c.
func cloneChannel(c *Channel) *Channel {
	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)

	return &cp
}

// cloneDatastore returns a deep copy of d.
func cloneDatastore(d *Datastore) *Datastore {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)

	return &cp
}

// cloneDataset returns a deep copy of d.
func cloneDataset(d *Dataset) *Dataset {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)

	return &cp
}

// clonePipeline returns a deep copy of p.
func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)

	if p.ReprocessingSummaries != nil {
		cp.ReprocessingSummaries = make([]string, len(p.ReprocessingSummaries))
		copy(cp.ReprocessingSummaries, p.ReprocessingSummaries)
	}

	cp.Reprocessings = make(map[string]*PipelineReprocessing, len(p.Reprocessings))
	for k, v := range p.Reprocessings {
		rpCp := *v
		cp.Reprocessings[k] = &rpCp
	}

	return &cp
}

// CreateChannel creates a new IoT Analytics channel.
func (b *InMemoryBackend) CreateChannel(name string, tags map[string]string) (*Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.channels[name]; ok {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := channelARN(name)
	c := &Channel{
		Name:         name,
		ARN:          arn,
		Status:       "ACTIVE",
		CreationTime: now,
		LastUpdate:   now,
		Tags:         make(map[string]string),
	}
	maps.Copy(c.Tags, tags)
	b.channels[name] = c
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneChannel(c), nil
}

// DescribeChannel returns channel metadata.
func (b *InMemoryBackend) DescribeChannel(name string) (*Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.channels[name]
	if !ok {
		return nil, ErrChannelNotFound
	}

	return cloneChannel(c), nil
}

// UpdateChannel updates a channel's last update time.
func (b *InMemoryBackend) UpdateChannel(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.channels[name]
	if !ok {
		return ErrChannelNotFound
	}

	c.LastUpdate = epochSeconds(time.Now())

	return nil
}

// DeleteChannel deletes a channel.
func (b *InMemoryBackend) DeleteChannel(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.channels[name]
	if !ok {
		return ErrChannelNotFound
	}

	delete(b.tags, c.ARN)
	delete(b.channels, name)

	return nil
}

// ListChannels returns all channels sorted by name.
func (b *InMemoryBackend) ListChannels() []*Channel {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.channels)
	result := make([]*Channel, 0, len(b.channels))

	for _, k := range keys {
		result = append(result, cloneChannel(b.channels[k]))
	}

	return result
}

// AddChannelInternal seeds a channel by name (test helper).
func (b *InMemoryBackend) AddChannelInternal(name string) *Channel {
	c, _ := b.CreateChannel(name, nil)

	return c
}

// CreateDatastore creates a new IoT Analytics datastore.
func (b *InMemoryBackend) CreateDatastore(name string, tags map[string]string) (*Datastore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datastores[name]; ok {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := datastoreARN(name)
	d := &Datastore{
		Name:         name,
		ARN:          arn,
		Status:       "ACTIVE",
		CreationTime: now,
		LastUpdate:   now,
		Tags:         make(map[string]string),
	}
	maps.Copy(d.Tags, tags)
	b.datastores[name] = d
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneDatastore(d), nil
}

// DescribeDatastore returns datastore metadata.
func (b *InMemoryBackend) DescribeDatastore(name string) (*Datastore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.datastores[name]
	if !ok {
		return nil, ErrDatastoreNotFound
	}

	return cloneDatastore(d), nil
}

// UpdateDatastore updates a datastore's last update time.
func (b *InMemoryBackend) UpdateDatastore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datastores[name]
	if !ok {
		return ErrDatastoreNotFound
	}

	d.LastUpdate = epochSeconds(time.Now())

	return nil
}

// DeleteDatastore deletes a datastore.
func (b *InMemoryBackend) DeleteDatastore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datastores[name]
	if !ok {
		return ErrDatastoreNotFound
	}

	delete(b.tags, d.ARN)
	delete(b.datastores, name)

	return nil
}

// ListDatastores returns all datastores sorted by name.
func (b *InMemoryBackend) ListDatastores() []*Datastore {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.datastores)
	result := make([]*Datastore, 0, len(b.datastores))

	for _, k := range keys {
		result = append(result, cloneDatastore(b.datastores[k]))
	}

	return result
}

// AddDatastoreInternal seeds a datastore by name (test helper).
func (b *InMemoryBackend) AddDatastoreInternal(name string) *Datastore {
	d, _ := b.CreateDatastore(name, nil)

	return d
}

// CreateDataset creates a new IoT Analytics dataset.
func (b *InMemoryBackend) CreateDataset(name string, tags map[string]string) (*Dataset, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datasets[name]; ok {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := datasetARN(name)
	d := &Dataset{
		Name:         name,
		ARN:          arn,
		Status:       "ACTIVE",
		CreationTime: now,
		LastUpdate:   now,
		Tags:         make(map[string]string),
	}
	maps.Copy(d.Tags, tags)
	b.datasets[name] = d
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneDataset(d), nil
}

// DescribeDataset returns dataset metadata.
func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.datasets[name]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	return cloneDataset(d), nil
}

// UpdateDataset updates a dataset's last update time.
func (b *InMemoryBackend) UpdateDataset(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datasets[name]
	if !ok {
		return ErrDatasetNotFound
	}

	d.LastUpdate = epochSeconds(time.Now())

	return nil
}

// DeleteDataset deletes a dataset.
func (b *InMemoryBackend) DeleteDataset(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datasets[name]
	if !ok {
		return ErrDatasetNotFound
	}

	delete(b.tags, d.ARN)
	delete(b.datasets, name)

	return nil
}

// ListDatasets returns all datasets sorted by name.
func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.datasets)
	result := make([]*Dataset, 0, len(b.datasets))

	for _, k := range keys {
		result = append(result, cloneDataset(b.datasets[k]))
	}

	return result
}

// AddDatasetInternal seeds a dataset by name (test helper).
func (b *InMemoryBackend) AddDatasetInternal(name string) *Dataset {
	d, _ := b.CreateDataset(name, nil)

	return d
}

// CreatePipeline creates a new IoT Analytics pipeline.
func (b *InMemoryBackend) CreatePipeline(name string, tags map[string]string) (*Pipeline, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.pipelines[name]; ok {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := pipelineARN(name)
	p := &Pipeline{
		Name:                  name,
		ARN:                   arn,
		CreationTime:          now,
		LastUpdate:            now,
		Tags:                  make(map[string]string),
		Reprocessings:         make(map[string]*PipelineReprocessing),
		ReprocessingSummaries: []string{},
	}
	maps.Copy(p.Tags, tags)
	b.pipelines[name] = p
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return clonePipeline(p), nil
}

// DescribePipeline returns pipeline metadata.
func (b *InMemoryBackend) DescribePipeline(name string) (*Pipeline, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.pipelines[name]
	if !ok {
		return nil, ErrPipelineNotFound
	}

	return clonePipeline(p), nil
}

// UpdatePipeline updates a pipeline's last update time.
func (b *InMemoryBackend) UpdatePipeline(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pipelines[name]
	if !ok {
		return ErrPipelineNotFound
	}

	p.LastUpdate = epochSeconds(time.Now())

	return nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pipelines[name]
	if !ok {
		return ErrPipelineNotFound
	}

	delete(b.tags, p.ARN)
	delete(b.pipelines, name)

	return nil
}

// ListPipelines returns all pipelines sorted by name.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock()
	defer b.mu.RUnlock()

	keys := sortedKeys(b.pipelines)
	result := make([]*Pipeline, 0, len(b.pipelines))

	for _, k := range keys {
		result = append(result, clonePipeline(b.pipelines[k]))
	}

	return result
}

// AddPipelineInternal seeds a pipeline by name (test helper).
func (b *InMemoryBackend) AddPipelineInternal(name string) *Pipeline {
	p, _ := b.CreatePipeline(name, nil)

	return p
}

// ListTagsForResource returns tags for a resource ARN, sorted by key.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]TagDTO, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return nil, ErrResourceNotFound
	}

	return mapToTagsSorted(m), nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []TagDTO) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}

// BatchPutMessage ingests messages into a channel, capping at maxChannelMessages per channel.
func (b *InMemoryBackend) BatchPutMessage(
	channelName string,
	messages []messageInput,
) ([]BatchPutMessageErrorEntry, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var errs []BatchPutMessageErrorEntry

	if _, ok := b.channels[channelName]; !ok {
		for _, msg := range messages {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    "ResourceNotFoundException",
				ErrorMessage: "channel not found: " + channelName,
				MessageID:    msg.MessageID,
			})
		}

		if errs == nil {
			errs = []BatchPutMessageErrorEntry{}
		}

		return errs, nil
	}

	for _, msg := range messages {
		current := b.channelMessages[channelName]
		if len(current) < maxChannelMessages {
			b.channelMessages[channelName] = append(current, msg.Payload)
		}
	}

	return []BatchPutMessageErrorEntry{}, nil
}

// SampleChannelData returns up to 10 sample messages from a channel.
func (b *InMemoryBackend) SampleChannelData(channelName string) ([][]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.channels[channelName]; !ok {
		return nil, ErrChannelNotFound
	}

	msgs := b.channelMessages[channelName]
	if len(msgs) == 0 {
		return [][]byte{}, nil
	}

	const maxSamples = 10

	end := min(len(msgs), maxSamples)

	result := make([][]byte, end)
	copy(result, msgs[:end])

	return result, nil
}

// StartPipelineReprocessing creates a new reprocessing job for a pipeline.
func (b *InMemoryBackend) StartPipelineReprocessing(pipelineName string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pipelines[pipelineName]
	if !ok {
		return "", ErrPipelineNotFound
	}

	id := uuid.NewString()
	now := epochSeconds(time.Now())

	rp := &PipelineReprocessing{
		ID:           id,
		Status:       "RUNNING",
		CreationTime: now,
	}

	if p.Reprocessings == nil {
		p.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	p.Reprocessings[id] = rp
	p.ReprocessingSummaries = append(p.ReprocessingSummaries, id)

	return id, nil
}

// CancelPipelineReprocessing cancels a running pipeline reprocessing job.
func (b *InMemoryBackend) CancelPipelineReprocessing(pipelineName, reprocessingID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pipelines[pipelineName]
	if !ok {
		return ErrPipelineNotFound
	}

	rp, ok := p.Reprocessings[reprocessingID]
	if !ok {
		return ErrReprocessingNotFound
	}

	rp.Status = "CANCELLED"
	rp.EndTime = epochSeconds(time.Now())

	return nil
}

// CreateDatasetContent creates a new content version for a dataset.
func (b *InMemoryBackend) CreateDatasetContent(datasetName string) (*DatasetContent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datasets[datasetName]; !ok {
		return nil, ErrDatasetNotFound
	}

	now := epochSeconds(time.Now())
	content := &DatasetContent{
		VersionID:      uuid.NewString(),
		Status:         "SUCCEEDED",
		CreationTime:   now,
		CompletionTime: now,
	}

	b.datasetContents[datasetName] = append(b.datasetContents[datasetName], content)

	return content, nil
}

// GetDatasetContent retrieves a specific or the latest content version of a dataset.
func (b *InMemoryBackend) GetDatasetContent(datasetName, versionID string) (*DatasetContent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.datasets[datasetName]; !ok {
		return nil, ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]
	if len(contents) == 0 {
		return nil, ErrDatasetContentNotFound
	}

	if versionID == "" || versionID == "$latest" {
		return contents[len(contents)-1], nil
	}

	for _, c := range contents {
		if c.VersionID == versionID {
			return c, nil
		}
	}

	return nil, ErrDatasetContentNotFound
}

// ListDatasetContents returns all content versions for a dataset, sorted by creation time descending.
func (b *InMemoryBackend) ListDatasetContents(datasetName string) ([]*DatasetContent, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, ok := b.datasets[datasetName]; !ok {
		return nil, ErrDatasetNotFound
	}

	contents := b.datasetContents[datasetName]
	result := make([]*DatasetContent, len(contents))
	copy(result, contents)

	slices.SortFunc(result, func(a, b *DatasetContent) int {
		return cmp.Compare(b.CreationTime, a.CreationTime)
	})

	return result, nil
}

// DeleteDatasetContent deletes a specific content version (or all if versionID is empty).
func (b *InMemoryBackend) DeleteDatasetContent(datasetName, versionID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datasets[datasetName]; !ok {
		return ErrDatasetNotFound
	}

	if versionID == "" {
		b.datasetContents[datasetName] = nil

		return nil
	}

	contents := b.datasetContents[datasetName]

	for i, c := range contents {
		if c.VersionID == versionID {
			b.datasetContents[datasetName] = append(contents[:i], contents[i+1:]...)

			return nil
		}
	}

	return ErrDatasetContentNotFound
}

// DescribeLoggingOptions returns the current IoT Analytics logging options.
func (b *InMemoryBackend) DescribeLoggingOptions() (*LoggingOptions, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.loggingOptions == nil {
		return nil, ErrLoggingOptionsNotFound
	}

	opts := *b.loggingOptions

	return &opts, nil
}

// PutLoggingOptions sets the IoT Analytics logging options.
func (b *InMemoryBackend) PutLoggingOptions(options *LoggingOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	opts := *options
	b.loggingOptions = &opts

	return nil
}

// RunPipelineActivity runs payloads through a pipeline activity and returns the results.
// For the in-memory backend this is a pass-through; payloads are returned unchanged.
func (b *InMemoryBackend) RunPipelineActivity(payloads [][]byte) ([][]byte, error) {
	result := make([][]byte, len(payloads))
	copy(result, payloads)

	return result, nil
}
