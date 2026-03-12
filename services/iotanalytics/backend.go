package iotanalytics

import (
	"fmt"
	"maps"
	"sync"
	"time"
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
}

// InMemoryBackend is the in-memory backend for IoT Analytics.
type InMemoryBackend struct {
	channels   map[string]*Channel
	datastores map[string]*Datastore
	datasets   map[string]*Dataset
	pipelines  map[string]*Pipeline
	tags       map[string]map[string]string
	mu         sync.RWMutex
}

// NewInMemoryBackend creates a new in-memory IoT Analytics backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		channels:   make(map[string]*Channel),
		datastores: make(map[string]*Datastore),
		datasets:   make(map[string]*Dataset),
		pipelines:  make(map[string]*Pipeline),
		tags:       make(map[string]map[string]string),
	}
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

// tagsToMap converts a slice of tagDTO to a map.
func tagsToMap(tags []TagDTO) map[string]string {
	m := make(map[string]string, len(tags))
	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTags converts a map to a slice of tagDTO.
func mapToTags(m map[string]string) []TagDTO {
	result := make([]TagDTO, 0, len(m))
	for k, v := range m {
		result = append(result, TagDTO{Key: k, Value: v})
	}

	return result
}

// CreateChannel creates a new IoT Analytics channel.
func (b *InMemoryBackend) CreateChannel(name string, tags map[string]string) (*Channel, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.channels[name]; ok {
		return b.channels[name], nil
	}

	now := formatTime(time.Now())
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

	return c, nil
}

// DescribeChannel returns channel metadata.
func (b *InMemoryBackend) DescribeChannel(name string) (*Channel, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	c, ok := b.channels[name]
	if !ok {
		return nil, ErrChannelNotFound
	}

	return c, nil
}

// UpdateChannel updates a channel's last update time.
func (b *InMemoryBackend) UpdateChannel(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	c, ok := b.channels[name]
	if !ok {
		return ErrChannelNotFound
	}

	c.LastUpdate = formatTime(time.Now())

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

// ListChannels returns all channels.
func (b *InMemoryBackend) ListChannels() []*Channel {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Channel, 0, len(b.channels))
	for _, c := range b.channels {
		result = append(result, c)
	}

	return result
}

// CreateDatastore creates a new IoT Analytics datastore.
func (b *InMemoryBackend) CreateDatastore(name string, tags map[string]string) (*Datastore, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datastores[name]; ok {
		return b.datastores[name], nil
	}

	now := formatTime(time.Now())
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

	return d, nil
}

// DescribeDatastore returns datastore metadata.
func (b *InMemoryBackend) DescribeDatastore(name string) (*Datastore, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.datastores[name]
	if !ok {
		return nil, ErrDatastoreNotFound
	}

	return d, nil
}

// UpdateDatastore updates a datastore's last update time.
func (b *InMemoryBackend) UpdateDatastore(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datastores[name]
	if !ok {
		return ErrDatastoreNotFound
	}

	d.LastUpdate = formatTime(time.Now())

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

// ListDatastores returns all datastores.
func (b *InMemoryBackend) ListDatastores() []*Datastore {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Datastore, 0, len(b.datastores))
	for _, d := range b.datastores {
		result = append(result, d)
	}

	return result
}

// CreateDataset creates a new IoT Analytics dataset.
func (b *InMemoryBackend) CreateDataset(name string, tags map[string]string) (*Dataset, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.datasets[name]; ok {
		return b.datasets[name], nil
	}

	now := formatTime(time.Now())
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

	return d, nil
}

// DescribeDataset returns dataset metadata.
func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	d, ok := b.datasets[name]
	if !ok {
		return nil, ErrDatasetNotFound
	}

	return d, nil
}

// UpdateDataset updates a dataset's last update time.
func (b *InMemoryBackend) UpdateDataset(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	d, ok := b.datasets[name]
	if !ok {
		return ErrDatasetNotFound
	}

	d.LastUpdate = formatTime(time.Now())

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

// ListDatasets returns all datasets.
func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Dataset, 0, len(b.datasets))
	for _, d := range b.datasets {
		result = append(result, d)
	}

	return result
}

// CreatePipeline creates a new IoT Analytics pipeline.
func (b *InMemoryBackend) CreatePipeline(name string, tags map[string]string) (*Pipeline, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.pipelines[name]; ok {
		return b.pipelines[name], nil
	}

	now := formatTime(time.Now())
	arn := pipelineARN(name)
	p := &Pipeline{
		Name:         name,
		ARN:          arn,
		CreationTime: now,
		LastUpdate:   now,
		Tags:         make(map[string]string),
	}
	maps.Copy(p.Tags, tags)
	b.pipelines[name] = p
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return p, nil
}

// DescribePipeline returns pipeline metadata.
func (b *InMemoryBackend) DescribePipeline(name string) (*Pipeline, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	p, ok := b.pipelines[name]
	if !ok {
		return nil, ErrPipelineNotFound
	}

	return p, nil
}

// UpdatePipeline updates a pipeline's last update time.
func (b *InMemoryBackend) UpdatePipeline(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	p, ok := b.pipelines[name]
	if !ok {
		return ErrPipelineNotFound
	}

	p.LastUpdate = formatTime(time.Now())

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

// ListPipelines returns all pipelines.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]*Pipeline, 0, len(b.pipelines))
	for _, p := range b.pipelines {
		result = append(result, p)
	}

	return result
}

// ListTagsForResource returns tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]TagDTO, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return nil, ErrChannelNotFound
	}

	return mapToTags(m), nil
}

// TagResource adds or updates tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []TagDTO) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrChannelNotFound
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
		return ErrChannelNotFound
	}

	for _, k := range tagKeys {
		delete(m, k)
	}

	return nil
}
