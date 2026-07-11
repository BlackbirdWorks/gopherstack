package iotanalytics

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	statusActive          = "ACTIVE"
	errCodeInvalidRequest = "InvalidRequestException"
	maxRetentionDays      = 2147483647
	// defaultRegion is used when the request context carries no region (e.g.
	// an unsigned request); ARNs must still be well-formed.
	defaultRegion = config.DefaultRegion
)

// StorageBackend is the interface for the IoT Analytics backend.
type StorageBackend interface {
	CreateChannel(
		ctx context.Context,
		name string,
		tags map[string]string,
		storage *ChannelStorage,
		retention *RetentionPeriod,
	) (*Channel, error)
	DescribeChannel(name string) (*Channel, error)
	UpdateChannel(name string, storage *ChannelStorage, retention *RetentionPeriod) error
	DeleteChannel(name string) error
	ListChannels() []*Channel

	CreateDatastore(
		ctx context.Context,
		name string,
		tags map[string]string,
		storage *DatastoreStorage,
		retention *RetentionPeriod,
		fileFormat *FileFormatConfiguration,
		partitions *DatastorePartitions,
	) (*Datastore, error)
	DescribeDatastore(name string) (*Datastore, error)
	UpdateDatastore(
		name string,
		storage *DatastoreStorage,
		retention *RetentionPeriod,
		fileFormat *FileFormatConfiguration,
		partitions *DatastorePartitions,
	) error
	DeleteDatastore(name string) error
	ListDatastores() []*Datastore

	CreateDataset(
		ctx context.Context,
		name string,
		tags map[string]string,
		actions []DatasetAction,
		triggers []DatasetTrigger,
		contentDeliveryRules []ContentDeliveryRule,
		versioningConfig *VersioningConfiguration,
		lateDataRules []LateDataRule,
	) (*Dataset, error)
	DescribeDataset(name string) (*Dataset, error)
	UpdateDataset(
		name string,
		actions []DatasetAction,
		triggers []DatasetTrigger,
		contentDeliveryRules []ContentDeliveryRule,
		versioningConfig *VersioningConfiguration,
		lateDataRules []LateDataRule,
	) error
	DeleteDataset(name string) error
	ListDatasets() []*Dataset

	CreatePipeline(
		ctx context.Context,
		name string,
		tags map[string]string,
		activities []PipelineActivity,
	) (*Pipeline, error)
	DescribePipeline(name string) (*Pipeline, error)
	UpdatePipeline(name string, activities []PipelineActivity) error
	DeletePipeline(name string) error
	ListPipelines() []*Pipeline

	ListTagsForResource(resourceARN string) ([]TagDTO, error)
	TagResource(resourceARN string, tags []TagDTO) error
	UntagResource(resourceARN string, tagKeys []string) error

	BatchPutMessage(channelName string, messages []messageInput) ([]BatchPutMessageErrorEntry, error)
	SampleChannelData(channelName string, maxMessages int) ([][]byte, error)

	StartPipelineReprocessing(pipelineName string, startTime, endTime *float64) (string, error)
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

const (
	// maxChannelMessages caps the number of messages stored per channel.
	maxChannelMessages = 1000
	// maxDatasetContents caps the number of content versions stored per dataset.
	maxDatasetContents = 100
	// maxPipelineReprocessings caps reprocessing jobs stored per pipeline.
	maxPipelineReprocessings = 100
	// maxTagsPerResource caps the number of tags per resource, matching AWS limits.
	maxTagsPerResource = 50
	// maxResourceNameLen is the maximum allowed length for resource names.
	maxResourceNameLen = 128
	// maxTagKeyLen is the maximum allowed length for a tag key.
	maxTagKeyLen = 128
	// maxTagValueLen is the maximum allowed length for a tag value.
	maxTagValueLen = 256
	// maxBatchMessages is the maximum number of messages in a BatchPutMessage call.
	maxBatchMessages = 100
	// maxMessagePayloadBytes is the maximum payload size per message (128 KB).
	maxMessagePayloadBytes = 128 * 1024
	// maxBatchPayloadBytes is the maximum total payload size per batch (500 KB).
	maxBatchPayloadBytes = 500 * 1024
	// maxMessageIDLen is the maximum length for a message ID.
	maxMessageIDLen = 128
	// maxSampleMessages is the maximum number of sample messages.
	maxSampleMessages = 10
	// defaultSampleMessages is the default number of sample messages.
	defaultSampleMessages = 10
)

// validateResourceName checks that name is 1-128 ASCII letters, digits, or underscores.
// AWS IoT Analytics does not allow hyphens or non-ASCII letters.
func validateResourceName(name string) error {
	if len(name) == 0 || len(name) > maxResourceNameLen {
		return fmt.Errorf("%w: name must be 1-%d characters", ErrValidation, maxResourceNameLen)
	}

	for _, r := range name {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return fmt.Errorf("%w: name must contain only ASCII letters, digits, or underscores", ErrValidation)
		}

		if r > unicode.MaxASCII {
			return fmt.Errorf("%w: name must contain only ASCII characters", ErrValidation)
		}
	}

	return nil
}

// validateTagKey checks tag key constraints: 1-128 chars, valid charset, no aws: prefix.
func validateTagKey(key string) error {
	if len(key) == 0 || len(key) > maxTagKeyLen {
		return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
	}

	if strings.HasPrefix(key, "aws:") {
		return fmt.Errorf("%w: tag key must not start with 'aws:'", ErrValidation)
	}

	for _, r := range key {
		if !isValidTagChar(r) {
			return fmt.Errorf("%w: tag key contains invalid character %q", ErrValidation, r)
		}
	}

	return nil
}

// validateTagValue checks tag value constraints: 0-256 chars, valid charset.
func validateTagValue(value string) error {
	if len(value) > maxTagValueLen {
		return fmt.Errorf("%w: tag value must be 0-%d characters", ErrValidation, maxTagValueLen)
	}

	for _, r := range value {
		if !isValidTagChar(r) {
			return fmt.Errorf("%w: tag value contains invalid character %q", ErrValidation, r)
		}
	}

	return nil
}

// isValidTagChar returns true for characters allowed in tag keys and values.
// AWS allows: [a-zA-Z0-9_.:/=+\-@].
func isValidTagChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) ||
		r == '_' || r == '.' || r == ':' || r == '/' ||
		r == '=' || r == '+' || r == '-' || r == '@'
}

// validateTags validates a slice of tag DTOs.
func validateTags(tags []TagDTO) error {
	for _, t := range tags {
		if err := validateTagKey(t.Key); err != nil {
			return err
		}

		if err := validateTagValue(t.Value); err != nil {
			return err
		}
	}

	return nil
}

// validateRetentionPeriod checks that retention period has exactly one of Unlimited or NumberOfDays set.
func validateRetentionPeriod(rp *RetentionPeriod) error {
	if rp == nil {
		return nil
	}

	if rp.Unlimited && rp.NumberOfDays > 0 {
		return fmt.Errorf("%w: retentionPeriod: exactly one of unlimited or numberOfDays must be set", ErrValidation)
	}

	if !rp.Unlimited && rp.NumberOfDays < 1 {
		return fmt.Errorf("%w: retentionPeriod: numberOfDays must be >= 1 when unlimited is false", ErrValidation)
	}

	if rp.NumberOfDays > maxRetentionDays {
		return fmt.Errorf("%w: retentionPeriod: numberOfDays must be <= 2147483647", ErrValidation)
	}

	return nil
}

// InMemoryBackend is the in-memory backend for IoT Analytics.
//
// channels, datastores, datasets, and pipelines are each a *store.Table[T]
// (see store_setup.go): all four key off a real, non-json:"-" identity field
// the value type already carries (Name), so none need a DTO wrapper, and none
// need a secondary store.Index since nothing in this backend does an
// ARN-keyed reverse lookup against them (resolveARNResource parses the
// resource name back out of the ARN string and looks it up directly). tags,
// channelMessages, and datasetContents are left as plain maps: none of their
// value types is a *T (map[string]string, [][]byte, and []*DatasetContent
// respectively), so none fits store.Table's keyed-by-single-identity-value
// shape. See persistence.go for how they round-trip alongside the registered
// tables.
type InMemoryBackend struct {
	loggingOptions  *LoggingOptions
	channelMessages map[string][][]byte
	datasetContents map[string][]*DatasetContent
	tags            map[string]map[string]string
	channels        *store.Table[Channel]
	datastores      *store.Table[Datastore]
	datasets        *store.Table[Dataset]
	pipelines       *store.Table[Pipeline]
	registry        *store.Registry
	svcCtx          context.Context
	mu              *lockmetrics.RWMutex
}

// NewInMemoryBackend creates a new in-memory IoT Analytics backend with a background service context.
func NewInMemoryBackend() *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background())
}

// NewInMemoryBackendWithContext creates a new in-memory IoT Analytics backend whose
// background goroutines are bounded by svcCtx. If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	b := &InMemoryBackend{
		tags:            make(map[string]map[string]string),
		channelMessages: make(map[string][][]byte),
		datasetContents: make(map[string][]*DatasetContent),
		registry:        store.NewRegistry(),
		svcCtx:          svcCtx,
		mu:              lockmetrics.New("iotanalytics"),
	}

	registerAllTables(b)

	return b
}

// Reset clears all backend state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.registry.ResetAll()
	b.tags = make(map[string]map[string]string)
	b.channelMessages = make(map[string][][]byte)
	b.datasetContents = make(map[string][]*DatasetContent)
	b.loggingOptions = nil
}

// arnIdentity resolves the region and account for ARN construction from the
// request ctxbag, falling back to the service default region when none is set.
func arnIdentity(ctx context.Context) (string, string) {
	region := awsmeta.Region(ctx)
	if region == "" {
		region = defaultRegion
	}

	return region, awsmeta.Account(ctx)
}

// resourceARN builds an IoT Analytics ARN for the given resource type and name
// using the request-scoped region and account from the ctxbag.
func resourceARN(ctx context.Context, resourceType, name string) string {
	region, account := arnIdentity(ctx)

	return arn.Build("iotanalytics", region, account, fmt.Sprintf("%s/%s", resourceType, name))
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

// cloneRetentionPeriod deep-copies a RetentionPeriod pointer.
func cloneRetentionPeriod(rp *RetentionPeriod) *RetentionPeriod {
	if rp == nil {
		return nil
	}

	cp := *rp

	return &cp
}

// cloneChannelStorage deep-copies a ChannelStorage pointer.
func cloneChannelStorage(s *ChannelStorage) *ChannelStorage {
	if s == nil {
		return nil
	}

	cp := *s

	if s.ServiceManagedS3 != nil {
		sm := *s.ServiceManagedS3
		cp.ServiceManagedS3 = &sm
	}

	if s.CustomerManagedS3 != nil {
		cm := *s.CustomerManagedS3
		cp.CustomerManagedS3 = &cm
	}

	return &cp
}

// cloneDatastoreStorage deep-copies a DatastoreStorage pointer.
func cloneDatastoreStorage(s *DatastoreStorage) *DatastoreStorage {
	if s == nil {
		return nil
	}

	cp := *s

	if s.ServiceManagedS3 != nil {
		sm := *s.ServiceManagedS3
		cp.ServiceManagedS3 = &sm
	}

	if s.CustomerManagedS3 != nil {
		cm := *s.CustomerManagedS3
		cp.CustomerManagedS3 = &cm
	}

	if s.IotSiteWiseMultiLayerStorage != nil {
		sw := *s.IotSiteWiseMultiLayerStorage
		if sw.CustomerManagedS3Storage != nil {
			cm := *sw.CustomerManagedS3Storage
			sw.CustomerManagedS3Storage = &cm
		}

		cp.IotSiteWiseMultiLayerStorage = &sw
	}

	return &cp
}

// cloneFileFormatConfiguration deep-copies a FileFormatConfiguration pointer.
func cloneFileFormatConfiguration(f *FileFormatConfiguration) *FileFormatConfiguration {
	if f == nil {
		return nil
	}

	cp := *f

	if f.JSONConfiguration != nil {
		jc := *f.JSONConfiguration
		cp.JSONConfiguration = &jc
	}

	if f.ParquetConfiguration != nil {
		pc := *f.ParquetConfiguration

		if f.ParquetConfiguration.SchemaDefinition != nil {
			sd := *f.ParquetConfiguration.SchemaDefinition
			sd.Columns = make([]ColumnSchema, len(f.ParquetConfiguration.SchemaDefinition.Columns))
			copy(sd.Columns, f.ParquetConfiguration.SchemaDefinition.Columns)
			pc.SchemaDefinition = &sd
		}

		cp.ParquetConfiguration = &pc
	}

	return &cp
}

// cloneDatastorePartitions deep-copies a DatastorePartitions pointer.
func cloneDatastorePartitions(p *DatastorePartitions) *DatastorePartitions {
	if p == nil {
		return nil
	}

	cp := DatastorePartitions{
		Partitions: make([]DatastorePartitionEntry, len(p.Partitions)),
	}

	for i, part := range p.Partitions {
		entry := DatastorePartitionEntry{}

		if part.AttributePartition != nil {
			ap := *part.AttributePartition
			entry.AttributePartition = &ap
		}

		if part.TimestampPartition != nil {
			tp := *part.TimestampPartition
			entry.TimestampPartition = &tp
		}

		cp.Partitions[i] = entry
	}

	return &cp
}

// cloneVersioningConfiguration deep-copies a VersioningConfiguration pointer.
func cloneVersioningConfiguration(v *VersioningConfiguration) *VersioningConfiguration {
	if v == nil {
		return nil
	}

	cp := *v

	return &cp
}

// cloneChannel returns a deep copy of c.
func cloneChannel(c *Channel) *Channel {
	cp := *c
	cp.Tags = make(map[string]string, len(c.Tags))
	maps.Copy(cp.Tags, c.Tags)
	cp.Storage = cloneChannelStorage(c.Storage)
	cp.RetentionPeriod = cloneRetentionPeriod(c.RetentionPeriod)

	return &cp
}

// cloneDatastore returns a deep copy of d.
func cloneDatastore(d *Datastore) *Datastore {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)
	cp.Storage = cloneDatastoreStorage(d.Storage)
	cp.RetentionPeriod = cloneRetentionPeriod(d.RetentionPeriod)
	cp.FileFormatConfiguration = cloneFileFormatConfiguration(d.FileFormatConfiguration)
	cp.Partitions = cloneDatastorePartitions(d.Partitions)

	return &cp
}

// cloneDatasetActions deep-copies a slice of DatasetAction.
func cloneDatasetActions(actions []DatasetAction) []DatasetAction {
	if actions == nil {
		return nil
	}

	cp := make([]DatasetAction, len(actions))
	copy(cp, actions)

	return cp
}

// cloneDatasetTriggers deep-copies a slice of DatasetTrigger.
func cloneDatasetTriggers(triggers []DatasetTrigger) []DatasetTrigger {
	if triggers == nil {
		return nil
	}

	cp := make([]DatasetTrigger, len(triggers))
	copy(cp, triggers)

	return cp
}

// cloneContentDeliveryRules deep-copies a slice of ContentDeliveryRule.
func cloneContentDeliveryRules(rules []ContentDeliveryRule) []ContentDeliveryRule {
	if rules == nil {
		return nil
	}

	cp := make([]ContentDeliveryRule, len(rules))
	copy(cp, rules)

	return cp
}

// cloneLateDataRules deep-copies a slice of LateDataRule.
func cloneLateDataRules(rules []LateDataRule) []LateDataRule {
	if rules == nil {
		return nil
	}

	cp := make([]LateDataRule, len(rules))
	copy(cp, rules)

	return cp
}

// cloneDataset returns a deep copy of d.
func cloneDataset(d *Dataset) *Dataset {
	cp := *d
	cp.Tags = make(map[string]string, len(d.Tags))
	maps.Copy(cp.Tags, d.Tags)
	cp.Actions = cloneDatasetActions(d.Actions)
	cp.Triggers = cloneDatasetTriggers(d.Triggers)
	cp.ContentDeliveryRules = cloneContentDeliveryRules(d.ContentDeliveryRules)
	cp.LateDataRules = cloneLateDataRules(d.LateDataRules)
	cp.VersioningConfiguration = cloneVersioningConfiguration(d.VersioningConfiguration)

	return &cp
}

// clonePipelineActivities deep-copies a slice of PipelineActivity.
func clonePipelineActivities(activities []PipelineActivity) []PipelineActivity {
	if activities == nil {
		return nil
	}

	cp := make([]PipelineActivity, len(activities))
	copy(cp, activities)

	return cp
}

// clonePipeline returns a deep copy of p.
func clonePipeline(p *Pipeline) *Pipeline {
	cp := *p
	cp.Tags = make(map[string]string, len(p.Tags))
	maps.Copy(cp.Tags, p.Tags)
	cp.Activities = clonePipelineActivities(p.Activities)

	if len(p.Reprocessings) > 0 {
		cp.Reprocessings = make(map[string]*PipelineReprocessing, len(p.Reprocessings))
		for k, v := range p.Reprocessings {
			rpCp := *v
			cp.Reprocessings[k] = &rpCp
		}
	} else {
		cp.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	return &cp
}

// reprocessingSummariesSorted returns reprocessing summaries sorted by creation time ascending.
func reprocessingSummariesSorted(reprocessings map[string]*PipelineReprocessing) []pipelineReprocessingSummary {
	if len(reprocessings) == 0 {
		return nil
	}

	summaries := make([]pipelineReprocessingSummary, 0, len(reprocessings))

	for _, rp := range reprocessings {
		summaries = append(summaries, pipelineReprocessingSummary{
			ID:           rp.ID,
			Status:       rp.Status,
			CreationTime: rp.CreationTime,
			StartTime:    rp.StartTime,
			EndTime:      rp.EndTime,
		})
	}

	slices.SortFunc(summaries, func(a, b pipelineReprocessingSummary) int {
		return cmp.Compare(a.CreationTime, b.CreationTime)
	})

	return summaries
}

// CreateChannel creates a new IoT Analytics channel.
func (b *InMemoryBackend) CreateChannel(
	ctx context.Context,
	name string,
	tags map[string]string,
	storage *ChannelStorage,
	retention *RetentionPeriod,
) (*Channel, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	if err := validateRetentionPeriod(retention); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateChannel")
	defer b.mu.Unlock()

	if b.channels.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "channel", name)
	c := &Channel{
		Name:            name,
		ARN:             arn,
		Status:          statusActive,
		CreationTime:    now,
		LastUpdate:      now,
		Tags:            make(map[string]string),
		Storage:         cloneChannelStorage(storage),
		RetentionPeriod: cloneRetentionPeriod(retention),
	}
	maps.Copy(c.Tags, tags)
	b.channels.Put(c)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneChannel(c), nil
}

// DescribeChannel returns channel metadata.
func (b *InMemoryBackend) DescribeChannel(name string) (*Channel, error) {
	b.mu.RLock("DescribeChannel")
	defer b.mu.RUnlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return nil, ErrChannelNotFound
	}

	return cloneChannel(c), nil
}

// UpdateChannel updates a channel's storage configuration, retention period, and last update time.
func (b *InMemoryBackend) UpdateChannel(name string, storage *ChannelStorage, retention *RetentionPeriod) error {
	if err := validateRetentionPeriod(retention); err != nil {
		return err
	}

	b.mu.Lock("UpdateChannel")
	defer b.mu.Unlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return ErrChannelNotFound
	}

	c.LastUpdate = epochSeconds(time.Now())

	if storage != nil {
		c.Storage = cloneChannelStorage(storage)
	}

	if retention != nil {
		c.RetentionPeriod = cloneRetentionPeriod(retention)
	}

	return nil
}

// DeleteChannel deletes a channel and its associated messages.
func (b *InMemoryBackend) DeleteChannel(name string) error {
	b.mu.Lock("DeleteChannel")
	defer b.mu.Unlock()

	c, ok := b.channels.Get(name)
	if !ok {
		return ErrChannelNotFound
	}

	delete(b.tags, c.ARN)
	b.channels.Delete(name)
	delete(b.channelMessages, name)

	return nil
}

// ListChannels returns all channels sorted by name.
func (b *InMemoryBackend) ListChannels() []*Channel {
	b.mu.RLock("ListChannels")
	defer b.mu.RUnlock()

	items := b.channels.Snapshot()
	result := make([]*Channel, 0, len(items))

	for _, c := range items {
		result = append(result, cloneChannel(c))
	}

	return result
}

// AddChannelInternal seeds a channel by name (test helper).
func (b *InMemoryBackend) AddChannelInternal(name string) *Channel {
	c, _ := b.CreateChannel(b.svcCtx, name, nil, nil, nil)

	return c
}

// CreateDatastore creates a new IoT Analytics datastore.
func (b *InMemoryBackend) CreateDatastore(
	ctx context.Context,
	name string,
	tags map[string]string,
	storage *DatastoreStorage,
	retention *RetentionPeriod,
	fileFormat *FileFormatConfiguration,
	partitions *DatastorePartitions,
) (*Datastore, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	if err := validateRetentionPeriod(retention); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDatastore")
	defer b.mu.Unlock()

	if b.datastores.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "datastore", name)
	d := &Datastore{
		Name:                    name,
		ARN:                     arn,
		Status:                  statusActive,
		CreationTime:            now,
		LastUpdate:              now,
		Tags:                    make(map[string]string),
		Storage:                 cloneDatastoreStorage(storage),
		RetentionPeriod:         cloneRetentionPeriod(retention),
		FileFormatConfiguration: cloneFileFormatConfiguration(fileFormat),
		Partitions:              cloneDatastorePartitions(partitions),
	}
	maps.Copy(d.Tags, tags)
	b.datastores.Put(d)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneDatastore(d), nil
}

// DescribeDatastore returns datastore metadata.
func (b *InMemoryBackend) DescribeDatastore(name string) (*Datastore, error) {
	b.mu.RLock("DescribeDatastore")
	defer b.mu.RUnlock()

	d, ok := b.datastores.Get(name)
	if !ok {
		return nil, ErrDatastoreNotFound
	}

	return cloneDatastore(d), nil
}

// UpdateDatastore updates a datastore's configuration and last update time.
func (b *InMemoryBackend) UpdateDatastore(
	name string,
	storage *DatastoreStorage,
	retention *RetentionPeriod,
	fileFormat *FileFormatConfiguration,
	partitions *DatastorePartitions,
) error {
	if err := validateRetentionPeriod(retention); err != nil {
		return err
	}

	b.mu.Lock("UpdateDatastore")
	defer b.mu.Unlock()

	d, ok := b.datastores.Get(name)
	if !ok {
		return ErrDatastoreNotFound
	}

	d.LastUpdate = epochSeconds(time.Now())

	if storage != nil {
		d.Storage = cloneDatastoreStorage(storage)
	}

	if retention != nil {
		d.RetentionPeriod = cloneRetentionPeriod(retention)
	}

	if fileFormat != nil {
		d.FileFormatConfiguration = cloneFileFormatConfiguration(fileFormat)
	}

	if partitions != nil {
		d.Partitions = cloneDatastorePartitions(partitions)
	}

	return nil
}

// DeleteDatastore deletes a datastore.
func (b *InMemoryBackend) DeleteDatastore(name string) error {
	b.mu.Lock("DeleteDatastore")
	defer b.mu.Unlock()

	d, ok := b.datastores.Get(name)
	if !ok {
		return ErrDatastoreNotFound
	}

	delete(b.tags, d.ARN)
	b.datastores.Delete(name)

	return nil
}

// ListDatastores returns all datastores sorted by name.
func (b *InMemoryBackend) ListDatastores() []*Datastore {
	b.mu.RLock("ListDatastores")
	defer b.mu.RUnlock()

	items := b.datastores.Snapshot()
	result := make([]*Datastore, 0, len(items))

	for _, d := range items {
		result = append(result, cloneDatastore(d))
	}

	return result
}

// AddDatastoreInternal seeds a datastore by name (test helper).
func (b *InMemoryBackend) AddDatastoreInternal(name string) *Datastore {
	d, _ := b.CreateDatastore(b.svcCtx, name, nil, nil, nil, nil, nil)

	return d
}

// CreateDataset creates a new IoT Analytics dataset.
func (b *InMemoryBackend) CreateDataset(
	ctx context.Context,
	name string,
	tags map[string]string,
	actions []DatasetAction,
	triggers []DatasetTrigger,
	contentDeliveryRules []ContentDeliveryRule,
	versioningConfig *VersioningConfiguration,
	lateDataRules []LateDataRule,
) (*Dataset, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateDataset")
	defer b.mu.Unlock()

	if b.datasets.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "dataset", name)
	d := &Dataset{
		Name:                    name,
		ARN:                     arn,
		Status:                  statusActive,
		CreationTime:            now,
		LastUpdate:              now,
		Tags:                    make(map[string]string),
		Actions:                 cloneDatasetActions(actions),
		Triggers:                cloneDatasetTriggers(triggers),
		ContentDeliveryRules:    cloneContentDeliveryRules(contentDeliveryRules),
		LateDataRules:           cloneLateDataRules(lateDataRules),
		VersioningConfiguration: cloneVersioningConfiguration(versioningConfig),
	}
	maps.Copy(d.Tags, tags)
	b.datasets.Put(d)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return cloneDataset(d), nil
}

// DescribeDataset returns dataset metadata.
func (b *InMemoryBackend) DescribeDataset(name string) (*Dataset, error) {
	b.mu.RLock("DescribeDataset")
	defer b.mu.RUnlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return nil, ErrDatasetNotFound
	}

	return cloneDataset(d), nil
}

// UpdateDataset updates a dataset's actions, triggers, and configuration.
func (b *InMemoryBackend) UpdateDataset(
	name string,
	actions []DatasetAction,
	triggers []DatasetTrigger,
	contentDeliveryRules []ContentDeliveryRule,
	versioningConfig *VersioningConfiguration,
	lateDataRules []LateDataRule,
) error {
	b.mu.Lock("UpdateDataset")
	defer b.mu.Unlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return ErrDatasetNotFound
	}

	d.LastUpdate = epochSeconds(time.Now())

	if actions != nil {
		d.Actions = cloneDatasetActions(actions)
	}

	if triggers != nil {
		d.Triggers = cloneDatasetTriggers(triggers)
	}

	if contentDeliveryRules != nil {
		d.ContentDeliveryRules = cloneContentDeliveryRules(contentDeliveryRules)
	}

	if lateDataRules != nil {
		d.LateDataRules = cloneLateDataRules(lateDataRules)
	}

	if versioningConfig != nil {
		d.VersioningConfiguration = cloneVersioningConfiguration(versioningConfig)
	}

	return nil
}

// DeleteDataset deletes a dataset and its associated content versions.
func (b *InMemoryBackend) DeleteDataset(name string) error {
	b.mu.Lock("DeleteDataset")
	defer b.mu.Unlock()

	d, ok := b.datasets.Get(name)
	if !ok {
		return ErrDatasetNotFound
	}

	delete(b.tags, d.ARN)
	b.datasets.Delete(name)
	delete(b.datasetContents, name)

	return nil
}

// ListDatasets returns all datasets sorted by name.
func (b *InMemoryBackend) ListDatasets() []*Dataset {
	b.mu.RLock("ListDatasets")
	defer b.mu.RUnlock()

	items := b.datasets.Snapshot()
	result := make([]*Dataset, 0, len(items))

	for _, d := range items {
		result = append(result, cloneDataset(d))
	}

	return result
}

// AddDatasetInternal seeds a dataset by name (test helper).
func (b *InMemoryBackend) AddDatasetInternal(name string) *Dataset {
	d, _ := b.CreateDataset(b.svcCtx, name, nil, nil, nil, nil, nil, nil)

	return d
}

// CreatePipeline creates a new IoT Analytics pipeline.
func (b *InMemoryBackend) CreatePipeline(
	ctx context.Context,
	name string,
	tags map[string]string,
	activities []PipelineActivity,
) (*Pipeline, error) {
	if err := validateResourceName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("CreatePipeline")
	defer b.mu.Unlock()

	if b.pipelines.Has(name) {
		return nil, ErrAlreadyExists
	}

	now := epochSeconds(time.Now())
	arn := resourceARN(ctx, "pipeline", name)
	p := &Pipeline{
		Name:          name,
		ARN:           arn,
		CreationTime:  now,
		LastUpdate:    now,
		Tags:          make(map[string]string),
		Reprocessings: make(map[string]*PipelineReprocessing),
		Activities:    clonePipelineActivities(activities),
	}
	maps.Copy(p.Tags, tags)
	b.pipelines.Put(p)
	b.tags[arn] = make(map[string]string)
	maps.Copy(b.tags[arn], tags)

	return clonePipeline(p), nil
}

// DescribePipeline returns pipeline metadata.
func (b *InMemoryBackend) DescribePipeline(name string) (*Pipeline, error) {
	b.mu.RLock("DescribePipeline")
	defer b.mu.RUnlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return nil, ErrPipelineNotFound
	}

	return clonePipeline(p), nil
}

// UpdatePipeline updates a pipeline's activities and last update time.
func (b *InMemoryBackend) UpdatePipeline(name string, activities []PipelineActivity) error {
	b.mu.Lock("UpdatePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	p.LastUpdate = epochSeconds(time.Now())

	if activities != nil {
		p.Activities = clonePipelineActivities(activities)
	}

	return nil
}

// DeletePipeline deletes a pipeline.
func (b *InMemoryBackend) DeletePipeline(name string) error {
	b.mu.Lock("DeletePipeline")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(name)
	if !ok {
		return ErrPipelineNotFound
	}

	delete(b.tags, p.ARN)
	b.pipelines.Delete(name)

	return nil
}

// ListPipelines returns all pipelines sorted by name.
func (b *InMemoryBackend) ListPipelines() []*Pipeline {
	b.mu.RLock("ListPipelines")
	defer b.mu.RUnlock()

	items := b.pipelines.Snapshot()
	result := make([]*Pipeline, 0, len(items))

	for _, p := range items {
		result = append(result, clonePipeline(p))
	}

	return result
}

// AddPipelineInternal seeds a pipeline by name (test helper).
func (b *InMemoryBackend) AddPipelineInternal(name string) *Pipeline {
	p, _ := b.CreatePipeline(b.svcCtx, name, nil, nil)

	return p
}

// resolveARNResource checks whether a resource ARN corresponds to an existing resource.
// Returns true if the resource exists.
func (b *InMemoryBackend) resolveARNResource(arn string) bool {
	// ARN format: arn:aws:iotanalytics:<region>:<account>:<resourceType>/<name>
	// Parse without assuming a specific region or account.
	const arnSplitParts = 6
	parts := strings.SplitN(arn, ":", arnSplitParts)
	if len(parts) != arnSplitParts || parts[0] != "arn" || parts[1] != "aws" || parts[2] != "iotanalytics" {
		return false
	}

	resource := parts[5]
	resourceType, name, found := strings.Cut(resource, "/")
	if !found {
		return false
	}

	switch resourceType {
	case "channel":
		return b.channels.Has(name)

	case "datastore":
		return b.datastores.Has(name)

	case "dataset":
		return b.datasets.Has(name)

	case "pipeline":
		return b.pipelines.Has(name)
	}

	return false
}

// ListTagsForResource returns tags for a resource ARN, sorted by key.
// Returns empty slice (not error) when the resource exists but has no tags.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) ([]TagDTO, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	// Check the tags map first (fast path for resources with tags initialized).
	m, ok := b.tags[resourceARN]
	if ok {
		return mapToTagsSorted(m), nil
	}

	// Distinguish "resource has no tag map" from "resource does not exist".
	if !b.resolveARNResource(resourceARN) {
		return nil, ErrResourceNotFound
	}

	return []TagDTO{}, nil
}

// TagResource adds or updates tags on a resource, enforcing the per-resource tag limit.
func (b *InMemoryBackend) TagResource(resourceARN string, tags []TagDTO) error {
	if err := validateTags(tags); err != nil {
		return err
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	m, ok := b.tags[resourceARN]
	if !ok {
		return ErrResourceNotFound
	}

	for _, t := range tags {
		if _, exists := m[t.Key]; !exists && len(m) >= maxTagsPerResource {
			return fmt.Errorf("%w: resource may not have more than %d tags", ErrValidation, maxTagsPerResource)
		}

		m[t.Key] = t.Value
	}

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
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

// BatchPutMessage ingests messages into a channel.
// Validates count ≤ 100, per-message payload ≤ 128 KB, messageId ≤ 128 chars, and total batch ≤ 500 KB.
func (b *InMemoryBackend) BatchPutMessage(
	channelName string,
	messages []messageInput,
) ([]BatchPutMessageErrorEntry, error) {
	var errs []BatchPutMessageErrorEntry

	b.mu.Lock("BatchPutMessage")
	defer b.mu.Unlock()

	if !b.channels.Has(channelName) {
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

	// Validate total batch payload size before processing individual messages.
	var totalPayloadBytes int

	for _, msg := range messages {
		totalPayloadBytes += len(msg.Payload)
	}

	if totalPayloadBytes > maxBatchPayloadBytes {
		for _, msg := range messages {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: "batch payload exceeds 500 KB limit",
				MessageID:    msg.MessageID,
			})
		}

		return errs, nil
	}

	for _, msg := range messages {
		if len(msg.MessageID) > maxMessageIDLen {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: fmt.Sprintf("messageId exceeds %d character limit", maxMessageIDLen),
				MessageID:    msg.MessageID,
			})

			continue
		}

		if len(msg.Payload) > maxMessagePayloadBytes {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    errCodeInvalidRequest,
				ErrorMessage: "message payload exceeds 128 KB limit",
				MessageID:    msg.MessageID,
			})

			continue
		}

		current := b.channelMessages[channelName]
		if len(current) < maxChannelMessages {
			b.channelMessages[channelName] = append(current, msg.Payload)
		} else {
			errs = append(errs, BatchPutMessageErrorEntry{
				ChannelName:  channelName,
				ErrorCode:    "InternalFailureException",
				ErrorMessage: "channel message capacity exceeded",
				MessageID:    msg.MessageID,
			})
		}
	}

	// Update lastMessageArrivalTime on any successful ingestion. The channel is
	// guaranteed present here: existence was already checked (and the lock has
	// been held continuously since) at the top of this function.
	if len(b.channelMessages[channelName]) > 0 {
		c, _ := b.channels.Get(channelName)
		c.LastMessageArrivalTime = epochSeconds(time.Now())
	}

	if errs == nil {
		errs = []BatchPutMessageErrorEntry{}
	}

	return errs, nil
}

// SampleChannelData returns up to maxMessages sample messages from a channel.
// Returns InvalidRequestException for maxMessages <= 0 or > 10 (AWS behaviour).
func (b *InMemoryBackend) SampleChannelData(channelName string, maxMessages int) ([][]byte, error) {
	b.mu.RLock("SampleChannelData")
	defer b.mu.RUnlock()

	if !b.channels.Has(channelName) {
		return nil, ErrChannelNotFound
	}

	if maxMessages <= 0 || maxMessages > maxSampleMessages {
		return nil, fmt.Errorf("%w: maxMessages must be between 1 and %d", ErrValidation, maxSampleMessages)
	}

	msgs := b.channelMessages[channelName]
	if len(msgs) == 0 {
		return [][]byte{}, nil
	}

	end := min(len(msgs), maxMessages)

	result := make([][]byte, end)
	copy(result, msgs[:end])

	return result, nil
}

// StartPipelineReprocessing creates a new reprocessing job for a pipeline.
// Optional startTime and endTime define the message window to reprocess.
func (b *InMemoryBackend) StartPipelineReprocessing(pipelineName string, startTime, endTime *float64) (string, error) {
	if startTime != nil && endTime != nil && *startTime >= *endTime {
		return "", fmt.Errorf("%w: startTime must be before endTime", ErrValidation)
	}

	b.mu.Lock("StartPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return "", ErrPipelineNotFound
	}

	if len(p.Reprocessings) >= maxPipelineReprocessings {
		return "", fmt.Errorf("%w: pipeline reprocessing limit (%d) exceeded", ErrValidation, maxPipelineReprocessings)
	}

	id := uuid.NewString()
	now := epochSeconds(time.Now())

	rp := &PipelineReprocessing{
		ID:           id,
		Status:       "RUNNING",
		CreationTime: now,
	}

	if startTime != nil {
		rp.StartTime = *startTime
	}

	if endTime != nil {
		rp.EndTime = *endTime
	}

	if p.Reprocessings == nil {
		p.Reprocessings = make(map[string]*PipelineReprocessing)
	}

	p.Reprocessings[id] = rp

	return id, nil
}

// CancelPipelineReprocessing cancels a running pipeline reprocessing job.
func (b *InMemoryBackend) CancelPipelineReprocessing(pipelineName, reprocessingID string) error {
	b.mu.Lock("CancelPipelineReprocessing")
	defer b.mu.Unlock()

	p, ok := b.pipelines.Get(pipelineName)
	if !ok {
		return ErrPipelineNotFound
	}

	rp, ok := p.Reprocessings[reprocessingID]
	if !ok {
		return ErrReprocessingNotFound
	}

	if rp.Status == "CANCELLED" {
		return fmt.Errorf("%w: reprocessing job is already cancelled", ErrValidation)
	}

	rp.Status = "CANCELLED"
	rp.EndTime = epochSeconds(time.Now())

	return nil
}

// CreateDatasetContent creates a new content version for a dataset.
func (b *InMemoryBackend) CreateDatasetContent(datasetName string) (*DatasetContent, error) {
	b.mu.Lock("CreateDatasetContent")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetName) {
		return nil, ErrDatasetNotFound
	}

	now := epochSeconds(time.Now())
	content := &DatasetContent{
		VersionID:      uuid.NewString(),
		Status:         "SUCCEEDED",
		CreationTime:   now,
		CompletionTime: now,
	}

	contents := b.datasetContents[datasetName]
	if len(contents) >= maxDatasetContents {
		contents = contents[1:]
	}

	b.datasetContents[datasetName] = append(contents, content)

	return content, nil
}

// GetDatasetContent retrieves a specific or the latest content version of a dataset.
func (b *InMemoryBackend) GetDatasetContent(datasetName, versionID string) (*DatasetContent, error) {
	b.mu.RLock("GetDatasetContent")
	defer b.mu.RUnlock()

	if !b.datasets.Has(datasetName) {
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
	b.mu.RLock("ListDatasetContents")
	defer b.mu.RUnlock()

	if !b.datasets.Has(datasetName) {
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
	b.mu.Lock("DeleteDatasetContent")
	defer b.mu.Unlock()

	if !b.datasets.Has(datasetName) {
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
	b.mu.RLock("DescribeLoggingOptions")
	defer b.mu.RUnlock()

	if b.loggingOptions == nil {
		return nil, ErrLoggingOptionsNotFound
	}

	opts := *b.loggingOptions

	return &opts, nil
}

// PutLoggingOptions sets the IoT Analytics logging options.
// Validates: level must be "ERROR"; roleArn is required when enabled is true.
func (b *InMemoryBackend) PutLoggingOptions(options *LoggingOptions) error {
	if options.Level != "ERROR" {
		return fmt.Errorf("%w: loggingOptions.level must be ERROR", ErrValidation)
	}

	if options.Enabled && options.RoleARN == "" {
		return fmt.Errorf("%w: loggingOptions.roleArn is required when enabled is true", ErrValidation)
	}

	b.mu.Lock("PutLoggingOptions")
	defer b.mu.Unlock()

	opts := *options
	b.loggingOptions = &opts

	return nil
}

// RunPipelineActivity runs payloads through a pipeline activity and returns the results.
// The in-memory implementation returns payloads unchanged (pass-through).
func (b *InMemoryBackend) RunPipelineActivity(payloads [][]byte) ([][]byte, error) {
	result := make([][]byte, len(payloads))
	copy(result, payloads)

	return result, nil
}
