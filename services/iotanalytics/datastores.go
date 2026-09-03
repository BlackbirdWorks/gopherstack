package iotanalytics

import (
	"context"
	"maps"
	"time"
)

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

	if err := validateDatastorePartitions(partitions); err != nil {
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
// Partitions are not accepted here: the real UpdateDatastoreInput has no
// partitions member, so they can only be set at CreateDatastore.
func (b *InMemoryBackend) UpdateDatastore(
	name string,
	storage *DatastoreStorage,
	retention *RetentionPeriod,
	fileFormat *FileFormatConfiguration,
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
