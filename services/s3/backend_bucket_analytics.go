package s3

import (
	"context"
)

// PutBucketAnalyticsConfiguration stores an analytics configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketAnalyticsConfiguration")
	defer bucket.mu.Unlock()

	if bucket.AnalyticsConfigs == nil {
		bucket.AnalyticsConfigs = make(map[string]string)
	}

	bucket.AnalyticsConfigs[id] = configXML

	return nil
}

// GetBucketAnalyticsConfiguration returns an analytics configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketAnalyticsConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.AnalyticsConfigs[id]
	if !ok {
		return "", ErrNoAnalyticsConfig
	}

	return config, nil
}

// DeleteBucketAnalyticsConfiguration removes an analytics configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketAnalyticsConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketAnalyticsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketAnalyticsConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.AnalyticsConfigs, id)

	return nil
}

// ListBucketAnalyticsConfigurations returns all analytics configurations for a bucket.
func (b *InMemoryBackend) ListBucketAnalyticsConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketAnalyticsConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketAnalyticsConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.AnalyticsConfigs))
	for _, v := range bucket.AnalyticsConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketIntelligentTieringConfiguration stores an Intelligent-Tiering configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketIntelligentTieringConfiguration")
	defer bucket.mu.Unlock()

	if bucket.IntelligentTieringConfigs == nil {
		bucket.IntelligentTieringConfigs = make(map[string]string)
	}

	bucket.IntelligentTieringConfigs[id] = configXML

	return nil
}

// GetBucketIntelligentTieringConfiguration returns an Intelligent-Tiering configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketIntelligentTieringConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.IntelligentTieringConfigs[id]
	if !ok {
		return "", ErrNoIntelligentTieringConfig
	}

	return config, nil
}

// DeleteBucketIntelligentTieringConfiguration removes an Intelligent-Tiering configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketIntelligentTieringConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketIntelligentTieringConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketIntelligentTieringConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.IntelligentTieringConfigs, id)

	return nil
}

// ListBucketIntelligentTieringConfigurations returns all Intelligent-Tiering configurations for a bucket.
func (b *InMemoryBackend) ListBucketIntelligentTieringConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketIntelligentTieringConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketIntelligentTieringConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.IntelligentTieringConfigs))
	for _, v := range bucket.IntelligentTieringConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketInventoryConfiguration stores an inventory configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketInventoryConfiguration")
	defer bucket.mu.Unlock()

	if bucket.InventoryConfigs == nil {
		bucket.InventoryConfigs = make(map[string]string)
	}

	bucket.InventoryConfigs[id] = configXML

	return nil
}

// GetBucketInventoryConfiguration returns an inventory configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketInventoryConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.InventoryConfigs[id]
	if !ok {
		return "", ErrNoInventoryConfig
	}

	return config, nil
}

// DeleteBucketInventoryConfiguration removes an inventory configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketInventoryConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketInventoryConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketInventoryConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.InventoryConfigs, id)

	return nil
}

// ListBucketInventoryConfigurations returns all inventory configurations for a bucket.
func (b *InMemoryBackend) ListBucketInventoryConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketInventoryConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketInventoryConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.InventoryConfigs))
	for _, v := range bucket.InventoryConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}

// PutBucketMetricsConfiguration stores a metrics configuration for a bucket by ID.
func (b *InMemoryBackend) PutBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id, configXML string,
) error {
	b.mu.RLock("PutBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketMetricsConfiguration")
	defer bucket.mu.Unlock()

	if bucket.MetricsConfigs == nil {
		bucket.MetricsConfigs = make(map[string]string)
	}

	bucket.MetricsConfigs[id] = configXML

	return nil
}

// GetBucketMetricsConfiguration returns a metrics configuration for a bucket by ID.
func (b *InMemoryBackend) GetBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id string,
) (string, error) {
	b.mu.RLock("GetBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetricsConfiguration")
	defer bucket.mu.RUnlock()

	config, ok := bucket.MetricsConfigs[id]
	if !ok {
		return "", ErrNoMetricsConfig
	}

	return config, nil
}

// DeleteBucketMetricsConfiguration removes a metrics configuration from a bucket by ID.
func (b *InMemoryBackend) DeleteBucketMetricsConfiguration(
	_ context.Context,
	bucketName, id string,
) error {
	b.mu.RLock("DeleteBucketMetricsConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetricsConfiguration")
	defer bucket.mu.Unlock()

	delete(bucket.MetricsConfigs, id)

	return nil
}

// ListBucketMetricsConfigurations returns all metrics configurations for a bucket.
func (b *InMemoryBackend) ListBucketMetricsConfigurations(
	_ context.Context,
	bucketName string,
) ([]string, error) {
	b.mu.RLock("ListBucketMetricsConfigurations")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListBucketMetricsConfigurations")
	defer bucket.mu.RUnlock()

	configs := make([]string, 0, len(bucket.MetricsConfigs))
	for _, v := range bucket.MetricsConfigs {
		configs = append(configs, v)
	}

	return configs, nil
}
