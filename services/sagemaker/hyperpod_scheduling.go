package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig
// ---------------------------------------------------------------------------

var (
	// ErrClusterSchedulerConfigNotFound is returned when a cluster scheduler config does not exist.
	ErrClusterSchedulerConfigNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrClusterSchedulerConfigAlreadyExists is returned when a cluster scheduler config already exists.
	ErrClusterSchedulerConfigAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ClusterSchedulerConfig represents a SageMaker cluster scheduler configuration.
type ClusterSchedulerConfig struct {
	CreationTime               time.Time         `json:"CreationTime"`
	LastModifiedTime           time.Time         `json:"LastModifiedTime"`
	Tags                       map[string]string `json:"Tags,omitempty"`
	ClusterSchedulerConfigName string            `json:"ClusterSchedulerConfigName"`
	ClusterSchedulerConfigArn  string            `json:"ClusterSchedulerConfigArn"`
	ClusterArn                 string            `json:"ClusterArn,omitempty"`
	Status                     string            `json:"Status"`
}

func cloneClusterSchedulerConfig(c *ClusterSchedulerConfig) *ClusterSchedulerConfig {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeClusterSchedulerConfig.
func (c *ClusterSchedulerConfig) MarshalJSON() ([]byte, error) {
	type alias ClusterSchedulerConfig

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(c),
		CreationTime:     epochSeconds(c.CreationTime),
		LastModifiedTime: epochSeconds(c.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [ClusterSchedulerConfig.MarshalJSON], read
// by persistence.go's snapshot restore path.
func (c *ClusterSchedulerConfig) UnmarshalJSON(data []byte) error {
	type alias ClusterSchedulerConfig

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(c)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	c.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	c.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateClusterSchedulerConfigOptions holds input fields for CreateClusterSchedulerConfig.
type CreateClusterSchedulerConfigOptions struct {
	Tags                       map[string]string
	ClusterSchedulerConfigName string
	ClusterArn                 string
}

// CreateClusterSchedulerConfig creates a SageMaker cluster scheduler configuration.
func (b *InMemoryBackend) CreateClusterSchedulerConfig(
	ctx context.Context,
	opts CreateClusterSchedulerConfigOptions,
) (*ClusterSchedulerConfig, error) {
	if opts.ClusterSchedulerConfigName == "" {
		return nil, fmt.Errorf("%w: ClusterSchedulerConfigName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateClusterSchedulerConfig", opts.ClusterSchedulerConfigName, "cluster-scheduler-config",
		b.clusterSchedulerConfigsStore,
		func(n string) error {
			return fmt.Errorf(
				"%w: cluster scheduler config %q already exists",
				ErrClusterSchedulerConfigAlreadyExists,
				n,
			)
		},
		func(arnStr string, now time.Time) *ClusterSchedulerConfig {
			return &ClusterSchedulerConfig{
				ClusterSchedulerConfigName: opts.ClusterSchedulerConfigName,
				ClusterSchedulerConfigArn:  arnStr,
				ClusterArn:                 opts.ClusterArn,
				Status:                     statusCreating,
				Tags:                       mergeTags(nil, opts.Tags),
				CreationTime:               now,
				LastModifiedTime:           now,
			}
		},
		cloneClusterSchedulerConfig,
	)
}

// DescribeClusterSchedulerConfig returns a cluster scheduler config by name.
func (b *InMemoryBackend) DescribeClusterSchedulerConfig(
	ctx context.Context,
	name string,
) (*ClusterSchedulerConfig, error) {
	b.mu.RLock("DescribeClusterSchedulerConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := b.clusterSchedulerConfigsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	return cloneClusterSchedulerConfig(c), nil
}

// ListClusterSchedulerConfigs returns all cluster scheduler configs with pagination.
func (b *InMemoryBackend) ListClusterSchedulerConfigs(
	ctx context.Context,
	nextToken string,
) ([]*ClusterSchedulerConfig, string) {
	b.mu.RLock("ListClusterSchedulerConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.clusterSchedulerConfigsStoreRO(region),
		nextToken,
		cloneClusterSchedulerConfig,
		func(v *ClusterSchedulerConfig) string { return v.ClusterSchedulerConfigName },
	)
}

// UpdateClusterSchedulerConfig updates a cluster scheduler config's cluster ARN.
func (b *InMemoryBackend) UpdateClusterSchedulerConfig(ctx context.Context, name, clusterArn string) error {
	b.mu.Lock("UpdateClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.clusterSchedulerConfigsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	if clusterArn != "" {
		c.ClusterArn = clusterArn
	}

	c.LastModifiedTime = time.Now()

	return nil
}

// DeleteClusterSchedulerConfig deletes a cluster scheduler config by name.
func (b *InMemoryBackend) DeleteClusterSchedulerConfig(ctx context.Context, name string) error {
	b.mu.Lock("DeleteClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.clusterSchedulerConfigsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ---------------------------------------------------------------------------
// ComputeQuota
// ---------------------------------------------------------------------------

var (
	// ErrComputeQuotaNotFound is returned when a compute quota does not exist.
	ErrComputeQuotaNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrComputeQuotaAlreadyExists is returned when a compute quota already exists.
	ErrComputeQuotaAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// ComputeQuota represents a SageMaker compute quota.
type ComputeQuota struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	ComputeQuotaName string            `json:"ComputeQuotaName"`
	ComputeQuotaArn  string            `json:"ComputeQuotaArn"`
	Status           string            `json:"Status"`
	ClusterArn       string            `json:"ClusterArn,omitempty"`
}

func cloneComputeQuota(q *ComputeQuota) *ComputeQuota {
	cp := *q
	cp.Tags = maps.Clone(q.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeComputeQuota.
func (q *ComputeQuota) MarshalJSON() ([]byte, error) {
	type alias ComputeQuota

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(q),
		CreationTime:     epochSeconds(q.CreationTime),
		LastModifiedTime: epochSeconds(q.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [ComputeQuota.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (q *ComputeQuota) UnmarshalJSON(data []byte) error {
	type alias ComputeQuota

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(q)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	q.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	q.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateComputeQuotaOptions holds input fields for CreateComputeQuota.
type CreateComputeQuotaOptions struct {
	Tags             map[string]string
	ComputeQuotaName string
	ClusterArn       string
}

// CreateComputeQuota creates a SageMaker compute quota.
func (b *InMemoryBackend) CreateComputeQuota(
	ctx context.Context,
	opts CreateComputeQuotaOptions,
) (*ComputeQuota, error) {
	if opts.ComputeQuotaName == "" {
		return nil, fmt.Errorf("%w: ComputeQuotaName is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateComputeQuota", opts.ComputeQuotaName, "compute-quota",
		b.computeQuotasStore,
		func(n string) error {
			return fmt.Errorf("%w: compute quota %q already exists", ErrComputeQuotaAlreadyExists, n)
		},
		func(arnStr string, now time.Time) *ComputeQuota {
			return &ComputeQuota{
				ComputeQuotaName: opts.ComputeQuotaName,
				ComputeQuotaArn:  arnStr,
				ClusterArn:       opts.ClusterArn,
				Status:           statusCreated,
				Tags:             mergeTags(nil, opts.Tags),
				CreationTime:     now,
				LastModifiedTime: now,
			}
		},
		cloneComputeQuota,
	)
}

// DescribeComputeQuota returns a compute quota by name.
func (b *InMemoryBackend) DescribeComputeQuota(ctx context.Context, name string) (*ComputeQuota, error) {
	b.mu.RLock("DescribeComputeQuota")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	q, ok := b.computeQuotasStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	return cloneComputeQuota(q), nil
}

// ListComputeQuotas returns all compute quotas with pagination.
func (b *InMemoryBackend) ListComputeQuotas(ctx context.Context, nextToken string) ([]*ComputeQuota, string) {
	b.mu.RLock("ListComputeQuotas")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.computeQuotasStoreRO(region),
		nextToken,
		cloneComputeQuota,
		func(v *ComputeQuota) string { return v.ComputeQuotaName },
	)
}

// UpdateComputeQuota updates a compute quota's cluster ARN.
func (b *InMemoryBackend) UpdateComputeQuota(ctx context.Context, name, clusterArn string) error {
	b.mu.Lock("UpdateComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	q, ok := b.computeQuotasStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	if clusterArn != "" {
		q.ClusterArn = clusterArn
	}

	q.LastModifiedTime = time.Now()

	return nil
}

// DeleteComputeQuota deletes a compute quota by name.
func (b *InMemoryBackend) DeleteComputeQuota(ctx context.Context, name string) error {
	b.mu.Lock("DeleteComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.computeQuotasStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, name)
	}

	store.Delete(name)

	return nil
}
