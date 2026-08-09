package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig
// ---------------------------------------------------------------------------

var (
	// ErrClusterSchedulerConfigNotFound is returned when a cluster scheduler config does not
	// exist. sagemaker@v1.263.2 api_op_DescribeClusterSchedulerConfig.go:1 (and
	// Update/Delete) list only ResourceNotFound as an error, not ValidationException.
	ErrClusterSchedulerConfigNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrClusterSchedulerConfigAlreadyExists is returned when a cluster scheduler config
	// already exists. Create's only documented error is ConflictException — see
	// api_op_CreateClusterSchedulerConfig.go's addOperation error list.
	ErrClusterSchedulerConfigAlreadyExists = awserr.New("ConflictException", ErrConflictException)
	// ErrClusterSchedulerConfigVersionConflict is returned when Update's required
	// TargetVersion doesn't match the resource's current version.
	ErrClusterSchedulerConfigVersionConflict = awserr.New("ConflictException", ErrConflictException)
)

// ClusterSchedulerConfig represents a SageMaker cluster scheduler configuration.
type ClusterSchedulerConfig struct {
	CreationTime                  time.Time         `json:"CreationTime"`
	LastModifiedTime              time.Time         `json:"LastModifiedTime"`
	Tags                          map[string]string `json:"Tags,omitempty"`
	ClusterSchedulerConfigName    string            `json:"Name"`
	ClusterSchedulerConfigArn     string            `json:"ClusterSchedulerConfigArn"`
	ClusterSchedulerConfigID      string            `json:"ClusterSchedulerConfigId"`
	ClusterArn                    string            `json:"ClusterArn,omitempty"`
	Status                        string            `json:"Status"`
	ClusterSchedulerConfigVersion int32             `json:"ClusterSchedulerConfigVersion"`
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
				ClusterSchedulerConfigName:    opts.ClusterSchedulerConfigName,
				ClusterSchedulerConfigArn:     arnStr,
				ClusterSchedulerConfigID:      generateID()[:idPatternLen],
				ClusterSchedulerConfigVersion: 1,
				ClusterArn:                    opts.ClusterArn,
				Status:                        statusCreating,
				Tags:                          mergeTags(nil, opts.Tags),
				CreationTime:                  now,
				LastModifiedTime:              now,
			}
		},
		cloneClusterSchedulerConfig,
	)
}

// clusterSchedulerConfigByID scans tbl for the entry with the given
// ClusterSchedulerConfigId. Describe/Update/Delete key off id (real API,
// sagemaker@v1.263.2 api_op_DescribeClusterSchedulerConfig.go:33), but the
// table's primary key stays Name to preserve Create's name-dedup check and
// List's existing ordering.
func clusterSchedulerConfigByID(tbl *store.Table[ClusterSchedulerConfig], id string) (*ClusterSchedulerConfig, bool) {
	var found *ClusterSchedulerConfig

	tbl.Range(func(v *ClusterSchedulerConfig) bool {
		if v.ClusterSchedulerConfigID != id {
			return true
		}

		found = v

		return false
	})

	return found, found != nil
}

// DescribeClusterSchedulerConfig returns a cluster scheduler config by id.
func (b *InMemoryBackend) DescribeClusterSchedulerConfig(
	ctx context.Context,
	id string,
) (*ClusterSchedulerConfig, error) {
	b.mu.RLock("DescribeClusterSchedulerConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := clusterSchedulerConfigByID(b.clusterSchedulerConfigsStoreRO(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, id)
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

// UpdateClusterSchedulerConfig applies an optimistic-concurrency update gated
// by targetVersion. sagemaker@v1.263.2 api_op_UpdateClusterSchedulerConfig.go:29
// requires ClusterSchedulerConfigId and TargetVersion; ClusterArn is not a
// member of the real input, so it is not settable here — it is Create-only.
func (b *InMemoryBackend) UpdateClusterSchedulerConfig(
	ctx context.Context,
	id string,
	targetVersion int32,
) (*ClusterSchedulerConfig, error) {
	b.mu.Lock("UpdateClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := clusterSchedulerConfigByID(b.clusterSchedulerConfigsStore(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, id)
	}

	if targetVersion != c.ClusterSchedulerConfigVersion {
		return nil, fmt.Errorf(
			"%w: cluster scheduler config %q target version %d does not match current version %d",
			ErrClusterSchedulerConfigVersionConflict, id, targetVersion, c.ClusterSchedulerConfigVersion,
		)
	}

	c.ClusterSchedulerConfigVersion++
	c.LastModifiedTime = time.Now()

	return cloneClusterSchedulerConfig(c), nil
}

// DeleteClusterSchedulerConfig deletes a cluster scheduler config by id.
func (b *InMemoryBackend) DeleteClusterSchedulerConfig(ctx context.Context, id string) error {
	b.mu.Lock("DeleteClusterSchedulerConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.clusterSchedulerConfigsStore(region)

	c, ok := clusterSchedulerConfigByID(tbl, id)
	if !ok {
		return fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, id)
	}

	tbl.Delete(c.ClusterSchedulerConfigName)

	return nil
}

// ---------------------------------------------------------------------------
// ComputeQuota
// ---------------------------------------------------------------------------

var (
	// ErrComputeQuotaNotFound is returned when a compute quota does not exist.
	// sagemaker@v1.263.2 api_op_DescribeComputeQuota.go (and Update/Delete) list
	// only ResourceNotFound as an error, not ValidationException.
	ErrComputeQuotaNotFound = awserr.New("ResourceNotFound", ErrResourceNotFound)
	// ErrComputeQuotaAlreadyExists is returned when a compute quota already
	// exists. Create's only documented error is ConflictException.
	ErrComputeQuotaAlreadyExists = awserr.New("ConflictException", ErrConflictException)
	// ErrComputeQuotaVersionConflict is returned when Update's required
	// TargetVersion doesn't match the resource's current version.
	ErrComputeQuotaVersionConflict = awserr.New("ConflictException", ErrConflictException)
)

// ComputeQuota represents a SageMaker compute quota.
type ComputeQuota struct {
	CreationTime        time.Time         `json:"CreationTime"`
	LastModifiedTime    time.Time         `json:"LastModifiedTime"`
	Tags                map[string]string `json:"Tags,omitempty"`
	ComputeQuotaName    string            `json:"Name"`
	ComputeQuotaArn     string            `json:"ComputeQuotaArn"`
	ComputeQuotaID      string            `json:"ComputeQuotaId"`
	Status              string            `json:"Status"`
	ClusterArn          string            `json:"ClusterArn,omitempty"`
	ComputeQuotaVersion int32             `json:"ComputeQuotaVersion"`
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
				ComputeQuotaName:    opts.ComputeQuotaName,
				ComputeQuotaArn:     arnStr,
				ComputeQuotaID:      generateID()[:idPatternLen],
				ComputeQuotaVersion: 1,
				ClusterArn:          opts.ClusterArn,
				Status:              statusCreated,
				Tags:                mergeTags(nil, opts.Tags),
				CreationTime:        now,
				LastModifiedTime:    now,
			}
		},
		cloneComputeQuota,
	)
}

// computeQuotaByID scans tbl for the entry with the given ComputeQuotaId.
// Describe/Update/Delete key off id (real API, sagemaker@v1.263.2
// api_op_DescribeComputeQuota.go:29), but the table's primary key stays Name
// to preserve Create's name-dedup check and List's existing ordering.
func computeQuotaByID(tbl *store.Table[ComputeQuota], id string) (*ComputeQuota, bool) {
	var found *ComputeQuota

	tbl.Range(func(v *ComputeQuota) bool {
		if v.ComputeQuotaID != id {
			return true
		}

		found = v

		return false
	})

	return found, found != nil
}

// DescribeComputeQuota returns a compute quota by id.
func (b *InMemoryBackend) DescribeComputeQuota(ctx context.Context, id string) (*ComputeQuota, error) {
	b.mu.RLock("DescribeComputeQuota")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	q, ok := computeQuotaByID(b.computeQuotasStoreRO(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, id)
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

// UpdateComputeQuota applies an optimistic-concurrency update gated by
// targetVersion. sagemaker@v1.263.2 api_op_UpdateComputeQuota.go requires
// ComputeQuotaId and TargetVersion; ClusterArn is not a member of the real
// input, so it is not settable here — it is Create-only.
func (b *InMemoryBackend) UpdateComputeQuota(
	ctx context.Context,
	id string,
	targetVersion int32,
) (*ComputeQuota, error) {
	b.mu.Lock("UpdateComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	q, ok := computeQuotaByID(b.computeQuotasStore(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, id)
	}

	if targetVersion != q.ComputeQuotaVersion {
		return nil, fmt.Errorf(
			"%w: compute quota %q target version %d does not match current version %d",
			ErrComputeQuotaVersionConflict, id, targetVersion, q.ComputeQuotaVersion,
		)
	}

	q.ComputeQuotaVersion++
	q.LastModifiedTime = time.Now()

	return cloneComputeQuota(q), nil
}

// DeleteComputeQuota deletes a compute quota by id.
func (b *InMemoryBackend) DeleteComputeQuota(ctx context.Context, id string) error {
	b.mu.Lock("DeleteComputeQuota")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	tbl := b.computeQuotasStore(region)

	q, ok := computeQuotaByID(tbl, id)
	if !ok {
		return fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, id)
	}

	tbl.Delete(q.ComputeQuotaName)

	return nil
}
