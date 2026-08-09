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

// SchedulerConfig is a ClusterSchedulerConfig's task-prioritization and
// fair-share policy. sagemaker@v1.263.2 types/types.go:20581.
type SchedulerConfig struct {
	FairShare           string          `json:"FairShare,omitempty"`
	IdleResourceSharing string          `json:"IdleResourceSharing,omitempty"`
	PriorityClasses     []PriorityClass `json:"PriorityClasses,omitempty"`
}

// PriorityClass is one entry of a SchedulerConfig's PriorityClasses list.
// sagemaker@v1.263.2 types/types.go:17733.
type PriorityClass struct {
	Name   string `json:"Name"`
	Weight int32  `json:"Weight"`
}

func cloneSchedulerConfig(c *SchedulerConfig) *SchedulerConfig {
	if c == nil {
		return nil
	}

	cp := *c
	if c.PriorityClasses != nil {
		cp.PriorityClasses = append([]PriorityClass(nil), c.PriorityClasses...)
	}

	return &cp
}

// ClusterSchedulerConfig represents a SageMaker cluster scheduler configuration.
type ClusterSchedulerConfig struct {
	CreationTime                  time.Time         `json:"CreationTime"`
	LastModifiedTime              time.Time         `json:"LastModifiedTime"`
	SchedulerConfig               *SchedulerConfig  `json:"SchedulerConfig,omitempty"`
	Tags                          map[string]string `json:"Tags,omitempty"`
	ClusterSchedulerConfigName    string            `json:"Name"`
	ClusterSchedulerConfigArn     string            `json:"ClusterSchedulerConfigArn"`
	ClusterSchedulerConfigID      string            `json:"ClusterSchedulerConfigId"`
	ClusterArn                    string            `json:"ClusterArn,omitempty"`
	Description                   string            `json:"Description,omitempty"`
	Status                        string            `json:"Status"`
	ClusterSchedulerConfigVersion int32             `json:"ClusterSchedulerConfigVersion"`
}

func cloneClusterSchedulerConfig(c *ClusterSchedulerConfig) *ClusterSchedulerConfig {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)
	cp.SchedulerConfig = cloneSchedulerConfig(c.SchedulerConfig)

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
	SchedulerConfig            *SchedulerConfig
	Tags                       map[string]string
	ClusterSchedulerConfigName string
	ClusterArn                 string
	Description                string
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
				Description:                   opts.Description,
				SchedulerConfig:               cloneSchedulerConfig(opts.SchedulerConfig),
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

// UpdateClusterSchedulerConfigOptions holds the optional, settable-on-update
// fields for UpdateClusterSchedulerConfig. A nil field means "not provided in
// the request" and leaves the stored value unchanged.
type UpdateClusterSchedulerConfigOptions struct {
	SchedulerConfig *SchedulerConfig
	Description     *string
}

// UpdateClusterSchedulerConfig applies an optimistic-concurrency update gated
// by targetVersion. sagemaker@v1.263.2 api_op_UpdateClusterSchedulerConfig.go:29
// requires ClusterSchedulerConfigId and TargetVersion; ClusterArn is not a
// member of the real input, so it is not settable here — it is Create-only.
func (b *InMemoryBackend) UpdateClusterSchedulerConfig(
	ctx context.Context,
	id string,
	targetVersion int32,
	opts UpdateClusterSchedulerConfigOptions,
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

	if opts.SchedulerConfig != nil {
		c.SchedulerConfig = cloneSchedulerConfig(opts.SchedulerConfig)
	}

	if opts.Description != nil {
		c.Description = *opts.Description
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

// AcceleratorPartitionConfig is a ComputeQuotaResourceConfig's fractional-GPU
// allocation. sagemaker@v1.263.2 types/types.go:11 (accelerator_partition_config.go).
type AcceleratorPartitionConfig struct {
	Type  string `json:"Type"`
	Count int32  `json:"Count"`
}

// ComputeQuotaResourceConfig is one resource allocation entry, used both by
// ComputeQuotaConfig.ComputeQuotaResources and
// ResourceSharingConfig.AbsoluteBorrowLimits. sagemaker@v1.263.2
// types/types.go:6114.
type ComputeQuotaResourceConfig struct {
	AcceleratorPartition *AcceleratorPartitionConfig `json:"AcceleratorPartition,omitempty"`
	Accelerators         *int32                      `json:"Accelerators,omitempty"`
	Count                *int32                      `json:"Count,omitempty"`
	MemoryInGiB          *float32                    `json:"MemoryInGiB,omitempty"`
	VCpu                 *float32                    `json:"VCpu,omitempty"`
	InstanceType         string                      `json:"InstanceType"`
}

// ResourceSharingConfig defines how a ComputeQuota lends and borrows idle
// compute with other entities. sagemaker@v1.263.2 types/types.go:19878.
type ResourceSharingConfig struct {
	BorrowLimit          *int32                       `json:"BorrowLimit,omitempty"`
	Strategy             string                       `json:"Strategy"`
	AbsoluteBorrowLimits []ComputeQuotaResourceConfig `json:"AbsoluteBorrowLimits,omitempty"`
}

// ComputeQuotaConfig is a ComputeQuota's resource allocation configuration.
// sagemaker@v1.263.2 types/types.go:6094.
type ComputeQuotaConfig struct {
	ResourceSharingConfig *ResourceSharingConfig       `json:"ResourceSharingConfig,omitempty"`
	PreemptTeamTasks      string                       `json:"PreemptTeamTasks,omitempty"`
	ComputeQuotaResources []ComputeQuotaResourceConfig `json:"ComputeQuotaResources,omitempty"`
}

// ComputeQuotaTarget is the entity a ComputeQuota allocates compute to.
// sagemaker@v1.263.2 types/types.go:6208.
type ComputeQuotaTarget struct {
	FairShareWeight *int32 `json:"FairShareWeight,omitempty"`
	TeamName        string `json:"TeamName"`
}

func cloneComputeQuotaResourceConfig(r ComputeQuotaResourceConfig) ComputeQuotaResourceConfig {
	if r.AcceleratorPartition != nil {
		ap := *r.AcceleratorPartition
		r.AcceleratorPartition = &ap
	}

	if r.Accelerators != nil {
		v := *r.Accelerators
		r.Accelerators = &v
	}

	if r.Count != nil {
		v := *r.Count
		r.Count = &v
	}

	if r.MemoryInGiB != nil {
		v := *r.MemoryInGiB
		r.MemoryInGiB = &v
	}

	if r.VCpu != nil {
		v := *r.VCpu
		r.VCpu = &v
	}

	return r
}

func cloneComputeQuotaResourceConfigs(rs []ComputeQuotaResourceConfig) []ComputeQuotaResourceConfig {
	if rs == nil {
		return nil
	}

	cp := make([]ComputeQuotaResourceConfig, len(rs))
	for i, r := range rs {
		cp[i] = cloneComputeQuotaResourceConfig(r)
	}

	return cp
}

func cloneResourceSharingConfig(c *ResourceSharingConfig) *ResourceSharingConfig {
	if c == nil {
		return nil
	}

	cp := *c
	cp.AbsoluteBorrowLimits = cloneComputeQuotaResourceConfigs(c.AbsoluteBorrowLimits)

	if c.BorrowLimit != nil {
		v := *c.BorrowLimit
		cp.BorrowLimit = &v
	}

	return &cp
}

func cloneComputeQuotaConfig(c *ComputeQuotaConfig) *ComputeQuotaConfig {
	if c == nil {
		return nil
	}

	cp := *c
	cp.ComputeQuotaResources = cloneComputeQuotaResourceConfigs(c.ComputeQuotaResources)
	cp.ResourceSharingConfig = cloneResourceSharingConfig(c.ResourceSharingConfig)

	return &cp
}

func cloneComputeQuotaTarget(t *ComputeQuotaTarget) *ComputeQuotaTarget {
	if t == nil {
		return nil
	}

	cp := *t
	if t.FairShareWeight != nil {
		v := *t.FairShareWeight
		cp.FairShareWeight = &v
	}

	return &cp
}

// ComputeQuota represents a SageMaker compute quota.
type ComputeQuota struct {
	CreationTime        time.Time           `json:"CreationTime"`
	LastModifiedTime    time.Time           `json:"LastModifiedTime"`
	ComputeQuotaConfig  *ComputeQuotaConfig `json:"ComputeQuotaConfig,omitempty"`
	ComputeQuotaTarget  *ComputeQuotaTarget `json:"ComputeQuotaTarget,omitempty"`
	Tags                map[string]string   `json:"Tags,omitempty"`
	ComputeQuotaName    string              `json:"Name"`
	ComputeQuotaArn     string              `json:"ComputeQuotaArn"`
	ComputeQuotaID      string              `json:"ComputeQuotaId"`
	Status              string              `json:"Status"`
	ActivationState     string              `json:"ActivationState,omitempty"`
	ClusterArn          string              `json:"ClusterArn,omitempty"`
	Description         string              `json:"Description,omitempty"`
	ComputeQuotaVersion int32               `json:"ComputeQuotaVersion"`
}

func cloneComputeQuota(q *ComputeQuota) *ComputeQuota {
	cp := *q
	cp.Tags = maps.Clone(q.Tags)
	cp.ComputeQuotaConfig = cloneComputeQuotaConfig(q.ComputeQuotaConfig)
	cp.ComputeQuotaTarget = cloneComputeQuotaTarget(q.ComputeQuotaTarget)

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

// activationStateEnabled is the CreateComputeQuota/UpdateComputeQuota default
// when ActivationState is omitted, per sagemaker@v1.263.2
// api_op_CreateComputeQuota.go's ActivationState doc.
const activationStateEnabled = "Enabled"

// CreateComputeQuotaOptions holds input fields for CreateComputeQuota.
type CreateComputeQuotaOptions struct {
	ComputeQuotaConfig *ComputeQuotaConfig
	ComputeQuotaTarget *ComputeQuotaTarget
	Tags               map[string]string
	ComputeQuotaName   string
	ClusterArn         string
	ActivationState    string
	Description        string
}

// CreateComputeQuota creates a SageMaker compute quota.
func (b *InMemoryBackend) CreateComputeQuota(
	ctx context.Context,
	opts CreateComputeQuotaOptions,
) (*ComputeQuota, error) {
	if opts.ComputeQuotaName == "" {
		return nil, fmt.Errorf("%w: ComputeQuotaName is required", ErrValidation)
	}

	activationState := opts.ActivationState
	if activationState == "" {
		activationState = activationStateEnabled
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
				ActivationState:     activationState,
				Description:         opts.Description,
				ComputeQuotaConfig:  cloneComputeQuotaConfig(opts.ComputeQuotaConfig),
				ComputeQuotaTarget:  cloneComputeQuotaTarget(opts.ComputeQuotaTarget),
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

// UpdateComputeQuotaOptions holds the optional, settable-on-update fields for
// UpdateComputeQuota. A nil field means "not provided in the request" and
// leaves the stored value unchanged.
type UpdateComputeQuotaOptions struct {
	ComputeQuotaConfig *ComputeQuotaConfig
	ComputeQuotaTarget *ComputeQuotaTarget
	ActivationState    *string
	Description        *string
}

// UpdateComputeQuota applies an optimistic-concurrency update gated by
// targetVersion. sagemaker@v1.263.2 api_op_UpdateComputeQuota.go requires
// ComputeQuotaId and TargetVersion; ClusterArn is not a member of the real
// input, so it is not settable here — it is Create-only.
func (b *InMemoryBackend) UpdateComputeQuota(
	ctx context.Context,
	id string,
	targetVersion int32,
	opts UpdateComputeQuotaOptions,
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

	if opts.ComputeQuotaConfig != nil {
		q.ComputeQuotaConfig = cloneComputeQuotaConfig(opts.ComputeQuotaConfig)
	}

	if opts.ComputeQuotaTarget != nil {
		q.ComputeQuotaTarget = cloneComputeQuotaTarget(opts.ComputeQuotaTarget)
	}

	if opts.ActivationState != nil {
		q.ActivationState = *opts.ActivationState
	}

	if opts.Description != nil {
		q.Description = *opts.Description
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
