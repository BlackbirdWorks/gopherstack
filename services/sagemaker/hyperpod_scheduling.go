package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
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
//
// FailureReason/StatusDetails (both on DescribeClusterSchedulerConfigOutput,
// api_op_DescribeClusterSchedulerConfig.go:82-94) are not modeled: Status
// never reaches a Failed state in this backend (no failure FSM), so there is
// no real failure to report a reason or per-status detail for. CreatedBy/
// LastModifiedBy (types.UserContext) are disclosed absent, the same
// no-caller-identity-model gap already disclosed for every other
// CreatedBy/LastModifiedBy field in this service (e.g. handler_mlflow.go,
// model_packages.go).
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
				// statusCreated, not statusCreating: this backend has no failure
				// FSM to ever advance a "Creating" resource out of that state, so
				// leaving it there (as this line previously did) meant every
				// ClusterSchedulerConfig stayed "Creating" for its entire
				// lifetime — Describe/List never showed the terminal state
				// ComputeQuota's sibling Create (below) already lands on.
				Status:           statusCreated,
				Tags:             mergeTags(nil, opts.Tags),
				CreationTime:     now,
				LastModifiedTime: now,
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
// version, if non-nil, must equal the resource's current
// ClusterSchedulerConfigVersion: this backend keeps only the live version
// counter, not a historical snapshot per version
// (api_op_DescribeClusterSchedulerConfig.go:38's optional
// ClusterSchedulerConfigVersion), so a request for any other version returns
// NotFound rather than fabricating a snapshot that was never stored.
func (b *InMemoryBackend) DescribeClusterSchedulerConfig(
	ctx context.Context,
	id string,
	version *int32,
) (*ClusterSchedulerConfig, error) {
	b.mu.RLock("DescribeClusterSchedulerConfig")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := clusterSchedulerConfigByID(b.clusterSchedulerConfigsStoreRO(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: cluster scheduler config %q", ErrClusterSchedulerConfigNotFound, id)
	}

	if version != nil && *version != c.ClusterSchedulerConfigVersion {
		return nil, fmt.Errorf(
			"%w: cluster scheduler config %q version %d", ErrClusterSchedulerConfigNotFound, id, *version,
		)
	}

	return cloneClusterSchedulerConfig(c), nil
}

// ListClusterSchedulerConfigsParams bundles ListClusterSchedulerConfigs'
// filter/sort/pagination criteria (api_op_ListClusterSchedulerConfigs.go:
// 30-68, sagemaker@v1.263.2). Unlike ListImages/ListNotebookInstances, this
// op has no LastModifiedTime filters at all — read from this op's own field
// list, not assumed by analogy.
type ListClusterSchedulerConfigsParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	ClusterArn    string
	NameContains  string
	NextToken     string
	SortBy        string
	SortOrder     string
	Status        string
	MaxResults    int32
}

// ListClusterSchedulerConfigs returns cluster scheduler configs matching
// params, sorted by params.SortBy (default CreationTime, undocumented —
// api_op_ListClusterSchedulerConfigs.go's SortBy doc names no default) /
// params.SortOrder (default Descending, documented at :62), capped at
// params.MaxResults.
func (b *InMemoryBackend) ListClusterSchedulerConfigs(
	ctx context.Context,
	params ListClusterSchedulerConfigsParams,
) ([]*ClusterSchedulerConfig, string) {
	b.mu.RLock("ListClusterSchedulerConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tbl := b.clusterSchedulerConfigsStoreRO(region)
	list := make([]*ClusterSchedulerConfig, 0, tbl.Len())

	for _, c := range tbl.Snapshot() {
		if !matchesClusterSchedulerConfigListParams(c, params) {
			continue
		}

		list = append(list, cloneClusterSchedulerConfig(c))
	}

	asc := strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := clusterSchedulerConfigSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesClusterSchedulerConfigListParams reports whether c satisfies every
// filter in params.
func matchesClusterSchedulerConfigListParams(c *ClusterSchedulerConfig, p ListClusterSchedulerConfigsParams) bool {
	if p.ClusterArn != "" && c.ClusterArn != p.ClusterArn {
		return false
	}

	if p.NameContains != "" && !strings.Contains(c.ClusterSchedulerConfigName, p.NameContains) {
		return false
	}

	if p.Status != "" && c.Status != p.Status {
		return false
	}

	if p.CreatedAfter != nil && !c.CreationTime.After(*p.CreatedAfter) {
		return false
	}

	if p.CreatedBefore != nil && !c.CreationTime.Before(*p.CreatedBefore) {
		return false
	}

	return true
}

// clusterSchedulerConfigSortLess orders two cluster scheduler configs by
// sortBy — one of SortClusterSchedulerConfigBy's real values (Name/
// CreationTime/Status, types/enums.go:9123-9125), mixed-case like most other
// List ops in this service (unlike the Image family's all-caps enums).
func clusterSchedulerConfigSortLess(a, b *ClusterSchedulerConfig, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		if a.ClusterSchedulerConfigName != b.ClusterSchedulerConfigName {
			return a.ClusterSchedulerConfigName < b.ClusterSchedulerConfigName
		}
	case keyStatus:
		if a.Status != b.Status {
			return a.Status < b.Status
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.ClusterSchedulerConfigName < b.ClusterSchedulerConfigName
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
//
// FailureReason (DescribeComputeQuotaOutput, api_op_DescribeComputeQuota.go's
// FailureReason field) is not modeled: Status never reaches a Failed state in
// this backend (no failure FSM). CreatedBy/LastModifiedBy (types.UserContext)
// are disclosed absent, the same no-caller-identity-model gap as
// ClusterSchedulerConfig above.
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

// DescribeComputeQuota returns a compute quota by id. version, if non-nil,
// must equal the resource's current ComputeQuotaVersion — same rationale as
// DescribeClusterSchedulerConfig's version parameter above: no historical
// snapshot exists to honor any other value.
func (b *InMemoryBackend) DescribeComputeQuota(
	ctx context.Context,
	id string,
	version *int32,
) (*ComputeQuota, error) {
	b.mu.RLock("DescribeComputeQuota")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	q, ok := computeQuotaByID(b.computeQuotasStoreRO(region), id)
	if !ok {
		return nil, fmt.Errorf("%w: compute quota %q", ErrComputeQuotaNotFound, id)
	}

	if version != nil && *version != q.ComputeQuotaVersion {
		return nil, fmt.Errorf("%w: compute quota %q version %d", ErrComputeQuotaNotFound, id, *version)
	}

	return cloneComputeQuota(q), nil
}

// ListComputeQuotasParams bundles ListComputeQuotas' filter/sort/pagination
// criteria (api_op_ListComputeQuotas.go:30-68, sagemaker@v1.263.2). Like
// ListClusterSchedulerConfigs, this op has no LastModifiedTime filters.
type ListComputeQuotasParams struct {
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	ClusterArn    string
	NameContains  string
	NextToken     string
	SortBy        string
	SortOrder     string
	Status        string
	MaxResults    int32
}

// ListComputeQuotas returns compute quotas matching params, sorted by
// params.SortBy (default CreationTime, undocumented) / params.SortOrder
// (default Descending, documented at api_op_ListComputeQuotas.go:62), capped
// at params.MaxResults.
func (b *InMemoryBackend) ListComputeQuotas(
	ctx context.Context,
	params ListComputeQuotasParams,
) ([]*ComputeQuota, string) {
	b.mu.RLock("ListComputeQuotas")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	tbl := b.computeQuotasStoreRO(region)
	list := make([]*ComputeQuota, 0, tbl.Len())

	for _, q := range tbl.Snapshot() {
		if !matchesComputeQuotaListParams(q, params) {
			continue
		}

		list = append(list, cloneComputeQuota(q))
	}

	asc := strings.EqualFold(params.SortOrder, "Ascending")
	sort.Slice(list, func(i, j int) bool {
		less := computeQuotaSortLess(list[i], list[j], params.SortBy)
		if asc {
			return less
		}

		return !less
	})

	return paginateSlice(list, params.NextToken, params.MaxResults)
}

// matchesComputeQuotaListParams reports whether q satisfies every filter in params.
func matchesComputeQuotaListParams(q *ComputeQuota, p ListComputeQuotasParams) bool {
	if p.ClusterArn != "" && q.ClusterArn != p.ClusterArn {
		return false
	}

	if p.NameContains != "" && !strings.Contains(q.ComputeQuotaName, p.NameContains) {
		return false
	}

	if p.Status != "" && q.Status != p.Status {
		return false
	}

	if p.CreatedAfter != nil && !q.CreationTime.After(*p.CreatedAfter) {
		return false
	}

	if p.CreatedBefore != nil && !q.CreationTime.Before(*p.CreatedBefore) {
		return false
	}

	return true
}

// computeQuotaSortLess orders two compute quotas by sortBy — one of
// SortQuotaBy's real values (Name/CreationTime/Status/ClusterArn,
// types/enums.go:9301-9304). Unlike SortClusterSchedulerConfigBy just above,
// this sibling enum has a fourth value, ClusterArn — read from each op's own
// enum, not assumed shared.
func computeQuotaSortLess(a, b *ComputeQuota, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		if a.ComputeQuotaName != b.ComputeQuotaName {
			return a.ComputeQuotaName < b.ComputeQuotaName
		}
	case keyStatus:
		if a.Status != b.Status {
			return a.Status < b.Status
		}
	case keyClusterArn:
		if a.ClusterArn != b.ClusterArn {
			return a.ClusterArn < b.ClusterArn
		}
	default:
		if !a.CreationTime.Equal(b.CreationTime) {
			return a.CreationTime.Before(b.CreationTime)
		}
	}

	return a.ComputeQuotaName < b.ComputeQuotaName
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
