package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrFeatureGroupNotFound is returned when a feature group does not exist.
	ErrFeatureGroupNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrFeatureGroupAlreadyExists is returned when a feature group already exists.
	ErrFeatureGroupAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// FeatureDefinition holds the definition of a single feature.
type FeatureDefinition struct {
	FeatureName string `json:"FeatureName"`
	FeatureType string `json:"FeatureType,omitempty"`
}

// TTLDuration mirrors types.TtlDuration.
type TTLDuration struct {
	Value *int32 `json:"Value,omitempty"`
	Unit  string `json:"Unit,omitempty"`
}

// OnlineStoreSecurityConfig mirrors types.OnlineStoreSecurityConfig.
type OnlineStoreSecurityConfig struct {
	KmsKeyID string `json:"KmsKeyId,omitempty"`
}

// OnlineStoreConfig mirrors types.OnlineStoreConfig.
type OnlineStoreConfig struct {
	SecurityConfig    *OnlineStoreSecurityConfig `json:"SecurityConfig,omitempty"`
	TTLDuration       *TTLDuration               `json:"TtlDuration,omitempty"`
	EnableOnlineStore *bool                      `json:"EnableOnlineStore,omitempty"`
	StorageType       string                     `json:"StorageType,omitempty"`
}

// S3StorageConfig mirrors types.S3StorageConfig.
type S3StorageConfig struct {
	S3URI               string `json:"S3Uri"`
	KmsKeyID            string `json:"KmsKeyId,omitempty"`
	ResolvedOutputS3URI string `json:"ResolvedOutputS3Uri,omitempty"`
}

// DataCatalogConfig mirrors types.DataCatalogConfig.
type DataCatalogConfig struct {
	Catalog   string `json:"Catalog,omitempty"`
	Database  string `json:"Database,omitempty"`
	TableName string `json:"TableName,omitempty"`
}

// OfflineStoreConfig mirrors types.OfflineStoreConfig.
type OfflineStoreConfig struct {
	S3StorageConfig          *S3StorageConfig   `json:"S3StorageConfig,omitempty"`
	DataCatalogConfig        *DataCatalogConfig `json:"DataCatalogConfig,omitempty"`
	DisableGlueTableCreation *bool              `json:"DisableGlueTableCreation,omitempty"`
	TableFormat              string             `json:"TableFormat,omitempty"`
}

// ThroughputConfig mirrors both types.ThroughputConfig (CreateFeatureGroupInput)
// and types.ThroughputConfigDescription (DescribeFeatureGroupOutput) — the two
// SDK types have identical fields, so this backend stores/emits one shape for both.
//
// ThroughputMode is opaque string passthrough, not validated against the real
// ThroughputModeOnDemand/ThroughputModeProvisioned ("OnDemand"/"Provisioned")
// enum values -- matching this campaign's established convention for deeply
// nested config passthrough (parity-21's AppImageConfig kernel configs).
type ThroughputConfig struct {
	ProvisionedReadCapacityUnits  *int32 `json:"ProvisionedReadCapacityUnits,omitempty"`
	ProvisionedWriteCapacityUnits *int32 `json:"ProvisionedWriteCapacityUnits,omitempty"`
	ThroughputMode                string `json:"ThroughputMode,omitempty"`
}

// OnlineStoreConfigUpdate mirrors types.OnlineStoreConfigUpdate
// (UpdateFeatureGroupInput.OnlineStoreConfig).
type OnlineStoreConfigUpdate struct {
	TTLDuration *TTLDuration `json:"TtlDuration,omitempty"`
}

// ThroughputConfigUpdate mirrors types.ThroughputConfigUpdate
// (UpdateFeatureGroupInput.ThroughputConfig).
type ThroughputConfigUpdate struct {
	ProvisionedReadCapacityUnits  *int32 `json:"ProvisionedReadCapacityUnits,omitempty"`
	ProvisionedWriteCapacityUnits *int32 `json:"ProvisionedWriteCapacityUnits,omitempty"`
	ThroughputMode                string `json:"ThroughputMode,omitempty"`
}

// LastUpdateStatus mirrors types.LastUpdateStatus (DescribeFeatureGroupOutput).
// This backend applies every UpdateFeatureGroup call synchronously, so
// Status is always Successful -- there is no InProgress window to observe,
// disclosed rather than modeled as a real async FSM.
type LastUpdateStatus struct {
	Status        string `json:"Status"`
	FailureReason string `json:"FailureReason,omitempty"`
}

// FeatureGroup represents a SageMaker Feature Store feature group.
// OfflineStoreStatus is a static "Active" once an OfflineStoreConfig is set
// (this backend never simulates offline replication failure).
type FeatureGroup struct {
	CreationTime                time.Time           `json:"CreationTime"`
	LastModifiedTime            time.Time           `json:"LastModifiedTime"`
	Tags                        map[string]string   `json:"Tags,omitempty"`
	OnlineStoreConfig           *OnlineStoreConfig  `json:"OnlineStoreConfig,omitempty"`
	OfflineStoreConfig          *OfflineStoreConfig `json:"OfflineStoreConfig,omitempty"`
	ThroughputConfig            *ThroughputConfig   `json:"ThroughputConfig,omitempty"`
	LastUpdateStatus            *LastUpdateStatus   `json:"LastUpdateStatus,omitempty"`
	FeatureGroupName            string              `json:"FeatureGroupName"`
	FeatureGroupArn             string              `json:"FeatureGroupArn"`
	RecordIdentifierFeatureName string              `json:"RecordIdentifierFeatureName,omitempty"`
	EventTimeFeatureName        string              `json:"EventTimeFeatureName,omitempty"`
	FeatureGroupStatus          string              `json:"FeatureGroupStatus"`
	OfflineStoreStatus          string              `json:"OfflineStoreStatus,omitempty"`
	Description                 string              `json:"Description,omitempty"`
	RoleArn                     string              `json:"RoleArn,omitempty"`
	FeatureDefinitions          []FeatureDefinition `json:"FeatureDefinitions,omitempty"`
}

func cloneFeatureGroup(fg *FeatureGroup) *FeatureGroup {
	cp := *fg
	cp.Tags = maps.Clone(fg.Tags)
	cp.FeatureDefinitions = make([]FeatureDefinition, len(fg.FeatureDefinitions))
	copy(cp.FeatureDefinitions, fg.FeatureDefinitions)

	if fg.OnlineStoreConfig != nil {
		osc := *fg.OnlineStoreConfig
		cp.OnlineStoreConfig = &osc
	}

	if fg.OfflineStoreConfig != nil {
		ofc := *fg.OfflineStoreConfig
		cp.OfflineStoreConfig = &ofc
	}

	if fg.ThroughputConfig != nil {
		tc := *fg.ThroughputConfig
		cp.ThroughputConfig = &tc
	}

	if fg.LastUpdateStatus != nil {
		lus := *fg.LastUpdateStatus
		cp.LastUpdateStatus = &lus
	}

	return &cp
}

// CreateFeatureGroupOptions holds the parameters CreateFeatureGroup accepts.
type CreateFeatureGroupOptions struct {
	Tags                        map[string]string
	OnlineStoreConfig           *OnlineStoreConfig
	OfflineStoreConfig          *OfflineStoreConfig
	ThroughputConfig            *ThroughputConfig
	FeatureGroupName            string
	RecordIdentifierFeatureName string
	EventTimeFeatureName        string
	Description                 string
	RoleArn                     string
	FeatureDefinitions          []FeatureDefinition
}

// CreateFeatureGroup creates a new feature group.
func (b *InMemoryBackend) CreateFeatureGroup(
	ctx context.Context,
	opts CreateFeatureGroupOptions,
) (*FeatureGroup, error) {
	b.mu.Lock("CreateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.featureGroupsStore(region).Get(opts.FeatureGroupName); ok {
		return nil, fmt.Errorf(
			"%w: feature group %s already exists",
			ErrFeatureGroupAlreadyExists,
			opts.FeatureGroupName,
		)
	}

	fgArn := arn.Build("sagemaker", region, b.accountID, "feature-group/"+opts.FeatureGroupName)
	storedDefs := make([]FeatureDefinition, len(opts.FeatureDefinitions))
	copy(storedDefs, opts.FeatureDefinitions)

	now := time.Now()

	offlineStoreStatus := ""
	if opts.OfflineStoreConfig != nil {
		offlineStoreStatus = "Active"
	}

	fg := &FeatureGroup{
		FeatureGroupName:            opts.FeatureGroupName,
		FeatureGroupArn:             fgArn,
		RecordIdentifierFeatureName: opts.RecordIdentifierFeatureName,
		EventTimeFeatureName:        opts.EventTimeFeatureName,
		Description:                 opts.Description,
		RoleArn:                     opts.RoleArn,
		FeatureDefinitions:          storedDefs,
		FeatureGroupStatus:          "Created",
		CreationTime:                now,
		LastModifiedTime:            now,
		Tags:                        mergeTags(nil, opts.Tags),
		OnlineStoreConfig:           opts.OnlineStoreConfig,
		OfflineStoreConfig:          opts.OfflineStoreConfig,
		ThroughputConfig:            opts.ThroughputConfig,
		OfflineStoreStatus:          offlineStoreStatus,
	}
	b.featureGroupsStore(region).Put(fg)

	return cloneFeatureGroup(fg), nil
}

// DescribeFeatureGroup returns a feature group by name.
func (b *InMemoryBackend) DescribeFeatureGroup(ctx context.Context, name string) (*FeatureGroup, error) {
	b.mu.RLock("DescribeFeatureGroup")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	return cloneFeatureGroup(fg), nil
}

// ListFeatureGroupsFilter bundles the filter/sort criteria for
// ListFeatureGroups (api_op_ListFeatureGroups.go:29-64). Previously this
// decoded only NextToken and dropped every filter and sort control the op's
// own request shape declares.
type ListFeatureGroupsFilter struct {
	CreationTimeAfter        *time.Time
	CreationTimeBefore       *time.Time
	FeatureGroupStatusEquals string
	NameContains             string
	OfflineStoreStatusEquals string
	SortBy                   string
	SortOrder                string
	MaxResults               int32
}

// ListFeatureGroups returns feature groups matching f, sorted and paginated.
// The op's own doc text states no explicit default for SortBy/SortOrder
// (unlike ListExperiments/ListCompilationJobs, which do); Name/Ascending is
// kept as this backend's pre-existing default rather than invented.
func (b *InMemoryBackend) ListFeatureGroups(
	ctx context.Context,
	nextToken string,
	f ListFeatureGroupsFilter,
) ([]*FeatureGroup, string) {
	b.mu.RLock("ListFeatureGroups")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*FeatureGroup, 0, b.featureGroupsStoreRO(region).Len())

	for _, fg := range b.featureGroupsStoreRO(region).All() {
		if featureGroupMatchesFilter(fg, f) {
			list = append(list, cloneFeatureGroup(fg))
		}
	}

	desc := f.SortOrder == sortOrderDescending
	sort.SliceStable(list, func(i, k int) bool {
		var less bool

		switch f.SortBy {
		case "FeatureGroupStatus":
			less = list[i].FeatureGroupStatus < list[k].FeatureGroupStatus
		case "OfflineStoreStatus":
			less = list[i].OfflineStoreStatus < list[k].OfflineStoreStatus
		case "CreationTime":
			less = list[i].CreationTime.Before(list[k].CreationTime)
		default:
			less = list[i].FeatureGroupName < list[k].FeatureGroupName
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}

func featureGroupMatchesFilter(fg *FeatureGroup, f ListFeatureGroupsFilter) bool {
	if f.FeatureGroupStatusEquals != "" && fg.FeatureGroupStatus != f.FeatureGroupStatusEquals {
		return false
	}

	if f.OfflineStoreStatusEquals != "" && fg.OfflineStoreStatus != f.OfflineStoreStatusEquals {
		return false
	}

	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(fg.FeatureGroupName), strings.ToLower(f.NameContains)) {
		return false
	}

	return timeWindowOK(fg.CreationTime, f.CreationTimeAfter, f.CreationTimeBefore)
}

// DeleteFeatureGroup deletes a feature group.
func (b *InMemoryBackend) DeleteFeatureGroup(ctx context.Context, name string) error {
	b.mu.Lock("DeleteFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.featureGroupsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	store.Delete(name)

	return nil
}

// UpdateFeatureGroupOptions bundles the three independent update mechanisms
// UpdateFeatureGroupInput declares (api_op_UpdateFeatureGroup.go:38-63):
// FeatureAdditions, OnlineStoreConfig and ThroughputConfig. Previously only
// FeatureAdditions was decoded at all -- the other two were entirely absent
// from the wire struct, so a client updating a feature group's online-store
// TtlDuration or switching its throughput mode got a 200 and no effect
// whatsoever, an accept-and-drop gap on two of the op's three real update
// paths.
type UpdateFeatureGroupOptions struct {
	OnlineStoreConfig *OnlineStoreConfigUpdate
	ThroughputConfig  *ThroughputConfigUpdate
	FeatureAdditions  []FeatureDefinition
}

// UpdateFeatureGroup mutates FeatureDefinitions, the online store's
// TtlDuration and/or the throughput configuration on an existing feature
// group. LastUpdateStatus is set to Successful synchronously -- see
// [LastUpdateStatus]'s doc for the disclosure on why this backend has no
// InProgress window.
func (b *InMemoryBackend) UpdateFeatureGroup(
	ctx context.Context,
	name string,
	opts UpdateFeatureGroupOptions,
) (*FeatureGroup, error) {
	b.mu.Lock("UpdateFeatureGroup")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	fg, ok := b.featureGroupsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: feature group %q not found", ErrFeatureGroupNotFound, name)
	}

	if len(opts.FeatureAdditions) > 0 {
		fg.FeatureDefinitions = append(fg.FeatureDefinitions, opts.FeatureAdditions...)
	}

	if opts.OnlineStoreConfig != nil {
		if fg.OnlineStoreConfig == nil {
			fg.OnlineStoreConfig = &OnlineStoreConfig{}
		}

		fg.OnlineStoreConfig.TTLDuration = opts.OnlineStoreConfig.TTLDuration
	}

	if opts.ThroughputConfig != nil {
		if fg.ThroughputConfig == nil {
			fg.ThroughputConfig = &ThroughputConfig{}
		}

		fg.ThroughputConfig.ProvisionedReadCapacityUnits = opts.ThroughputConfig.ProvisionedReadCapacityUnits
		fg.ThroughputConfig.ProvisionedWriteCapacityUnits = opts.ThroughputConfig.ProvisionedWriteCapacityUnits

		if opts.ThroughputConfig.ThroughputMode != "" {
			fg.ThroughputConfig.ThroughputMode = opts.ThroughputConfig.ThroughputMode
		}
	}

	fg.LastModifiedTime = time.Now()
	fg.LastUpdateStatus = &LastUpdateStatus{Status: statusSuccessful}

	return cloneFeatureGroup(fg), nil
}
