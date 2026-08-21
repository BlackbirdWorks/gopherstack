package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrAppImageConfigNotFound is returned when an app image config does not exist.
var ErrAppImageConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// AppImageConfig
// ---------------------------------------------------------------------------

// AppImageConfig represents a SageMaker app image configuration.
// KernelGatewayImageConfig/JupyterLabAppImageConfig/CodeEditorAppImageConfig
// are stored as opaque JSON (the json.RawMessage passthrough convention
// already used elsewhere in this campaign): this backend never actually
// runs a Studio app image, so echoing the client's Create/Update payload
// back verbatim is wire-accurate for every field a real client would read.
type AppImageConfig struct {
	CreationTime             time.Time         `json:"CreationTime"`
	LastModifiedTime         time.Time         `json:"LastModifiedTime"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	AppImageConfigName       string            `json:"AppImageConfigName"`
	AppImageConfigArn        string            `json:"AppImageConfigArn"`
	KernelGatewayImageConfig json.RawMessage   `json:"KernelGatewayImageConfig,omitempty"`
	JupyterLabAppImageConfig json.RawMessage   `json:"JupyterLabAppImageConfig,omitempty"`
	CodeEditorAppImageConfig json.RawMessage   `json:"CodeEditorAppImageConfig,omitempty"`
}

func cloneAppImageConfig(a *AppImageConfig) *AppImageConfig {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)
	cp.KernelGatewayImageConfig = append(json.RawMessage(nil), a.KernelGatewayImageConfig...)
	cp.JupyterLabAppImageConfig = append(json.RawMessage(nil), a.JupyterLabAppImageConfig...)
	cp.CodeEditorAppImageConfig = append(json.RawMessage(nil), a.CodeEditorAppImageConfig...)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeAppImageConfig.
func (a *AppImageConfig) MarshalJSON() ([]byte, error) {
	type alias AppImageConfig

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(a),
		CreationTime:     epochSeconds(a.CreationTime),
		LastModifiedTime: epochSeconds(a.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [AppImageConfig.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (a *AppImageConfig) UnmarshalJSON(data []byte) error {
	type alias AppImageConfig

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(a)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	a.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	a.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateAppImageConfigOptions holds input fields for CreateAppImageConfig
// (api_op_CreateAppImageConfig.go:28-52).
type CreateAppImageConfigOptions struct {
	AppImageConfigName       string
	Tags                     map[string]string
	KernelGatewayImageConfig json.RawMessage
	JupyterLabAppImageConfig json.RawMessage
	CodeEditorAppImageConfig json.RawMessage
}

// CreateAppImageConfig creates an app image config.
func (b *InMemoryBackend) CreateAppImageConfig(
	ctx context.Context,
	opts CreateAppImageConfigOptions,
) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAppImageConfig")
	defer b.mu.Unlock()

	if opts.AppImageConfigName == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", ErrValidation)
	}

	store := b.appImageConfigsStore(region)

	if _, ok := store.Get(opts.AppImageConfigName); ok {
		return nil, fmt.Errorf("%w: app image config %q already exists", ErrValidation, opts.AppImageConfigName)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "app-image-config/"+opts.AppImageConfigName)
	now := time.Now()

	a := &AppImageConfig{
		AppImageConfigName:       opts.AppImageConfigName,
		AppImageConfigArn:        configARN,
		Tags:                     mergeTags(nil, opts.Tags),
		CreationTime:             now,
		LastModifiedTime:         now,
		KernelGatewayImageConfig: opts.KernelGatewayImageConfig,
		JupyterLabAppImageConfig: opts.JupyterLabAppImageConfig,
		CodeEditorAppImageConfig: opts.CodeEditorAppImageConfig,
	}
	store.Put(a)

	return cloneAppImageConfig(a), nil
}

// DescribeAppImageConfig returns an app image config by name.
func (b *InMemoryBackend) DescribeAppImageConfig(ctx context.Context, name string) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAppImageConfig")
	defer b.mu.RUnlock()

	a, ok := b.appImageConfigsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	return cloneAppImageConfig(a), nil
}

// UpdateAppImageConfigOptions holds input fields for UpdateAppImageConfig
// (api_op_UpdateAppImageConfig.go:28-45). Previously this op decoded and
// applied nothing beyond the name, so a client attempting to actually change
// an image's kernel/container/filesystem config had every field silently
// dropped -- the same "accept and drop" bug class as UpdateTrainingJob.
type UpdateAppImageConfigOptions struct {
	KernelGatewayImageConfig json.RawMessage
	JupyterLabAppImageConfig json.RawMessage
	CodeEditorAppImageConfig json.RawMessage
}

// UpdateAppImageConfig updates an app image config's kernel/container
// configuration.
func (b *InMemoryBackend) UpdateAppImageConfig(
	ctx context.Context,
	name string,
	opts UpdateAppImageConfigOptions,
) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateAppImageConfig")
	defer b.mu.Unlock()

	a, ok := b.appImageConfigsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	if len(opts.KernelGatewayImageConfig) > 0 {
		a.KernelGatewayImageConfig = opts.KernelGatewayImageConfig
	}

	if len(opts.JupyterLabAppImageConfig) > 0 {
		a.JupyterLabAppImageConfig = opts.JupyterLabAppImageConfig
	}

	if len(opts.CodeEditorAppImageConfig) > 0 {
		a.CodeEditorAppImageConfig = opts.CodeEditorAppImageConfig
	}

	a.LastModifiedTime = time.Now()

	return cloneAppImageConfig(a), nil
}

// DeleteAppImageConfig removes an app image config by name.
func (b *InMemoryBackend) DeleteAppImageConfig(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAppImageConfig")
	defer b.mu.Unlock()

	store := b.appImageConfigsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListAppImageConfigsFilter bundles the filter/sort criteria for
// ListAppImageConfigs (api_op_ListAppImageConfigs.go:31-64).
type ListAppImageConfigsFilter struct {
	CreationTimeAfter  *time.Time
	CreationTimeBefore *time.Time
	ModifiedTimeAfter  *time.Time
	ModifiedTimeBefore *time.Time
	NameContains       string
	SortBy             string
	SortOrder          string
	MaxResults         int32
}

// ListAppImageConfigs lists app image configs, optionally filtered and
// sorted. Previously this decoded only NextToken and dropped every filter
// and sort control the op's own request shape declares.
func (b *InMemoryBackend) ListAppImageConfigs(
	ctx context.Context,
	nextToken string,
	f ListAppImageConfigsFilter,
) ([]*AppImageConfig, string) {
	b.mu.RLock("ListAppImageConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*AppImageConfig, 0, b.appImageConfigsStoreRO(region).Len())

	for _, a := range b.appImageConfigsStoreRO(region).All() {
		if appImageConfigMatchesFilter(a, f) {
			list = append(list, cloneAppImageConfig(a))
		}
	}

	// api_op_ListAppImageConfigs.go:63,66: real defaults are
	// CreationTime/Descending.
	desc := f.SortOrder != sortOrderAscending
	sort.Slice(list, func(i, k int) bool {
		var less bool

		switch f.SortBy {
		case keyGenericName:
			less = list[i].AppImageConfigName < list[k].AppImageConfigName
		case keyLastModifiedTime:
			less = list[i].LastModifiedTime.Before(list[k].LastModifiedTime)
		default:
			less = list[i].CreationTime.Before(list[k].CreationTime)
		}

		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, f.MaxResults)
}

func appImageConfigMatchesFilter(a *AppImageConfig, f ListAppImageConfigsFilter) bool {
	if f.NameContains != "" &&
		!strings.Contains(strings.ToLower(a.AppImageConfigName), strings.ToLower(f.NameContains)) {
		return false
	}

	if !timeWindowOK(a.CreationTime, f.CreationTimeAfter, f.CreationTimeBefore) {
		return false
	}

	return timeWindowOK(a.LastModifiedTime, f.ModifiedTimeAfter, f.ModifiedTimeBefore)
}
