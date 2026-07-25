package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
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
type AppImageConfig struct {
	CreationTime       time.Time         `json:"CreationTime"`
	LastModifiedTime   time.Time         `json:"LastModifiedTime"`
	Tags               map[string]string `json:"Tags,omitempty"`
	AppImageConfigName string            `json:"AppImageConfigName"`
	AppImageConfigArn  string            `json:"AppImageConfigArn"`
}

func cloneAppImageConfig(a *AppImageConfig) *AppImageConfig {
	cp := *a
	cp.Tags = maps.Clone(a.Tags)

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

// CreateAppImageConfig creates an app image config.
func (b *InMemoryBackend) CreateAppImageConfig(
	ctx context.Context,
	name string,
	tags map[string]string,
) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateAppImageConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: AppImageConfigName is required", ErrValidation)
	}

	store := b.appImageConfigsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: app image config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "app-image-config/"+name)
	now := time.Now()

	a := &AppImageConfig{
		AppImageConfigName: name,
		AppImageConfigArn:  configARN,
		Tags:               mergeTags(nil, tags),
		CreationTime:       now,
		LastModifiedTime:   now,
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

// UpdateAppImageConfig updates an app image config (marks it modified).
func (b *InMemoryBackend) UpdateAppImageConfig(ctx context.Context, name string) (*AppImageConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateAppImageConfig")
	defer b.mu.Unlock()

	a, ok := b.appImageConfigsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: app image config %q not found", ErrAppImageConfigNotFound, name)
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

// ListAppImageConfigs returns all App image configs.
func (b *InMemoryBackend) ListAppImageConfigs(ctx context.Context, nextToken string) ([]*AppImageConfig, string) {
	b.mu.RLock("ListAppImageConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.appImageConfigsStoreRO(region),
		nextToken,
		cloneAppImageConfig,
		func(v *AppImageConfig) string { return v.AppImageConfigName },
	)
}
