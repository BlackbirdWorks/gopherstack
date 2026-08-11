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

// ErrStudioLifecycleConfigNotFound is returned when a studio lifecycle config does not exist.
var ErrStudioLifecycleConfigNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// StudioLifecycleConfig
// ---------------------------------------------------------------------------

// StudioLifecycleConfig represents a SageMaker Studio lifecycle configuration.
type StudioLifecycleConfig struct {
	CreationTime                 time.Time         `json:"CreationTime"`
	LastModifiedTime             time.Time         `json:"LastModifiedTime"`
	Tags                         map[string]string `json:"Tags,omitempty"`
	StudioLifecycleConfigName    string            `json:"StudioLifecycleConfigName"`
	StudioLifecycleConfigArn     string            `json:"StudioLifecycleConfigArn"`
	StudioLifecycleConfigAppType string            `json:"StudioLifecycleConfigAppType,omitempty"`
	StudioLifecycleConfigContent string            `json:"StudioLifecycleConfigContent,omitempty"`
}

func cloneStudioLifecycleConfig(s *StudioLifecycleConfig) *StudioLifecycleConfig {
	cp := *s
	cp.Tags = maps.Clone(s.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeStudioLifecycleConfig.
func (s *StudioLifecycleConfig) MarshalJSON() ([]byte, error) {
	type alias StudioLifecycleConfig

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(s),
		CreationTime:     epochSeconds(s.CreationTime),
		LastModifiedTime: epochSeconds(s.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [StudioLifecycleConfig.MarshalJSON], read
// by persistence.go's snapshot restore path.
func (s *StudioLifecycleConfig) UnmarshalJSON(data []byte) error {
	type alias StudioLifecycleConfig

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(s)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	s.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	s.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateStudioLifecycleConfig creates a Studio lifecycle configuration.
func (b *InMemoryBackend) CreateStudioLifecycleConfig(
	ctx context.Context,
	name, appType, content string,
	tags map[string]string,
) (*StudioLifecycleConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateStudioLifecycleConfig")
	defer b.mu.Unlock()

	if name == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigName is required", ErrValidation)
	}

	if content == "" {
		return nil, fmt.Errorf("%w: StudioLifecycleConfigContent is required", ErrValidation)
	}

	store := b.studioLifecycleConfigsStore(region)

	if _, ok := store.Get(name); ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q already exists", ErrValidation, name)
	}

	configARN := arn.Build("sagemaker", region, b.accountID, "studio-lifecycle-config/"+name)
	now := time.Now()

	s := &StudioLifecycleConfig{
		StudioLifecycleConfigName:    name,
		StudioLifecycleConfigArn:     configARN,
		StudioLifecycleConfigAppType: appType,
		StudioLifecycleConfigContent: content,
		Tags:                         mergeTags(nil, tags),
		CreationTime:                 now,
		LastModifiedTime:             now,
	}
	store.Put(s)

	return cloneStudioLifecycleConfig(s), nil
}

// DescribeStudioLifecycleConfig returns a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DescribeStudioLifecycleConfig(
	ctx context.Context,
	name string,
) (*StudioLifecycleConfig, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeStudioLifecycleConfig")
	defer b.mu.RUnlock()

	s, ok := b.studioLifecycleConfigsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	return cloneStudioLifecycleConfig(s), nil
}

// DeleteStudioLifecycleConfig removes a Studio lifecycle configuration by name.
func (b *InMemoryBackend) DeleteStudioLifecycleConfig(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteStudioLifecycleConfig")
	defer b.mu.Unlock()

	store := b.studioLifecycleConfigsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: Studio lifecycle config %q not found", ErrStudioLifecycleConfigNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListStudioLifecycleConfigs returns all Studio lifecycle configs.
func (b *InMemoryBackend) ListStudioLifecycleConfigs(
	ctx context.Context,
	nextToken string,
) ([]*StudioLifecycleConfig, string) {
	b.mu.RLock("ListStudioLifecycleConfigs")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.studioLifecycleConfigsStoreRO(region),
		nextToken,
		cloneStudioLifecycleConfig,
		func(v *StudioLifecycleConfig) string { return v.StudioLifecycleConfigName },
	)
}
