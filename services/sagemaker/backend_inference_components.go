package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

const statusCreating = "Creating"

// ---------------------------------------------------------------------------
// InferenceComponent
// ---------------------------------------------------------------------------

var (
	// ErrInferenceComponentNotFound is returned when an inference component does not exist.
	ErrInferenceComponentNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrInferenceComponentAlreadyExists is returned when an inference component already exists.
	ErrInferenceComponentAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// InferenceComponent represents a SageMaker inference component.
type InferenceComponent struct {
	CreationTime             time.Time         `json:"CreationTime"`
	LastModifiedTime         time.Time         `json:"LastModifiedTime"`
	Tags                     map[string]string `json:"Tags,omitempty"`
	InferenceComponentName   string            `json:"InferenceComponentName"`
	InferenceComponentArn    string            `json:"InferenceComponentArn"`
	EndpointName             string            `json:"EndpointName"`
	VariantName              string            `json:"VariantName,omitempty"`
	InferenceComponentStatus string            `json:"InferenceComponentStatus"`
	CopyCount                int               `json:"CopyCount,omitempty"`
	CurrentCopyCount         int               `json:"CurrentCopyCount,omitempty"`
}

func cloneInferenceComponent(c *InferenceComponent) *InferenceComponent {
	cp := *c
	cp.Tags = maps.Clone(c.Tags)

	return &cp
}

// CreateInferenceComponentOptions holds input fields for CreateInferenceComponent.
type CreateInferenceComponentOptions struct {
	Tags                   map[string]string
	InferenceComponentName string
	EndpointName           string
	VariantName            string
	CopyCount              int
}

// CreateInferenceComponent creates a SageMaker inference component.
func (b *InMemoryBackend) CreateInferenceComponent(
	ctx context.Context,
	opts CreateInferenceComponentOptions,
) (*InferenceComponent, error) {
	b.mu.Lock("CreateInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.InferenceComponentName == "" {
		return nil, fmt.Errorf("%w: InferenceComponentName is required", ErrValidation)
	}

	if _, ok := b.inferenceComponentsStore(region).Get(opts.InferenceComponentName); ok {
		return nil, fmt.Errorf(
			"%w: inference component %q already exists",
			ErrInferenceComponentAlreadyExists,
			opts.InferenceComponentName,
		)
	}

	compARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"inference-component/"+opts.InferenceComponentName,
	)
	now := time.Now()

	c := &InferenceComponent{
		InferenceComponentName:   opts.InferenceComponentName,
		InferenceComponentArn:    compARN,
		EndpointName:             opts.EndpointName,
		VariantName:              opts.VariantName,
		InferenceComponentStatus: statusCreating,
		CopyCount:                opts.CopyCount,
		CurrentCopyCount:         0,
		Tags:                     mergeTags(nil, opts.Tags),
		CreationTime:             now,
		LastModifiedTime:         now,
	}
	b.inferenceComponentsStore(region).Put(c)

	return cloneInferenceComponent(c), nil
}

// DescribeInferenceComponent returns an inference component by name.
func (b *InMemoryBackend) DescribeInferenceComponent(ctx context.Context, name string) (*InferenceComponent, error) {
	b.mu.RLock("DescribeInferenceComponent")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	return cloneInferenceComponent(c), nil
}

// ListInferenceComponents returns all inference components with pagination.
func (b *InMemoryBackend) ListInferenceComponents(
	ctx context.Context,
	endpointFilter, nextToken string,
) ([]*InferenceComponent, string) {
	b.mu.RLock("ListInferenceComponents")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceComponentsStoreRO(region)

	keys := make([]string, 0, store.Len())
	for _, c := range store.All() {
		if endpointFilter != "" && c.EndpointName != endpointFilter {
			continue
		}

		keys = append(keys, c.InferenceComponentName)
	}

	sort.Strings(keys)

	start := 0
	if nextToken != "" {
		for i, k := range keys {
			if k == nextToken {
				start = i

				break
			}
		}
	}

	end := min(start+sagemakerDefaultPageSize, len(keys))

	out := make([]*InferenceComponent, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, cloneInferenceComponent(tableGet(store, k)))
	}

	next := ""
	if end < len(keys) {
		next = keys[end]
	}

	return out, next
}

// UpdateInferenceComponent updates an inference component's variant or copy count.
func (b *InMemoryBackend) UpdateInferenceComponent(ctx context.Context, name, variantName string, copyCount int) error {
	b.mu.Lock("UpdateInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	if variantName != "" {
		c.VariantName = variantName
	}

	if copyCount > 0 {
		c.CopyCount = copyCount
	}

	c.LastModifiedTime = time.Now()

	return nil
}

// UpdateInferenceComponentRuntimeConfig updates the copy count for an inference component.
func (b *InMemoryBackend) UpdateInferenceComponentRuntimeConfig(ctx context.Context, name string, copyCount int) error {
	b.mu.Lock("UpdateInferenceComponentRuntimeConfig")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	c, ok := b.inferenceComponentsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	c.CopyCount = copyCount
	c.CurrentCopyCount = copyCount
	c.LastModifiedTime = time.Now()

	return nil
}

// DeleteInferenceComponent deletes an inference component by name.
func (b *InMemoryBackend) DeleteInferenceComponent(ctx context.Context, name string) error {
	b.mu.Lock("DeleteInferenceComponent")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	store := b.inferenceComponentsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: inference component %q", ErrInferenceComponentNotFound, name)
	}

	store.Delete(name)

	return nil
}
