package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrAlgorithmNotFound is returned when an algorithm does not exist.
	ErrAlgorithmNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrAlgorithmAlreadyExists is returned when an algorithm already exists.
	ErrAlgorithmAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// CreateAlgorithmOptions holds the optional fields accepted by CreateAlgorithm.
type CreateAlgorithmOptions struct {
	Tags                    map[string]string
	AlgorithmName           string
	AlgorithmDescription    string
	TrainingSpecification   json.RawMessage
	InferenceSpecification  json.RawMessage
	ValidationSpecification json.RawMessage
	CertifyForMarketplace   bool
}

// CreateAlgorithm creates a SageMaker algorithm specification.
func (b *InMemoryBackend) CreateAlgorithm(ctx context.Context, opts CreateAlgorithmOptions) (*Algorithm, error) {
	b.mu.Lock("CreateAlgorithm")
	defer b.mu.Unlock()

	if opts.AlgorithmName == "" {
		return nil, fmt.Errorf("%w: AlgorithmName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	algoStore := b.algorithmsStore(region)

	if _, ok := algoStore.Get(opts.AlgorithmName); ok {
		return nil, fmt.Errorf("%w: algorithm %q already exists", ErrAlgorithmAlreadyExists, opts.AlgorithmName)
	}

	algorithmARN := arn.Build("sagemaker", region, b.accountID, "algorithm/"+opts.AlgorithmName)

	statusDetails := AlgorithmStatusDetails{
		ImageScanStatuses:  []AlgorithmStatusItem{},
		ValidationStatuses: []AlgorithmStatusItem{},
	}

	al := &Algorithm{
		AlgorithmName:           opts.AlgorithmName,
		AlgorithmArn:            algorithmARN,
		AlgorithmDescription:    opts.AlgorithmDescription,
		AlgorithmStatus:         algorithmStatusCompleted,
		AlgorithmStatusDetails:  statusDetails,
		TrainingSpecification:   opts.TrainingSpecification,
		InferenceSpecification:  opts.InferenceSpecification,
		ValidationSpecification: opts.ValidationSpecification,
		CertifyForMarketplace:   opts.CertifyForMarketplace,
		Tags:                    mergeTags(nil, opts.Tags),
		CreationTime:            time.Now(),
	}
	algoStore.Put(al)
	b.algorithmARNIndexStore(region)[algorithmARN] = opts.AlgorithmName

	return cloneAlgorithm(al), nil
}

// DescribeAlgorithm returns an algorithm by name.
func (b *InMemoryBackend) DescribeAlgorithm(ctx context.Context, name string) (*Algorithm, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAlgorithm")
	defer b.mu.RUnlock()

	al, ok := b.algorithmsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: algorithm %q not found", ErrAlgorithmNotFound, name)
	}

	return cloneAlgorithm(al), nil
}

// DeleteAlgorithm removes an algorithm by name.
func (b *InMemoryBackend) DeleteAlgorithm(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteAlgorithm")
	defer b.mu.Unlock()

	store := b.algorithmsStore(region)

	al, ok := store.Get(name)
	if !ok {
		return fmt.Errorf("%w: algorithm %q not found", ErrAlgorithmNotFound, name)
	}

	store.Delete(name)
	delete(b.algorithmARNIndexStore(region), al.AlgorithmArn)

	return nil
}

// ListAlgorithms returns a page of algorithms, ordered by name.
func (b *InMemoryBackend) ListAlgorithms(ctx context.Context, nextToken string) ([]*Algorithm, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListAlgorithms")
	defer b.mu.RUnlock()

	return sagemakerListKeyPaged(
		b.algorithmsStoreRO(region),
		nextToken,
		cloneAlgorithm,
		func(v *Algorithm) string { return v.AlgorithmName },
	)
}

// AddAlgorithmInternal adds an algorithm directly for seeding tests.
func (b *InMemoryBackend) AddAlgorithmInternal(ctx context.Context, name string) *Algorithm {
	b.mu.Lock("AddAlgorithmInternal")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)
	algorithmARN := arn.Build("sagemaker", region, b.accountID, "algorithm/"+name)
	al := &Algorithm{
		AlgorithmName:   name,
		AlgorithmArn:    algorithmARN,
		AlgorithmStatus: algorithmStatusCompleted,
		CreationTime:    time.Now(),
		Tags:            make(map[string]string),
	}
	b.algorithmsStore(region).Put(al)
	b.algorithmARNIndexStore(region)[algorithmARN] = al.AlgorithmName

	return cloneAlgorithm(al)
}
