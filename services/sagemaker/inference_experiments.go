package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrInferenceExperimentNotFound is returned when an inference experiment does not exist.
var ErrInferenceExperimentNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

// InferenceExperiment represents a SageMaker inference experiment.
type InferenceExperiment struct {
	CreationTime     time.Time         `json:"CreationTime"`
	LastModifiedTime time.Time         `json:"LastModifiedTime"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Name             string            `json:"Name"`
	Arn              string            `json:"Arn"`
	Status           string            `json:"Status"`
	Type             string            `json:"Type,omitempty"`
	RoleArn          string            `json:"RoleArn,omitempty"`
	Description      string            `json:"Description,omitempty"`
}

func cloneInferenceExperiment(e *InferenceExperiment) *InferenceExperiment {
	cp := *e
	cp.Tags = maps.Clone(e.Tags)

	return &cp
}

// MarshalJSON emits CreationTime/LastModifiedTime as AWS awsjson1.1
// epoch-seconds numbers rather than Go's default RFC3339 strings — this
// struct is marshaled directly by handleDescribeInferenceExperiment.
func (e *InferenceExperiment) MarshalJSON() ([]byte, error) {
	type alias InferenceExperiment

	return json.Marshal(struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{
		alias:            (*alias)(e),
		CreationTime:     epochSeconds(e.CreationTime),
		LastModifiedTime: epochSeconds(e.LastModifiedTime),
	})
}

// UnmarshalJSON is the inverse of [InferenceExperiment.MarshalJSON], read by
// persistence.go's snapshot restore path.
func (e *InferenceExperiment) UnmarshalJSON(data []byte) error {
	type alias InferenceExperiment

	aux := struct {
		*alias
		CreationTime     float64 `json:"CreationTime"`
		LastModifiedTime float64 `json:"LastModifiedTime"`
	}{alias: (*alias)(e)}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	e.CreationTime = timeFromEpochSeconds(aux.CreationTime)
	e.LastModifiedTime = timeFromEpochSeconds(aux.LastModifiedTime)

	return nil
}

// CreateInferenceExperiment creates an inference experiment.
func (b *InMemoryBackend) CreateInferenceExperiment(
	ctx context.Context,
	name, expType, roleArn string,
	tags map[string]string,
) (*InferenceExperiment, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrValidation)
	}

	return sagemakerCreate(ctx, b,
		"CreateInferenceExperiment", name, "inference-experiment",
		b.inferenceExperimentsStore,
		func(n string) error { return sagemakerDupErr("inference experiment", n) },
		func(arnStr string, now time.Time) *InferenceExperiment {
			return &InferenceExperiment{
				Name:             name,
				Arn:              arnStr,
				Status:           "Running",
				Type:             expType,
				RoleArn:          roleArn,
				Tags:             mergeTags(nil, tags),
				CreationTime:     now,
				LastModifiedTime: now,
			}
		},
		cloneInferenceExperiment,
	)
}

// DescribeInferenceExperiment returns an inference experiment by name.
func (b *InMemoryBackend) DescribeInferenceExperiment(ctx context.Context, name string) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeInferenceExperiment")
	defer b.mu.RUnlock()

	e, ok := b.inferenceExperimentsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	return cloneInferenceExperiment(e), nil
}

// StopInferenceExperiment sets an inference experiment status to "Cancelled".
func (b *InMemoryBackend) StopInferenceExperiment(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StopInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	e.Status = "Cancelled"
	e.LastModifiedTime = time.Now()

	return nil
}

// StartInferenceExperiment transitions an inference experiment to "Running".
func (b *InMemoryBackend) StartInferenceExperiment(ctx context.Context, name string) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("StartInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	e.Status = statusRunning
	e.LastModifiedTime = time.Now()

	return cloneInferenceExperiment(e), nil
}

// UpdateInferenceExperiment updates an inference experiment's description.
func (b *InMemoryBackend) UpdateInferenceExperiment(
	ctx context.Context,
	name, description string,
) (*InferenceExperiment, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateInferenceExperiment")
	defer b.mu.Unlock()

	e, ok := b.inferenceExperimentsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	if description != "" {
		e.Description = description
	}

	e.LastModifiedTime = time.Now()

	return cloneInferenceExperiment(e), nil
}

// DeleteInferenceExperiment removes an inference experiment by name.
func (b *InMemoryBackend) DeleteInferenceExperiment(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteInferenceExperiment")
	defer b.mu.Unlock()

	store := b.inferenceExperimentsStore(region)

	if _, ok := store.Get(name); !ok {
		return fmt.Errorf("%w: inference experiment %q not found", ErrInferenceExperimentNotFound, name)
	}

	store.Delete(name)

	return nil
}

// ListInferenceExperiments returns all inference experiments.
func (b *InMemoryBackend) ListInferenceExperiments(
	ctx context.Context,
	nextToken string,
) ([]*InferenceExperiment, string) {
	b.mu.RLock("ListInferenceExperiments")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.inferenceExperimentsStoreRO(region),
		nextToken,
		cloneInferenceExperiment,
		func(v *InferenceExperiment) string { return v.Name },
	)
}
