package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrEndpointNotFound is returned when an endpoint does not exist.
	ErrEndpointNotFound = awserr.New("ValidationException", awserr.ErrNotFound)
	// ErrEndpointAlreadyExists is returned when an endpoint already exists.
	ErrEndpointAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

type Endpoint struct {
	CreationTime       time.Time                  `json:"CreationTime"`
	LastModifiedTime   time.Time                  `json:"LastModifiedTime"`
	Tags               map[string]string          `json:"Tags,omitempty"`
	EndpointName       string                     `json:"EndpointName"`
	EndpointArn        string                     `json:"EndpointArn"`
	EndpointConfigName string                     `json:"EndpointConfigName"`
	EndpointStatus     string                     `json:"EndpointStatus"`
	FailureReason      string                     `json:"FailureReason,omitempty"`
	ProductionVariants []ProductionVariantSummary `json:"ProductionVariants,omitempty"`
}

// ProductionVariantStatus describes the current deployment stage of a
// production variant on a deployed endpoint.
type ProductionVariantStatus struct {
	Status        string `json:"Status"`
	StatusMessage string `json:"StatusMessage,omitempty"`
}

// ProductionVariantSummary describes a production variant as deployed on a
// live endpoint (the shape returned by DescribeEndpoint). This is distinct
// from ProductionVariant, which is the EndpointConfig-time configuration:
// AWS renames Initial* to Desired* and adds Current* fields that reflect
// deployed state once the endpoint has finished (re)deploying.
type ProductionVariantSummary struct {
	CurrentWeight        *float64                  `json:"CurrentWeight,omitempty"`
	DesiredWeight        *float64                  `json:"DesiredWeight,omitempty"`
	CurrentInstanceCount *int32                    `json:"CurrentInstanceCount,omitempty"`
	DesiredInstanceCount *int32                    `json:"DesiredInstanceCount,omitempty"`
	VariantName          string                    `json:"VariantName"`
	VariantStatus        []ProductionVariantStatus `json:"VariantStatus,omitempty"`
}

// newVariantSummaries builds the initial ProductionVariantSummary list for an
// endpoint from an endpoint config's ProductionVariants. Desired* fields are
// populated from the config's Initial* fields; Current* fields are left nil
// until the endpoint (re)reaches InService.
func newVariantSummaries(pvs []ProductionVariant) []ProductionVariantSummary {
	summaries := make([]ProductionVariantSummary, len(pvs))

	for i, pv := range pvs {
		weight := pv.InitialVariantWeight
		count := pv.InitialInstanceCount
		summaries[i] = ProductionVariantSummary{
			VariantName:          pv.VariantName,
			DesiredWeight:        &weight,
			DesiredInstanceCount: &count,
			VariantStatus:        []ProductionVariantStatus{{Status: "Creating"}},
		}
	}

	return summaries
}

// cloneEndpoint returns a deep copy of ep.
func cloneEndpoint(ep *Endpoint) *Endpoint {
	cp := *ep
	cp.Tags = maps.Clone(ep.Tags)
	cp.ProductionVariants = make([]ProductionVariantSummary, len(ep.ProductionVariants))
	for i, pv := range ep.ProductionVariants {
		cp.ProductionVariants[i] = cloneProductionVariantSummary(pv)
	}

	return &cp
}

// cloneProductionVariantSummary returns a deep copy of a ProductionVariantSummary.
func cloneProductionVariantSummary(pv ProductionVariantSummary) ProductionVariantSummary {
	if pv.CurrentWeight != nil {
		w := *pv.CurrentWeight
		pv.CurrentWeight = &w
	}
	if pv.DesiredWeight != nil {
		w := *pv.DesiredWeight
		pv.DesiredWeight = &w
	}
	if pv.CurrentInstanceCount != nil {
		c := *pv.CurrentInstanceCount
		pv.CurrentInstanceCount = &c
	}
	if pv.DesiredInstanceCount != nil {
		c := *pv.DesiredInstanceCount
		pv.DesiredInstanceCount = &c
	}
	pv.VariantStatus = append([]ProductionVariantStatus(nil), pv.VariantStatus...)

	return pv
}

// TrainingJob represents a SageMaker training job.

// ---------------------------------------------------------------------------
// Backend field additions via a separate map — we extend InMemoryBackend with
// a parallel struct to avoid modifying the original large file.
// ---------------------------------------------------------------------------

// extendedState holds the new resource maps added in this file.
// It is embedded into InMemoryBackend via the ext field initialised in
// NewInMemoryBackendExtended (called from NewInMemoryBackend after init).
// We keep it simple: InMemoryBackend itself is extended with four new fields.

// CreateEndpoint creates a new SageMaker endpoint.
func (b *InMemoryBackend) CreateEndpoint(
	ctx context.Context,
	name, endpointConfigName string,
	tags map[string]string,
) (*Endpoint, error) {
	b.mu.Lock("CreateEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.endpointsStore(region).Get(name); ok {
		return nil, fmt.Errorf("%w: endpoint %s already exists", ErrEndpointAlreadyExists, name)
	}

	ec, ok := b.endpointConfigsStore(region).Get(endpointConfigName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			endpointConfigName,
		)
	}

	epARN := arn.Build("sagemaker", region, b.accountID, "endpoint/"+name)
	now := time.Now()
	ep := &Endpoint{
		EndpointName:       name,
		EndpointArn:        epARN,
		EndpointConfigName: endpointConfigName,
		EndpointStatus:     "Creating",
		CreationTime:       now,
		LastModifiedTime:   now,
		Tags:               mergeTags(nil, tags),
		ProductionVariants: newVariantSummaries(ec.ProductionVariants),
	}
	b.endpointsStore(region).Put(ep)
	b.endpointARNIndexStore(region)[epARN] = name

	return cloneEndpoint(ep), nil
}

// DescribeEndpoint returns an endpoint by name.
func (b *InMemoryBackend) DescribeEndpoint(ctx context.Context, name string) (*Endpoint, error) {
	b.mu.RLock("DescribeEndpoint")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	return cloneEndpoint(ep), nil
}

// ListEndpoints returns endpoints sorted by name with optional pagination.
func (b *InMemoryBackend) ListEndpoints(ctx context.Context, nextToken string) ([]*Endpoint, string) {
	b.mu.RLock("ListEndpoints")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListPaged(b.endpointsStoreRO(region), nextToken, cloneEndpoint,
		func(a, b *Endpoint) bool { return a.EndpointName < b.EndpointName })
}

// DeleteEndpoint deletes an endpoint by name.
func (b *InMemoryBackend) DeleteEndpoint(ctx context.Context, name string) error {
	b.mu.Lock("DeleteEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	arnIdx := b.endpointARNIndexStore(region)
	delete(arnIdx, ep.EndpointArn)
	endpoints := b.endpointsStore(region)
	endpoints.Delete(name)

	return nil
}

// UpdateEndpoint updates the endpoint config for an existing endpoint.
func (b *InMemoryBackend) UpdateEndpoint(ctx context.Context, name, endpointConfigName string) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpoint")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	ec, ok := b.endpointConfigsStore(region).Get(endpointConfigName)
	if !ok {
		return nil, fmt.Errorf(
			"%w: could not find endpoint configuration %q",
			ErrEndpointConfigNotFound,
			endpointConfigName,
		)
	}

	newVariants := newVariantSummaries(ec.ProductionVariants)
	// Carry over Current* values from any same-named variant that was already
	// deployed, since traffic keeps flowing on the old counts until the
	// update finishes rolling out.
	for i := range newVariants {
		for _, old := range ep.ProductionVariants {
			if old.VariantName == newVariants[i].VariantName {
				newVariants[i].CurrentWeight = old.CurrentWeight
				newVariants[i].CurrentInstanceCount = old.CurrentInstanceCount

				break
			}
		}
	}

	ep.EndpointConfigName = endpointConfigName
	ep.EndpointStatus = "Updating"
	ep.LastModifiedTime = time.Now()
	ep.ProductionVariants = newVariants

	return cloneEndpoint(ep), nil
}

// ---------------------------------------------------------------------------
// Endpoint lifecycle FSM (#9)
// ---------------------------------------------------------------------------

// scheduleEndpointTransition drives an endpoint to nextStatus after delay.
// ctx must be b.lifecycleCtx captured by the caller while holding b.mu.
// region must be captured by the caller before the lock is released.
func (b *InMemoryBackend) scheduleEndpointTransition(
	ctx context.Context,
	region, name, nextStatus string,
	delay time.Duration,
) {
	b.runDelayed(ctx, delay, func() {
		b.mu.Lock("scheduleEndpointTransition.goroutine")
		defer b.mu.Unlock()

		if ep, ok := b.endpointsStore(region).Get(name); ok {
			ep.EndpointStatus = nextStatus
			ep.LastModifiedTime = time.Now()

			if nextStatus == statusInService {
				for i := range ep.ProductionVariants {
					ep.ProductionVariants[i].CurrentWeight = ep.ProductionVariants[i].DesiredWeight
					ep.ProductionVariants[i].CurrentInstanceCount = ep.ProductionVariants[i].DesiredInstanceCount
					ep.ProductionVariants[i].VariantStatus = []ProductionVariantStatus{{Status: statusInService}}
				}
			}
		}
	})
}

// CreateEndpointFSM creates an endpoint and schedules Creating → InService.
func (b *InMemoryBackend) CreateEndpointFSM(
	ctx context.Context,
	name, endpointConfigName string,
	tags map[string]string,
) (*Endpoint, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CreateEndpointFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	ep, err := b.CreateEndpoint(ctx, name, endpointConfigName, tags)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(lifecycleCtx, region, name, statusInService, endpointCreatingToInService)

	return ep, nil
}

// UpdateEndpointFSM updates config and drives InService → Updating → InService.
func (b *InMemoryBackend) UpdateEndpointFSM(ctx context.Context, name, endpointConfigName string) (*Endpoint, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("UpdateEndpointFSM.ctx")
	lifecycleCtx := b.lifecycleCtx
	b.mu.RUnlock()

	ep, err := b.UpdateEndpoint(ctx, name, endpointConfigName)
	if err != nil {
		return nil, err
	}
	b.scheduleEndpointTransition(lifecycleCtx, region, name, statusInService, endpointUpdatingToInService)

	return ep, nil
}

// UpdateEndpointWeightsAndCapacitiesFull applies weight/capacity changes and drives Updating → InService.
func (b *InMemoryBackend) UpdateEndpointWeightsAndCapacitiesFull(
	ctx context.Context,
	name string,
	changes []DesiredWeightAndCapacity,
) (*Endpoint, error) {
	b.mu.Lock("UpdateEndpointWeightsAndCapacitiesFull")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: endpoint %q not found", ErrEndpointNotFound, name)
	}

	// Apply weight/capacity changes to the endpoint's variant snapshots.
	for _, change := range changes {
		found := false
		for i := range ep.ProductionVariants {
			if ep.ProductionVariants[i].VariantName == change.VariantName {
				if change.DesiredWeight != nil {
					w := *change.DesiredWeight
					ep.ProductionVariants[i].DesiredWeight = &w
				}
				if change.DesiredInstanceCount != nil {
					c := *change.DesiredInstanceCount
					ep.ProductionVariants[i].DesiredInstanceCount = &c
				}
				found = true

				break
			}
		}
		if !found {
			return nil, fmt.Errorf(
				"%w: variant %q not found in endpoint %q",
				ErrValidation,
				change.VariantName,
				name,
			)
		}
	}

	ep.EndpointStatus = "Updating"
	ep.LastModifiedTime = time.Now()

	cp := cloneEndpoint(ep)
	b.scheduleEndpointTransition(b.lifecycleCtx, region, name, statusInService, endpointUpdatingToInService)

	return cp, nil
}

// DesiredWeightAndCapacity is one entry in UpdateEndpointWeightsAndCapacities.
type DesiredWeightAndCapacity struct {
	DesiredWeight        *float64 `json:"DesiredWeight,omitempty"`
	DesiredInstanceCount *int32   `json:"DesiredInstanceCount,omitempty"`
	VariantName          string   `json:"VariantName"`
}
