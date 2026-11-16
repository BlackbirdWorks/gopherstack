package sagemaker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const icVariantKey = "inference-component"

var errICNoContainer = errors.New("inference component specification defines no container")

type icPlan struct {
	base  string
	model Model
}

// startInferenceComponentLocked begins provisioning the component's container. Callers hold b.mu.
func (b *InMemoryBackend) startInferenceComponentLocked(eng *engineState, region, name string) {
	ctx, cancel := b.jobContextLocked(region)
	key := engineKey("ic", region, name)

	eng.track(key, cancel)

	b.goEngine(func() {
		defer cancel()
		defer eng.untrack(key)

		b.runInferenceComponentProvision(ctx, eng, region, name)
	})
}

func (b *InMemoryBackend) planInferenceComponent(region, name string) (icPlan, error) {
	b.mu.RLock("planInferenceComponent")
	defer b.mu.RUnlock()

	c, ok := b.inferenceComponentsStoreRO(region).Get(name)
	if !ok {
		return icPlan{}, ErrInferenceComponentNotFound
	}

	spec := c.Specification
	if spec == nil && len(c.Specifications) > 0 {
		spec = &c.Specifications[0]
	}

	switch {
	case spec == nil:
		return icPlan{}, errICNoContainer
	case spec.BaseInferenceComponentName != "":
		return icPlan{base: spec.BaseInferenceComponentName}, nil
	case spec.Container != nil:
		return icPlan{model: Model{PrimaryContainer: &ContainerDefinition{
			Image: spec.Container.Image, ModelDataURL: spec.Container.ArtifactURL,
			Environment: spec.Container.Environment,
		}}}, nil
	case spec.ModelName != "":
		m, found := b.modelsStoreRO(region).Get(spec.ModelName)
		if !found {
			return icPlan{}, fmt.Errorf("%w: %s", errEndpointModelMissing, spec.ModelName)
		}

		return icPlan{model: *cloneModel(m)}, nil
	default:
		return icPlan{}, errICNoContainer
	}
}

func (b *InMemoryBackend) runInferenceComponentProvision(ctx context.Context, eng *engineState, region, name string) {
	plan, err := b.planInferenceComponent(region, name)
	if err != nil {
		b.settleInferenceComponent(region, name, err.Error())

		return
	}

	if plan.base != "" {
		b.settleInferenceComponent(region, name, "")

		return
	}

	model, err := b.resolveModelPackages(region, plan.model)
	if err != nil {
		b.settleInferenceComponent(region, name, err.Error())

		return
	}

	md, err := eng.startModel(ctx, modelStart{
		model: model, region: region, label: "ic-" + name, variant: icVariantKey,
		logGroup: "/aws/sagemaker/InferenceComponents/" + name, logStreamBase: name,
	})
	if err != nil {
		if ctx.Err() == nil {
			b.settleInferenceComponent(region, name, failReason(err))
		}

		return
	}

	ed := &endpointDeployment{variants: map[string]*modelDeployment{icVariantKey: md}}
	old := eng.swapDeployment(engineKey("ic", region, name), ed)

	if ctx.Err() != nil || !b.settleInferenceComponent(region, name, "") {
		eng.mu.Lock("runInferenceComponentProvision.rollback")

		if eng.deployments[engineKey("ic", region, name)] == ed {
			delete(eng.deployments, engineKey("ic", region, name))
		}

		eng.mu.Unlock()
		ed.stop()
	}

	old.stop()
}

// settleInferenceComponent ends a Creating or Updating component: InService, or Failed on create
// and InService with FailureReason on update (the previous deployment keeps serving).
func (b *InMemoryBackend) settleInferenceComponent(region, name, failure string) bool {
	b.mu.Lock("settleInferenceComponent")
	defer b.mu.Unlock()

	c, ok := b.inferenceComponentsStore(region).Get(name)
	if !ok || (c.InferenceComponentStatus != statusCreating && c.InferenceComponentStatus != statusUpdating) {
		return false
	}

	c.LastModifiedTime = time.Now()
	c.FailureReason = failure

	switch {
	case failure != "" && c.InferenceComponentStatus == statusCreating:
		c.InferenceComponentStatus = endpointStatusFailed
	default:
		c.InferenceComponentStatus = statusInService
		c.CurrentCopyCount = c.CopyCount
	}

	return failure == ""
}

// inferenceComponentDeployment returns the model deployment serving an InService component; an
// adapter component is served by its base component's containers.
func (b *InMemoryBackend) inferenceComponentDeployment(
	eng *engineState, region, endpoint, name string,
) (*modelDeployment, string, error) {
	b.mu.RLock("inferenceComponentDeployment")
	c, ok := b.inferenceComponentsStoreRO(region).Get(name)

	var (
		variant, serving string
		status           string
	)

	if ok && c.EndpointName == endpoint {
		variant, status, serving = c.VariantName, c.InferenceComponentStatus, name

		spec := c.Specification
		if spec == nil && len(c.Specifications) > 0 {
			spec = &c.Specifications[0]
		}

		if spec != nil && spec.BaseInferenceComponentName != "" {
			serving = spec.BaseInferenceComponentName
		}
	}

	b.mu.RUnlock()

	if serving == "" {
		return nil, "", fmt.Errorf("%w: inference component %s", ErrEndpointNotFound, name)
	}

	if status != statusInService {
		return nil, "", fmt.Errorf("%w: inference component %s is %s", ErrEndpointNotRunning, name, status)
	}

	ed := eng.deployment(engineKey("ic", region, serving))
	if ed == nil || ed.variants[icVariantKey] == nil {
		return nil, "", fmt.Errorf("%w: inference component %s", ErrEndpointNotRunning, name)
	}

	return ed.variants[icVariantKey], variant, nil
}
