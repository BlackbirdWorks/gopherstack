package sagemaker

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"
)

var errEndpointModelMissing = errors.New("model not found")

// endpointDeployment is the live container set behind one endpoint, one model deployment per variant.
type endpointDeployment struct {
	variants map[string]*modelDeployment
}

func (ed *endpointDeployment) stop() {
	if ed == nil {
		return
	}

	for _, md := range ed.variants {
		md.stop()
	}
}

func (e *engineState) deployment(key string) *endpointDeployment {
	e.mu.RLock("deployment")
	defer e.mu.RUnlock()

	return e.deployments[key]
}

// removeDeployment detaches and returns the deployment registered under key (nil when absent).
func (e *engineState) removeDeployment(key string) *endpointDeployment {
	e.mu.Lock("removeDeployment")
	defer e.mu.Unlock()

	ed := e.deployments[key]
	delete(e.deployments, key)

	return ed
}

// swapDeployment registers ed under key and returns the one it replaces.
func (e *engineState) swapDeployment(key string, ed *endpointDeployment) *endpointDeployment {
	e.mu.Lock("swapDeployment")
	defer e.mu.Unlock()

	old := e.deployments[key]
	e.deployments[key] = ed

	return old
}

// provisionEndpoint starts the endpoint's containers in the background. prev is the endpoint as it
// was before an update, nil for create.
func (b *InMemoryBackend) provisionEndpoint(eng *engineState, region, name string, prev *Endpoint) {
	ctx, cancel := b.jobContext(region)
	key := engineKey("endpoint", region, name)
	eng.track(key, cancel)

	b.goEngine(func() {
		defer cancel()
		defer eng.untrack(key)

		b.runProvision(ctx, eng, region, name, prev)
	})
}

type variantPlan struct {
	name    string
	model   Model
	timeout time.Duration
}

func (b *InMemoryBackend) planEndpoint(region, name string) ([]variantPlan, string, error) {
	b.mu.RLock("planEndpoint")
	defer b.mu.RUnlock()

	ep, ok := b.endpointsStoreRO(region).Get(name)
	if !ok {
		return nil, "", ErrEndpointNotFound
	}

	ec, ok := b.endpointConfigsStoreRO(region).Get(ep.EndpointConfigName)
	if !ok {
		return nil, "", fmt.Errorf("%w: endpoint configuration %s", errEndpointModelMissing, ep.EndpointConfigName)
	}

	plans := make([]variantPlan, 0, len(ec.ProductionVariants))

	for _, pv := range ec.ProductionVariants {
		if pv.ModelName == "" {
			continue
		}

		m, found := b.modelsStoreRO(region).Get(pv.ModelName)
		if !found {
			return nil, ec.EndpointConfigName, fmt.Errorf("%w: %s", errEndpointModelMissing, pv.ModelName)
		}

		plans = append(plans, variantPlan{
			name: pv.VariantName, model: *cloneModel(m),
			timeout: time.Duration(pv.ContainerStartupHealthCheckTimeoutInSeconds) * time.Second,
		})
	}

	return plans, ec.EndpointConfigName, nil
}

func (b *InMemoryBackend) runProvision(ctx context.Context, eng *engineState, region, name string, prev *Endpoint) {
	plans, configName, err := b.planEndpoint(region, name)
	if err != nil {
		b.failEndpoint(region, name, err.Error(), prev)

		return
	}

	ed := &endpointDeployment{variants: make(map[string]*modelDeployment, len(plans))}

	for _, p := range plans {
		md, serr := eng.startModel(ctx, modelStart{
			model: p.model, region: region, label: name + "-" + p.name, variant: p.name,
			logGroup: "/aws/sagemaker/Endpoints/" + name, logStreamBase: p.name, startupTimeout: p.timeout,
		})
		if serr != nil {
			ed.stop()

			if ctx.Err() == nil {
				b.failEndpoint(region, name, failReason(serr), prev)
			}

			return
		}

		ed.variants[p.name] = md
	}

	old := eng.swapDeployment(engineKey("endpoint", region, name), ed)

	if !b.commitEndpoint(region, name, configName) || ctx.Err() != nil {
		eng.mu.Lock("runProvision.rollback")

		if eng.deployments[engineKey("endpoint", region, name)] == ed {
			delete(eng.deployments, engineKey("endpoint", region, name))
		}

		eng.mu.Unlock()
		ed.stop()
	}

	old.stop()
}

// commitEndpoint flips a Creating or Updating endpoint to InService when it still points at configName.
func (b *InMemoryBackend) commitEndpoint(region, name, configName string) bool {
	b.mu.Lock("commitEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok || ep.EndpointConfigName != configName ||
		(ep.EndpointStatus != statusCreating && ep.EndpointStatus != statusUpdating) {
		return false
	}

	ep.EndpointStatus = statusInService
	ep.FailureReason = ""
	ep.LastModifiedTime = time.Now()
	markEndpointInService(ep)

	return true
}

// failEndpoint records a failed deployment: a failed create leaves the endpoint Failed, a failed
// update rolls back to the previous configuration, which keeps serving.
func (b *InMemoryBackend) failEndpoint(region, name, reason string, prev *Endpoint) {
	b.mu.Lock("failEndpoint")
	defer b.mu.Unlock()

	ep, ok := b.endpointsStore(region).Get(name)
	if !ok || (ep.EndpointStatus != statusCreating && ep.EndpointStatus != statusUpdating) {
		return
	}

	ep.FailureReason = reason
	ep.LastModifiedTime = time.Now()

	if prev != nil && ep.EndpointStatus == statusUpdating {
		ep.EndpointConfigName = prev.EndpointConfigName
		ep.ProductionVariants = prev.ProductionVariants
		ep.ShadowProductionVariants = prev.ShadowProductionVariants
		ep.DataCaptureConfig = prev.DataCaptureConfig
		ep.AsyncInferenceConfig = prev.AsyncInferenceConfig
		ep.DeploymentConfig = prev.DeploymentConfig
		ep.EndpointStatus = statusInService

		return
	}

	ep.EndpointStatus = endpointStatusFailed

	for i := range ep.ProductionVariants {
		ep.ProductionVariants[i].VariantStatus = nil
	}
}

const endpointStatusFailed = "Failed"

// choose picks the variant deployment that serves an invocation: the named TargetVariant, else a
// weighted draw over the variants that are running.
func (ed *endpointDeployment) choose(ep *Endpoint, target string) (*modelDeployment, string, error) {
	if target != "" {
		if md, ok := ed.variants[target]; ok {
			return md, target, nil
		}

		return nil, "", fmt.Errorf("%w: variant %s", ErrEndpointNotRunning, target)
	}

	var (
		names   []string
		weights []float64
		total   float64
	)

	for _, pv := range ep.ProductionVariants {
		if _, ok := ed.variants[pv.VariantName]; !ok {
			continue
		}

		w := 1.0

		switch {
		case pv.CurrentWeight != nil:
			w = *pv.CurrentWeight
		case pv.DesiredWeight != nil:
			w = *pv.DesiredWeight
		}

		names = append(names, pv.VariantName)
		weights = append(weights, w)
		total += w
	}

	if len(names) == 0 {
		return nil, "", ErrEndpointNotRunning
	}

	pick := 0

	if total > 0 {
		r := rand.Float64() * total //nolint:gosec // traffic splitting, not security
		for i, w := range weights {
			if r < w {
				pick = i

				break
			}

			r -= w
		}
	}

	return ed.variants[names[pick]], names[pick], nil
}
