package sagemakerruntime

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const (
	headerInferenceComponent = "X-Amzn-Sagemaker-Inference-Component"
	headerTargetContainer    = "X-Amzn-Sagemaker-Target-Container-Hostname"
)

type componentLookup interface {
	DescribeInferenceComponent(ctx context.Context, name string) (*sagemaker.InferenceComponent, error)
}

type containerLookup interface {
	DescribeEndpointConfig(ctx context.Context, name string) (*sagemaker.EndpointConfig, error)
	DescribeModel(ctx context.Context, name string) (*sagemaker.Model, error)
}

// invokeTargets is the outcome of validateInvokeTargets.
type invokeTargets struct {
	variant string
	errMsg  string
}

// validateInvokeTargets checks InferenceComponentName and TargetContainerHostname against the
// wired SageMaker registry; variant is the component's variant name when a component is used.
func (b *InMemoryBackend) validateInvokeTargets(
	ctx context.Context, endpointName, component, container, targetVariant string,
) invokeTargets {
	b.mu.RLock("validateInvokeTargets")
	lookup := b.endpointLookup
	b.mu.RUnlock()

	var out invokeTargets

	if component != "" {
		msg, variant, skip := resolveComponent(ctx, lookup, endpointName, component)
		if msg != "" {
			return invokeTargets{errMsg: msg}
		}

		if skip {
			return out
		}

		out.variant = variant
	}

	if targetVariant != "" && component == "" && !variantExists(ctx, lookup, endpointName, targetVariant) {
		return invokeTargets{errMsg: fmt.Sprintf("Variant %s not found on endpoint %s.", targetVariant, endpointName)}
	}

	if container != "" && !containerExists(ctx, lookup, endpointName, container, targetVariant) {
		return invokeTargets{errMsg: fmt.Sprintf("Container %s not found on endpoint %s.", container, endpointName)}
	}

	return out
}

// containerExists is true when the lookup cannot resolve the endpoint's models.
func containerExists(ctx context.Context, lookup EndpointLookup, endpointName, hostname, targetVariant string) bool {
	cl, ok := lookup.(containerLookup)
	if !ok {
		return true
	}

	ep, err := lookup.DescribeEndpoint(ctx, endpointName)
	if err != nil || ep == nil {
		return true
	}

	cfg, err := cl.DescribeEndpointConfig(ctx, ep.EndpointConfigName)
	if err != nil || cfg == nil {
		return true
	}

	resolved := false

	for _, pv := range cfg.ProductionVariants {
		if targetVariant != "" && pv.VariantName != targetVariant {
			continue
		}

		m, merr := cl.DescribeModel(ctx, pv.ModelName)
		if merr != nil || m == nil {
			continue
		}

		resolved = true

		if modelHasContainer(m, hostname) {
			return true
		}
	}

	return !resolved
}

func modelHasContainer(m *sagemaker.Model, hostname string) bool {
	if m.PrimaryContainer != nil && m.PrimaryContainer.ContainerHostname == hostname {
		return true
	}

	for i := range m.Containers {
		if m.Containers[i].ContainerHostname == hostname {
			return true
		}
	}

	return false
}

// variantExists is true when the endpoint's variants cannot be resolved.
func variantExists(ctx context.Context, lookup EndpointLookup, endpointName, variant string) bool {
	cl, ok := lookup.(containerLookup)
	if !ok {
		return true
	}

	ep, err := lookup.DescribeEndpoint(ctx, endpointName)
	if err != nil || ep == nil {
		return true
	}

	cfg, err := cl.DescribeEndpointConfig(ctx, ep.EndpointConfigName)
	if err != nil || cfg == nil || len(cfg.ProductionVariants) == 0 {
		return true
	}

	for _, pv := range cfg.ProductionVariants {
		if pv.VariantName == variant {
			return true
		}
	}

	return false
}

// resolveComponent looks the inference component up; skip is true when no registry is available.
func resolveComponent(
	ctx context.Context,
	lookup EndpointLookup,
	endpointName, component string,
) (string, string, bool) {
	cl, isCL := lookup.(componentLookup)
	if !isCL {
		return "", "", false
	}

	ic, err := cl.DescribeInferenceComponent(ctx, component)
	if errors.Is(err, errEndpointLookupUnavailable) {
		return "", "", true
	}

	if err != nil || ic == nil || ic.EndpointName != endpointName || ic.InferenceComponentStatus != "InService" {
		return fmt.Sprintf("Inference Component %s not found on endpoint %s.", component, endpointName), "", false
	}

	return "", ic.VariantName, false
}
