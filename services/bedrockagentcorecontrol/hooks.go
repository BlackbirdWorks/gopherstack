package bedrockagentcorecontrol

import (
	"errors"
	"slices"
	"sync"
)

func errIs(err, target error) bool { return errors.Is(err, target) }

// mapOf returns v as an object, or nil when it is not one.
func mapOf(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

func objOf(m map[string]any, key string) map[string]any {
	v, _ := m[key].(map[string]any)

	return v
}

func strOf(m map[string]any, key string) string {
	v, _ := m[key].(string)

	return v
}

func listOf(m map[string]any, key string) []any {
	v, _ := m[key].([]any)

	return v
}

// byARN finds any resource of the scope by ARN.
func (b *InMemoryBackend) byARN(sc scope, resourceARN string) (*Resource, bool) {
	if resourceARN == "" {
		return nil, false
	}

	for _, r := range b.resources.All() {
		if r.ARN == resourceARN && r.Account == sc.account && r.Region == sc.region && b.live(r) {
			return r, true
		}
	}

	return nil, false
}

type createValidator func(b *InMemoryBackend, sc scope, k *kindSpec, in args) error

type createDecorator func(b *InMemoryBackend, sc scope, r *Resource, in args) error

//nolint:gochecknoglobals // immutable dispatch table
var createValidators = sync.OnceValue(func() map[string]createValidator {
	provider := func(b *InMemoryBackend, _ scope, k *kindSpec, in args) error { return b.validateProviderInput(k, in) }
	sandbox := func(_ *InMemoryBackend, _ scope, k *kindSpec, in args) error { return validateSandboxInput(k, in) }

	return map[string]createValidator{
		kindRuntime: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error { return b.validateRuntimeInput(sc, in) },
		kindGateway: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error { return b.validateGatewayInput(sc, in) },
		kindGatewayTarget: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error {
			return b.validateTargetInput(sc, in)
		},
		kindMemory:          func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error { return validateMemoryInput(in) },
		kindBrowser:         sandbox,
		kindCodeInterpreter: sandbox,
		kindDataset: func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error {
			return validateDatasetInput(in)
		},
		kindOnlineEval: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error {
			return b.validateOnlineEvalInput(sc, in)
		},
		kindPolicy:      func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error { return validatePolicyInput(in) },
		kindGatewayRule: func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error { return validateRuleInput(in) },
		kindRegistryRecord: func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error {
			return validateRecordInput(in)
		},
		kindAPIKeyProvider:  provider,
		kindOAuthProvider:   provider,
		kindPaymentProvider: provider,
		kindConsentPortal: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error {
			return b.validateConsentCreate(sc, in)
		},
		kindPaymentConnector: func(b *InMemoryBackend, sc scope, _ *kindSpec, in args) error {
			return b.validateConnectorInput(sc, in)
		},
		kindBundle: func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error {
			return validateBundleInput(in)
		},
		kindCapacityProvider: func(_ *InMemoryBackend, _ scope, _ *kindSpec, in args) error {
			return validateCapacityInput(in)
		},
	}
})

func (b *InMemoryBackend) validateCreate(sc scope, k *kindSpec, in args) error {
	if v, ok := createValidators()[k.Name]; ok {
		return v(b, sc, k, in)
	}

	return nil
}

//nolint:gochecknoglobals // immutable dispatch table
var createDecorators = sync.OnceValue(func() map[string]createDecorator {
	pure := func(f func(r *Resource)) createDecorator {
		return func(_ *InMemoryBackend, _ scope, r *Resource, _ args) error {
			f(r)

			return nil
		}
	}
	sandbox := pure(func(r *Resource) { r.Doc["type"] = "CUSTOM" })

	return map[string]createDecorator{
		kindRuntime: func(b *InMemoryBackend, _ scope, r *Resource, _ args) error {
			decorateRuntime(b, r)

			return nil
		},
		kindRuntimeEndpoint: func(b *InMemoryBackend, sc scope, r *Resource, in args) error {
			return b.decorateEndpoint(sc, r, in)
		},
		kindGateway: func(b *InMemoryBackend, _ scope, r *Resource, _ args) error {
			b.decorateGateway(r)

			return nil
		},
		kindGatewayTarget: func(b *InMemoryBackend, _ scope, r *Resource, _ args) error {
			b.decorateTarget(r)

			return nil
		},
		kindGatewayRule: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error { return b.decorateRule(sc, r) },
		kindGatewayRateLimit: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			return b.decorateRateLimit(sc, r)
		},
		kindMemory: func(b *InMemoryBackend, _ scope, r *Resource, in args) error {
			b.decorateMemory(r, in)

			return nil
		},
		kindBrowser:         sandbox,
		kindCodeInterpreter: sandbox,
		kindDataset: func(b *InMemoryBackend, _ scope, r *Resource, in args) error {
			b.decorateDataset(r, in)

			return nil
		},
		kindHarness: func(b *InMemoryBackend, _ scope, r *Resource, _ args) error {
			b.decorateHarness(r)

			return nil
		},
		kindHarnessEndpoint: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			return b.decorateHarnessEndpoint(sc, r)
		},
		kindBundle: func(b *InMemoryBackend, _ scope, r *Resource, in args) error {
			b.decorateBundle(r, in)

			return nil
		},
		kindOnlineEval: func(_ *InMemoryBackend, _ scope, r *Resource, in args) error {
			decorateOnlineEval(r, in)

			return nil
		},
		kindConsentPortal: func(b *InMemoryBackend, _ scope, r *Resource, _ args) error {
			b.decorateConsent(r)

			return nil
		},
		kindPolicyGeneration: pure(decoratePolicyGeneration),
		kindRegistryRecord: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			return b.decorateRecord(sc, r)
		},
		kindPaymentConnector: pure(decorateConnector),
		kindOAuthProvider: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			decorateOAuth(b, sc, r)

			return nil
		},
		kindAPIKeyProvider: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			decorateAPIKey(b, sc, r)

			return nil
		},
		kindPaymentProvider: func(b *InMemoryBackend, sc scope, r *Resource, _ args) error {
			decoratePaymentProvider(b, sc, r)

			return nil
		},
		kindWorkloadIdentity: pure(decorateWorkloadIdentity),
	}
})

func (b *InMemoryBackend) decorateCreate(sc scope, k *kindSpec, r *Resource, in args) error {
	if d, ok := createDecorators()[k.Name]; ok {
		return d(b, sc, r, in)
	}

	return nil
}

func (b *InMemoryBackend) afterCreate(sc scope, k *kindSpec, r *Resource) {
	switch k.Name {
	case kindRuntime:
		b.createDefaultEndpoint(sc, r)
		b.createManagedIdentity(sc, r, strOf(r.Doc, "agentRuntimeName"))
	case kindGateway:
		b.createManagedIdentity(sc, r, strOf(r.Doc, keyName))
	case kindPaymentManager:
		b.createManagedIdentity(sc, r, strOf(r.Doc, keyName))
	}
}

func (b *InMemoryBackend) validateUpdate(sc scope, k *kindSpec, r *Resource, patch map[string]any) error {
	switch k.Name {
	case kindRuntime:
		return b.validateRuntimeUpdate(sc, patch)
	case kindRuntimeEndpoint:
		return b.validateEndpointUpdate(r, patch)
	case kindGateway:
		return b.validateGatewayInput(sc, patch)
	case kindGatewayTarget:
		return b.validateTargetInput(sc, patch)
	case kindMemory:
		return validateMemoryUpdate(r, patch)
	case kindGatewayRule:
		return b.validateRulePriority(sc, r, patch)
	case kindHarnessEndpoint:
		return b.validateHarnessEndpointUpdate(r, patch)
	case kindBundle:
		return validateBundleUpdate(r, patch)
	case kindPolicy:
		if _, ok := patch["definition"]; ok {
			return validatePolicyInput(patch)
		}
	case kindOnlineEval:
		return b.validateOnlineEvalInput(sc, patch)
	case kindConsentPortal:
		return b.validateConsentCreate(sc, patch)
	case kindAPIKeyProvider, kindOAuthProvider, kindPaymentProvider:
		return b.validateProviderUpdate(r, patch)
	case kindRegistryRecord:
		return validateRecordUpdate(r, patch)
	}

	return nil
}

// applyUpdate merges patch into r and returns members whose response value differs from the stored one.
func (b *InMemoryBackend) applyUpdate(k *kindSpec, r *Resource, patch map[string]any) map[string]any {
	switch k.Name {
	case kindMemory:
		b.applyMemoryUpdate(r, patch)

		return nil
	case kindRuntime:
		return b.applyRuntimeUpdate(r, patch)
	case kindRuntimeEndpoint:
		return b.applyEndpointUpdate(r, patch)
	case kindHarness:
		return b.applyHarnessUpdate(r, patch)
	case kindBundle:
		return b.applyBundleUpdate(r, patch)
	case kindGatewayRateLimit, kindGatewayRule, kindGatewayTarget:
		mergeDoc(r, patch)
		b.afterTargetOrRuleUpdate(k, r)

		return nil
	case kindOAuthProvider, kindAPIKeyProvider, kindPaymentProvider:
		mergeDoc(r, patch)
		reDecorateProvider(b, k, r)

		return nil
	case kindRegistryRecord:
		mergeDoc(r, patch)
		bumpRecordVersion(r, patch)

		return nil
	case kindDataset, kindPolicyEngine, kindWorkloadIdentity:
		mergeDoc(r, patch)

		return nil
	}

	mergeDoc(r, patch)

	return nil
}

func mergeDoc(r *Resource, patch map[string]any) {
	for key, v := range patch {
		if v == nil {
			delete(r.Doc, key)

			continue
		}

		r.Doc[key] = v
	}
}

func (b *InMemoryBackend) canDelete(sc scope, k *kindSpec, r *Resource, in args) error {
	switch k.Name {
	case kindRuntime:
		return b.runtimeDeletable(sc, r, in)
	case kindGateway:
		return b.requireNoChildren(sc, r, kindGatewayTarget, "targets")
	case kindPolicyEngine:
		return b.policyEngineDeletable(sc, r)
	case kindCapacityProvider:
		return b.capacityProviderDeletable(sc, r)
	case kindRegistry:
		return b.requireNoChildren(sc, r, kindRegistryRecord, "records")
	case kindPaymentManager:
		return b.requireNoChildren(sc, r, kindPaymentConnector, "connectors")
	case kindEvaluator:
		return b.evaluatorDeletable(sc, r)
	}

	return nil
}

func (b *InMemoryBackend) requireNoChildren(sc scope, r *Resource, childKind, noun string) error {
	if len(b.scoped(sc, b.kinds[childKind], r.ID)) > 0 {
		return conflict("%s %q still has %s; delete them first", humanKind(b.kinds[r.Kind]), r.ID, noun)
	}

	return nil
}

// cascade removes resources that cannot outlive r.
func (b *InMemoryBackend) cascade(sc scope, k *kindSpec, r *Resource) {
	for _, child := range b.kinds {
		if child.Parent != k.Name {
			continue
		}

		for _, c := range b.scoped(sc, child, r.ID) {
			b.resources.Delete(resourceKey(c))
			b.policies.Delete(c.ARN)
		}
	}

	if id := strOf(r.Aux2(), "managedIdentity"); id != "" {
		b.resources.Delete(makeKey(sc.account, sc.region, kindWorkloadIdentity, "", id))
	}
}

// Aux2 returns the auxiliary map, never nil.
func (r *Resource) Aux2() map[string]any {
	if r.Aux == nil {
		r.Aux = map[string]any{}
	}

	return r.Aux
}

func (b *InMemoryBackend) versionView(k *kindSpec, r *Resource, in args) (*Resource, error) {
	switch k.Name {
	case kindRuntime:
		return b.runtimeVersionView(r, in.str("version"))
	case kindHarness:
		return b.harnessVersionView(r, in.str("harnessVersion"))
	case kindDataset:
		return b.datasetVersionView(r, in.str("datasetVersion"))
	case kindBundle:
		return b.bundleBranchView(r, in.str(keyBranchName))
	}

	return r, nil
}

func (b *InMemoryBackend) deleteVersion(sc scope, k *kindSpec, r *Resource, in args) (bool, error) {
	switch k.Name {
	case kindRuntime:
		return b.deleteRuntimeVersion(sc, r, in.str("version"))
	case kindDataset:
		return b.deleteDatasetVersion(r, in.str("datasetVersion"))
	}

	return false, nil
}

func isTransitional(s string) bool {
	return slices.Contains([]string{stateCreating, stateUpdating, stateDeleting, "GENERATING", "SYNCHRONIZING"}, s)
}
