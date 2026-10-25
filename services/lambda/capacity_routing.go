package lambda

import (
	"fmt"
	"math"
	"strings"
)

// Defaults per docs.aws.amazon.com/lambda/latest/dg/lambda-managed-instances-{scaling,capacity-providers}.html:
// 3 minimum execution environments, MaxVCpuCount 400, 2 GiB per vCPU (smallest size 2 GB / 1 vCPU).
const (
	capacityDefaultMemoryGiBPerVCPU = 2.0
	capacityDefaultMinEnvironments  = 3
	capacityDefaultMaxVCpuCount     = 400
	capacityMiBPerGiB               = 1024
	capacityNodeJSPerVCpu           = 64
	capacityJVMDotnetPerVCpu        = 32
	capacityPythonPerVCpu           = 16
)

// capacityConcurrencyPerVCpu is the documented default PerExecutionEnvironmentMaxConcurrency per vCPU,
// from the per-runtime Lambda Managed Instances guides (nodejs 64, java 32, dotnet 32, python 16).
// Other runtimes document no default, so they are uncapped per environment.
func capacityConcurrencyPerVCpu(runtime string) int {
	switch {
	case strings.HasPrefix(runtime, "nodejs"):
		return capacityNodeJSPerVCpu
	case strings.HasPrefix(runtime, "java"), strings.HasPrefix(runtime, "dotnet"):
		return capacityJVMDotnetPerVCpu
	case strings.HasPrefix(runtime, "python"):
		return capacityPythonPerVCpu
	default:
		return math.MaxInt32
	}
}

// capacityPool is the set of execution environments serving one function on its
// capacity provider; envs[i] is the in-flight invocation count of environment i.
type capacityPool struct {
	provider string
	envs     []int
	vcpu     int
}

func (p *capacityPool) inFlight() int {
	n := 0
	for _, c := range p.envs {
		n += c
	}

	return n
}

func capacityEnvVCpu(fn *FunctionConfiguration, cfg *ManagedInstancesConfig) int {
	ratio := capacityDefaultMemoryGiBPerVCPU
	if cfg.ExecutionEnvironmentMemoryGiBPerVCpu != nil && *cfg.ExecutionEnvironmentMemoryGiBPerVCpu > 0 {
		ratio = *cfg.ExecutionEnvironmentMemoryGiBPerVCpu
	}

	v := int(math.Ceil(float64(fn.MemorySize) / capacityMiBPerGiB / ratio))

	return max(v, 1)
}

func capacityPerEnvConcurrency(cfg *ManagedInstancesConfig, runtime string, vcpu int) int {
	if cfg.PerExecutionEnvironmentMaxConcurrency != nil && *cfg.PerExecutionEnvironmentMaxConcurrency > 0 {
		return int(*cfg.PerExecutionEnvironmentMaxConcurrency)
	}

	return min(vcpu*capacityConcurrencyPerVCpu(runtime), math.MaxInt32)
}

// providerVCpuUsedLocked sums the vCPUs of every environment on the provider.
func (b *InMemoryBackend) providerVCpuUsedLocked(provider string) int {
	used := 0

	for _, p := range b.capacityPools {
		if p.provider == provider {
			used += len(p.envs) * p.vcpu
		}
	}

	return used
}

// acquireCapacityEnvLocked places one invocation of fn on an execution
// environment of its capacity provider. Caller holds b.mu for write.
func (b *InMemoryBackend) acquireCapacityEnvLocked(fn *FunctionConfiguration, qualifier string) error {
	if fn.CapacityProviderConfig == nil || fn.CapacityProviderConfig.ManagedInstances == nil {
		return nil
	}

	cfg := fn.CapacityProviderConfig.ManagedInstances

	cp, err := b.activeCapacityProviderLocked(cfg.CapacityProviderArn)
	if err != nil {
		return err
	}

	name, _ := capacityProviderNameFromARN(cfg.CapacityProviderArn)

	vcpu := capacityEnvVCpu(fn, cfg)
	perEnv := capacityPerEnvConcurrency(cfg, fn.Runtime, vcpu)

	pool := b.capacityPools[fn.FunctionName]
	if pool == nil || pool.provider != name {
		pool = &capacityPool{provider: name}
		b.capacityPools[fn.FunctionName] = pool
	}

	pool.vcpu = vcpu

	sc := b.functionScalingConfigs[permissionMapKey(fn.FunctionName, qualifier)]
	if sc == nil {
		sc = b.functionScalingConfigs[permissionMapKey(fn.FunctionName, versionLatestPublished)]
	}

	minEnvs, maxEnvs := capacityEnvBounds(sc)
	budget := capacityDefaultMaxVCpuCount

	if pc := cp.CapacityProviderScalingConfig; pc != nil && pc.MaxVCpuCount != nil {
		budget = int(*pc.MaxVCpuCount)
	}

	grow := func() bool {
		if len(pool.envs) >= maxEnvs || b.providerVCpuUsedLocked(name)+vcpu > budget {
			return false
		}

		pool.envs = append(pool.envs, 0)

		return true
	}

	for len(pool.envs) < minEnvs {
		if !grow() {
			break
		}
	}

	best := pool.leastLoaded(perEnv)
	if best < 0 {
		if !grow() {
			return fmt.Errorf("%w: capacity provider %s has no execution environment capacity for function %s",
				ErrTooManyRequests, name, fn.FunctionName)
		}

		best = len(pool.envs) - 1
	}

	pool.envs[best]++

	return nil
}

// leastLoaded returns the index of the environment with room for another
// invocation and the fewest in flight, or -1.
func (p *capacityPool) leastLoaded(perEnv int) int {
	best := -1

	for i, c := range p.envs {
		if c < perEnv && (best < 0 || c < p.envs[best]) {
			best = i
		}
	}

	return best
}

// activeCapacityProviderLocked resolves the provider ARN to an Active provider.
func (b *InMemoryBackend) activeCapacityProviderLocked(cpArn string) (*CapacityProvider, error) {
	name, ok := capacityProviderNameFromARN(cpArn)
	if !ok {
		return nil, fmt.Errorf("%w: invalid capacity provider ARN", ErrFunctionNotFound)
	}

	cp, found := b.capacityProviders.Get(name)
	if !found {
		return nil, fmt.Errorf("%w: capacity provider %s not found", ErrFunctionNotFound, name)
	}

	settleCapacityProviderLocked(cp)

	if cp.State != CapacityProviderStateActive {
		return nil, fmt.Errorf("%w: capacity provider %s is in state %s", ErrResourceNotReady, name, cp.State)
	}

	return cp, nil
}

// capacityEnvBounds returns the min/max execution environments for a function
// version; max is unbounded and min is 3 unless FunctionScalingConfig overrides
// them (min=max=0 deactivates the function).
func capacityEnvBounds(sc *FunctionScalingConfig) (int, int) {
	minEnvs, maxEnvs := capacityDefaultMinEnvironments, math.MaxInt

	if sc == nil {
		return minEnvs, maxEnvs
	}

	minEnvs = 0
	if sc.MinExecutionEnvironments != nil {
		minEnvs = int(*sc.MinExecutionEnvironments)
	}

	if sc.MaxExecutionEnvironments != nil {
		maxEnvs = int(*sc.MaxExecutionEnvironments)
	}

	return min(minEnvs, maxEnvs), maxEnvs
}

// releaseCapacityEnvLocked frees one in-flight slot from the busiest environment.
func (b *InMemoryBackend) releaseCapacityEnvLocked(functionName string) {
	pool := b.capacityPools[functionName]
	if pool == nil {
		return
	}

	best := -1

	for i, c := range pool.envs {
		if c > 0 && (best < 0 || c > pool.envs[best]) {
			best = i
		}
	}

	if best >= 0 {
		pool.envs[best]--
	}
}

// CapacityEnvironments reports the execution environment count and in-flight
// invocations of a capacity-provider function.
func (b *InMemoryBackend) CapacityEnvironments(functionName string) (int, int) {
	b.mu.RLock("CapacityEnvironments")
	defer b.mu.RUnlock()

	p := b.capacityPools[functionName]
	if p == nil {
		return 0, 0
	}

	return len(p.envs), p.inFlight()
}
