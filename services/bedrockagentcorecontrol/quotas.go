package bedrockagentcorecontrol

import "github.com/blackbirdworks/gopherstack/pkgs/restroute"

// quota is one resource-count limit from the AgentCore quotas page (bedrock-agentcore-limits.html);
// perParent counts children of one parent instead of the account and Region.
type quota struct {
	limit     int
	perParent bool
}

const (
	limitRuntime          = 1000
	limitRuntimeEndpoint  = 10
	limitGateway          = 1000
	limitGatewayTarget    = 100
	limitMemory           = 150
	limitCodeInterpreter  = 1000
	limitBrowser          = 1000
	limitBrowserProfile   = 100
	limitCapacityProvider = 1000
	limitBundle           = 1000
	limitEvaluator        = 10000
	limitOnlineEval       = 1000
	limitPolicyEngine     = 1000
	limitPolicy           = 1000
	limitRegistry         = 10
	limitAPIKeyProvider   = 50
	limitOAuthProvider    = 50
	limitPaymentProvider  = 50
	limitWorkloadIdentity = 11000

	limitMemoryStrategies = 6
	limitEnvironmentBytes = 4 << 10
	limitInlineSchema     = 1 << 20
	limitS3Schema         = 10 << 20
	limitCodePackage      = 250 << 20
	limitDatasetObject    = 256 << 20
	limitToolsPerTarget   = 1000
	limitToolNameLen      = 256
)

//nolint:gochecknoglobals // static lookup table
var resourceQuotas = map[string]quota{
	kindRuntime:          {limit: limitRuntime},
	kindRuntimeEndpoint:  {limit: limitRuntimeEndpoint, perParent: true},
	kindGateway:          {limit: limitGateway},
	kindGatewayTarget:    {limit: limitGatewayTarget, perParent: true},
	kindMemory:           {limit: limitMemory},
	kindCodeInterpreter:  {limit: limitCodeInterpreter},
	kindBrowser:          {limit: limitBrowser},
	kindBrowserProfile:   {limit: limitBrowserProfile},
	kindCapacityProvider: {limit: limitCapacityProvider},
	kindBundle:           {limit: limitBundle},
	kindEvaluator:        {limit: limitEvaluator},
	kindOnlineEval:       {limit: limitOnlineEval},
	kindPolicyEngine:     {limit: limitPolicyEngine},
	kindPolicy:           {limit: limitPolicy, perParent: true},
	kindRegistry:         {limit: limitRegistry},
	kindAPIKeyProvider:   {limit: limitAPIKeyProvider},
	kindOAuthProvider:    {limit: limitOAuthProvider},
	kindPaymentProvider:  {limit: limitPaymentProvider},
	kindWorkloadIdentity: {limit: limitWorkloadIdentity},
}

// checkQuotaLocked rejects a create that would exceed the kind's documented count limit.
func (b *InMemoryBackend) checkQuotaLocked(sc scope, k *kindSpec, parentID string) error {
	q, ok := resourceQuotas[k.Name]
	if !ok {
		return nil
	}

	n := 0

	for _, r := range b.resources.All() {
		if r.Kind != k.Name || r.Account != sc.account || r.Region != sc.region || !b.live(r) {
			continue
		}

		if (q.perParent && r.Parent != parentID) || r.ID == defaultEndpoint {
			continue
		}

		n++
	}

	if n >= q.limit {
		return restroute.QuotaExceeded("the quota of %d %s resources has been reached", q.limit, humanKind(k))
	}

	return nil
}

// validateEnvironmentSize enforces the 4 KB total of environment variables per agent.
func validateEnvironmentSize(env map[string]any) error {
	total := 0

	for k, v := range env {
		s, _ := v.(string)
		total += len(k) + len(s)
	}

	if total > limitEnvironmentBytes {
		return invalid("environmentVariables total %d bytes, the maximum is %d", total, limitEnvironmentBytes)
	}

	return nil
}

// checkToolLimits enforces the per-target tool count and tool name length quotas.
func checkToolLimits(names []string) error {
	if len(names) > limitToolsPerTarget {
		return restroute.QuotaExceeded("a target can expose at most %d tools", limitToolsPerTarget)
	}

	for _, n := range names {
		if len(n) > limitToolNameLen {
			return invalid("tool name %q exceeds %d characters", n, limitToolNameLen)
		}
	}

	return nil
}
