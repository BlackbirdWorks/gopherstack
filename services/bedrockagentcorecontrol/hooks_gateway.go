package bedrockagentcorecontrol

import (
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
)

const (
	authCustomJWT = "CUSTOM_JWT"
	maxRulePrio   = 1000000
)

func (b *InMemoryBackend) validateGatewayInput(sc scope, in map[string]any) error {
	if t, _ := in["authorizerType"].(string); t == authCustomJWT {
		ac, _ := in["authorizerConfiguration"].(map[string]any)
		if ac == nil {
			return invalid("authorizerConfiguration is required when authorizerType is %s", authCustomJWT)
		}
	}

	if ac, ok := in["authorizerConfiguration"].(map[string]any); ok {
		if err := validateJWTAuthorizer(ac); err != nil {
			return err
		}
	}

	if pe, ok := in["policyEngineConfiguration"].(map[string]any); ok {
		if r, found := b.byARN(sc, strOf(pe, keyARN)); !found || r.Kind != kindPolicyEngine {
			return notFound("policy engine %q not found", strOf(pe, keyARN))
		}
	}

	return nil
}

func (b *InMemoryBackend) decorateGateway(r *Resource) {
	r.Doc["gatewayUrl"] = fmt.Sprintf("https://%s.gateway.bedrock-agentcore.%s.amazonaws.com/mcp", r.ID, r.Region)

	if _, ok := r.Doc["protocolType"]; !ok {
		r.Doc["protocolType"] = "MCP"
	}
}

func targetTypes() []string { return []string{"http", "inference", "mcp"} }

func mcpTargetTypes() []string {
	return []string{"apiGateway", "connector", "lambda", "mcpServer", "openApiSchema", "smithyModel"}
}

func exactlyOne(m map[string]any, keys []string) (string, bool) {
	found := ""

	for _, k := range keys {
		if m[k] != nil {
			if found != "" {
				return "", false
			}

			found = k
		}
	}

	return found, found != ""
}

func (b *InMemoryBackend) validateTargetInput(sc scope, in map[string]any) error {
	tc, _ := in["targetConfiguration"].(map[string]any)
	if tc == nil {
		return nil
	}

	kind, ok := exactlyOne(tc, targetTypes())
	if !ok {
		return invalid("targetConfiguration must set exactly one of %s", strings.Join(targetTypes(), ", "))
	}

	if kind == "mcp" {
		if err := b.validateMCPTarget(objOf(tc, "mcp")); err != nil {
			return err
		}
	}

	return b.validateCredentialConfigs(sc, listOf(in, "credentialProviderConfigurations"))
}

func (b *InMemoryBackend) validateMCPTarget(mcp map[string]any) error {
	kind, ok := exactlyOne(mcp, mcpTargetTypes())
	if !ok {
		return invalid("targetConfiguration.mcp must set exactly one of %s", strings.Join(mcpTargetTypes(), ", "))
	}

	switch kind {
	case "lambda":
		l := objOf(mcp, "lambda")
		arn := strOf(l, "lambdaArn")

		if !lambdaPattern.MatchString(arn) {
			return invalid("lambdaArn %q is not a valid Lambda function ARN", arn)
		}

		if l["toolSchema"] == nil {
			return invalid("lambda targets require a toolSchema")
		}

		if b.refs != nil && !b.refs.LambdaFunctionExists(arn) {
			return invalid("Lambda function %q does not exist", arn)
		}
	case "mcpServer":
		if u := strOf(objOf(mcp, "mcpServer"), "endpoint"); !strings.HasPrefix(u, "https://") {
			return invalid("mcpServer endpoint must be an https URL")
		}
	}

	return nil
}

func (b *InMemoryBackend) validateCredentialConfigs(sc scope, cfgs []any) error {
	for _, c := range cfgs {
		cm, _ := c.(map[string]any)

		typ := strOf(cm, "credentialProviderType")
		if !slices.Contains([]string{"GATEWAY_IAM_ROLE", "OAUTH", "API_KEY"}, typ) {
			return invalid("credentialProviderType %q must be GATEWAY_IAM_ROLE, OAUTH or API_KEY", typ)
		}

		prov := objOf(cm, "credentialProvider")

		for _, key := range []string{"oauthCredentialProvider", "apiKeyCredentialProvider"} {
			if p := objOf(prov, key); p != nil {
				if _, ok := b.byARN(sc, strOf(p, "providerArn")); !ok {
					return notFound("credential provider %q not found", strOf(p, "providerArn"))
				}
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) decorateTarget(r *Resource) {
	proto := "MCP"

	if tc := objOf(r.Doc, "targetConfiguration"); tc != nil {
		if kind, ok := exactlyOne(tc, targetTypes()); ok {
			proto = strings.ToUpper(kind)
		}
	}

	r.Doc["protocolType"] = proto
	r.Doc["statusReasons"] = []any{}
}

func (b *InMemoryBackend) afterTargetOrRuleUpdate(k *kindSpec, r *Resource) {
	if k.Name == kindGatewayTarget {
		b.decorateTarget(r)
	}
}

func validateRuleInput(in args) error {
	if p, ok := in.integer("priority"); ok && (p < 1 || p > maxRulePrio) {
		return invalid("priority must be between 1 and %d", maxRulePrio)
	}

	if len(in.list("actions")) == 0 {
		return invalid("actions must not be empty")
	}

	return nil
}

func (b *InMemoryBackend) rulePriorityTaken(sc scope, r *Resource, prio int) (string, bool) {
	for _, o := range b.scoped(sc, b.kinds[kindGatewayRule], r.Parent) {
		if p, ok := o.Doc["priority"].(float64); ok && int(p) == prio && o.ID != r.ID {
			return o.ID, true
		}
	}

	return "", false
}

func (b *InMemoryBackend) decorateRule(sc scope, r *Resource) error {
	if p, ok := r.Doc["priority"].(float64); ok {
		if other, taken := b.rulePriorityTaken(sc, r, int(p)); taken {
			return conflict("priority %d is already used by rule %q", int(p), other)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateRulePriority(sc scope, r *Resource, patch map[string]any) error {
	if p, ok := patch["priority"].(float64); ok {
		if int(p) < 1 || int(p) > maxRulePrio {
			return invalid("priority must be between 1 and %d", maxRulePrio)
		}

		if other, taken := b.rulePriorityTaken(sc, r, int(p)); taken {
			return conflict("priority %d is already used by rule %q", int(p), other)
		}
	}

	return nil
}

func (b *InMemoryBackend) decorateRateLimit(sc scope, r *Resource) error {
	if len(listOf(r.Doc, "dimensionKeys")) == 0 || len(listOf(r.Doc, "entries")) == 0 {
		return invalid("dimensionKeys and entries must not be empty")
	}

	_ = sc

	return nil
}

// batchPutRateLimits replaces every rate limit of a gateway with the supplied set, all or nothing.
func (b *InMemoryBackend) batchPutRateLimits(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("batchPutRateLimits")
	defer b.mu.Unlock()

	b.purgeLocked()

	k := b.kinds[kindGatewayRateLimit]

	gw, err := b.parentOf(sc, k, in)
	if err != nil {
		return nil, err
	}

	entries := in.list("rateLimits")
	if len(entries) == 0 {
		return nil, invalid("rateLimits must not be empty")
	}

	seen := map[string]bool{}
	staged := make([]*Resource, 0, len(entries))
	now := b.now()

	for _, e := range entries {
		em, _ := e.(map[string]any)
		if len(listOf(em, "dimensionKeys")) == 0 || len(listOf(em, "entries")) == 0 {
			return nil, invalid("every rate limit needs dimensionKeys and entries")
		}

		id := strOf(em, keyRateLimitID)
		if id == "" {
			id = strings.ToUpper(awsid.NewN("", idSuffixLen))
		}

		if seen[id] {
			return nil, invalid("rateLimitId %q appears more than once", id)
		}

		seen[id] = true

		doc := copyDoc(em, keyRateLimitID)
		staged = append(staged, &Resource{
			Kind: k.Name, Account: sc.account, Region: sc.region, Parent: gw.ID, ParentARN: gw.ARN, ID: id,
			Doc: doc, Created: now, Updated: now, SettleAt: now, Status: stateActive,
		})
	}

	for _, old := range b.scoped(sc, k, gw.ID) {
		b.resources.Delete(resourceKey(old))
	}

	out := make([]any, 0, len(staged))

	for _, r := range staged {
		b.resources.Put(r)
		out = append(out, b.project(k, r, elemKeys(op, "rateLimits"), timesOf(op, "rateLimits"), stateActive, nil))
	}

	return &reply{body: map[string]any{"rateLimits": out}, status: http.StatusOK}, nil
}

func timesOf(op, key string) map[string]string {
	for _, f := range specs()[op].Out {
		if f.Key == key {
			return f.Times
		}
	}

	return nil
}

// synchronizeTargets refreshes the named gateway targets.
func (b *InMemoryBackend) synchronizeTargets(sc scope, op string, in args) (*reply, error) {
	b.mu.Lock("synchronizeTargets")
	defer b.mu.Unlock()

	b.purgeLocked()

	k := b.kinds[kindGatewayTarget]

	gw, err := b.parentOf(sc, k, in)
	if err != nil {
		return nil, err
	}

	ids := in.list("targetIdList")
	if len(ids) == 0 {
		return nil, invalid("targetIdList must not be empty")
	}

	var targets []*Resource

	for _, v := range ids {
		id, _ := v.(string)

		t, found := b.find(sc, k, gw.ID, id)
		if !found {
			return nil, notFound("gateway target %q not found", id)
		}

		if isTransitional(b.visible(t)) {
			return nil, conflict("gateway target %q is %s", id, b.visible(t))
		}

		targets = append(targets, t)
	}

	out := make([]any, 0, len(targets))

	for _, t := range targets {
		t.Doc["lastSynchronizedAt"] = b.now().UTC().Format(time.RFC3339Nano)
		b.settle(t, "SYNCHRONIZING")
		out = append(out, b.project(k, t, elemKeys(op, "targets"), timesOf(op, "targets"), "SYNCHRONIZING", nil))
	}

	return &reply{body: map[string]any{"targets": out}, status: http.StatusAccepted}, nil
}

func elemKeys(op, key string) []string {
	for _, f := range specs()[op].Out {
		if f.Key == key {
			return f.Elem
		}
	}

	return nil
}
