package route53recoveryreadiness

import (
	"context"
	"slices"
	"strings"
	"time"
)

type ruleKind int

const (
	kindExists ruleKind = iota
	kindHealthy
)

type ruleDef struct {
	Type        string
	ID          string
	Description string
	kind        ruleKind
}

// ruleCatalog lists the rules this emulator evaluates against sibling services; all are defined by gopherstack.
func ruleCatalog() []ruleDef {
	return []ruleDef{
		{typeELBv2, "ElbV2LoadBalancerExists", "The load balancer exists", kindExists},
		{
			typeELBv2,
			"ElbV2LoadBalancerActive",
			"The load balancer is in the active state",
			kindHealthy,
		},
		{typeHealthCheck, "Route53HealthCheckExists", "The health check exists", kindExists},
		{typeHealthCheck, "Route53HealthCheckHealthy", "The health check reports Healthy", kindHealthy},
		{typeSQS, "SqsQueueExists", "The queue exists", kindExists},
		{typeSNS, "SnsTopicExists", "The topic exists", kindExists},
		{typeLambda, "LambdaFunctionExists", "The function exists", kindExists},
		{typeVPC, "Ec2VpcExists", "The VPC exists", kindExists},
		{typeAlarm, "CloudWatchAlarmExists", "The alarm exists", kindExists},
		{typeAlarm, "CloudWatchAlarmNotInAlarm", "The alarm is not in the ALARM state", kindHealthy},
		{typeTable, "DynamoDbTableExists", "The table exists", kindExists},
		{typeTable, "DynamoDbTableActive", "The table status is ACTIVE", kindHealthy},
		{typeDNSTarget, "DnsTargetRecordExists", "The DNS record exists in the hosted zone", kindExists},
	}
}

type ruleOutcome struct {
	At        time.Time
	ID        string
	Readiness string
	Messages  []string
}

type resourceOutcome struct {
	At        time.Time
	Readiness string
	Note      string
	Resource  Resource
	Rules     []ruleOutcome
}

func (r Resource) identifier() string {
	if r.ResourceArn != "" {
		return r.ResourceArn
	}

	return r.ComponentID
}

// combine reduces rule or resource readiness values to one: NOT_READY > NOT_AUTHORIZED > UNKNOWN > READY.
func combine(values []string) string {
	if len(values) == 0 {
		return readinessUnknown
	}

	for _, want := range []string{readinessNotReady, readinessNotAuthorized, readinessUnknown} {
		if slices.Contains(values, want) {
			return want
		}
	}

	return readinessReady
}

func rulesFor(typ string) []ruleDef {
	var out []ruleDef

	for _, d := range ruleCatalog() {
		if d.Type == typ {
			out = append(out, d)
		}
	}

	return out
}

// authorized reports whether the resource may be read, plus the foreign account ID when it may not.
func (b *InMemoryBackend) authorized(resourceArn string, auths []string) (bool, string) {
	p, ok := parseARN(resourceArn)
	if !ok || p.AccountID == "" || p.AccountID == b.accountID {
		return true, ""
	}

	return slices.ContainsFunc(auths, func(a string) bool { return strings.Contains(a, p.AccountID) }), p.AccountID
}

func outcomeFor(def ruleDef, p probe, now time.Time) ruleOutcome {
	ro := ruleOutcome{At: now, ID: def.ID, Readiness: readinessReady}

	ok := p.exists
	if def.kind == kindHealthy {
		ok = p.exists && p.healthy
	}

	if !ok {
		ro.Readiness = readinessNotReady
		ro.Messages = []string{def.Description + ": not satisfied, " + p.detail}
	}

	return ro
}

// evaluateResource derives readiness from the live state of the dependent sibling service.
func (b *InMemoryBackend) evaluateResource(
	ctx context.Context,
	typ string,
	res Resource,
	auths []string,
	now time.Time,
) resourceOutcome {
	out := resourceOutcome{At: now, Resource: res}
	defs := rulesFor(typ)

	if len(defs) == 0 {
		out.Readiness = readinessUnknown
		out.Note = "no readiness rules are evaluated for resource type " + typ

		return out
	}

	var p probe

	switch {
	case res.DNS != nil:
		p = b.lookupDNS(res.DNS)
	default:
		if ok, account := b.authorized(res.ResourceArn, auths); !ok {
			for _, d := range defs {
				out.Rules = append(out.Rules, ruleOutcome{
					At: now, ID: d.ID, Readiness: readinessNotAuthorized,
					Messages: []string{"no cross-account authorization exists for account " + account},
				})
			}

			out.Readiness = readinessNotAuthorized

			return out
		}

		p = resolver{cfg: b.appConfig}.lookup(ctx, typ, res.ResourceArn)
	}

	if !p.answered {
		for _, d := range defs {
			out.Rules = append(out.Rules, ruleOutcome{
				At:        now,
				ID:        d.ID,
				Readiness: readinessUnknown,
				Messages: []string{
					"the service that owns this resource type is not reachable from the readiness evaluator",
				},
			})
		}

		out.Readiness = readinessUnknown

		return out
	}

	values := make([]string, 0, len(defs))

	for _, d := range defs {
		ro := outcomeFor(d, p, now)
		out.Rules = append(out.Rules, ro)
		values = append(values, ro.Readiness)
	}

	out.Readiness = combine(values)

	return out
}

// checkInputs copies what a readiness check needs so sibling lookups run without the backend lock.
type checkInputs struct {
	typ       string
	resources []Resource
	auths     []string
}

func (b *InMemoryBackend) inputsFor(checkName string) (checkInputs, error) {
	b.mu.RLock("readinessInputs")
	defer b.mu.RUnlock()

	c, err := b.getCheck(checkName)
	if err != nil {
		return checkInputs{}, err
	}

	s, err := b.getSet(c.ResourceSet)
	if err != nil {
		return checkInputs{}, err
	}

	in := checkInputs{typ: s.Type, resources: slices.Clone(s.Resources)}
	for _, a := range b.auths.All() {
		in.auths = append(in.auths, a.Value)
	}

	return in, nil
}

func (b *InMemoryBackend) evaluateAll(ctx context.Context, in checkInputs) []resourceOutcome {
	now := time.Now().UTC()
	out := make([]resourceOutcome, 0, len(in.resources))

	for _, r := range in.resources {
		out = append(out, b.evaluateResource(ctx, in.typ, r, in.auths, now))
	}

	slices.SortStableFunc(out, func(a, c resourceOutcome) int {
		return strings.Compare(a.Resource.identifier(), c.Resource.identifier())
	})

	return out
}

func readinessOf(outs []resourceOutcome) string {
	values := make([]string, 0, len(outs))
	for _, o := range outs {
		values = append(values, o.Readiness)
	}

	return combine(values)
}
