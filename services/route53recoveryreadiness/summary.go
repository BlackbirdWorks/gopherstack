package route53recoveryreadiness

import (
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Message is a readiness message.
type Message struct {
	MessageText string `json:"messageText"`
}

// ResourceResult is one resource's readiness in a check.
type ResourceResult struct {
	LastCheckedTimestamp string `json:"lastCheckedTimestamp"`
	Readiness            string `json:"readiness"`
	ComponentID          string `json:"componentId,omitempty"`
	ResourceArn          string `json:"resourceArn,omitempty"`
}

// RuleResult is one rule's readiness for a resource.
type RuleResult struct {
	LastCheckedTimestamp string    `json:"lastCheckedTimestamp"`
	Readiness            string    `json:"readiness"`
	RuleID               string    `json:"ruleId"`
	Messages             []Message `json:"messages"`
}

// CheckSummary is one check's readiness in a cell or recovery group summary.
type CheckSummary struct {
	Readiness          string `json:"readiness"`
	ReadinessCheckName string `json:"readinessCheckName"`
}

// RuleInfo describes an evaluated rule.
type RuleInfo struct {
	ResourceType    string `json:"resourceType"`
	RuleDescription string `json:"ruleDescription"`
	RuleID          string `json:"ruleId"`
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ReadinessCheckStatus evaluates every resource of the check now.
func (b *InMemoryBackend) ReadinessCheckStatus(
	ctx context.Context,
	name, token string,
	maxResults int,
) (string, []Message, []ResourceResult, string, error) {
	in, err := b.inputsFor(name)
	if err != nil {
		return "", nil, nil, "", err
	}

	outs := b.evaluateAll(ctx, in)

	var msgs []Message

	if i := slices.IndexFunc(outs, func(o resourceOutcome) bool { return o.Note != "" }); i >= 0 {
		msgs = append(msgs, Message{MessageText: outs[i].Note})
	}

	if len(outs) == 0 {
		msgs = append(msgs, Message{MessageText: "the resource set has no resources to evaluate"})
	}

	pg, err := restroute.Paginate(outs, token, maxResults, listDefault, listMax)
	if err != nil {
		return "", nil, nil, "", err
	}

	res := make([]ResourceResult, 0, len(pg.Data))
	for _, o := range pg.Data {
		res = append(res, ResourceResult{
			LastCheckedTimestamp: ts(
				o.At,
			),
			Readiness:   o.Readiness,
			ComponentID: o.Resource.ComponentID,
			ResourceArn: o.Resource.ResourceArn,
		})
	}

	return readinessOf(outs), msgs, res, pg.Next, nil
}

// ReadinessCheckResourceStatus evaluates one resource of the check, identified by ARN or component ID.
func (b *InMemoryBackend) ReadinessCheckResourceStatus(
	ctx context.Context, name, identifier, token string, maxResults int,
) (string, []RuleResult, string, error) {
	in, err := b.inputsFor(name)
	if err != nil {
		return "", nil, "", err
	}

	idx := slices.IndexFunc(in.resources, func(r Resource) bool { return r.identifier() == identifier })
	if idx < 0 {
		return "", nil, "", restroute.NotFound("Resource %s is not part of readiness check %s", identifier, name)
	}

	in.resources = in.resources[idx : idx+1]
	out := b.evaluateAll(ctx, in)[0]

	pg, err := restroute.Paginate(out.Rules, token, maxResults, listDefault, listMax)
	if err != nil {
		return "", nil, "", err
	}

	rules := make([]RuleResult, 0, len(pg.Data))

	for _, r := range pg.Data {
		msgs := make([]Message, 0, len(r.Messages))
		for _, m := range r.Messages {
			msgs = append(msgs, Message{MessageText: m})
		}

		rules = append(
			rules,
			RuleResult{LastCheckedTimestamp: ts(r.At), Readiness: r.Readiness, RuleID: r.ID, Messages: msgs},
		)
	}

	return out.Readiness, rules, pg.Next, nil
}

// descendantScopes returns the ARN of a cell or group plus every cell below it.
func (b *InMemoryBackend) descendantScopes(root string, children []string) map[string]bool {
	scopes := map[string]bool{root: true}
	queue := slices.Clone(children)

	for len(queue) > 0 {
		a := queue[0]
		queue = queue[1:]

		if scopes[a] {
			continue
		}

		scopes[a] = true

		if c, ok := b.cellByARN(a); ok {
			queue = append(queue, c.Cells...)
		}
	}

	return scopes
}

type scopedInputs struct {
	name string
	in   checkInputs
}

// scopedChecks copies, under the lock, every check that has a resource scoped to one of scopes.
func (b *InMemoryBackend) scopedChecks(scopes map[string]bool) []scopedInputs {
	var out []scopedInputs

	for _, c := range sorted(b.checks.All(), func(c *ReadinessCheck) string { return c.Name }) {
		s, ok := b.sets.Get(c.ResourceSet)
		if !ok {
			continue
		}

		in := checkInputs{typ: s.Type}

		for _, r := range s.Resources {
			if slices.ContainsFunc(r.ReadinessScopes, func(sc string) bool { return scopes[sc] }) {
				in.resources = append(in.resources, r)
			}
		}

		if len(in.resources) == 0 {
			continue
		}

		for _, a := range b.auths.All() {
			in.auths = append(in.auths, a.Value)
		}

		out = append(out, scopedInputs{name: c.Name, in: in})
	}

	return out
}

func (b *InMemoryBackend) summarize(
	ctx context.Context,
	checks []scopedInputs,
	token string,
	maxResults int,
) (string, []CheckSummary, string, error) {
	pg, err := restroute.Paginate(checks, token, maxResults, listDefault, listMax)
	if err != nil {
		return "", nil, "", err
	}

	all := make([]string, 0, len(checks))
	page := make([]CheckSummary, 0, len(pg.Data))

	for _, c := range checks {
		all = append(all, readinessOf(b.evaluateAll(ctx, c.in)))
	}

	for i := range pg.Data {
		idx := slices.IndexFunc(checks, func(c scopedInputs) bool { return c.name == pg.Data[i].name })
		page = append(page, CheckSummary{Readiness: all[idx], ReadinessCheckName: pg.Data[i].name})
	}

	return combine(all), page, pg.Next, nil
}

// CellReadinessSummary summarises the checks covering a cell and the cells below it.
func (b *InMemoryBackend) CellReadinessSummary(
	ctx context.Context,
	name, token string,
	maxResults int,
) (string, []CheckSummary, string, error) {
	b.mu.RLock("CellReadinessSummary")

	c, err := b.getCell(name)
	if err != nil {
		b.mu.RUnlock()

		return "", nil, "", err
	}

	checks := b.scopedChecks(b.descendantScopes(c.ARN, c.Cells))
	b.mu.RUnlock()

	return b.summarize(ctx, checks, token, maxResults)
}

// RecoveryGroupReadinessSummary summarises the checks covering a group and its cells.
func (b *InMemoryBackend) RecoveryGroupReadinessSummary(
	ctx context.Context,
	name, token string,
	maxResults int,
) (string, []CheckSummary, string, error) {
	b.mu.RLock("RecoveryGroupReadinessSummary")

	g, err := b.getGroup(name)
	if err != nil {
		b.mu.RUnlock()

		return "", nil, "", err
	}

	checks := b.scopedChecks(b.descendantScopes(g.ARN, g.Cells))
	b.mu.RUnlock()

	return b.summarize(ctx, checks, token, maxResults)
}

// ListRules lists the rules this emulator evaluates, optionally for one resource type.
func (b *InMemoryBackend) ListRules(resourceType, token string, maxResults int) ([]RuleInfo, string, error) {
	var all []RuleInfo

	for _, d := range ruleCatalog() {
		if resourceType == "" || d.Type == resourceType {
			all = append(all, RuleInfo{ResourceType: d.Type, RuleDescription: d.Description, RuleID: d.ID})
		}
	}

	pg, err := restroute.Paginate(all, token, maxResults, listDefault, listMax)
	if err != nil {
		return nil, "", err
	}

	return append([]RuleInfo{}, pg.Data...), pg.Next, nil
}
