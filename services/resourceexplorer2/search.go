package resourceexplorer2

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxSearchResults = 1000

// PropertyWire is a resource property.
type PropertyWire struct {
	LastReportedAt time.Time `json:"LastReportedAt"`
	Data           any       `json:"Data"`
	Name           string    `json:"Name"`
}

// ResourceWire is a search hit.
type ResourceWire struct {
	LastReportedAt  time.Time      `json:"LastReportedAt"`
	Arn             string         `json:"Arn"`
	CfnResourceType string         `json:"CfnResourceType,omitempty"`
	OwningAccountID string         `json:"OwningAccountId"`
	Region          string         `json:"Region"`
	ResourceType    string         `json:"ResourceType"`
	Service         string         `json:"Service"`
	Properties      []PropertyWire `json:"Properties,omitempty"`
}

// SearchResult is the output of Search and ListResources.
type SearchResult struct {
	ViewARN   string
	Next      string
	Resources []ResourceWire
	Total     int
	Complete  bool
}

type searchPlan struct {
	source    ResourceSource
	view      View
	account   string
	regions   []string
	withTags  bool
	hasSource bool
}

func (b *InMemoryBackend) planSearch(ctx context.Context, viewARN string) (*searchPlan, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("Search")
	defer b.mu.RUnlock()

	if viewARN == "" {
		d, ok := b.defaults.Get(defaultKey(account, region))
		if !ok {
			return nil, unauthorized("there is no default view in %s; pass ViewArn or associate a default view", region)
		}

		viewARN = d.ViewARN
	}

	v, err := b.viewLocked(account, region, viewARN)
	if err != nil {
		return nil, err
	}

	idx := b.indexIn(account, region)
	if idx == nil {
		return nil, notFound("there is no index in %s", region)
	}

	regions := []string{region}

	if idx.Type == indexTypeAggregator {
		for _, o := range b.liveIndexes(account) {
			if o.Region != region {
				regions = append(regions, o.Region)
			}
		}
	}

	return &searchPlan{
		source: b.source, hasSource: b.source != nil, view: *v, account: account, regions: regions,
		withTags: slices.Contains(v.IncludedProperties, propertyTags),
	}, nil
}

// Search returns the resources matching query within the view, newest-index semantics aside, ordered by ARN.
func (b *InMemoryBackend) Search(
	ctx context.Context, query, viewARN, token string, maxResults int,
) (*SearchResult, error) {
	clauses, err := parseQuery(query)
	if err != nil {
		return nil, err
	}

	return b.run(ctx, clauses, viewARN, token, maxResults)
}

// ListResources lists the resources a view exposes, narrowed by an optional filter.
func (b *InMemoryBackend) ListResources(
	ctx context.Context, filter *SearchFilter, viewARN, token string, maxResults int,
) (*SearchResult, error) {
	var clauses []clause

	if filter != nil {
		var err error
		if clauses, err = parseQuery(filter.FilterString); err != nil {
			return nil, err
		}
	}

	return b.run(ctx, clauses, viewARN, token, maxResults)
}

func (b *InMemoryBackend) run(
	ctx context.Context, clauses []clause, viewARN, token string, maxResults int,
) (*SearchResult, error) {
	plan, err := b.planSearch(ctx, viewARN)
	if err != nil {
		return nil, err
	}

	viewClauses, err := parseQuery(plan.view.FilterString)
	if err != nil {
		return nil, err
	}

	matched := b.gather(ctx, plan, viewClauses, clauses)

	total, complete := len(matched), true
	if total > maxSearchResults {
		matched, total, complete = matched[:maxSearchResults], maxSearchResults, false
	}

	pg, err := paginate(matched, token, maxResults, maxPage)
	if err != nil {
		return nil, err
	}

	return b.render(plan, pg, total, complete), nil
}

func (b *InMemoryBackend) gather(ctx context.Context, plan *searchPlan, viewClauses, clauses []clause) []*candidate {
	if !plan.hasSource {
		return nil
	}

	seen := map[string]bool{}

	var out []*candidate

	for _, region := range plan.regions {
		for _, src := range plan.source.Resources(awsmeta.WithRegion(ctx, region)) {
			c, ok := toCandidate(src, region, plan.account)
			if !ok || seen[c.arn] || c.account != plan.account || (c.region != region && c.region != globalRegion) {
				continue
			}

			if evaluate(c, viewClauses) && evaluate(c, clauses) {
				seen[c.arn] = true
				out = append(out, c)
			}
		}
	}

	slices.SortFunc(out, func(x, y *candidate) int { return cmp.Compare(x.arn, y.arn) })

	return out
}

func (b *InMemoryBackend) render(plan *searchPlan, pg page.Page[*candidate], total int, complete bool) *SearchResult {
	now := b.now()
	res := &SearchResult{ViewARN: plan.view.ARN, Next: pg.Next, Total: total, Complete: complete}
	res.Resources = make([]ResourceWire, 0, len(pg.Data))

	for _, c := range pg.Data {
		r := ResourceWire{
			Arn: c.arn, CfnResourceType: cfnTypeOf(c.resourceType), LastReportedAt: now, OwningAccountID: c.account,
			Region: c.region, ResourceType: c.resourceType, Service: c.service,
		}

		if plan.withTags && len(c.tags) > 0 {
			data := make([]map[string]string, 0, len(c.tags))
			for _, k := range sortedKeys(c.tags) {
				data = append(data, map[string]string{"Key": k, "Value": c.tags[k]})
			}

			r.Properties = []PropertyWire{{Name: propertyTags, LastReportedAt: now, Data: data}}
		}

		res.Resources = append(res.Resources, r)
	}

	return res
}
