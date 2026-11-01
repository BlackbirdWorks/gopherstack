package azurearm

import (
	"context"
	"fmt"
	"maps"
	"strings"
)

const cosmosFieldFailoverPolicies = "failoverPolicies"

// cosmosRegionsOf lists the account's locations in failover-priority order.
func cosmosRegionsOf(a *storedCosmosAccount) []CosmosRegion {
	locs := requestedCosmosLocations(a.properties, a)
	out := make([]CosmosRegion, 0, len(locs))

	for _, l := range locs {
		out = append(out, CosmosRegion{Name: l.name, Priority: l.priority})
	}

	return out
}

// syncRegions publishes the account's topology to the data plane, which serves one account: the latest write wins.
func (p *CosmosProvider) syncRegions(a *storedCosmosAccount) {
	multi, _ := a.properties["enableMultipleWriteLocations"].(bool)
	p.dataPlane.SetRegions(cosmosRegionsOf(a), multi)

	if pr, ok := p.dataPlane.(CosmosPolicyResources); ok {
		pr.SetAccountPolicies(cosmosPoliciesOf(a))
	}
}

// cosmosPoliciesOf reads the consistency, failover and backup settings of the account.
func cosmosPoliciesOf(a *storedCosmosAccount) CosmosAccountPolicies {
	cons := fillConsistency(a.properties[cosmosFieldConsistency])
	out := CosmosAccountPolicies{Consistency: fmt.Sprint(cons["defaultConsistencyLevel"]), Backup: "Periodic"}

	out.MaxStalenessPrefix = int64(wholeNumber(cons["maxStalenessPrefix"]))
	out.MaxIntervalSeconds = wholeNumber(cons["maxIntervalInSeconds"])

	out.AutomaticFailover, _ = a.properties["enableAutomaticFailover"].(bool)

	backup := fillBackupPolicy(a.properties[cosmosFieldBackup])
	if strings.EqualFold(fmt.Sprint(backup[cosmosFieldType]), "Continuous") {
		out.Backup = "Continuous30Days"

		if m, ok := backup["continuousModeProperties"].(map[string]any); ok {
			out.Backup = fmt.Sprint(m[cosmosFieldTier])
		}
	}

	return out
}

// FailoverPriorityChange applies a manual failover: the body must give every location exactly one unique
// priority, and the location with priority 0 becomes the write region.
func (p *CosmosProvider) FailoverPriorityChange(_ context.Context, id ResourceID, body map[string]any) error {
	p.mu.Lock("FailoverPriorityChange")
	defer p.mu.Unlock()

	a, ok := p.owned(id.ResourceGroup, id.LeafName())
	if !ok {
		return ErrCosmosAccountNotFound
	}

	prio, err := parseFailoverPolicies(body, cosmosRegionsOf(a))
	if err != nil {
		return err
	}

	list, _ := a.properties[cosmosFieldLocations].([]any)
	if len(list) == 0 {
		list = []any{map[string]any{cosmosFieldLocationName: a.location}}
	}

	updated := make([]any, 0, len(list))

	for _, raw := range list {
		m, _ := raw.(map[string]any)
		name, _ := m[cosmosFieldLocationName].(string)
		next := make(map[string]any, len(m))

		maps.Copy(next, m)

		next[cosmosFieldFailoverPriority] = float64(prio[strings.ToLower(name)])
		updated = append(updated, next)
	}

	a.properties[cosmosFieldLocations] = updated
	p.syncRegions(a)

	return nil
}

func parseFailoverPolicies(body map[string]any, current []CosmosRegion) (map[string]int, error) {
	list, _ := body[cosmosFieldFailoverPolicies].([]any)
	if len(list) != len(current) {
		return nil, fmt.Errorf("%w: failoverPolicies must list every location of the account exactly once",
			ErrCosmosBadRequest)
	}

	known := make(map[string]struct{}, len(current))
	for _, r := range current {
		known[strings.ToLower(r.Name)] = struct{}{}
	}

	prio := make(map[string]int, len(list))
	used := make(map[int]struct{}, len(list))

	for _, raw := range list {
		m, _ := raw.(map[string]any)
		name, _ := m[cosmosFieldLocationName].(string)
		f, hasPrio := m[cosmosFieldFailoverPriority].(float64)
		key := strings.ToLower(name)

		if _, ok := known[key]; !ok || !hasPrio || f < 0 || f != float64(int(f)) {
			return nil, fmt.Errorf("%w: invalid failover policy for location %q", ErrCosmosBadRequest, name)
		}

		if _, dup := prio[key]; dup {
			return nil, fmt.Errorf("%w: location %q appears twice", ErrCosmosBadRequest, name)
		}

		if _, dup := used[int(f)]; dup {
			return nil, fmt.Errorf("%w: failover priority %d is used twice", ErrCosmosBadRequest, int(f))
		}

		prio[key], used[int(f)] = int(f), struct{}{}
	}

	if _, hasWrite := used[0]; !hasWrite {
		return nil, fmt.Errorf("%w: exactly one location must have failover priority 0", ErrCosmosBadRequest)
	}

	return prio, nil
}

// wholeNumber reads a JSON number that may have been stored as an int or a float.
func wholeNumber(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case float64:
		return int(n)
	}

	return 0
}
