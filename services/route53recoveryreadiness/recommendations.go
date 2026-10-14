package route53recoveryreadiness

import (
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// ArchitectureRecommendations derives advice from the group's real cell, resource set and readiness check layout.
func (b *InMemoryBackend) ArchitectureRecommendations(
	name, token string,
	maxResults int,
) (time.Time, []string, string, error) {
	b.mu.RLock("GetArchitectureRecommendations")
	defer b.mu.RUnlock()

	g, err := b.getGroup(name)
	if err != nil {
		return time.Time{}, nil, "", err
	}

	scopes := b.descendantScopes(g.ARN, g.Cells)
	texts := b.recommendationTexts(g, scopes)

	pg, err := restroute.Paginate(texts, token, maxResults, listDefault, listMax)
	if err != nil {
		return time.Time{}, nil, "", err
	}

	return time.Now().UTC(), append([]string{}, pg.Data...), pg.Next, nil
}

func (b *InMemoryBackend) recommendationTexts(g *RecoveryGroup, scopes map[string]bool) []string {
	var out []string

	switch len(g.Cells) {
	case 0:
		out = append(
			out,
			"Recovery group "+g.Name+" has no cells. Add at least two cells, one per failure domain, "+
				"so traffic has somewhere to fail over to.",
		)
	case 1:
		out = append(
			out,
			"Recovery group "+g.Name+" has a single cell. "+
				"Add a second cell in a different failure domain to have a failover target.",
		)
	}

	typesByCell := map[string]map[string]bool{}

	for _, cellArn := range g.Cells {
		typesByCell[cellArn] = map[string]bool{}
	}

	checked := map[string]bool{}
	for _, c := range b.checks.All() {
		checked[c.ResourceSet] = true
	}

	for _, s := range sorted(b.sets.All(), func(s *ResourceSet) string { return s.Name }) {
		inGroup := false

		for _, r := range s.Resources {
			for _, sc := range r.ReadinessScopes {
				if !scopes[sc] {
					continue
				}

				inGroup = true

				if m, top := typesByCell[b.topCell(sc, g)]; top {
					m[s.Type] = true
				}
			}
		}

		if inGroup && !checked[s.Name] {
			out = append(
				out,
				"Resource set "+s.Name+" is scoped to recovery group "+g.Name+
					" but has no readiness check; create one to evaluate its readiness.",
			)
		}
	}

	return append(out, b.coverageGaps(g, typesByCell)...)
}

// topCell maps a scope ARN to the group's top-level cell that contains it ("" when outside the group).
func (b *InMemoryBackend) topCell(scope string, g *RecoveryGroup) string {
	cur := scope

	for range len(g.Cells) + len(b.cells.All()) + 1 {
		if slices.Contains(g.Cells, cur) {
			return cur
		}

		c, ok := b.cellByARN(cur)
		if !ok || c.Parent == "" {
			return ""
		}

		cur = c.Parent
	}

	return ""
}

func (b *InMemoryBackend) coverageGaps(g *RecoveryGroup, typesByCell map[string]map[string]bool) []string {
	all := map[string]bool{}

	for _, m := range typesByCell {
		for t := range m {
			all[t] = true
		}
	}

	var out []string

	if len(all) == 0 && len(g.Cells) > 0 {
		return []string{
			"No resource set is scoped to recovery group " + g.Name +
				". Create resource sets whose resources list its cells in readinessScopes.",
		}
	}

	for _, t := range sortedKeys(all) {
		for _, cellArn := range g.Cells {
			if !typesByCell[cellArn][t] {
				out = append(
					out,
					"Resource type "+t+" is present in other cells of "+g.Name+
						" but missing from cell "+cellArn+"; replicate it so the cells are symmetric.",
				)
			}
		}
	}

	return out
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}
