package elasticache

import (
	"fmt"
	"slices"
	"strconv"
)

const (
	azModeSingle = "single-az"
	azModeCross  = "cross-az"

	outpostModeSingle = "single-outpost"
	outpostModeCross  = "cross-outpost"

	maxMemcachedNodes = 40
	zoneLetterCount   = 3
)

// validAZMode reports whether mode is empty or a types.AZMode value.
func validAZMode(mode string) bool { return mode == "" || mode == azModeSingle || mode == azModeCross }

// validOutpostMode reports whether mode is empty or a types.OutpostMode value.
func validOutpostMode(mode string) bool {
	return mode == "" || mode == outpostModeSingle || mode == outpostModeCross
}

func nodeIDFor(n int) string { return fmt.Sprintf("%04d", n) }

// zoneFor returns the i-th default zone of region (a, b, c, a, ...).
func zoneFor(region string, i int) string { return region + string(rune('a'+i%zoneLetterCount)) }

// planNodeAZs places count new nodes. Explicit zones win (cycled), cross-az
// spreads across zones starting at offset, anything else lands in one zone.
func planNodeAZs(region, azMode, single string, zones []string, count, offset int) []string {
	out := make([]string, count)

	for i := range out {
		switch {
		case single != "":
			out[i] = single
		case len(zones) > 0:
			out[i] = zones[i%len(zones)]
		case azMode == azModeCross:
			out[i] = zoneFor(region, offset+i)
		default:
			out[i] = zoneFor(region, 0)
		}
	}

	return out
}

// initClusterNodes assigns node IDs 0001.. and zones to a new cluster.
func initClusterNodes(c *Cluster, region string) {
	n := max(c.NumCacheNodes, 1)
	c.CacheNodeIDs = make([]string, n)

	for i := range c.CacheNodeIDs {
		c.CacheNodeIDs[i] = nodeIDFor(i + 1)
	}

	c.CacheNodeAZs = planNodeAZs(region, c.AZMode, c.PreferredAvailabilityZone, c.PreferredAvailabilityZones, n, 0)
}

// nodeIDsOf returns the cluster's node IDs, falling back to sequential IDs
// for clusters restored without them.
func nodeIDsOf(c *Cluster) []string {
	if len(c.CacheNodeIDs) > 0 {
		return c.CacheNodeIDs
	}

	n := max(c.NumCacheNodes, 1)
	ids := make([]string, n)

	for i := range ids {
		ids[i] = nodeIDFor(i + 1)
	}

	return ids
}

// nextNodeNumber returns one past the highest node number ever used.
func nextNodeNumber(c *Cluster) int {
	highest := 0

	for _, id := range nodeIDsOf(c) {
		if v, err := strconv.Atoi(id); err == nil {
			highest = max(highest, v)
		}
	}

	return highest + 1
}

// scaleNodes applies a ModifyCacheCluster node-count change to a Memcached
// cluster. target is NumCacheNodes (0 = unchanged).
func scaleNodes(c *Cluster, region string, target int, opts *ModifyClusterOptions) error {
	if opts == nil {
		opts = &ModifyClusterOptions{}
	}

	if !validAZMode(opts.AZMode) {
		return fmt.Errorf("%w: AZMode must be single-az or cross-az", ErrInvalidParameterValue)
	}

	if c.Engine != engineMemcached {
		return scaleNonMemcached(target, opts)
	}

	ids := slices.Clone(nodeIDsOf(c))
	azs := slices.Clone(c.CacheNodeAZs)

	if len(azs) != len(ids) {
		azs = planNodeAZs(region, c.AZMode, c.PreferredAvailabilityZone, c.PreferredAvailabilityZones, len(ids), 0)
	}

	if opts.AZMode == azModeSingle && len(distinctStrings(azs)) > 1 {
		return fmt.Errorf("%w: single-az cannot be used when nodes are already in different zones",
			ErrInvalidParameterCombination)
	}

	ids, azs, err := resizeNodes(c, region, ids, azs, target, opts)
	if err != nil {
		return err
	}

	c.CacheNodeIDs, c.CacheNodeAZs = ids, azs
	c.NumCacheNodes = len(ids)

	if opts.AZMode != "" {
		c.AZMode = opts.AZMode
	}

	return nil
}

// resizeNodes grows or shrinks the node list towards target (0 or the current size keeps it).
func resizeNodes(
	c *Cluster, region string, ids, azs []string, target int, opts *ModifyClusterOptions,
) ([]string, []string, error) {
	switch {
	case target == 0 || target == len(ids):
		if len(opts.CacheNodeIDsToRemove) > 0 || len(opts.NewAvailabilityZones) > 0 {
			return nil, nil, fmt.Errorf(
				"%w: CacheNodeIdsToRemove and NewAvailabilityZones require a changed NumCacheNodes",
				ErrInvalidParameterCombination,
			)
		}

		return ids, azs, nil
	case target > maxMemcachedNodes:
		return nil, nil, fmt.Errorf("%w: NumCacheNodes must be between 1 and %d for Memcached",
			ErrInvalidParameterValue, maxMemcachedNodes)
	case target > len(ids):
		return addNodes(c, region, ids, azs, target, opts)
	}

	return removeNodes(ids, azs, target, opts)
}

func scaleNonMemcached(target int, opts *ModifyClusterOptions) error {
	if opts.AZMode != "" || len(opts.CacheNodeIDsToRemove) > 0 || len(opts.NewAvailabilityZones) > 0 {
		return fmt.Errorf("%w: AZMode, CacheNodeIdsToRemove and NewAvailabilityZones are Memcached-only",
			ErrInvalidParameterCombination)
	}

	if target > 1 {
		return fmt.Errorf("%w: NumCacheNodes must be 1 for Valkey and Redis OSS clusters", ErrInvalidParameterValue)
	}

	return nil
}

func addNodes(
	c *Cluster, region string, ids, azs []string, target int, opts *ModifyClusterOptions,
) ([]string, []string, error) {
	added := target - len(ids)

	if len(opts.CacheNodeIDsToRemove) > 0 {
		return nil, nil, fmt.Errorf("%w: CacheNodeIdsToRemove is only valid when NumCacheNodes shrinks",
			ErrInvalidParameterCombination)
	}

	if len(opts.NewAvailabilityZones) > 0 && len(opts.NewAvailabilityZones) != added {
		return nil, nil, fmt.Errorf(
			"%w: NewAvailabilityZones must list exactly %d zones",
			ErrInvalidParameterCombination,
			added,
		)
	}

	mode := opts.AZMode
	if mode == "" {
		mode = c.AZMode
	}

	single := ""
	if mode != azModeCross && len(opts.NewAvailabilityZones) == 0 && len(azs) > 0 {
		single = azs[0]
	}

	next := nextNodeNumber(c)
	newAZs := planNodeAZs(region, mode, single, opts.NewAvailabilityZones, added, len(ids))

	for i := range added {
		ids = append(ids, nodeIDFor(next+i))
	}

	return ids, append(azs, newAZs...), nil
}

func removeNodes(ids, azs []string, target int, opts *ModifyClusterOptions) ([]string, []string, error) {
	removing := len(ids) - target

	if len(opts.NewAvailabilityZones) > 0 {
		return nil, nil, fmt.Errorf("%w: NewAvailabilityZones is only valid when NumCacheNodes grows",
			ErrInvalidParameterCombination)
	}

	if len(opts.CacheNodeIDsToRemove) != removing {
		return nil, nil, fmt.Errorf("%w: CacheNodeIdsToRemove must list exactly %d node IDs", ErrInvalidParameterValue,
			removing)
	}

	drop := make(map[string]bool, removing)

	for _, id := range opts.CacheNodeIDsToRemove {
		if !slices.Contains(ids, id) {
			return nil, nil, fmt.Errorf("%w: cache node %s does not exist", ErrInvalidParameterValue, id)
		}

		drop[id] = true
	}

	keptIDs := make([]string, 0, target)
	keptAZs := make([]string, 0, target)

	for i, id := range ids {
		if !drop[id] {
			keptIDs = append(keptIDs, id)
			keptAZs = append(keptAZs, azs[i])
		}
	}

	if len(keptIDs) != target {
		return nil, nil, fmt.Errorf("%w: CacheNodeIdsToRemove contains duplicates", ErrInvalidParameterValue)
	}

	return keptIDs, keptAZs, nil
}

func distinctStrings(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)

	return slices.Compact(out)
}
