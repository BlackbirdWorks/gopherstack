package ec2

import (
	"fmt"
	"sort"
)

// describeByIDsOrNotFound looks up each of ids via get and returns the found
// values sorted by less, or fails the whole call with notFoundErr (wrapping
// the first missing id) -- the real-AWS-matching "explicit ID filter fails
// hard on any miss" shape shared by DescribeDhcpOptions and
// DescribeCustomerGateways (and, by the same reasoning, most of this
// package's other Describe-by-ID ops; not swept here).
func describeByIDsOrNotFound[T any](
	ids []string, get func(string) (*T, bool), notFoundErr error, less func(a, b *T) bool,
) ([]*T, error) {
	out := make([]*T, 0, len(ids))

	for _, id := range ids {
		v, ok := get(id)
		if !ok {
			return nil, fmt.Errorf("%w: %s", notFoundErr, id)
		}

		cp := *v
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return less(out[i], out[j]) })

	return out, nil
}

// firstMissingID reports the lexicographically-first id in requested that has
// no entry in found, wrapped in notFoundErr -- for Describe ops (e.g.
// DescribeImages) that build their found set while scanning a pre-existing
// slice rather than doing per-id map lookups.
func firstMissingID(requested, found map[string]struct{}, notFoundErr error) error {
	if len(requested) == 0 {
		return nil
	}

	missing := make([]string, 0, len(requested))
	for id := range requested {
		if _, ok := found[id]; !ok {
			missing = append(missing, id)
		}
	}

	if len(missing) == 0 {
		return nil
	}

	sort.Strings(missing)

	return fmt.Errorf("%w: %s", notFoundErr, missing[0])
}

// requireAllIDsPresent fails with notFoundErr (wrapping the first missing id)
// when requested is non-empty and results, extracted via idOf, doesn't cover
// every requested id -- for Describe ops whose backend method already
// returns a filtered slice that silently drops unknown explicit ids instead
// of erroring, matching real AWS's "explicit ID filter fails hard on any
// miss" behavior without changing that method's signature.
func requireAllIDsPresent[T any](requested []string, results []T, idOf func(T) string, notFoundErr error) error {
	if len(requested) == 0 {
		return nil
	}

	req := make(map[string]struct{}, len(requested))
	for _, id := range requested {
		req[id] = struct{}{}
	}

	found := make(map[string]struct{}, len(results))
	for _, r := range results {
		found[idOf(r)] = struct{}{}
	}

	return firstMissingID(req, found, notFoundErr)
}
