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
