package ec2

import (
	"fmt"
	"sort"
	"time"
)

// ec2TombstoneTTL is how long a deleted resource stays describable by id in
// "deleted" state before describeWithTombstones treats it as gone and the
// janitor's sweepExpiredTombstones prunes it. No per-resource-type duration
// is documented for the TGW route table / VPC attachment / peering
// attachment, NAT gateway, Fleet, or VPN connection tombstones below; this
// reuses the one duration the pinned SDK does document for a deleted EC2
// resource staying visible (aws-sdk-go-v2/service/ec2 v1.329.0
// api_op_DescribeInstances.go: "Recently terminated instances might appear
// in the returned results. This interval is usually less than one hour.").
const ec2TombstoneTTL = time.Hour

// tombstone pairs a deleted resource's last known state with when it was
// deleted, so describeWithTombstones and the janitor can expire it instead
// of keeping it in memory forever.
type tombstone[T any] struct {
	value     *T
	deletedAt time.Time
}

// pruneExpiredTombstones removes every entry older than ec2TombstoneTTL.
// Caller must hold the backend write lock.
func pruneExpiredTombstones[T any](tombstones map[string]tombstone[T], now time.Time) {
	for id, tomb := range tombstones {
		if now.Sub(tomb.deletedAt) > ec2TombstoneTTL {
			delete(tombstones, id)
		}
	}
}

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

// describeWithTombstones copies matching live items plus, for any requested
// id not found live, its tombstone -- an unfiltered Describe never surfaces
// tombstones, matching real AWS's list-vs-get behavior. Result sorted by id.
func describeWithTombstones[T any](
	live []*T,
	tombstones map[string]tombstone[T],
	ids []string,
	idOf func(*T) string,
) []*T {
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*T, 0, len(live))
	found := make(map[string]bool, len(ids))

	for _, item := range live {
		itemID := idOf(item)
		if len(idSet) > 0 && !idSet[itemID] {
			continue
		}

		cp := *item
		out = append(out, &cp)
		found[itemID] = true
	}

	for _, id := range ids {
		if found[id] {
			continue
		}

		tomb, ok := tombstones[id]
		if !ok || time.Since(tomb.deletedAt) > ec2TombstoneTTL {
			continue
		}

		cp := *tomb.value
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return idOf(out[i]) < idOf(out[j]) })

	return out
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
