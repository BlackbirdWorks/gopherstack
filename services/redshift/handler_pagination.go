package redshift

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
)

// Shared Marker/MaxRecords convention documented on nearly every redshift
// Describe operation (e.g. DescribeHsmClientCertificatesInput's docs:
// "Default: 100  Constraints: minimum 20, maximum 100").
const (
	redshiftDefaultPageSize = 100
	redshiftMinPageSize     = 20
	redshiftMaxPageSize     = 100
)

// parseRedshiftMaxRecords reads and validates the MaxRecords request
// parameter shared by nearly every redshift Describe operation. An absent
// MaxRecords defaults to redshiftDefaultPageSize (matching AWS's documented
// default), same as every other page-size convention already in this file.
func parseRedshiftMaxRecords(vals url.Values) (int, error) {
	s := vals.Get("MaxRecords")
	if s == "" {
		return redshiftDefaultPageSize, nil
	}

	n, err := strconv.Atoi(s)
	if err != nil || n < redshiftMinPageSize || n > redshiftMaxPageSize {
		return 0, fmt.Errorf(
			"%w: MaxRecords must be between %d and %d", ErrInvalidParameter, redshiftMinPageSize, redshiftMaxPageSize,
		)
	}

	return n, nil
}

// paginateByMarker applies the shared Marker (exclusive last-seen-identifier
// cursor)/MaxRecords convention -- see DescribeClusters in store.go for the
// canonical version this mirrors generically -- to an already sorted,
// already filtered slice.
func paginateByMarker[V any](sorted []V, marker string, maxRecords int, nameOf func(V) string) ([]V, string) {
	if marker != "" {
		cut := 0
		for cut < len(sorted) && nameOf(sorted[cut]) <= marker {
			cut++
		}

		sorted = sorted[cut:]
	}

	nextMarker := ""
	if len(sorted) > maxRecords {
		sorted = sorted[:maxRecords]
		nextMarker = nameOf(sorted[len(sorted)-1])
	}

	return sorted, nextMarker
}

// describePaginated runs the shared Describe shape (list from the backend, map
// to XML, sort by key, apply Marker/MaxRecords, wrap in the result struct) used
// by DescribeEventSubscriptions, DescribeClusterParameterGroups and
// DescribeReservedNodes.
func describePaginated[T, X any](
	vals url.Values,
	list func() ([]T, error),
	toXML func(*T) X,
	keyOf func(X) string,
	wrap func(items []X, marker string) any,
) (any, error) {
	items, err := list()
	if err != nil {
		return nil, err
	}

	maxRecords, err := parseRedshiftMaxRecords(vals)
	if err != nil {
		return nil, err
	}

	members := make([]X, 0, len(items))
	for _, it := range items {
		itp := it
		members = append(members, toXML(&itp))
	}

	sort.Slice(members, func(i, j int) bool { return keyOf(members[i]) < keyOf(members[j]) })

	members, nextMarker := paginateByMarker(members, vals.Get("Marker"), maxRecords, keyOf)

	return wrap(members, nextMarker), nil
}
