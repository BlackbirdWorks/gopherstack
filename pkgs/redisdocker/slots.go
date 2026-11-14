// Package redisdocker runs real Redis or Valkey containers as a replication group or a sharded cluster and manages
// their ACLs, TLS and RDB snapshots.
package redisdocker

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// TotalSlots is the size of the Redis cluster hash-slot space.
const TotalSlots = 16384

// ErrSlots is returned when a slot assignment is malformed or does not cover the hash-slot space exactly once.
var ErrSlots = errors.New("invalid slot assignment")

// SlotRange is an inclusive range of hash slots.
type SlotRange struct {
	Start int
	End   int
}

// String formats r as "start-end".
func (r SlotRange) String() string { return fmt.Sprintf("%d-%d", r.Start, r.End) }

// ParseSlots parses "0-5461" or a comma or space separated list such as "0-100,200-300,500".
func ParseSlots(s string) ([]SlotRange, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
	out := make([]SlotRange, 0, len(fields))

	for _, f := range fields {
		lo, hi, isRange := strings.Cut(f, "-")

		start, err := strconv.Atoi(lo)
		if err != nil {
			return nil, fmt.Errorf("%w: %q", ErrSlots, f)
		}

		end := start
		if isRange {
			if end, err = strconv.Atoi(hi); err != nil {
				return nil, fmt.Errorf("%w: %q", ErrSlots, f)
			}
		}

		if start < 0 || end < start || end >= TotalSlots {
			return nil, fmt.Errorf("%w: %q is outside 0-%d", ErrSlots, f, TotalSlots-1)
		}

		out = append(out, SlotRange{Start: start, End: end})
	}

	return out, nil
}

// EvenSlots splits the hash-slot space across n shards; the last shard takes the remainder.
func EvenSlots(n int) [][]SlotRange {
	if n < 1 {
		return nil
	}

	size := TotalSlots / n
	out := make([][]SlotRange, n)

	for i := range n {
		end := i*size + size - 1
		if i == n-1 {
			end = TotalSlots - 1
		}

		out[i] = []SlotRange{{Start: i * size, End: end}}
	}

	return out
}

// ValidateSlotMap checks that the shards together cover every hash slot exactly once.
func ValidateSlotMap(shards [][]SlotRange) error {
	var all []SlotRange

	for _, s := range shards {
		all = append(all, s...)
	}

	sort.Slice(all, func(i, j int) bool { return all[i].Start < all[j].Start })

	next := 0

	for _, r := range all {
		if r.Start != next {
			return fmt.Errorf("%w: slot %d is %s", ErrSlots, next, gapOrOverlap(r.Start, next))
		}

		next = r.End + 1
	}

	if next != TotalSlots {
		return fmt.Errorf("%w: slots %d-%d are unassigned", ErrSlots, next, TotalSlots-1)
	}

	return nil
}

func gapOrOverlap(start, next int) string {
	if start > next {
		return "unassigned"
	}

	return "assigned twice"
}
