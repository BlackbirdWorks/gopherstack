package gcpbigtable

import (
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
)

const maxGCDepth = 20

func validateGC(r *adminpb.GcRule, depth int) error {
	if depth > maxGCDepth {
		return errInvalid("GC rule nesting is too deep.")
	}

	switch rt := r.GetRule().(type) {
	case *adminpb.GcRule_MaxNumVersions:
		if rt.MaxNumVersions < 1 {
			return errInvalid("max_num_versions must be at least 1.")
		}
	case *adminpb.GcRule_MaxAge:
		if rt.MaxAge.AsDuration() < time.Millisecond {
			return errInvalid("max_age must be at least 1 millisecond.")
		}
	case *adminpb.GcRule_Intersection_:
		return validateGCList(rt.Intersection.GetRules(), "intersection", depth)
	case *adminpb.GcRule_Union_:
		return validateGCList(rt.Union.GetRules(), "union", depth)
	default:
		return errInvalid("GC rule must have exactly one of max_num_versions, max_age, intersection or union set.")
	}

	return nil
}

func validateGCList(rules []*adminpb.GcRule, kind string, depth int) error {
	if len(rules) == 0 {
		return errInvalid("A GC rule %s needs at least one rule.", kind)
	}

	for _, sub := range rules {
		if err := validateGC(sub, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// gcExpired reports whether a cell of the given rank (0 is newest) and timestamp is garbage under r.
func gcExpired(r *adminpb.GcRule, rank int, ts, nowMicros int64) bool {
	switch rt := r.GetRule().(type) {
	case *adminpb.GcRule_MaxNumVersions:
		return int64(rank) >= int64(rt.MaxNumVersions)
	case *adminpb.GcRule_MaxAge:
		return nowMicros-ts > rt.MaxAge.AsDuration().Microseconds()
	case *adminpb.GcRule_Intersection_:
		for _, sub := range rt.Intersection.GetRules() {
			if !gcExpired(sub, rank, ts, nowMicros) {
				return false
			}
		}

		return len(rt.Intersection.GetRules()) > 0
	case *adminpb.GcRule_Union_:
		for _, sub := range rt.Union.GetRules() {
			if gcExpired(sub, rank, ts, nowMicros) {
				return true
			}
		}
	}

	return false
}

// live returns the cells of a column that survive the family's GC rule, newest first.
func live(r *adminpb.GcRule, cells []cell, nowMicros int64) []cell {
	if r == nil {
		return cells
	}

	for i, c := range cells {
		if gcExpired(r, i, c.ts, nowMicros) {
			if r.GetMaxNumVersions() != 0 {
				return cells[:i]
			}

			return keepLive(r, cells, nowMicros)
		}
	}

	return cells
}

func keepLive(r *adminpb.GcRule, cells []cell, nowMicros int64) []cell {
	out := make([]cell, 0, len(cells))

	for i, c := range cells {
		if !gcExpired(r, i, c.ts, nowMicros) {
			out = append(out, c)
		}
	}

	return out
}
