package serverlessrepo

import (
	"cmp"
	"strconv"
	"strings"
)

// compareSemver orders two semantic versions per semver.org section 11; build
// metadata is ignored for precedence and used only as a final deterministic tiebreak.
func compareSemver(a, b string) int {
	coreA, preA := splitSemver(a)
	coreB, preB := splitSemver(b)

	partsA, partsB := strings.Split(coreA, "."), strings.Split(coreB, ".")
	for i := range min(len(partsA), len(partsB)) {
		if c := compareNumeric(partsA[i], partsB[i]); c != 0 {
			return c
		}
	}

	if c := comparePrerelease(preA, preB); c != 0 {
		return c
	}

	return strings.Compare(a, b)
}

func splitSemver(v string) (string, string) {
	v, _, _ = strings.Cut(v, "+")
	core, pre, _ := strings.Cut(v, "-")

	return core, pre
}

func compareNumeric(a, b string) int {
	na, errA := strconv.ParseUint(a, 10, 64)
	nb, errB := strconv.ParseUint(b, 10, 64)

	if errA != nil || errB != nil {
		return cmp.Or(cmp.Compare(len(a), len(b)), strings.Compare(a, b))
	}

	return cmp.Compare(na, nb)
}

func comparePrerelease(a, b string) int {
	switch {
	case a == b:
		return 0
	case a == "":
		return 1
	case b == "":
		return -1
	}

	idsA, idsB := strings.Split(a, "."), strings.Split(b, ".")
	for i := range min(len(idsA), len(idsB)) {
		if c := comparePrereleaseID(idsA[i], idsB[i]); c != 0 {
			return c
		}
	}

	return cmp.Compare(len(idsA), len(idsB))
}

func comparePrereleaseID(a, b string) int {
	_, errA := strconv.ParseUint(a, 10, 64)
	_, errB := strconv.ParseUint(b, 10, 64)

	switch {
	case errA == nil && errB == nil:
		return compareNumeric(a, b)
	case errA == nil:
		return -1
	case errB == nil:
		return 1
	}

	return strings.Compare(a, b)
}
