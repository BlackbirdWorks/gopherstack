package main

import "sort"

const (
	minStructOverlap = 3
	minOverlapShare  = 0.75
)

// resolveByOverlap resolves wireKey via the SDK struct sharing the most field names with a
// local struct; a tie between structs that resolve differently yields fieldUnknownType.
func (reg *enumRegistry) resolveByOverlap(wireKey string, localWireNames []string) (fieldResolution, string) {
	goName := exportedFieldName(wireKey)
	want := map[string]bool{}

	for _, n := range localWireNames {
		want[exportedFieldName(n)] = true
	}

	var best []string

	bestScore := 0

	for typeName, fields := range reg.sdkFieldTypes {
		if _, has := fields[goName]; !has {
			continue
		}

		score := 0

		for name := range fields {
			if want[name] {
				score++
			}
		}

		switch {
		case score > bestScore:
			best, bestScore = append(best[:0], typeName), score
		case score == bestScore:
			best = append(best, typeName)
		}
	}

	if bestScore < minStructOverlap || float64(bestScore) < minOverlapShare*float64(len(want)) {
		return fieldUnknownType, ""
	}

	sort.Strings(best)

	return reg.agreeingResolution(best, goName)
}

func (reg *enumRegistry) agreeingResolution(typeNames []string, goName string) (fieldResolution, string) {
	var (
		res  fieldResolution
		enum string
	)

	for i, typeName := range typeNames {
		r, e := reg.classifyFieldType(reg.sdkFieldTypes[typeName][goName])
		if i > 0 && (r != res || e != enum) {
			return fieldUnknownType, ""
		}

		res, enum = r, e
	}

	return res, enum
}
