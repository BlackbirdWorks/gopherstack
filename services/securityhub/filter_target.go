package securityhub

import (
	"maps"
	"time"
)

// filterTarget is a document a composite filter is evaluated against. Fields the
// target has no source for resolve to nothing: include comparisons never match and
// exclude comparisons always do.
type filterTarget interface {
	stringValues(field string) []string
	numberMatches(field string, filter map[string]any) bool
	dateMatches(field string, filter map[string]any) bool
	mapValues(field, key string) []string
	ipMatches(field, cidr string) bool
	boolMatches(field string, want bool) bool
}

const (
	fieldResourceID   = "ResourceId"
	fieldResourceType = "ResourceType"
)

func filterEntry(m map[string]any) (string, map[string]any) {
	fieldName, _ := m["FieldName"].(string)
	filter, _ := m["Filter"].(map[string]any)

	return fieldName, filter
}

func matchesStringEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)
	if filter == nil {
		return true
	}

	comp, _ := filter["Comparison"].(string)
	val, _ := filter["Value"].(string)

	return matchesStringCandidates(comp, t.stringValues(fieldName), val)
}

func matchesNumberEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)
	if filter == nil {
		return true
	}

	return t.numberMatches(fieldName, filter)
}

func matchesDateEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)
	if filter == nil {
		return true
	}

	return t.dateMatches(fieldName, filter)
}

func matchesMapEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)
	if filter == nil {
		return true
	}

	key, _ := filter["Key"].(string)
	val, _ := filter["Value"].(string)
	comp, _ := filter["Comparison"].(string)

	return compareMapFilter(comp, t.mapValues(fieldName, key), val)
}

func matchesIPEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)

	cidr, _ := filter["Cidr"].(string)
	if cidr == "" {
		return true
	}

	return t.ipMatches(fieldName, cidr)
}

func matchesBoolEntry(t filterTarget, m map[string]any) bool {
	fieldName, filter := filterEntry(m)

	want, hasWant := filter["Value"].(bool)
	if !hasWant {
		return true
	}

	return t.boolMatches(fieldName, want)
}

// dateFilterMatches applies a DateFilter (DateRange or absolute Start/End) to fieldTime.
func dateFilterMatches(fieldTime time.Time, filter map[string]any) bool {
	if dr, hasRange := filter["DateRange"].(map[string]any); hasRange {
		return matchesDateRange(fieldTime, dr)
	}

	return matchesDateStartEnd(fieldTime, filter)
}

// findingTarget evaluates OCSF filters against a stored ASFF finding.
type findingTarget map[string]any

func (f findingTarget) stringValues(field string) []string { return ocsfStringValues(f, field) }

func (f findingTarget) numberMatches(field string, filter map[string]any) bool {
	if field == "vulnerabilities.cve.cvss.base_score" {
		return matchesCvssBaseScore(f, filter)
	}

	asffField, ok := ocsfNumberFieldMap[field]
	if !ok {
		return false
	}

	fv, hasVal := findingNumberValue(f, asffField)

	return hasVal && numberFilterMatches(fv, filter)
}

func (f findingTarget) dateMatches(field string, filter map[string]any) bool {
	asffField, ok := ocsfDateFieldMap[field]
	if !ok {
		return false
	}

	raw, _ := f[asffField].(string)

	fieldTime, err := time.Parse(time.RFC3339, raw)

	return err == nil && dateFilterMatches(fieldTime, filter)
}

func (f findingTarget) mapValues(field, key string) []string {
	return mapFilterCandidates(f, field, key)
}

func (f findingTarget) ipMatches(field, cidr string) bool {
	keys, ok := ipFieldNetworkKeys[field]
	if !ok {
		return false
	}

	network, _ := f["Network"].(map[string]any)

	for _, key := range keys {
		if ipStr, hasVal := network[key].(string); hasVal && ipStr != "" && ipInCIDR(ipStr, cidr) {
			return true
		}
	}

	return false
}

// boolMatches: a finding matches if ANY Vulnerabilities entry has the requested
// boolean. ASFF YES/NO map to true/false; FixAvailable=PARTIAL has no documented OCSF
// boolean so it matches neither value.
func (f findingTarget) boolMatches(field string, want bool) bool {
	var asffKey string

	switch field {
	case "vulnerabilities.is_exploit_available":
		asffKey = "ExploitAvailable"
	case "vulnerabilities.is_fix_available":
		asffKey = "FixAvailable"
	default:
		return false
	}

	vulns, _ := f["Vulnerabilities"].([]any)
	for _, v := range vulns {
		vm, isMap := v.(map[string]any)
		if !isMap {
			continue
		}

		switch asFindingString(vm[asffKey]) {
		case "YES":
			if want {
				return true
			}
		case "NO":
			if !want {
				return true
			}
		}
	}

	return false
}

// resourceTarget evaluates ResourcesFilters against a resource view (see resourceView).
type resourceTarget map[string]any

func (r resourceTarget) stringValues(field string) []string {
	return nonEmptyString(asFindingString(r[field]))
}

func (r resourceTarget) numberMatches(string, map[string]any) bool { return false }

func (r resourceTarget) dateMatches(string, map[string]any) bool { return false }

func (r resourceTarget) mapValues(field, key string) []string {
	if field != "ResourceTags" {
		return nil
	}

	tags, _ := r["Tags"].(map[string]any)

	return nonEmptyString(asFindingString(tags[key]))
}

func (r resourceTarget) ipMatches(string, string) bool { return false }

func (r resourceTarget) boolMatches(string, bool) bool { return false }

// resourceViewKeys are the keys resourceView adds to the original resource.
var resourceViewKeys = []string{ //nolint:gochecknoglobals // read-only lookup data
	"AccountId", fieldResourceID, fieldResourceType, "ResourceRegion", "ResourceCloudPartition",
}

// resourceView flattens an ASFF Resource plus its finding's account into the
// ResourcesStringField / ResourceGroupByField vocabulary, keeping the original keys.
func resourceView(res map[string]any, accountID string) map[string]any {
	view := maps.Clone(res)
	view["AccountId"] = accountID
	view[fieldResourceID] = res["Id"]
	view[fieldResourceType] = res["Type"]
	view["ResourceRegion"] = res["Region"]
	view["ResourceCloudPartition"] = res["Partition"]

	return view
}

// filterRuleFilters returns a rule's Filters document, nil when absent.
func filterRuleFilters(rule map[string]any) map[string]any {
	filters, _ := rule["Filters"].(map[string]any)

	return filters
}
