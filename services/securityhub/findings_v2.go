package securityhub

import (
	"maps"
	"net"
	"regexp"
	"slices"
	"strings"
	"time"
)

// ocsfStringFieldMap maps a documented types.OcsfStringField wire value
// (GetFindingsV2's CompositeFilters.StringFilters[].FieldName) to the
// equivalent top-level ASFF field on the finding as stored by this backend.
// This backend only ingests findings via the V1 ASFF BatchImportFindings API
// -- there is no separate OCSF ingestion operation in the real API either, so
// GetFindingsV2 necessarily filters over the same ASFF-shaped documents
// BatchImportFindings created. Only fields with a direct, unambiguous
// scalar-string ASFF equivalent are mapped; unmapped/unrecognized field
// names are not filtered on, matching the "simplified filter, basic subset"
// precedent matchesFindingFilters already established for V1 GetFindings.
// class_name (OCSF) has no entry here: its closest ASFF analog, Types, is a
// string *array* (types.AwsSecurityFinding.Types), not a scalar this
// string-equality map can represent -- see matchesOcsfStringFilter.
var ocsfStringFieldMap = map[string]string{ //nolint:gochecknoglobals // read-only lookup data
	"cloud.account.uid":    keyAwsAccountID,
	"cloud.region":         "Region",
	"finding_info.uid":     "Id",
	"finding_info.title":   keyTitle,
	"finding_info.desc":    keyDescription,
	"metadata.product.uid": keyProductArn,
	"compliance.status":    keyFilterComplianceStatus,
	"status":               keyFilterWorkflowStatus,
	"severity":             keyFilterSeverityLabel,
	"resources.type":       "ResourceType",
	"resources.uid":        "ResourceId",
	"resources.region":     "Region",
	"comment":              "Comment",
}

// ocsfNumberFieldMap maps a documented types.OcsfNumberField wire value to
// the field this backend stores it under. severity_id/status_id round-trip
// the SeverityId/StatusId fields BatchUpdateFindingsV2 itself writes (see
// BatchUpdateFindingsV2) -- there's no ASFF equivalent to derive them from
// otherwise, since ASFF severity/workflow status are string enums, not the
// OCSF integer IDs. confidence_score maps to ASFF's own top-level Confidence
// field (AwsSecurityFinding.Confidence, an int 0-100) -- a genuine scalar
// match, not a guess. Every other documented OcsfNumberField (activity_id,
// compliance.status_id, finding_info.related_events_count, and the
// evidences.*/resources.image.*/vulnerabilities.cve.cvss.base_score/
// vendor_attributes.severity_id fields) has no scalar top-level ASFF
// equivalent: several exist only nested inside arrays this simple map can't
// address (e.g. Vulnerabilities[].Cvss[].BaseScore), and others reference
// concepts ASFF findings never carry at all (evidences, vendor_attributes).
// Left unmapped rather than fabricated.
var ocsfNumberFieldMap = map[string]string{ //nolint:gochecknoglobals // read-only lookup data
	"severity_id":      "SeverityId",
	"status_id":        "StatusId",
	"confidence_score": "Confidence",
}

// ocsfDateFieldMap maps a documented types.OcsfDateField wire value to the
// equivalent top-level ASFF timestamp field this backend stores. ASFF
// findings only ever carry finding-level timestamps
// (CreatedAt/UpdatedAt/FirstObservedAt/LastObservedAt); the
// resources.image.*/resources.modified_time_dt fields have no ASFF
// equivalent (ASFF's Resource object carries no image or per-resource
// modification timestamp) and are intentionally left unmapped.
var ocsfDateFieldMap = map[string]string{ //nolint:gochecknoglobals // read-only lookup data
	"finding_info.created_time_dt":    keyCreatedAt,
	"finding_info.first_seen_time_dt": keyFirstObservedAt,
	"finding_info.last_seen_time_dt":  keyLastObservedAt,
	"finding_info.modified_time_dt":   keyUpdatedAt,
}

// ipFieldNetworkKeys maps a documented types.OcsfIpField wire value to the
// ASFF top-level Network object field(s) that carry that endpoint's address.
// ASFF has no "evidences" concept -- Network.SourceIpV4/V6 and
// Network.DestinationIpV4/V6 (network-related information about the
// finding) are the only genuinely analogous source/destination IP data this
// backend stores, so they're used as-is rather than inventing an evidences
// array.
var ipFieldNetworkKeys = map[string][2]string{ //nolint:gochecknoglobals // read-only lookup data
	"evidences.src_endpoint.ip": {"SourceIpV4", "SourceIpV6"},
	"evidences.dst_endpoint.ip": {"DestinationIpV4", "DestinationIpV6"},
}

// maxNestedCompositeDepth bounds NestedCompositeFilters recursion. AWS
// documents the real structure as capped at three layers (CompositeFilters
// array -> CompositeFilter -> NestedCompositeFilters); this is a generous
// multiple of that as a defensive guard against a pathologically/
// maliciously deep hand-built request, not a limit real traffic should ever
// approach.
const maxNestedCompositeDepth = 5

// matchesCompositeFilterResultCap is the number of sub-filter categories
// matchesCompositeFilterDepth collects results from (String/Number/Date/
// Map/Ip/Boolean/Nested) -- used only to size the initial results slice.
const matchesCompositeFilterResultCap = 7

// matchesFindingFiltersV2 evaluates a GetFindingsV2 Filters.CompositeFilters
// document (types.OcsfFindingFilters) against a stored finding. An absent or
// empty CompositeFilters list matches every finding, matching the real API's
// "no filter = no restriction" behavior.
func matchesFindingFiltersV2(finding, filters map[string]any) bool {
	if len(filters) == 0 {
		return true
	}

	composite, _ := filters["CompositeFilters"].([]any)
	if len(composite) == 0 {
		return true
	}

	op, _ := filters["CompositeOperator"].(string)
	matchAny := op == "OR"

	for _, c := range composite {
		cf, ok := c.(map[string]any)
		if !ok {
			continue
		}

		matched := matchesCompositeFilter(finding, cf)
		if matched && matchAny {
			return true
		}

		if !matched && !matchAny {
			return false
		}
	}

	// AND: every entry matched (no early false). OR: none matched (no early true).
	return !matchAny
}

// matchesCompositeFilter evaluates one CompositeFilter's String/Number/Date/
// Map/Ip/Boolean sub-filters plus its NestedCompositeFilters against
// finding, combined by cf's Operator (AllowedOperators: AND/OR, default
// AND). Real AllowedOperators has no logical NOT combinator -- negation is
// expressed at the leaf via NOT_* comparators (e.g.
// StringFilterComparisonNotEquals), not a boolean-tree NOT node, so AND/OR
// is the complete real semantics here.
func matchesCompositeFilter(finding, cf map[string]any) bool {
	return matchesCompositeFilterDepth(finding, cf, 0)
}

// matchesCompositeFilterDepth is matchesCompositeFilter's recursive worker;
// depth tracks NestedCompositeFilters nesting (see maxNestedCompositeDepth).
func matchesCompositeFilterDepth(finding, cf map[string]any, depth int) bool {
	results := make([]bool, 0, matchesCompositeFilterResultCap)

	results = append(results, stringFilterResults(finding, cf)...)
	results = append(results, numberFilterResults(finding, cf)...)
	results = append(results, dateFilterResults(finding, cf)...)
	results = append(results, mapFilterResults(finding, cf)...)
	results = append(results, ipFilterResults(finding, cf)...)
	results = append(results, booleanFilterResults(finding, cf)...)
	results = append(results, nestedCompositeFilterResults(finding, cf, depth)...)

	if len(results) == 0 {
		return true
	}

	op, _ := cf["Operator"].(string)
	if op == "OR" {
		return slices.Contains(results, true)
	}

	return !slices.Contains(results, false)
}

// nestedCompositeFilterResults recursively evaluates each entry of cf's
// NestedCompositeFilters ([]types.CompositeFilter, the real recursive shape)
// as its own composite filter, joining into the parent's result list under
// the parent's own Operator -- matching the documented "third layer" nested
// structure. A partially-evaluated boolean tree would return wrong results
// (not merely unfiltered ones), so this recurses fully rather than treating
// nested entries as a no-op; depth is capped defensively (see
// maxNestedCompositeDepth) against pathological input rather than crashing
// or silently mis-evaluating.
func nestedCompositeFilterResults(finding, cf map[string]any, depth int) []bool {
	nested, ok := cf["NestedCompositeFilters"].([]any)
	if !ok || len(nested) == 0 {
		return nil
	}

	if depth >= maxNestedCompositeDepth {
		return nil
	}

	var results []bool

	for _, n := range nested {
		if m, isMap := n.(map[string]any); isMap {
			results = append(results, matchesCompositeFilterDepth(finding, m, depth+1))
		}
	}

	return results
}

// stringFilterResults evaluates cf's StringFilters against finding.
func stringFilterResults(finding, cf map[string]any) []bool {
	sf, ok := cf["StringFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range sf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfStringFilter(finding, m))
		}
	}

	return results
}

// numberFilterResults evaluates cf's NumberFilters against finding.
func numberFilterResults(finding, cf map[string]any) []bool {
	nf, ok := cf["NumberFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range nf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfNumberFilter(finding, m))
		}
	}

	return results
}

// dateFilterResults evaluates cf's DateFilters against finding.
func dateFilterResults(finding, cf map[string]any) []bool {
	df, ok := cf["DateFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range df {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfDateFilter(finding, m))
		}
	}

	return results
}

// mapFilterResults evaluates cf's MapFilters against finding.
func mapFilterResults(finding, cf map[string]any) []bool {
	mf, ok := cf["MapFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range mf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfMapFilter(finding, m))
		}
	}

	return results
}

// ipFilterResults evaluates cf's IpFilters against finding.
func ipFilterResults(finding, cf map[string]any) []bool {
	ipf, ok := cf["IpFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range ipf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfIPFilter(finding, m))
		}
	}

	return results
}

// booleanFilterResults evaluates cf's BooleanFilters against finding.
func booleanFilterResults(finding, cf map[string]any) []bool {
	bf, ok := cf["BooleanFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range bf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesOcsfBooleanFilter(finding, m))
		}
	}

	return results
}

// matchesOcsfStringFilter evaluates one OcsfStringFilter (FieldName + a
// StringFilter Comparison/Value pair) against finding. An unmapped
// FieldName is not filtered on -- see ocsfStringFieldMap.
func matchesOcsfStringFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)

	asffField, ok := ocsfStringFieldMap[fieldName]
	if !ok {
		return true
	}

	filter, _ := m["Filter"].(map[string]any)
	if filter == nil {
		return true
	}

	comp, _ := filter["Comparison"].(string)
	val, _ := filter["Value"].(string)
	fieldVal := findingFieldString(finding, asffField)

	return compareStringFilter(comp, fieldVal, val)
}

// matchesOcsfNumberFilter evaluates one OcsfNumberFilter (FieldName + a
// NumberFilter Eq/Gt/Gte/Lt/Lte set) against finding. An unmapped FieldName
// is not filtered on -- see ocsfNumberFieldMap. A mapped field that was
// never set on the finding cannot satisfy any numeric bound, so it's
// excluded rather than silently passing.
func matchesOcsfNumberFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)

	asffField, ok := ocsfNumberFieldMap[fieldName]
	if !ok {
		return true
	}

	filter, _ := m["Filter"].(map[string]any)
	if filter == nil {
		return true
	}

	fv, hasVal := findingNumberValue(finding, asffField)
	if !hasVal {
		return false
	}

	if eq, hasEq := filter["Eq"].(float64); hasEq && fv != eq {
		return false
	}

	if gt, hasGt := filter["Gt"].(float64); hasGt && fv <= gt {
		return false
	}

	if gte, hasGte := filter["Gte"].(float64); hasGte && fv < gte {
		return false
	}

	if lt, hasLt := filter["Lt"].(float64); hasLt && fv >= lt {
		return false
	}

	if lte, hasLte := filter["Lte"].(float64); hasLte && fv > lte {
		return false
	}

	return true
}

// findingNumberValue reads field off finding as a float64, accepting both
// the float64 shape json.Unmarshal produces and a plain int (as stored
// internally by BatchUpdateFindingsV2).
func findingNumberValue(finding map[string]any, field string) (float64, bool) {
	switch v := finding[field].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	default:
		return 0, false
	}
}

// matchesOcsfDateFilter evaluates one OcsfDateFilter (FieldName + a
// DateFilter Start/End/DateRange set) against finding. An unmapped
// FieldName is not filtered on -- see ocsfDateFieldMap. A mapped field
// that's missing or not a valid timestamp cannot satisfy any date bound, so
// it's excluded rather than silently passing (same policy as
// matchesOcsfNumberFilter).
func matchesOcsfDateFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)

	asffField, ok := ocsfDateFieldMap[fieldName]
	if !ok {
		return true
	}

	filter, _ := m["Filter"].(map[string]any)
	if filter == nil {
		return true
	}

	raw, _ := finding[asffField].(string)

	fieldTime, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return false
	}

	if dr, hasRange := filter["DateRange"].(map[string]any); hasRange {
		return matchesDateRange(fieldTime, dr)
	}

	return matchesDateStartEnd(fieldTime, filter)
}

// matchesDateRange evaluates a relative DateRange{Comparison,Unit,Value}
// against fieldTime. AWS documents Unit as DAYS-only (the sole
// DateRangeUnit value in this SDK version) and Comparison as WITHIN
// (default) or OLDER_THAN, relative to now. WITHIN is implemented as "at or
// after now minus Value days" (a finding timestamped now or in the future
// still counts as within the window); OLDER_THAN is its strict complement.
func matchesDateRange(fieldTime time.Time, dr map[string]any) bool {
	value, _ := dr["Value"].(float64)
	cutoff := time.Now().UTC().AddDate(0, 0, -int(value))

	comparison, _ := dr["Comparison"].(string)
	if comparison == "OLDER_THAN" {
		return fieldTime.Before(cutoff)
	}

	return !fieldTime.Before(cutoff)
}

// matchesDateStartEnd evaluates the absolute Start/End bounds of a
// DateFilter against fieldTime. Either bound may be absent; an
// unparsable/absent bound is not enforced.
func matchesDateStartEnd(fieldTime time.Time, filter map[string]any) bool {
	if startStr, ok := filter["Start"].(string); ok && startStr != "" {
		if start, err := time.Parse(time.RFC3339, startStr); err == nil && fieldTime.Before(start) {
			return false
		}
	}

	if endStr, ok := filter["End"].(string); ok && endStr != "" {
		if end, err := time.Parse(time.RFC3339, endStr); err == nil && fieldTime.After(end) {
			return false
		}
	}

	return true
}

// matchesOcsfMapFilter evaluates one OcsfMapFilter (FieldName + a MapFilter
// Key/Value/Comparison set) against finding. An unmapped FieldName (e.g.
// databucket.tags -- see mapFilterCandidates) is not filtered on.
func matchesOcsfMapFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)

	filter, _ := m["Filter"].(map[string]any)
	if filter == nil {
		return true
	}

	key, _ := filter["Key"].(string)

	candidates, ok := mapFilterCandidates(finding, fieldName, key)
	if !ok {
		return true
	}

	val, _ := filter["Value"].(string)
	comp, _ := filter["Comparison"].(string)

	return compareMapFilter(comp, candidates, val)
}

// compareMapFilter evaluates a MapFilterComparison (EQUALS/NOT_EQUALS/
// CONTAINS/NOT_CONTAINS) against the set of candidate values found for a
// MapFilter's Key. Multiple resources/parameters can share the same key
// name (e.g. a tag key present on several Resources entries), so a
// positive comparison (EQUALS/CONTAINS) matches if ANY candidate satisfies
// it, while a negative comparison (NOT_EQUALS/NOT_CONTAINS) requires that
// NONE do -- mirroring the documented OR-for-positive/AND-for-negative
// combination rule for repeated filters on the same field.
func compareMapFilter(comp string, candidates []string, val string) bool {
	switch comp {
	case comparisonNotEquals:
		return !slices.Contains(candidates, val)
	case comparisonNotContains:
		return !slices.ContainsFunc(candidates, func(c string) bool { return strings.Contains(c, val) })
	case "CONTAINS":
		return slices.ContainsFunc(candidates, func(c string) bool { return strings.Contains(c, val) })
	default: // EQUALS
		return slices.Contains(candidates, val)
	}
}

// mapFilterCandidates returns the candidate string value(s) found on
// finding for a MapFilter's FieldName+Key, and whether fieldName is one
// this backend can evaluate at all.
//
// resources.tags and finding_info.tags have direct ASFF equivalents
// (per-resource Resource.Tags, and the finding-level UserDefinedFields
// customer key/value map, respectively -- the closest real analog to a
// finding-level "tag"). compliance.control_parameters maps to ASFF's
// Compliance.SecurityControlParameters ([]SecurityControlParameter{Name,
// Value []string}). databucket.tags has no ASFF equivalent at all (ASFF
// findings carry no "databucket" concept) and is intentionally unmapped.
func mapFilterCandidates(finding map[string]any, fieldName, key string) ([]string, bool) {
	switch fieldName {
	case "resources.tags":
		return resourceTagValues(finding, key), true
	case "finding_info.tags":
		return userDefinedFieldValues(finding, key), true
	case "compliance.control_parameters":
		return complianceControlParamValues(finding, key), true
	default:
		return nil, false
	}
}

// resourceTagValues collects the value of tag key across every entry in
// finding's Resources array (ASFF Resource.Tags is per-resource).
func resourceTagValues(finding map[string]any, key string) []string {
	resources, _ := finding["Resources"].([]any)

	var values []string

	for _, r := range resources {
		rm, isMap := r.(map[string]any)
		if !isMap {
			continue
		}

		tags, _ := rm["Tags"].(map[string]any)
		if v, hasVal := tags[key].(string); hasVal {
			values = append(values, v)
		}
	}

	return values
}

// userDefinedFieldValues reads a single key out of finding's top-level
// UserDefinedFields map.
func userDefinedFieldValues(finding map[string]any, key string) []string {
	udf, _ := finding["UserDefinedFields"].(map[string]any)
	if v, ok := udf[key].(string); ok {
		return []string{v}
	}

	return nil
}

// complianceControlParamValues collects the Value list of every
// Compliance.SecurityControlParameters entry whose Name matches key.
func complianceControlParamValues(finding map[string]any, key string) []string {
	compliance, _ := finding["Compliance"].(map[string]any)
	params, _ := compliance["SecurityControlParameters"].([]any)

	var values []string

	for _, p := range params {
		pm, isMap := p.(map[string]any)
		if !isMap {
			continue
		}

		if name, _ := pm["Name"].(string); name != key {
			continue
		}

		vals, _ := pm["Value"].([]any)
		for _, v := range vals {
			if s, isStr := v.(string); isStr {
				values = append(values, s)
			}
		}
	}

	return values
}

// matchesOcsfIPFilter evaluates one OcsfIpFilter (FieldName + an IpFilter
// Cidr) against finding. An unmapped FieldName is not filtered on -- see
// ipFieldNetworkKeys.
func matchesOcsfIPFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)

	keys, ok := ipFieldNetworkKeys[fieldName]
	if !ok {
		return true
	}

	filter, _ := m["Filter"].(map[string]any)

	cidr, _ := filter["Cidr"].(string)
	if cidr == "" {
		return true
	}

	network, _ := finding["Network"].(map[string]any)
	if network == nil {
		return false
	}

	for _, key := range keys {
		if ipStr, hasVal := network[key].(string); hasVal && ipStr != "" && ipInCIDR(ipStr, cidr) {
			return true
		}
	}

	return false
}

// ipInCIDR reports whether ipStr falls inside cidr. AWS documents Cidr as
// accepting either a CIDR block or a bare IP address; a bare address is
// normalized to an exact-match /32 (IPv4) or /128 (IPv6) before testing
// containment.
func ipInCIDR(ipStr, cidr string) bool {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false
	}

	if !strings.Contains(cidr, "/") {
		if strings.Contains(cidr, ":") {
			cidr += "/128"
		} else {
			cidr += "/32"
		}
	}

	_, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}

	return ipNet.Contains(ip)
}

// matchesOcsfBooleanFilter evaluates one OcsfBooleanFilter (FieldName + a
// BooleanFilter Value) against finding.
//
// Only vulnerabilities.is_exploit_available is evaluated:
// Vulnerability.ExploitAvailable is a genuine two-valued ASFF enum
// (YES/NO), so it round-trips to a bool cleanly. A finding matches if ANY
// entry in its Vulnerabilities array has ExploitAvailable matching the
// requested boolean (array-membership semantics, consistent with
// resourceTagValues/complianceControlParamValues above).
//
// vulnerabilities.is_fix_available is intentionally NOT evaluated:
// Vulnerability.FixAvailable is three-valued (YES/NO/PARTIAL in the real
// SDK), and collapsing PARTIAL into either true or false would silently
// misclassify findings -- worse than leaving it unfiltered.
// compliance.assessments.meets_criteria has no ASFF backing at all (the
// ASFF Compliance object has no "assessments"/"meets_criteria" concept) and
// is also left unmapped.
func matchesOcsfBooleanFilter(finding, m map[string]any) bool {
	fieldName, _ := m["FieldName"].(string)
	if fieldName != "vulnerabilities.is_exploit_available" {
		return true
	}

	filter, _ := m["Filter"].(map[string]any)

	want, hasWant := filter["Value"].(bool)
	if !hasWant {
		return true
	}

	vulns, _ := finding["Vulnerabilities"].([]any)
	for _, v := range vulns {
		vm, isMap := v.(map[string]any)
		if !isMap {
			continue
		}

		if exploit, _ := vm["ExploitAvailable"].(string); (exploit == "YES") == want {
			return true
		}
	}

	return false
}

// matchesWholeWord implements the CONTAINS_WORD string comparison
// (types.StringFilterComparisonContainsWord), which AWS documents as
// supported "only in the GetFindingsV2, GetFindingStatisticsV2,
// GetResourcesV2, and GetResourcesStatisticsV2 APIs" -- unlike CONTAINS, a
// match requires word boundaries around word within fieldVal.
func matchesWholeWord(fieldVal, word string) bool {
	if word == "" {
		return false
	}

	re, err := regexp.Compile(`\b` + regexp.QuoteMeta(word) + `\b`)
	if err != nil {
		return false
	}

	return re.MatchString(fieldVal)
}

func (b *InMemoryBackend) GetFindingsV2(
	filters map[string]any,
	sortCriteria []map[string]any,
	nextToken string,
	maxResults int,
) ([]map[string]any, string) {
	b.mu.RLock("GetFindingsV2")
	defer b.mu.RUnlock()

	var results []map[string]any

	for _, f := range b.findings {
		if matchesFindingFiltersV2(f, filters) {
			results = append(results, f)
		}
	}

	sortFindings(results, sortCriteria)

	return paginateSlice(results, nextToken, maxResults, maxDefaultResults)
}

// BatchUpdateFindingsV2 updates findings identified either by
// findingIdentifiers (types.OcsfFindingIdentifier: CloudAccountUid +
// FindingInfoUid + MetadataProductUid) or metadataUids (a finding's
// metadata.uid). This backend maps CloudAccountUid/FindingInfoUid/
// MetadataProductUid onto the AwsAccountId/Id/ProductArn of the same
// ASFF-shaped store BatchImportFindings populates -- there is no separate V2
// ingestion operation in the real API, so this is the only way
// BatchUpdateFindingsV2 can resolve a finding in this mock.
//
// metadataUids can never resolve: this backend has no OCSF ingestion path
// that would ever hand a caller a metadata.uid to reference, so every
// metadataUids entry is reported unprocessed (ResourceNotFoundException).
func (b *InMemoryBackend) BatchUpdateFindingsV2(
	findingIdentifiers []map[string]any,
	metadataUids []string,
	updates map[string]any,
) ([]map[string]any, []map[string]any) {
	b.mu.Lock("BatchUpdateFindingsV2")
	defer b.mu.Unlock()

	var processed, unprocessed []map[string]any

	for _, ident := range findingIdentifiers {
		cloudAccountUID, _ := ident["CloudAccountUid"].(string)
		findingInfoUID, _ := ident["FindingInfoUid"].(string)
		productUID, _ := ident["MetadataProductUid"].(string)

		key := findingKey(productUID, findingInfoUID)

		f, exists := b.findings[key]
		acct, _ := f[keyAwsAccountID].(string)

		if !exists || acct != cloudAccountUID {
			unprocessed = append(unprocessed, map[string]any{
				keyFindingIdentifier: ident,
				keyErrorCode:         errCodeResourceNotFound,
				keyErrorMessage:      msgFindingNotFound,
			})

			continue
		}

		maps.Copy(f, updates)
		b.findings[key] = f

		processed = append(processed, map[string]any{
			keyFindingIdentifier: ident,
			keyMetadataUID:       key,
		})
	}

	for _, uid := range metadataUids {
		unprocessed = append(unprocessed, map[string]any{
			keyMetadataUID:  uid,
			keyErrorCode:    errCodeResourceNotFound,
			keyErrorMessage: msgFindingNotFound,
		})
	}

	if processed == nil {
		processed = []map[string]any{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return processed, unprocessed
}
