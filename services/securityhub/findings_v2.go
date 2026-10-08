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
// names resolve to no value (see ocsfStringValues).
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
	"resources.type":       fieldResourceType,
	"resources.uid":        fieldResourceID,
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
// evidences.*/resources.image.*/vendor_attributes.severity_id fields) has no
// scalar top-level ASFF equivalent and is left unmapped rather than
// fabricated; cvss base_score is handled by matchesCvssBaseScore.
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
	return matchesFiltersV2(findingTarget(finding), filters)
}

// matchesFiltersV2 evaluates a composite-filter document against any filterTarget.
func matchesFiltersV2(t filterTarget, filters map[string]any) bool {
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

		matched := matchesCompositeFilter(t, cf)
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
// the target, combined by cf's Operator (AllowedOperators: AND/OR, default
// AND). Real AllowedOperators has no logical NOT combinator -- negation is
// expressed at the leaf via NOT_* comparators (e.g.
// StringFilterComparisonNotEquals), not a boolean-tree NOT node, so AND/OR
// is the complete real semantics here.
func matchesCompositeFilter(t filterTarget, cf map[string]any) bool {
	return matchesCompositeFilterDepth(t, cf, 0)
}

// matchesCompositeFilterDepth is matchesCompositeFilter's recursive worker;
// depth tracks NestedCompositeFilters nesting (see maxNestedCompositeDepth).
func matchesCompositeFilterDepth(t filterTarget, cf map[string]any, depth int) bool {
	results := make([]bool, 0, matchesCompositeFilterResultCap)

	results = append(results, stringFilterResults(t, cf)...)
	results = append(results, numberFilterResults(t, cf)...)
	results = append(results, dateFilterResults(t, cf)...)
	results = append(results, mapFilterResults(t, cf)...)
	results = append(results, ipFilterResults(t, cf)...)
	results = append(results, booleanFilterResults(t, cf)...)
	results = append(results, nestedCompositeFilterResults(t, cf, depth)...)

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
func nestedCompositeFilterResults(t filterTarget, cf map[string]any, depth int) []bool {
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
			results = append(results, matchesCompositeFilterDepth(t, m, depth+1))
		}
	}

	return results
}

// stringFilterResults evaluates cf's StringFilters against the target.
func stringFilterResults(t filterTarget, cf map[string]any) []bool {
	sf, ok := cf["StringFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range sf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesStringEntry(t, m))
		}
	}

	return results
}

// numberFilterResults evaluates cf's NumberFilters against the target.
func numberFilterResults(t filterTarget, cf map[string]any) []bool {
	nf, ok := cf["NumberFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range nf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesNumberEntry(t, m))
		}
	}

	return results
}

// dateFilterResults evaluates cf's DateFilters against the target.
func dateFilterResults(t filterTarget, cf map[string]any) []bool {
	df, ok := cf["DateFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range df {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesDateEntry(t, m))
		}
	}

	return results
}

// mapFilterResults evaluates cf's MapFilters against the target.
func mapFilterResults(t filterTarget, cf map[string]any) []bool {
	mf, ok := cf["MapFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range mf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesMapEntry(t, m))
		}
	}

	return results
}

// ipFilterResults evaluates cf's IpFilters against the target.
func ipFilterResults(t filterTarget, cf map[string]any) []bool {
	ipf, ok := cf["IpFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range ipf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesIPEntry(t, m))
		}
	}

	return results
}

// booleanFilterResults evaluates cf's BooleanFilters against the target.
func booleanFilterResults(t filterTarget, cf map[string]any) []bool {
	bf, ok := cf["BooleanFilters"].([]any)
	if !ok {
		return nil
	}

	var results []bool

	for _, item := range bf {
		if m, isMap := item.(map[string]any); isMap {
			results = append(results, matchesBoolEntry(t, m))
		}
	}

	return results
}

// matchesStringCandidates applies comp to every candidate value: include
// comparisons need any candidate to match, exclude comparisons need none to.
func matchesStringCandidates(comp string, candidates []string, val string) bool {
	if isNegativeStringComparison(comp) {
		return !slices.ContainsFunc(candidates, func(c string) bool { return !compareStringFilter(comp, c, val) })
	}

	return slices.ContainsFunc(candidates, func(c string) bool { return compareStringFilter(comp, c, val) })
}

// ocsfStringValues returns the values finding carries for an OCSF string field:
// the direct ASFF scalar from ocsfStringFieldMap, metadata.uid (the finding's
// store key, also what BatchUpdateFindingsV2 resolves), or a derived ASFF
// value; nil when the finding has none.
func ocsfStringValues(finding map[string]any, field string) []string {
	if field == "metadata.uid" {
		return []string{findingKey(asFindingString(finding[keyProductArn]), asFindingString(finding["Id"]))}
	}

	if asffField, ok := ocsfStringFieldMap[field]; ok {
		return []string{findingFieldString(finding, asffField)}
	}

	switch field {
	case "finding_info.types":
		return anyStrings(finding["Types"])
	case "finding_info.src_url":
		return nonEmptyString(asFindingString(finding["SourceUrl"]))
	case "metadata.product.name":
		return nonEmptyString(asFindingString(finding["ProductName"]))
	case "metadata.product.vendor_name":
		return nonEmptyString(asFindingString(finding["CompanyName"]))
	case "compliance.control":
		return nonEmptyString(nestedFindingString(finding, "Compliance", "SecurityControlId"))
	case "compliance.standards":
		return complianceStandards(finding)
	case "remediation.desc":
		return nonEmptyString(remediationField(finding, "Text"))
	case "remediation.references":
		return nonEmptyString(remediationField(finding, "Url"))
	case "resources.cloud_partition":
		return resourceStrings(finding, "Partition")
	default:
		return nil
	}
}

func asFindingString(v any) string {
	s, _ := v.(string)

	return s
}

func nonEmptyString(s string) []string {
	if s == "" {
		return nil
	}

	return []string{s}
}

func anyStrings(v any) []string {
	items, _ := v.([]any)

	var out []string

	for _, item := range items {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

func remediationField(finding map[string]any, field string) string {
	remediation, _ := finding["Remediation"].(map[string]any)
	rec, _ := remediation["Recommendation"].(map[string]any)

	return asFindingString(rec[field])
}

func complianceStandards(finding map[string]any) []string {
	compliance, _ := finding["Compliance"].(map[string]any)
	standards, _ := compliance["AssociatedStandards"].([]any)

	var out []string

	for _, st := range standards {
		sm, _ := st.(map[string]any)
		if id := asFindingString(sm["StandardsId"]); id != "" {
			out = append(out, id)
		}
	}

	return out
}

func resourceStrings(finding map[string]any, field string) []string {
	resources, _ := finding["Resources"].([]any)

	var out []string

	for _, r := range resources {
		rm, _ := r.(map[string]any)
		if v := asFindingString(rm[field]); v != "" {
			out = append(out, v)
		}
	}

	return out
}

// numberFilterMatches reports whether fv satisfies every Eq/Gt/Gte/Lt/Lte
// bound present in filter.
func numberFilterMatches(fv float64, filter map[string]any) bool {
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

// matchesCvssBaseScore matches when any Vulnerabilities[].Cvss[].BaseScore
// satisfies filter; a finding with no scores cannot satisfy a bound.
func matchesCvssBaseScore(finding, filter map[string]any) bool {
	vulns, _ := finding["Vulnerabilities"].([]any)
	for _, v := range vulns {
		vm, _ := v.(map[string]any)
		scores, _ := vm["Cvss"].([]any)

		for _, c := range scores {
			cm, _ := c.(map[string]any)
			if score, ok := cm["BaseScore"].(float64); ok && numberFilterMatches(score, filter) {
				return true
			}
		}
	}

	return false
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
func mapFilterCandidates(finding map[string]any, fieldName, key string) []string {
	switch fieldName {
	case "resources.tags":
		return resourceTagValues(finding, key)
	case "finding_info.tags":
		return userDefinedFieldValues(finding, key)
	case "compliance.control_parameters":
		return complianceControlParamValues(finding, key)
	default:
		return nil
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

	for key, f := range b.findings {
		if matchesFindingFiltersV2(f, filters) {
			out := maps.Clone(f)
			out["metadata"] = map[string]any{"uid": key}
			results = append(results, out)
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
// A finding's metadata.uid is its store key (ProductArn|Id), as GetFindingsV2 reports it.
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
		f, exists := b.findings[uid]
		if !exists {
			unprocessed = append(unprocessed, map[string]any{
				keyMetadataUID:  uid,
				keyErrorCode:    errCodeResourceNotFound,
				keyErrorMessage: msgFindingNotFound,
			})

			continue
		}

		maps.Copy(f, updates)
		processed = append(processed, map[string]any{keyMetadataUID: uid})
	}

	if processed == nil {
		processed = []map[string]any{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return processed, unprocessed
}
