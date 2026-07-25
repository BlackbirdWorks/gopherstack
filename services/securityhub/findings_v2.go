package securityhub

import (
	"maps"
	"regexp"
	"slices"
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
	"compliance.status":    "ComplianceStatus",
	"status":               "WorkflowStatus",
	"severity":             "SeverityLabel",
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
// OCSF integer IDs.
var ocsfNumberFieldMap = map[string]string{ //nolint:gochecknoglobals // read-only lookup data
	"severity_id": "SeverityId",
	"status_id":   "StatusId",
}

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

// matchesCompositeFilter evaluates one CompositeFilter's String/Number
// sub-filters against finding, combined by cf's Operator (AND/OR, default
// AND). DateFilters, MapFilters, IpFilters, BooleanFilters, and
// NestedCompositeFilters have no reliable ASFF-backed equivalent in this
// mock and are intentionally not evaluated (documented gap -- see
// services/securityhub/PARITY.md).
func matchesCompositeFilter(finding, cf map[string]any) bool {
	var results []bool

	if sf, ok := cf["StringFilters"].([]any); ok {
		for _, item := range sf {
			if m, isMap := item.(map[string]any); isMap {
				results = append(results, matchesOcsfStringFilter(finding, m))
			}
		}
	}

	if nf, ok := cf["NumberFilters"].([]any); ok {
		for _, item := range nf {
			if m, isMap := item.(map[string]any); isMap {
				results = append(results, matchesOcsfNumberFilter(finding, m))
			}
		}
	}

	if len(results) == 0 {
		return true
	}

	op, _ := cf["Operator"].(string)
	if op == "OR" {
		return slices.Contains(results, true)
	}

	return !slices.Contains(results, false)
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
	fieldVal, _ := finding[asffField].(string)

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
