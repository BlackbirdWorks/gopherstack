package iot

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Fleet indexing index names and configuration mode values.
const (
	indexNameThings      = "AWS_Things"
	indexNameThingGroups = "AWS_ThingGroups"

	thingIndexingModeOff            = "OFF"
	thingIndexingModeRegistry       = "REGISTRY"
	thingIndexingModeRegistryShadow = "REGISTRY_AND_SHADOW"

	thingGroupIndexingModeOff = "OFF"
	thingGroupIndexingModeOn  = "ON"

	connectivityIndexingModeOff    = "OFF"
	connectivityIndexingModeStatus = "STATUS"

	namedShadowIndexingModeOff = "OFF"
	namedShadowIndexingModeOn  = "ON"

	indexStatusActive = "ACTIVE"

	defaultSearchIndexPageSize = 50
	defaultMaxBuckets          = 10
	maxBucketsLimit            = 100

	// knownIndexCount is the number of fleet index names gopherstack knows
	// about (AWS_Things, AWS_ThingGroups); used to size ListIndices' result.
	knownIndexCount = 2

	// percentScale converts a 0-100 percent value into a 0-1 fraction for
	// percentile-rank computation.
	percentScale = 100.0

	thingsIndexSchema = `{"thingId":"STRING","thingName":"STRING","thingTypeName":"STRING",` +
		`"attributes.*":"STRING","connectivity.connected":"BOOLEAN","shadow.*":"STRING"}`
	thingGroupsIndexSchema = `{"thingGroupId":"STRING","thingGroupName":"STRING",` +
		`"attributes.*":"STRING","parentGroupNames":"STRING"}`
)

// isValidThingIndexingMode reports whether mode is a recognized ThingIndexingMode value.
func isValidThingIndexingMode(mode string) bool {
	return mode == thingIndexingModeOff || mode == thingIndexingModeRegistry || mode == thingIndexingModeRegistryShadow
}

// isValidThingGroupIndexingMode reports whether mode is a recognized ThingGroupIndexingMode value.
func isValidThingGroupIndexingMode(mode string) bool {
	return mode == thingGroupIndexingModeOff || mode == thingGroupIndexingModeOn
}

// isValidConnectivityIndexingMode reports whether mode is a recognized ThingConnectivityIndexingMode value.
func isValidConnectivityIndexingMode(mode string) bool {
	return mode == connectivityIndexingModeOff || mode == connectivityIndexingModeStatus
}

// isValidNamedShadowIndexingMode reports whether mode is a recognized NamedShadowIndexingMode value.
func isValidNamedShadowIndexingMode(mode string) bool {
	return mode == namedShadowIndexingModeOff || mode == namedShadowIndexingModeOn
}

// defaultPercentiles mirrors the AWS default percentile set used when
// GetPercentilesInput does not specify a percents list.
func defaultPercentiles() []float64 {
	return []float64{1, 5, 25, 50, 75, 90, 95, 99}
}

// defaultThingIndexingConfiguration returns the AWS default (indexing disabled).
func defaultThingIndexingConfiguration() *ThingIndexingConfiguration {
	return &ThingIndexingConfiguration{
		ThingIndexingMode:             thingIndexingModeOff,
		ThingConnectivityIndexingMode: connectivityIndexingModeOff,
		NamedShadowIndexingMode:       namedShadowIndexingModeOff,
	}
}

// defaultThingGroupIndexingConfiguration returns the AWS default (indexing disabled).
func defaultThingGroupIndexingConfiguration() *ThingGroupIndexingConfiguration {
	return &ThingGroupIndexingConfiguration{ThingGroupIndexingMode: thingGroupIndexingModeOff}
}

// cloneThingIndexingConfiguration deep-copies a ThingIndexingConfiguration,
// returning the AWS default when c is nil.
func cloneThingIndexingConfiguration(c *ThingIndexingConfiguration) *ThingIndexingConfiguration {
	if c == nil {
		return defaultThingIndexingConfiguration()
	}

	cp := *c
	if c.Filter != nil {
		f := IndexingFilter{NamedShadowNames: append([]string(nil), c.Filter.NamedShadowNames...)}
		cp.Filter = &f
	}

	cp.CustomFields = append([]IndexingField(nil), c.CustomFields...)

	return &cp
}

// cloneThingGroupIndexingConfiguration deep-copies a ThingGroupIndexingConfiguration,
// returning the AWS default when c is nil.
func cloneThingGroupIndexingConfiguration(c *ThingGroupIndexingConfiguration) *ThingGroupIndexingConfiguration {
	if c == nil {
		return defaultThingGroupIndexingConfiguration()
	}

	cp := *c
	cp.CustomFields = append([]IndexingField(nil), c.CustomFields...)

	return &cp
}

// GetIndexingConfiguration returns the current thing/thing-group indexing configuration.
func (b *InMemoryBackend) GetIndexingConfiguration() *GetIndexingConfigurationOutput {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return &GetIndexingConfigurationOutput{
		ThingIndexingConfiguration:      cloneThingIndexingConfiguration(b.thingIndexingConfig),
		ThingGroupIndexingConfiguration: cloneThingGroupIndexingConfiguration(b.thingGroupIndexingConfig),
	}
}

// UpdateIndexingConfiguration validates and stores a new fleet indexing configuration.
func (b *InMemoryBackend) UpdateIndexingConfiguration(input *UpdateIndexingConfigurationInput) error {
	if input == nil {
		return fmt.Errorf("%w: request body is required", ErrValidation)
	}

	if err := validateThingIndexingConfiguration(input.ThingIndexingConfiguration); err != nil {
		return err
	}

	if err := validateThingGroupIndexingConfiguration(input.ThingGroupIndexingConfiguration); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if input.ThingIndexingConfiguration != nil {
		b.thingIndexingConfig = cloneThingIndexingConfiguration(input.ThingIndexingConfiguration)
	}

	if input.ThingGroupIndexingConfiguration != nil {
		b.thingGroupIndexingConfig = cloneThingGroupIndexingConfiguration(input.ThingGroupIndexingConfiguration)
	}

	return nil
}

func validateThingIndexingConfiguration(tic *ThingIndexingConfiguration) error {
	if tic == nil {
		return nil
	}

	if tic.ThingIndexingMode != "" && !isValidThingIndexingMode(tic.ThingIndexingMode) {
		return fmt.Errorf("%w: invalid thingIndexingMode %q", ErrValidation, tic.ThingIndexingMode)
	}

	if tic.ThingConnectivityIndexingMode != "" && !isValidConnectivityIndexingMode(tic.ThingConnectivityIndexingMode) {
		return fmt.Errorf(
			"%w: invalid thingConnectivityIndexingMode %q",
			ErrValidation, tic.ThingConnectivityIndexingMode,
		)
	}

	if tic.NamedShadowIndexingMode != "" && !isValidNamedShadowIndexingMode(tic.NamedShadowIndexingMode) {
		return fmt.Errorf("%w: invalid namedShadowIndexingMode %q", ErrValidation, tic.NamedShadowIndexingMode)
	}

	return nil
}

func validateThingGroupIndexingConfiguration(tgic *ThingGroupIndexingConfiguration) error {
	if tgic == nil {
		return nil
	}

	if tgic.ThingGroupIndexingMode != "" && !isValidThingGroupIndexingMode(tgic.ThingGroupIndexingMode) {
		return fmt.Errorf("%w: invalid thingGroupIndexingMode %q", ErrValidation, tgic.ThingGroupIndexingMode)
	}

	return nil
}

// ListIndices returns the names of the fleet indices currently active, derived
// from the stored indexing configuration.
func (b *InMemoryBackend) ListIndices() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, knownIndexCount)

	if tic := b.thingIndexingConfig; tic != nil && tic.ThingIndexingMode != thingIndexingModeOff {
		names = append(names, indexNameThings)
	}

	if tgic := b.thingGroupIndexingConfig; tgic != nil && tgic.ThingGroupIndexingMode != thingGroupIndexingModeOff {
		names = append(names, indexNameThingGroups)
	}

	return names
}

// DescribeIndex returns the status and schema of a known fleet index.
func (b *InMemoryBackend) DescribeIndex(indexName string) (*IndexDescription, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	switch indexName {
	case indexNameThings:
		return &IndexDescription{
			IndexName: indexNameThings, IndexStatus: indexStatusActive, Schema: thingsIndexSchema,
		}, nil
	case indexNameThingGroups:
		return &IndexDescription{
			IndexName: indexNameThingGroups, IndexStatus: indexStatusActive, Schema: thingGroupsIndexSchema,
		}, nil
	default:
		return nil, fmt.Errorf("%w: %s", ErrIndexNotFound, indexName)
	}
}

// SearchIndex runs QueryString against the fleet index named by IndexName
// (default AWS_Things) and returns matching Thing or ThingGroup documents.
func (b *InMemoryBackend) SearchIndex(input *SearchIndexInput) (*SearchIndexOutput, error) {
	if input == nil {
		input = &SearchIndexInput{}
	}

	indexName := input.IndexName
	if indexName == "" {
		indexName = indexNameThings
	}

	switch indexName {
	case indexNameThings:
		return b.searchThingsIndex(input), nil
	case indexNameThingGroups:
		return b.searchThingGroupsIndex(input), nil
	default:
		return nil, fmt.Errorf("%w: unknown index %q", ErrValidation, indexName)
	}
}

func (b *InMemoryBackend) searchThingsIndex(input *SearchIndexInput) *SearchIndexOutput {
	b.mu.RLock()
	defer b.mu.RUnlock()

	matched := make([]*SearchIndexThingResult, 0, b.things.Len())

	for _, v := range b.things.Snapshot() {
		t := v
		groups := append([]string(nil), b.thingThingGroups[t.ThingName]...)

		if !matchesThingQuery(t, groups, input.QueryString) {
			continue
		}

		matched = append(matched, &SearchIndexThingResult{
			ThingName:       t.ThingName,
			ThingID:         t.ThingID,
			ThingTypeName:   t.ThingTypeName,
			Attributes:      copyStringMap(t.Attributes),
			ThingGroupNames: groups,
		})
	}

	page, nextToken := paginateMaps(matched, searchPageSize(input.MaxResults), searchStartOffset(input.NextToken))

	return &SearchIndexOutput{Things: page, NextToken: nextToken}
}

func (b *InMemoryBackend) searchThingGroupsIndex(input *SearchIndexInput) *SearchIndexOutput {
	b.mu.RLock()
	defer b.mu.RUnlock()

	matched := make([]*SearchIndexThingGroupResult, 0, b.thingGroups.Len())

	for _, v := range b.thingGroups.Snapshot() {
		g := v
		if !matchesThingGroupQuery(g, input.QueryString) {
			continue
		}

		matched = append(matched, &SearchIndexThingGroupResult{
			ThingGroupName:  g.ThingGroupName,
			ThingGroupID:    g.ThingGroupID,
			Attributes:      copyStringMap(g.Attributes),
			ParentGroupName: g.ParentGroupName,
		})
	}

	page, nextToken := paginateMaps(matched, searchPageSize(input.MaxResults), searchStartOffset(input.NextToken))

	return &SearchIndexOutput{ThingGroups: page, NextToken: nextToken}
}

// searchPageSize returns the effective page size for a SearchIndex/aggregation
// request, defaulting when maxResults is unset.
func searchPageSize(maxResults int32) int {
	if maxResults > 0 {
		return int(maxResults)
	}

	return defaultSearchIndexPageSize
}

// searchStartOffset decodes an opaque offset-based nextToken, defaulting to 0
// when the token is empty or invalid.
func searchStartOffset(nextToken string) int {
	if nextToken == "" {
		return 0
	}

	n, err := strconv.Atoi(nextToken)
	if err != nil || n < 0 {
		return 0
	}

	return n
}

// matchesThingQuery reports whether a Thing (with its resolved group names)
// satisfies every whitespace-separated term of queryString. An empty
// queryString matches every Thing.
func matchesThingQuery(t *Thing, groupNames []string, queryString string) bool {
	terms := strings.Fields(queryString)
	if len(terms) == 0 {
		return true
	}

	for _, term := range terms {
		if !matchThingTerm(t, groupNames, term) {
			return false
		}
	}

	return true
}

func matchThingTerm(t *Thing, groupNames []string, term string) bool {
	key, value, hasKey := strings.Cut(term, ":")
	if !hasKey {
		return thingContainsSubstring(t, groupNames, term)
	}

	switch {
	case key == keyThingName:
		return strings.EqualFold(t.ThingName, value)
	case key == "thingTypeName":
		return strings.EqualFold(t.ThingTypeName, value)
	case key == "thingGroupNames":
		return containsFold(groupNames, value)
	case strings.HasPrefix(key, "attributes."):
		attrName := strings.TrimPrefix(key, "attributes.")
		av, ok := t.Attributes[attrName]

		return ok && strings.EqualFold(av, value)
	default:
		// Unknown key prefix: fall back to a substring match on the raw term.
		return thingContainsSubstring(t, groupNames, term)
	}
}

func thingContainsSubstring(t *Thing, groupNames []string, needle string) bool {
	needle = strings.ToLower(needle)

	if strings.Contains(strings.ToLower(t.ThingName), needle) ||
		strings.Contains(strings.ToLower(t.ThingTypeName), needle) {
		return true
	}

	for _, v := range t.Attributes {
		if strings.Contains(strings.ToLower(v), needle) {
			return true
		}
	}

	for _, g := range groupNames {
		if strings.Contains(strings.ToLower(g), needle) {
			return true
		}
	}

	return false
}

// matchesThingGroupQuery reports whether a ThingGroup satisfies every
// whitespace-separated term of queryString. An empty queryString matches
// every ThingGroup.
func matchesThingGroupQuery(g *ThingGroup, queryString string) bool {
	terms := strings.Fields(queryString)
	if len(terms) == 0 {
		return true
	}

	for _, term := range terms {
		if !matchThingGroupTerm(g, term) {
			return false
		}
	}

	return true
}

func matchThingGroupTerm(g *ThingGroup, term string) bool {
	key, value, hasKey := strings.Cut(term, ":")
	if !hasKey {
		return thingGroupContainsSubstring(g, term)
	}

	switch {
	case key == "thingGroupName":
		return strings.EqualFold(g.ThingGroupName, value)
	case key == "parentGroupNames":
		return strings.EqualFold(g.ParentGroupName, value)
	case strings.HasPrefix(key, "attributes."):
		attrName := strings.TrimPrefix(key, "attributes.")
		av, ok := g.Attributes[attrName]

		return ok && strings.EqualFold(av, value)
	default:
		return thingGroupContainsSubstring(g, term)
	}
}

func thingGroupContainsSubstring(g *ThingGroup, needle string) bool {
	needle = strings.ToLower(needle)

	if strings.Contains(strings.ToLower(g.ThingGroupName), needle) ||
		strings.Contains(strings.ToLower(g.ParentGroupName), needle) {
		return true
	}

	for _, v := range g.Attributes {
		if strings.Contains(strings.ToLower(v), needle) {
			return true
		}
	}

	return false
}

func containsFold(list []string, v string) bool {
	for _, item := range list {
		if strings.EqualFold(item, v) {
			return true
		}
	}

	return false
}

// matchedThings returns the Things (and their resolved group names) that
// satisfy queryString. Callers must hold at least a read lock.
func (b *InMemoryBackend) matchedThings(queryString string) []*Thing {
	out := make([]*Thing, 0, b.things.Len())

	for _, v := range b.things.Snapshot() {
		t := v
		if matchesThingQuery(t, b.thingThingGroups[t.ThingName], queryString) {
			out = append(out, t)
		}
	}

	return out
}

// aggregationFieldValue returns the raw string value of an aggregation field
// for a Thing. Fields may be given as "attributes.<name>", a top-level Thing
// property, or a bare attribute name.
func aggregationFieldValue(t *Thing, field string) (string, bool) {
	switch {
	case field == "thingTypeName":
		return t.ThingTypeName, t.ThingTypeName != ""
	case field == keyThingName:
		return t.ThingName, true
	case field == keyVersion:
		return strconv.FormatInt(t.Version, 10), true
	case strings.HasPrefix(field, "attributes."):
		v, ok := t.Attributes[strings.TrimPrefix(field, "attributes.")]

		return v, ok
	default:
		v, ok := t.Attributes[field]

		return v, ok
	}
}

// numericFieldValues extracts the parseable float64 values of field across things.
func numericFieldValues(things []*Thing, field string) []float64 {
	values := make([]float64, 0, len(things))

	for _, t := range things {
		raw, ok := aggregationFieldValue(t, field)
		if !ok {
			continue
		}

		if f, err := strconv.ParseFloat(raw, 64); err == nil {
			values = append(values, f)
		}
	}

	return values
}

// GetCardinality returns the number of distinct values of AggregationField
// among Things matching QueryString.
func (b *InMemoryBackend) GetCardinality(input *AggregationInput) (int64, error) {
	if input == nil || input.AggregationField == "" {
		return 0, fmt.Errorf("%w: aggregationField is required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	seen := make(map[string]struct{})

	for _, t := range b.matchedThings(input.QueryString) {
		if v, ok := aggregationFieldValue(t, input.AggregationField); ok {
			seen[v] = struct{}{}
		}
	}

	return int64(len(seen)), nil
}

// GetStatistics computes count/min/max/sum/average/stdDeviation of
// AggregationField (interpreted numerically) among Things matching QueryString.
func (b *InMemoryBackend) GetStatistics(input *AggregationInput) (*Statistics, error) {
	if input == nil || input.AggregationField == "" {
		return nil, fmt.Errorf("%w: aggregationField is required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	return computeStatistics(numericFieldValues(b.matchedThings(input.QueryString), input.AggregationField)), nil
}

func computeStatistics(values []float64) *Statistics {
	stats := &Statistics{}
	if len(values) == 0 {
		return stats
	}

	stats.Count = int64(len(values))
	stats.Minimum = values[0]
	stats.Maximum = values[0]

	var sum float64

	for _, v := range values {
		sum += v

		if v < stats.Minimum {
			stats.Minimum = v
		}

		if v > stats.Maximum {
			stats.Maximum = v
		}
	}

	stats.Sum = sum
	stats.Average = sum / float64(len(values))

	var sqDiff float64
	for _, v := range values {
		d := v - stats.Average
		sqDiff += d * d
	}

	stats.Variance = sqDiff / float64(len(values))
	stats.StdDeviation = math.Sqrt(stats.Variance)

	return stats
}

// GetPercentiles computes percentile values of AggregationField (interpreted
// numerically) among Things matching QueryString, using the nearest-rank
// (linear interpolation) method.
func (b *InMemoryBackend) GetPercentiles(input *PercentilesInput) ([]PercentileValue, error) {
	if input == nil || input.AggregationField == "" {
		return nil, fmt.Errorf("%w: aggregationField is required", ErrValidation)
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	values := numericFieldValues(b.matchedThings(input.QueryString), input.AggregationField)
	sort.Float64s(values)

	percents := input.Percents
	if len(percents) == 0 {
		percents = defaultPercentiles()
	}

	out := make([]PercentileValue, 0, len(percents))
	for _, p := range percents {
		out = append(out, PercentileValue{Percent: p, Value: percentileValue(values, p)})
	}

	return out, nil
}

func percentileValue(sorted []float64, percent float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	rank := (percent / percentScale) * float64(len(sorted)-1)
	lower := int(math.Floor(rank))
	upper := int(math.Ceil(rank))

	if lower == upper {
		return sorted[lower]
	}

	frac := rank - float64(lower)

	return sorted[lower] + frac*(sorted[upper]-sorted[lower])
}

// GetBucketsAggregation computes term buckets (distinct value -> match count)
// of AggregationField among Things matching QueryString, sorted by count
// descending then key ascending, limited to MaxBuckets.
func (b *InMemoryBackend) GetBucketsAggregation(input *BucketsAggregationInput) ([]Bucket, error) {
	if input == nil || input.AggregationField == "" {
		return nil, fmt.Errorf("%w: aggregationField is required", ErrValidation)
	}

	maxBuckets := input.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = defaultMaxBuckets
	}

	if maxBuckets > maxBucketsLimit {
		maxBuckets = maxBucketsLimit
	}

	b.mu.RLock()
	defer b.mu.RUnlock()

	counts := make(map[string]int64)

	for _, t := range b.matchedThings(input.QueryString) {
		if v, ok := aggregationFieldValue(t, input.AggregationField); ok {
			counts[v]++
		}
	}

	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}

		return keys[i] < keys[j]
	})

	if len(keys) > int(maxBuckets) {
		keys = keys[:maxBuckets]
	}

	out := make([]Bucket, 0, len(keys))
	for _, k := range keys {
		out = append(out, Bucket{KeyValue: k, Count: counts[k]})
	}

	return out, nil
}
