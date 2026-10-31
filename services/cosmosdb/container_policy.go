package cosmosdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"
)

const (
	maxUniqueKeys       = 10
	maxUniqueKeyPaths   = 16
	maxComputedProps    = 20
	geoTypeGeography    = "Geography"
	geoTypeGeometry     = "Geometry"
	defaultUniqueKeyPol = `{"uniqueKeys":[]}`
	defaultGeoConfig    = `{"type":"Geography"}`
)

// ContainerPolicies are the optional container settings that are stored and echoed verbatim; the unique-key
// policy is enforced on writes and computed properties are evaluated by queries.
type ContainerPolicies struct {
	UniqueKeyPolicy       json.RawMessage `json:"uniqueKeyPolicy"`
	GeospatialConfig      json.RawMessage `json:"geospatialConfig"`
	VectorEmbeddingPolicy json.RawMessage `json:"vectorEmbeddingPolicy"`
	FullTextPolicy        json.RawMessage `json:"fullTextPolicy"`
	ComputedProperties    json.RawMessage `json:"computedProperties"`
}

type uniqueKeyPolicy struct {
	UniqueKeys []struct {
		Paths []string `json:"paths"`
	} `json:"uniqueKeys"`
}

type computedProperty struct {
	Name  string `json:"name"`
	Query string `json:"query"`
}

func isJSONNull(raw json.RawMessage) bool {
	t := bytes.TrimSpace(raw)

	return len(t) == 0 || bytes.Equal(t, []byte(nullText))
}

func (p ContainerPolicies) stored() ContainerPolicies {
	for _, f := range []*json.RawMessage{
		&p.UniqueKeyPolicy, &p.GeospatialConfig, &p.VectorEmbeddingPolicy, &p.FullTextPolicy, &p.ComputedProperties,
	} {
		if isJSONNull(*f) {
			*f = nil
		}
	}

	return p
}

// validate rejects malformed policy settings; errors are BadRequest to the client.
func (p ContainerPolicies) validate() error {
	if err := validateUniqueKeyPolicy(p.UniqueKeyPolicy); err != nil {
		return err
	}

	if err := validateGeospatialConfig(p.GeospatialConfig); err != nil {
		return err
	}

	if _, err := parseComputedProperties(p.ComputedProperties); err != nil {
		return err
	}

	if err := validateVectorPolicy(p.VectorEmbeddingPolicy); err != nil {
		return err
	}

	return validateFullTextPolicy(p.FullTextPolicy)
}

// validateSearchIndexes checks the indexing policy's vector and full-text indexes against the policies they
// refer to.
func validateSearchIndexes(indexing json.RawMessage, p ContainerPolicies) error {
	if len(indexing) == 0 {
		return nil
	}

	var pol indexingPolicy
	if err := json.Unmarshal(indexing, &pol); err != nil {
		return fmt.Errorf("%w: indexingPolicy is malformed", ErrInvalidContainerPolicy)
	}

	if strings.EqualFold(pol.IndexingMode, indexModeLazy) {
		return fmt.Errorf("%w: %s", ErrInvalidContainerPolicy, msgLazyIndexing)
	}

	if spErr := validateSpatialIndexes(pol.SpatialIndexes, geospatialType(p.GeospatialConfig)); spErr != nil {
		return spErr
	}

	if cpErr := checkComputedIndexes(pol, p.ComputedProperties); cpErr != nil {
		return cpErr
	}

	vp, err := parseVectorPolicy(p.VectorEmbeddingPolicy)
	if err != nil {
		return err
	}

	if err = validateVectorIndexes(pol, vp); err != nil {
		return err
	}

	fp, err := parseFullTextPolicy(p.FullTextPolicy)
	if err != nil {
		return err
	}

	return validateFullTextIndexes(pol, fp)
}

// checkReplaceIndexes validates a replace request's indexing policy against the policies that will be in force.
func (c *storedContainer) checkReplaceIndexes(spec ContainerSpec) error {
	eff := spec.ContainerPolicies.stored()
	if eff.VectorEmbeddingPolicy == nil {
		eff.VectorEmbeddingPolicy = c.VectorEmbeddingPolicy
	}

	if eff.FullTextPolicy == nil {
		eff.FullTextPolicy = c.FullTextPolicy
	}

	if eff.GeospatialConfig == nil {
		eff.GeospatialConfig = c.GeospatialConfig
	}

	if eff.ComputedProperties == nil {
		eff.ComputedProperties = c.ComputedProperties
	}

	indexing := spec.IndexingPolicy
	if len(indexing) == 0 {
		indexing = c.IndexingPolicy
	}

	return validateSearchIndexes(indexing, eff)
}

// sameJSON reports whether two raw JSON values are semantically equal.
func sameJSON(a, b json.RawMessage) bool {
	var va, vb any

	return json.Unmarshal(a, &va) == nil && json.Unmarshal(b, &vb) == nil && reflect.DeepEqual(va, vb)
}

func parseUniqueKeyPolicy(raw json.RawMessage) (uniqueKeyPolicy, error) {
	var pol uniqueKeyPolicy
	if isJSONNull(raw) {
		return pol, nil
	}

	if err := json.Unmarshal(raw, &pol); err != nil {
		return pol, fmt.Errorf("%w: uniqueKeyPolicy is malformed", ErrInvalidContainerPolicy)
	}

	return pol, nil
}

func validateUniqueKeyPolicy(raw json.RawMessage) error {
	pol, err := parseUniqueKeyPolicy(raw)
	if err != nil {
		return err
	}

	if len(pol.UniqueKeys) > maxUniqueKeys {
		return fmt.Errorf("%w: at most %d unique keys are allowed", ErrInvalidContainerPolicy, maxUniqueKeys)
	}

	for _, uk := range pol.UniqueKeys {
		if len(uk.Paths) == 0 || len(uk.Paths) > maxUniqueKeyPaths {
			return fmt.Errorf("%w: a unique key needs 1 to %d paths", ErrInvalidContainerPolicy, maxUniqueKeyPaths)
		}

		for _, p := range uk.Paths {
			if !strings.HasPrefix(p, "/") || len(p) == 1 {
				return fmt.Errorf("%w: unique key path %q must start with '/'", ErrInvalidContainerPolicy, p)
			}
		}
	}

	return nil
}

func validateGeospatialConfig(raw json.RawMessage) error {
	if isJSONNull(raw) {
		return nil
	}

	var cfg struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(raw, &cfg); err != nil || (cfg.Type != geoTypeGeography && cfg.Type != geoTypeGeometry) {
		return fmt.Errorf("%w: geospatialConfig.type must be %s or %s",
			ErrInvalidContainerPolicy, geoTypeGeography, geoTypeGeometry)
	}

	return nil
}

// parseComputedProperties decodes and validates computedProperties: each query must be
// SELECT VALUE <expr> FROM <alias> with nothing else.
func parseComputedProperties(raw json.RawMessage) ([]computedProperty, error) {
	if isJSONNull(raw) {
		return nil, nil
	}

	var props []computedProperty
	if err := json.Unmarshal(raw, &props); err != nil {
		return nil, fmt.Errorf("%w: computedProperties is malformed", ErrInvalidContainerPolicy)
	}

	if len(props) > maxComputedProps {
		return nil, fmt.Errorf(
			"%w: at most %d computed properties are allowed",
			ErrInvalidContainerPolicy,
			maxComputedProps,
		)
	}

	seen := make(map[string]struct{}, len(props))

	for _, p := range props {
		if err := validateComputedProperty(p, seen); err != nil {
			return nil, err
		}
	}

	return props, nil
}

func validateComputedProperty(p computedProperty, seen map[string]struct{}) error {
	if p.Name == "" || p.Name == "id" || strings.HasPrefix(p.Name, "_") {
		return fmt.Errorf("%w: invalid computed property name %q", ErrInvalidContainerPolicy, p.Name)
	}

	if _, dup := seen[p.Name]; dup {
		return fmt.Errorf("%w: duplicate computed property %q", ErrInvalidContainerPolicy, p.Name)
	}

	seen[p.Name] = struct{}{}

	stmt, err := parseQuery(p.Query)
	if err != nil || !isBareSelectValue(stmt) {
		return fmt.Errorf("%w: computed property %q must be SELECT VALUE <expression> FROM <alias>",
			ErrInvalidContainerPolicy, p.Name)
	}

	if why := forbiddenComputedUse(stmt.items[0].expr); why != "" {
		return fmt.Errorf("%w: computed property %q cannot use %s", ErrInvalidContainerPolicy, p.Name, why)
	}

	return nil
}

// isBareSelectValue reports whether stmt is SELECT VALUE <expr> FROM <alias> with no other clause.
func isBareSelectValue(stmt *sqlSelectStmt) bool {
	clauses := stmt.where != nil || stmt.top != nil || stmt.offset != nil || stmt.limit != nil
	shaped := stmt.distinct || stmt.hasAgg || len(stmt.groupBy) > 0 || len(stmt.orderBy) > 0

	return stmt.selectValue && !clauses && !shaped && len(stmt.sources) == 1
}

// policiesEqualUnique reports whether two raw unique-key policies are semantically identical.
func policiesEqualUnique(a, b json.RawMessage) bool {
	pa, errA := parseUniqueKeyPolicy(a)
	pb, errB := parseUniqueKeyPolicy(b)

	if errA != nil || errB != nil || len(pa.UniqueKeys) != len(pb.UniqueKeys) {
		return false
	}

	for i := range pa.UniqueKeys {
		if strings.Join(pa.UniqueKeys[i].Paths, "\x00") != strings.Join(pb.UniqueKeys[i].Paths, "\x00") {
			return false
		}
	}

	return true
}

// applyPolicies folds a replace request's policies into the stored ones: omitted settings are kept, the unique
// key policy is immutable.
func (c *storedContainer) applyPolicies(p ContainerPolicies, replacing bool) error {
	p = p.stored()

	if replacing && p.UniqueKeyPolicy != nil && !policiesEqualUnique(c.UniqueKeyPolicy, p.UniqueKeyPolicy) {
		return ErrUniqueKeyPolicyImmutable
	}

	if replacing && (policyChanged(c.VectorEmbeddingPolicy, p.VectorEmbeddingPolicy) ||
		policyChanged(c.FullTextPolicy, p.FullTextPolicy)) {
		return fmt.Errorf("%w: vectorEmbeddingPolicy and fullTextPolicy cannot be modified after creation",
			ErrInvalidContainerPolicy)
	}

	if !replacing || p.UniqueKeyPolicy != nil {
		c.UniqueKeyPolicy = p.UniqueKeyPolicy
	}

	for _, f := range []struct{ dst, src *json.RawMessage }{
		{&c.GeospatialConfig, &p.GeospatialConfig},
		{&c.VectorEmbeddingPolicy, &p.VectorEmbeddingPolicy},
		{&c.FullTextPolicy, &p.FullTextPolicy},
		{&c.ComputedProperties, &p.ComputedProperties},
	} {
		if !replacing || *f.src != nil {
			*f.dst = *f.src
		}
	}

	return nil
}

// policyChanged reports whether a replace request carries a setting different from the stored one.
func policyChanged(stored, requested json.RawMessage) bool {
	return requested != nil && !sameJSON(stored, requested)
}

func (c *storedContainer) policies() ContainerPolicies {
	return ContainerPolicies{
		UniqueKeyPolicy: c.UniqueKeyPolicy, GeospatialConfig: c.GeospatialConfig,
		VectorEmbeddingPolicy: c.VectorEmbeddingPolicy, FullTextPolicy: c.FullTextPolicy,
		ComputedProperties: c.ComputedProperties,
	}
}

// touch stamps the container's modification time, advancing it past the previous stamp so its ETag changes.
func (c *storedContainer) touch(now time.Time) {
	c.Modified = bumpTimestampFrom(now, c.Modified, !c.Modified.IsZero())
}

// uniqueTuple is the canonical value tuple of body at the given unique key paths; an absent path is its own value.
func uniqueTuple(body map[string]any, paths []string) string {
	var sb strings.Builder

	for _, p := range paths {
		v, ok := extractPartitionKeyValue(body, p)
		if !ok {
			sb.WriteString("\x00u")
		} else {
			sb.WriteString(canonicalJSON(v))
		}

		sb.WriteByte(groupKeySeparator)
	}

	return sb.String()
}

// checkUnique returns ErrUniqueKeyViolation when doc would share a unique key value with another live document
// in its logical partition.
func (tx *containerTx) checkUnique(key documentCompositeKey, doc *storedDocument) error {
	pol, err := parseUniqueKeyPolicy(tx.c.UniqueKeyPolicy)
	if err != nil {
		return err
	}

	for _, uk := range pol.UniqueKeys {
		want := uniqueTuple(doc.Body, uk.Paths)

		for k, other := range tx.c.Documents {
			if k == key || k.PartitionKeyJSON != key.PartitionKeyJSON || !tx.c.alive(other, tx.now) {
				continue
			}

			if uniqueTuple(other.Body, uk.Paths) == want {
				return ErrUniqueKeyViolation
			}
		}
	}

	return nil
}

// computedExprs maps each computed property's name to its parsed expression.
func (b *InMemoryBackend) computedExprs(c *storedContainer) map[string]sqlExpr {
	props, err := parseComputedProperties(c.ComputedProperties)
	if err != nil || len(props) == 0 {
		return nil
	}

	out := make(map[string]sqlExpr, len(props))

	for _, p := range props {
		if stmt, parseErr := b.parseCached(p.Query); parseErr == nil && len(stmt.items) == 1 {
			out[p.Name] = stmt.items[0].expr
		}
	}

	return out
}
