package azuresearch

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

func badf(format string, args ...any) error {
	return wrapf(ErrBadRequest, "%s", fmt.Sprintf(format, args...))
}

func boolOf(m map[string]any, key string, def bool) bool {
	if v, ok := m[key].(bool); ok {
		return v
	}

	return def
}

func strOf(m map[string]any, key string) string {
	s, _ := m[key].(string)

	return s
}

func listOf(m map[string]any, key string) []any {
	l, _ := m[key].([]any)

	return l
}

func objOf(v any) map[string]any {
	m, _ := v.(map[string]any)

	return m
}

type compiler struct {
	comp  *compiled
	count int
}

// compileIndex validates def, fills attribute defaults in place and builds the runtime form.
func compileIndex(def map[string]any) (*compiled, error) {
	name := strOf(def, "name")
	if name == "" || len(name) > maxIndexName || !indexNameRE.MatchString(name) || strings.HasSuffix(name, "-") {
		return nil, badf("The index name '%s' is invalid. Index names must start with a lowercase letter or digit, "+
			"contain only lowercase letters, digits or dashes, and be at most 128 characters.", name)
	}

	fields := listOf(def, "fields")
	if len(fields) == 0 {
		return nil, badf("The request is invalid. Details: index : An index must have at least one field.")
	}

	c := &compiler{comp: &compiled{
		byPath: map[string]*fieldDef{}, suggesters: map[string]*suggester{}, profiles: map[string]*scoringProfile{},
		vecProfile: map[string]vectorProfile{}, analyzers: map[string]Analyzer{},
	}}

	if err := c.analyzers(def); err != nil {
		return nil, err
	}

	if err := c.vectorSearch(def); err != nil {
		return nil, err
	}

	for _, raw := range fields {
		fd, err := c.field(objOf(raw), "", 0)
		if err != nil {
			return nil, err
		}

		c.comp.fields = append(c.comp.fields, fd)
	}

	if err := c.finishKey(); err != nil {
		return nil, err
	}

	if err := c.suggesters(def); err != nil {
		return nil, err
	}

	if err := c.scoring(def); err != nil {
		return nil, err
	}

	return c.comp, nil
}

func (c *compiler) finishKey() error {
	for _, f := range c.comp.fields {
		if !f.Key {
			continue
		}

		if c.comp.key != nil {
			return badf("The request is invalid. Details: fields : Please specify exactly one key field.")
		}

		if f.Type != typeString {
			return badf("The request is invalid. Details: The key field '%s' must be of type Edm.String.", f.Name)
		}

		c.comp.key = f
	}

	if c.comp.key == nil {
		return badf("The request is invalid. Details: fields : Please specify exactly one key field.")
	}

	return nil
}

func (c *compiler) analyzers(def map[string]any) error {
	defs, err := buildAnalysisDefs(def)
	if err != nil {
		return err
	}

	c.comp.defs = defs

	for _, raw := range listOf(def, "analyzers") {
		a := objOf(raw)

		fn, aerr := customAnalyzer(a, defs)
		if aerr != nil {
			return aerr
		}

		c.comp.analyzers[strOf(a, "name")] = fn
	}

	return nil
}

func (c *compiler) analyzer(name string) (Analyzer, error) {
	if fn, ok := c.comp.analyzers[name]; ok {
		return fn, nil
	}

	if fn, ok := builtinAnalyzer(name); ok {
		return fn, nil
	}

	return nil, badf("The request is invalid. Details: The analyzer '%s' was not found.", name)
}

// checkHNSWParams enforces the HnswParameters ranges of the 2025-09-01 searchservice.json.
func checkHNSWParams(p map[string]any) error {
	for _, b := range []struct {
		name     string
		min, max float64
	}{{"m", 4, 10}, {"efConstruction", 100, 1000}, {"efSearch", 100, 1000}} {
		v, set := p[b.name]
		if !set || v == nil {
			continue
		}

		n, ok := v.(float64)
		if !ok || n != math.Trunc(n) || n < b.min || n > b.max {
			return badf("The request is invalid. Details: hnswParameters.%s must be an integer between %d and %d.",
				b.name, int(b.min), int(b.max))
		}
	}

	return nil
}

func (c *compiler) vectorSearch(def map[string]any) error {
	vs := objOf(def["vectorSearch"])
	if vs == nil {
		return nil
	}

	algs := map[string]vectorProfile{}

	for _, raw := range listOf(vs, "algorithms") {
		a := objOf(raw)
		kind := strOf(a, "kind")

		if kind != "hnsw" && kind != "exhaustiveKnn" {
			return badf("The request is invalid. Details: unsupported vector search algorithm kind '%s'.", kind)
		}

		if err := checkHNSWParams(objOf(a["hnswParameters"])); err != nil {
			return err
		}

		metric := strOf(objOf(a[kind+"Parameters"]), "metric")
		if metric == "" {
			metric = "cosine"
		}

		if !slices.Contains([]string{"cosine", "euclidean", "dotProduct", "hamming"}, metric) {
			return badf("The request is invalid. Details: unsupported vector similarity metric '%s'.", metric)
		}

		algs[strOf(a, "name")] = vectorProfile{Metric: metric, Kind: kind, Algorithm: strOf(a, "name")}
	}

	for _, raw := range listOf(vs, "profiles") {
		p := objOf(raw)

		alg, ok := algs[strOf(p, "algorithm")]
		if !ok {
			return badf(
				"The request is invalid. Details: vector search profile '%s' references unknown algorithm '%s'.",
				strOf(p, "name"),
				strOf(p, "algorithm"),
			)
		}

		c.comp.vecProfile[strOf(p, "name")] = alg
	}

	return nil
}

func (c *compiler) field(f map[string]any, parent string, depth int) (*fieldDef, error) {
	name := strOf(f, "name")
	if !fieldNameRE.MatchString(name) || len(name) > maxIndexName {
		return nil, badf("The request is invalid. Details: The field name '%s' is invalid.", name)
	}

	if depth >= maxNesting {
		return nil, badf("The request is invalid. Details: fields are nested too deeply.")
	}

	if c.count++; c.count > maxFields {
		return nil, badf("The request is invalid. Details: an index may have at most %d fields.", maxFields)
	}

	typ := strOf(f, "type")

	base, coll := splitType(typ)
	if !validBaseType(base) {
		return nil, badf("The request is invalid. Details: The field '%s' has unsupported type '%s'.", name, typ)
	}

	fd := &fieldDef{Name: name, Path: parent + name, Type: typ, Base: base, Collection: coll}
	if _, dup := c.comp.byPath[fd.Path]; dup {
		return nil, badf("The request is invalid. Details: duplicate field name '%s'.", fd.Path)
	}

	c.comp.byPath[fd.Path] = fd

	if err := c.attributes(fd, f); err != nil {
		return nil, err
	}

	if base == typeComplex {
		return fd, c.children(fd, f, depth)
	}

	return fd, nil
}

func (c *compiler) children(fd *fieldDef, f map[string]any, depth int) error {
	kids := listOf(f, "fields")
	if len(kids) == 0 {
		return badf("The request is invalid. Details: complex field '%s' must have sub-fields.", fd.Name)
	}

	for _, raw := range kids {
		child, err := c.field(objOf(raw), fd.Path+"/", depth+1)
		if err != nil {
			return err
		}

		if child.Key {
			return badf("The request is invalid. Details: key fields cannot be sub-fields.")
		}

		fd.Children = append(fd.Children, child)
	}

	return nil
}

// attributes applies defaults and validates attribute combinations, writing the resolved values back into f.
func (c *compiler) attributes(fd *fieldDef, f map[string]any) error {
	vector := fd.isVector()
	fd.applyDefaults(f)

	if err := c.checkAttributes(fd, f); err != nil {
		return err
	}

	if err := c.resolveAnalyzers(fd, f); err != nil {
		return err
	}

	if vector {
		if err := c.vectorField(fd, f); err != nil {
			return err
		}
	}

	fd.echo(f)

	return nil
}

func (fd *fieldDef) applyDefaults(f map[string]any) {
	primitive := fd.Base != typeComplex
	vector := fd.isVector()

	fd.Key = boolOf(f, fieldKey, false)
	fd.Searchable = boolOf(f, "searchable", fd.Base == typeString)
	fd.Filterable = boolOf(f, "filterable", primitive && !vector)
	fd.Sortable = boolOf(f, "sortable", primitive && !fd.Collection)
	fd.Facetable = boolOf(f, "facetable", primitive && !vector && fd.Base != typeGeo)
	fd.Retrievable = boolOf(f, "retrievable", true)

	if vector {
		fd.Searchable = boolOf(f, "searchable", true)
		fd.Filterable, fd.Sortable, fd.Facetable = false, false, false
	}
}

func (c *compiler) resolveAnalyzers(fd *fieldDef, f map[string]any) error {
	fd.Analyzer = strOf(f, "analyzer")
	fd.SearchAnalyzer = strOf(f, "searchAnalyzer")
	fd.IndexAnalyzer = strOf(f, "indexAnalyzer")

	for _, n := range []string{fd.Analyzer, fd.SearchAnalyzer, fd.IndexAnalyzer} {
		if n == "" {
			continue
		}

		if _, err := c.analyzer(n); err != nil {
			return err
		}
	}

	for _, sm := range listOf(f, "synonymMaps") {
		s, _ := sm.(string)
		fd.SynonymMaps = append(fd.SynonymMaps, s)
	}

	return nil
}

// echo writes the resolved attributes back so the stored definition shows the service defaults.
func (fd *fieldDef) echo(f map[string]any) {
	f[fieldKey], f["searchable"], f["filterable"], f["sortable"], f["facetable"], f["retrievable"] =
		fd.Key, fd.Searchable, fd.Filterable, fd.Sortable, fd.Facetable, fd.Retrievable
	f["stored"] = boolOf(f, "stored", true)

	for _, k := range []string{"analyzer", "searchAnalyzer", "indexAnalyzer", "dimensions", "vectorSearchProfile"} {
		if _, ok := f[k]; !ok {
			f[k] = nil
		}
	}

	if _, ok := f["synonymMaps"]; !ok {
		f["synonymMaps"] = []any{}
	}

	if fd.Base != typeComplex {
		delete(f, "fields")
	}
}

func (c *compiler) checkAttributes(fd *fieldDef, f map[string]any) error {
	if err := checkTypeRules(fd, f); err != nil {
		return err
	}

	name := fd.Name

	switch {
	case (fd.Analyzer != "" || fd.SearchAnalyzer != "" || fd.IndexAnalyzer != "") && !fd.Searchable:
		return badf("The request is invalid. Details: analyzers can only be set on searchable fields ('%s').", name)
	case fd.Analyzer != "" && (fd.SearchAnalyzer != "" || fd.IndexAnalyzer != ""):
		return badf(
			"The request is invalid. Details: 'analyzer' cannot be combined with searchAnalyzer/indexAnalyzer ('%s').",
			name,
		)
	}

	return nil
}

func checkTypeRules(fd *fieldDef, f map[string]any) error {
	name := fd.Name

	switch {
	case fd.Searchable && fd.Base != typeString && !fd.isVector():
		return badf(
			"The request is invalid. Details: The field '%s' cannot be searchable: only string fields can be.",
			name,
		)
	case fd.Base == typeComplex && hasTrue(f, "filterable", "sortable", "facetable", fieldKey):
		return badf(
			"The request is invalid. Details: complex field '%s' cannot set filterable, sortable, facetable or key.",
			name,
		)
	case fd.Sortable && fd.Collection:
		return badf("The request is invalid. Details: The collection field '%s' cannot be sortable.", name)
	case fd.Facetable && fd.Base == typeGeo:
		return badf("The request is invalid. Details: The field '%s' cannot be facetable.", name)
	case fd.Key && (fd.Collection || fd.Base != typeString):
		return badf("The request is invalid. Details: The key field '%s' must be of type Edm.String.", name)
	}

	return nil
}

func hasTrue(f map[string]any, keys ...string) bool {
	for _, k := range keys {
		if b, _ := f[k].(bool); b {
			return true
		}
	}

	return false
}

func (c *compiler) vectorField(fd *fieldDef, f map[string]any) error {
	dims, ok := f["dimensions"].(float64)
	if !ok || int(dims) < minDims || int(dims) > maxDims {
		return badf("The request is invalid. Details: vector field '%s' requires dimensions between %d and %d.",
			fd.Name, minDims, maxDims)
	}

	fd.Dimensions = int(dims)

	fd.Profile = strOf(f, "vectorSearchProfile")
	if _, known := c.comp.vecProfile[fd.Profile]; !known {
		return badf("The request is invalid. Details: vector field '%s' requires a known vectorSearchProfile.", fd.Name)
	}

	return nil
}

func (c *compiler) suggesters(def map[string]any) error {
	for _, raw := range listOf(def, "suggesters") {
		s := objOf(raw)
		sg := &suggester{Name: strOf(s, "name")}

		if sg.Name == "" {
			return badf("The request is invalid. Details: a suggester must have a name.")
		}

		for _, sf := range listOf(s, "sourceFields") {
			path, _ := sf.(string)

			fd, ok := c.comp.byPath[path]
			if !ok || !fd.isText() || !fd.Searchable {
				return badf(
					"The request is invalid. Details: suggester '%s' source field '%s' must be a searchable string field.",
					sg.Name,
					path,
				)
			}

			sg.SourceFields = append(sg.SourceFields, path)
		}

		if len(sg.SourceFields) == 0 {
			return badf("The request is invalid. Details: suggester '%s' needs sourceFields.", sg.Name)
		}

		if _, ok := s["searchMode"]; !ok {
			s["searchMode"] = "analyzingInfixMatching"
		}

		c.comp.suggesters[sg.Name] = sg
	}

	return nil
}

// clone returns a deep copy of a JSON-shaped value.
func clone(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = clone(x)
		}

		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = clone(x)
		}

		return out
	}

	return v
}

func cloneMap(m map[string]any) map[string]any {
	out, _ := clone(m).(map[string]any)
	if out == nil {
		out = map[string]any{}
	}

	return out
}
