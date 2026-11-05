package azuresearch

import (
	"fmt"
	"maps"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"unicode"
)

const (
	kindSynonymMaps = "synonymmaps"
	kindDataSources = "datasources"
	kindIndexers    = "indexers"
	kindSkillsets   = "skillsets"

	maxBatchActions = 1000
	maxKeyLen       = 1024
	etagKey         = "@odata.etag"
)

func objectKindList() []string {
	return []string{kindSynonymMaps, kindDataSources, kindIndexers, kindSkillsets}
}

var indexLimits = map[string]int{ //nolint:gochecknoglobals // documented per-SKU index limits
	"free": limitFree, "basic": limitBasic, "standard": limitStandard, "standard2": limitLarge, "standard3": limitLarge,
	"storage_optimized_l1": limitStorageOpt, "storage_optimized_l2": limitStorageOpt,
}

func (s *Service) nextETag() string {
	s.NextETagSeq++

	return fmt.Sprintf("\"0x%X\"", etagBase+s.NextETagSeq)
}

func (s *Service) index(name string) (*Index, error) {
	ix, ok := s.Indexes[name]
	if !ok {
		return nil, wrapf(ErrIndexNotFound, "No index with the name '%s' was found in the service '%s'.", name, s.Name)
	}

	return ix, nil
}

func checkPrecondition(current any, ifMatch, ifNone string) error {
	cur, exists := current.(string)

	switch {
	case ifNone == "*" && exists:
		return wrapf(ErrPrecondition, "The specified If-None-Match condition was not satisfied.")
	case ifMatch == "*" && !exists:
		return wrapf(ErrPrecondition, "The specified If-Match condition was not satisfied.")
	case ifMatch != "" && ifMatch != "*" && ifMatch != cur:
		return wrapf(ErrPrecondition, "The specified If-Match condition was not satisfied.")
	}

	return nil
}

// createIndex adds a new index; it fails when the name is taken.
func (s *Service) createIndex(def map[string]any) (map[string]any, error) {
	name := strOf(def, "name")
	if _, ok := s.Indexes[name]; ok {
		return nil, badf("The index '%s' already exists.", name)
	}

	out, _, err := s.putIndex(name, def, false, "", "")

	return out, err
}

func (s *Service) putIndex(
	name string,
	def map[string]any,
	allowDowntime bool,
	ifMatch, ifNone string,
) (map[string]any, bool, error) {
	if strOf(def, "name") == "" {
		def["name"] = name
	}

	if strOf(def, "name") != name {
		return nil, false, badf("The index name in the request body does not match the one in the URI.")
	}

	old, exists := s.Indexes[name]

	var etag any
	if exists {
		etag = old.Def[etagKey]
	}

	if err := checkPrecondition(etag, ifMatch, ifNone); err != nil {
		return nil, false, err
	}

	if !exists && len(s.Indexes) >= indexLimits[s.Config.SKU] && indexLimits[s.Config.SKU] > 0 {
		return nil, false, wrapf(
			ErrQuota,
			"You have exceeded the maximum number of indexes (%d) for the %s SKU.",
			indexLimits[s.Config.SKU],
			s.Config.SKU,
		)
	}

	def = cloneMap(def)
	delete(def, fieldContext)

	ix := &Index{Def: def, Docs: map[string]*Doc{}}
	if exists {
		ix.Docs, ix.NextSeq = old.Docs, old.NextSeq
	}

	if err := ix.rebuild(); err != nil {
		return nil, false, err
	}

	if err := s.checkSynonymMaps(ix); err != nil {
		return nil, false, err
	}

	if exists {
		if err := schemaChange(old, ix, allowDowntime); err != nil {
			return nil, false, err
		}

		if schemaChanged(old, ix) {
			ix.Docs = map[string]*Doc{}
			ix.NextSeq = 0
			_ = ix.rebuild()
		} else {
			_ = ix.rebuild()
		}
	}

	def[etagKey] = s.nextETag()
	s.Indexes[name] = ix

	return cloneMap(def), !exists, nil
}

func (s *Service) checkSynonymMaps(ix *Index) error {
	for _, f := range ix.comp.byPath {
		for _, m := range f.SynonymMaps {
			if _, ok := s.Objects[kindSynonymMaps][m]; !ok {
				return badf(
					"The request is invalid. Details: The synonym map '%s' referenced by field '%s' does not exist.",
					m,
					f.Name,
				)
			}
		}
	}

	return nil
}

func fieldAttrs(f *fieldDef) []any {
	return []any{f.Type, f.Key, f.Searchable, f.Filterable, f.Sortable, f.Facetable, f.Retrievable,
		f.Analyzer, f.SearchAnalyzer, f.IndexAnalyzer, f.Dimensions, f.Profile}
}

func schemaChanged(old, next *Index) bool {
	for path, of := range old.comp.byPath {
		nf, ok := next.comp.byPath[path]
		if !ok || !reflect.DeepEqual(fieldAttrs(of), fieldAttrs(nf)) {
			return true
		}
	}

	return false
}

func schemaChange(old, next *Index, allowDowntime bool) error {
	if schemaChanged(old, next) && !allowDowntime {
		return badf(
			"Index update not allowed because it would cause downtime. In order to add new fields to an index " +
				"without downtime, only additive changes are allowed; set allowIndexDowntime=true to drop the index data.",
		)
	}

	return nil
}

func (s *Service) deleteIndex(name, ifMatch string) error {
	ix, err := s.index(name)
	if err != nil {
		return err
	}

	if err = checkPrecondition(ix.Def[etagKey], ifMatch, ""); err != nil {
		return err
	}

	delete(s.Indexes, name)

	return nil
}

func (s *Service) listIndexes() []map[string]any {
	names := make([]string, 0, len(s.Indexes))
	for n := range s.Indexes {
		names = append(names, n)
	}

	sort.Strings(names)

	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, cloneMap(s.Indexes[n].Def))
	}

	return out
}

type actionResult struct {
	Key  string
	Err  string
	Code int
	OK   bool
}

type indexAction struct {
	fields map[string]any
	action string
}

func validDocKey(k string) bool {
	if k == "" || len(k) > maxKeyLen {
		return false
	}

	for _, r := range k {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '-' && r != '=' {
			return false
		}
	}

	return true
}

// applyBatch executes the batch and reports a result per action.
func (ix *Index) applyBatch(actions []indexAction, version string) []actionResult {
	out := make([]actionResult, 0, len(actions))
	for _, a := range actions {
		out = append(out, ix.applyAction(a, version))
	}

	return out
}

func (ix *Index) applyAction(a indexAction, version string) actionResult {
	keyName := ix.comp.key.Name

	key, _ := a.fields[keyName].(string)
	if _, isStr := a.fields[keyName].(string); !isStr || key == "" {
		return actionResult{Code: http.StatusBadRequest, Err: "Document key cannot be missing or empty."}
	}

	if !validDocKey(key) {
		return actionResult{
			Key:  key,
			Code: http.StatusBadRequest,
			Err: fmt.Sprintf("Invalid document key: '%s'. Keys can only contain letters, "+
				"digits, underscore (_), dash (-), or equal sign (=).", key),
		}
	}

	old := ix.Docs[key]

	switch a.action {
	case "delete":
		ix.removeDoc(key)

		return actionResult{Key: key, OK: true, Code: http.StatusOK}
	case actionUpload, "merge", "mergeOrUpload":
	default:
		return actionResult{
			Key:  key,
			Code: http.StatusBadRequest,
			Err:  fmt.Sprintf("Invalid @search.action '%s'.", a.action),
		}
	}

	if a.action == "merge" && old == nil {
		return actionResult{Key: key, Code: http.StatusNotFound, Err: "Document not found."}
	}

	norm, err := normalizeDoc(ix.comp, a.fields, version)
	if err != nil {
		return actionResult{
			Key:  key,
			Code: http.StatusBadRequest,
			Err:  strings.TrimPrefix(err.Error(), ErrBadRequest.Error()+": "),
		}
	}

	merged := norm
	if old != nil && a.action != actionUpload {
		merged = mergeFields(old.Fields, norm)
	}

	ix.putDoc(key, merged, old)

	code := http.StatusOK
	if old == nil {
		code = http.StatusCreated
	}

	return actionResult{Key: key, OK: true, Code: code}
}

func mergeFields(old, patch map[string]any) map[string]any {
	out := make(map[string]any, len(old)+len(patch))
	maps.Copy(out, old)

	maps.Copy(out, patch)

	return out
}

func (ix *Index) putDoc(key string, fields map[string]any, old *Doc) {
	nd := &Doc{Fields: fields}
	if old != nil {
		nd.Seq = old.Seq

		ix.stats.add(old, -1)
	} else {
		ix.NextSeq++
		nd.Seq = ix.NextSeq
	}

	ix.comp.analyzeDoc(nd)
	ix.stats.add(nd, 1)
	ix.Docs[key] = nd
}

func (ix *Index) removeDoc(key string) {
	if old, ok := ix.Docs[key]; ok {
		ix.stats.add(old, -1)
		delete(ix.Docs, key)
	}
}

// storageSize approximates the index size in bytes from its documents.
func (ix *Index) storageSize() (int64, int64) {
	var total, vector int64

	for _, d := range ix.Docs {
		for _, f := range ix.comp.fields {
			v := d.Fields[f.Name]
			n := int64(len(fmt.Sprint(v)))

			if f.isVector() {
				vector += n
			}

			total += n
		}
	}

	return total, vector
}
