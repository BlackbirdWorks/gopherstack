package azuresearch

import (
	"regexp"
	"strings"
)

const (
	typeString   = "Edm.String"
	typeInt32    = "Edm.Int32"
	typeInt64    = "Edm.Int64"
	typeDouble   = "Edm.Double"
	typeBool     = "Edm.Boolean"
	typeDate     = "Edm.DateTimeOffset"
	typeGeo      = "Edm.GeographyPoint"
	typeComplex  = "Edm.ComplexType"
	typeSingle   = "Edm.Single"
	typeHalf     = "Edm.Half"
	typeInt16    = "Edm.Int16"
	typeSByte    = "Edm.SByte"
	typeByte     = "Edm.Byte"
	collPrefix   = "Collection("
	maxIndexName = 128
	maxFields    = 1000
	maxNesting   = 10
	maxDims      = 4096
	minDims      = 2
)

var (
	indexNameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)     //nolint:gochecknoglobals // compiled once
	fieldNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`) //nolint:gochecknoglobals // compiled once
)

// Index is a search index: its definition, documents and runtime caches.
type Index struct {
	Def     map[string]any  `json:"def"`
	Docs    map[string]*Doc `json:"docs"`
	NextSeq int64           `json:"nextSeq"`
	comp    *compiled
	stats   *termStats
}

// Doc is one stored document.
type Doc struct {
	Fields map[string]any `json:"fields"`
	Seq    int64          `json:"seq"`
	tok    map[string]*fieldTokens
}

type fieldTokens struct {
	terms  map[string][]int
	toks   []Token
	length int
}

type fieldDef struct {
	Name           string
	Path           string
	Type           string
	Base           string
	Analyzer       string
	SearchAnalyzer string
	IndexAnalyzer  string
	Profile        string
	SynonymMaps    []string
	Children       []*fieldDef
	Dimensions     int
	Collection     bool
	Key            bool
	Searchable     bool
	Filterable     bool
	Sortable       bool
	Facetable      bool
	Retrievable    bool
}

func (f *fieldDef) isVector() bool {
	return f.Collection && vectorBase(f.Base)
}

func vectorBase(base string) bool {
	switch base {
	case typeSingle, typeHalf, typeInt16, typeSByte, typeByte:
		return true
	}

	return false
}

func (f *fieldDef) isText() bool { return f.Base == typeString }

type suggester struct {
	Name         string
	SourceFields []string
}

type compiled struct {
	byPath     map[string]*fieldDef
	suggesters map[string]*suggester
	profiles   map[string]*scoringProfile
	vecProfile map[string]vectorProfile
	analyzers  map[string]Analyzer
	key        *fieldDef
	fields     []*fieldDef
	searchable []*fieldDef
	defProfile string
}

type vectorProfile struct {
	Metric    string
	Kind      string
	Algorithm string
}

func splitType(t string) (base string, collection bool) {
	if inner, ok := strings.CutPrefix(t, collPrefix); ok {
		return strings.TrimSuffix(inner, ")"), true
	}

	return t, false
}

func validBaseType(base string) bool {
	switch base {
	case typeString, typeInt32, typeInt64, typeDouble, typeBool, typeDate, typeGeo, typeComplex,
		typeSingle, typeHalf, typeInt16, typeSByte, typeByte:
		return true
	}

	return false
}
