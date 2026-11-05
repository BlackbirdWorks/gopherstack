package sparql

type queryForm uint8

const (
	formSelect queryForm = iota
	formAsk
	formConstruct
	formDescribe
)

// node is a variable (Var != "") or a constant term in a pattern.
type node struct {
	Var  string
	Term Term
}

func (n node) isVar() bool { return n.Var != "" }

func varNode(v string) node { return node{Var: v} }

func termNode(t Term) node { return node{Term: t} }

type pathKind uint8

const (
	pLink pathKind = iota
	pInv
	pSeq
	pAlt
	pZeroOrMore
	pOneOrMore
	pZeroOrOne
	pNeg
)

type negEntry struct {
	iri Term
	inv bool
}

type path struct {
	iri  Term
	subs []*path
	neg  []negEntry
	kind pathKind
}

type triplePat struct {
	Path *path
	S, P node
	O    node
}

type quadPat struct {
	G node
	T triplePat
}

type projItem struct {
	Expr Expr
	Var  string
}

type orderCond struct {
	Expr Expr
	Desc bool
}

type valuesData struct {
	Vars []string
	Rows [][]Term
}

// Query is a parsed SPARQL query.
type Query struct {
	Where     *group
	Values    *valuesData
	Prefixes  map[string]string
	Base      string
	Proj      []projItem
	Template  []triplePat
	Describe  []node
	From      []Term
	FromNamed []Term
	GroupBy   []projItem
	Having    []Expr
	Order     []orderCond
	Limit     int
	Offset    int
	Form      queryForm
	Distinct  bool
	Star      bool
	grouped   bool
}

// element is one entry of a group graph pattern.
type element interface{ patternElement() }

type group struct{ Elems []element }

type bgpEl struct{ Triples []triplePat }

type optionalEl struct{ G *group }

type unionEl struct{ Alts []*group }

type minusEl struct{ G *group }

type filterEl struct{ E Expr }

type bindEl struct {
	E   Expr
	Var string
}

type valuesEl struct{ V *valuesData }

type graphEl struct {
	G    *group
	Name node
}

type subSelectEl struct{ Q *Query }

type groupEl struct{ G *group }

func (*bgpEl) patternElement() {}

func (*optionalEl) patternElement() {}

func (*unionEl) patternElement() {}

func (*minusEl) patternElement() {}

func (*filterEl) patternElement() {}

func (*bindEl) patternElement() {}

func (*valuesEl) patternElement() {}

func (*graphEl) patternElement() {}

func (*subSelectEl) patternElement() {}

func (*groupEl) patternElement() {}

// Expr is a SPARQL expression node.
type Expr any

type exprVar struct{ Name string }

type exprTerm struct{ T Term }

type exprUnary struct {
	E  Expr
	Op string
}

type exprBinary struct {
	L, R Expr
	Op   string
}

type exprCall struct {
	Name     string
	Sep      string
	Args     []Expr
	Distinct bool
	Star     bool
}

type exprIn struct {
	E    Expr
	List []Expr
	Not  bool
}

type exprExists struct {
	G   *group
	Not bool
}

type updateKind uint8

const (
	upInsertData updateKind = iota
	upDeleteData
	upModify
	upLoad
	upClear
	upDrop
	upCreate
	upAdd
	upMove
	upCopy
)

type graphTarget uint8

const (
	tgGraph graphTarget = iota
	tgDefault
	tgNamed
	tgAll
)

type updateOp struct {
	With       Term
	Where      *group
	Src        string
	Into       Term
	Graph      Term
	To         Term
	Del        []quadPat
	Ins        []quadPat
	Using      []Term
	UsingNamed []Term
	Target     graphTarget
	ToTarget   graphTarget
	Kind       updateKind
	Silent     bool
}

// Update is a parsed SPARQL update request.
type Update struct{ ops []updateOp }
