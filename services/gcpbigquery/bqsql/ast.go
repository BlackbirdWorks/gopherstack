package bqsql

type nodeKind uint8

const (
	nNull nodeKind = iota
	nInt
	nFloat
	nString
	nBytes
	nBool
	nTyped
	nParam
	nPath
	nStar
	nUnary
	nBinary
	nCall
	nCase
	nCast
	nExtract
	nBetween
	nIn
	nLike
	nIs
	nExists
	nSubquery
	nArrayQuery
	nArray
	nStruct
	nIndex
	nField
	nInterval
)

// node is one expression.
type node struct {
	query    *Query
	over     *windowSpec
	limit    *node
	castTo   *Type
	elemType *Type
	name     string
	str      string
	args     []*node
	names    []string
	parts    []string
	order    []orderItem
	except   []string
	replace  []selectItem
	pos      int
	kind     nodeKind
	not      bool
	distinct bool
	safe     bool
	nulls    bool
	hasElse  bool
	operand  bool
	index    int
}

type windowSpec struct {
	frame     string
	partition []*node
	order     []orderItem
}

type orderItem struct {
	expr *node
	desc bool
}

type selectItem struct {
	expr  *node
	alias string
}

type fromKind uint8

const (
	fromTable fromKind = iota
	fromSubquery
	fromUnnest
	fromJoin
)

type fromItem struct {
	left        *fromItem
	right       *fromItem
	query       *Query
	on          *node
	unnest      *node
	alias       string
	offsetAlias string
	joinType    string
	path        []string
	using       []string
	kind        fromKind
	withOffset  bool
}

type setExpr struct {
	sel   *selectStmt
	sub   *Query
	left  *setExpr
	right *setExpr
	op    string
}

type selectStmt struct {
	from     *fromItem
	where    *node
	having   *node
	qualify  *node
	items    []selectItem
	groupBy  []*node
	distinct bool
	asStruct bool
}

type cte struct {
	query *Query
	name  string
	cols  []string
}

// Query is a parsed SELECT statement, possibly with set operations, CTEs, ORDER BY and LIMIT.
type Query struct {
	body   *setExpr
	limit  *node
	offset *node
	with   []cte
	order  []orderItem
}

// StmtKind classifies a parsed statement.
type StmtKind uint8

// Statement kinds.
const (
	StmtQuery StmtKind = iota
	StmtInsert
	StmtUpdate
	StmtDelete
	StmtMerge
	StmtCreateTable
	StmtCreateView
	StmtCreateSchema
	StmtDrop
	StmtTruncate
	StmtAlter
)

// DML reports whether the statement modifies table rows.
func (k StmtKind) DML() bool {
	return k == StmtInsert || k == StmtUpdate || k == StmtDelete || k == StmtMerge || k == StmtTruncate
}

type assignment struct {
	value  *node
	target []string
}

type mergeWhen struct {
	cond      *node
	action    string
	insertVal []*node
	insertCol []string
	set       []assignment
	matched   bool
	bySource  bool
	insertAll bool
}

type colDef struct {
	name     string
	typ      Type
	required bool
}

// Statement is one parsed GoogleSQL statement.
type Statement struct {
	options   map[string]string
	where     *node
	on        *node
	from      *fromItem
	source    *fromItem
	query     *Query
	Text      string
	alias     string
	dropKind  string
	alterOp   string
	viewSQL   string
	target    []string
	cols      []string
	alterCols []colDef
	colDefs   []colDef
	whens     []mergeWhen
	set       []assignment
	rows      [][]*node
	Kind      StmtKind
	orReplace bool
	ifNot     bool
	ifExists  bool
	temp      bool
	cascade   bool
	hasValues bool
}
