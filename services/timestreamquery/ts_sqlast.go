package timestreamquery

const (
	opNot      = "NOT"
	opAnd      = "AND"
	fnCurTS    = "current_timestamp"
	fnLocalTS  = "localtimestamp"
	opNotLower = "not"
	fnFilter   = "filter"
	kwDate     = "date"
	kwMonth    = "month"
	kwYear     = "year"
	kwInteger  = "integer"
	colValue   = "value"
)

type nodeKind uint8

const (
	nLiteral nodeKind = iota
	nColumn
	nStar
	nCall
	nBinary
	nUnary
	nCase
	nCast
	nIsNull
	nBetween
	nInList
	nInSub
	nLike
	nExists
	nSubquery
	nArrayCtor
	nRowCtor
	nSubscript
	nField
	nLambda
	nTypedLit
	nInterval
	nVar
	nParam
)

// node is one SQL expression; start/end are byte offsets into the source.
type node struct {
	sub      *queryNode
	name     string
	args     []*node
	parts    []string
	params   []string
	typ      tsType
	start    int
	end      int
	opStart  int
	opEnd    int
	headEnd  int
	xs       int
	xe       int
	kind     nodeKind
	distinct bool
	star     bool
	over     bool
	neg      bool
	hasElse  bool
	operand  bool
}

type selectItem struct {
	expr  *node
	alias string
	qual  []string
	star  bool
}

type relKind uint8

const (
	relTable relKind = iota
	relSubquery
	relJoin
	relUnnest
	relValues
)

type relation struct {
	sub        *queryNode
	left       *relation
	right      *relation
	on         *node
	joinType   string
	ref        []string
	alias      string
	colAliases []string
	using      []string
	args       []*node
	rows       [][]*node
	kind       relKind
	ordinality bool
}

type cteDef struct {
	query *queryNode
	name  string
	cols  []string
}

type orderItem struct{ expr *node }

type selectSpec struct {
	where    *node
	having   *node
	from     *relation
	items    []selectItem
	groupBy  []*node
	distinct bool
}

type queryNode struct {
	sel     *selectSpec
	left    *queryNode
	right   *queryNode
	setOp   string
	ctes    []cteDef
	orderBy []orderItem
	limit   []*node
}

type typedEdit struct {
	text       string
	start, end int
	seq        int
	closing    bool
	checked    bool
}

func (n *node) arg(i int) *node {
	if i < len(n.args) {
		return n.args[i]
	}

	return nil
}

// extent is the node's source span including any enclosing parentheses.
func (n *node) extent() (int, int) {
	if n.xe != 0 {
		return n.xs, n.xe
	}

	return n.start, n.end
}
