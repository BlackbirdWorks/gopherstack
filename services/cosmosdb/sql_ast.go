package cosmosdb

// sqlExpr is a node of a parsed Cosmos SQL scalar expression.
type sqlExpr interface{ isSQLExpr() }

type exprLit struct{ v any }

type exprUndefined struct{}

type exprParam struct{ name string }

// exprVar is a reference to a FROM/JOIN/subquery binding; idx is its env slot, set by the resolver.
type exprVar struct {
	name string
	idx  int
}

type exprProp struct {
	base sqlExpr
	key  string
}

type exprIndex struct{ base, idx sqlExpr }

type exprUnary struct {
	x  sqlExpr
	op sqlTokenType
}

type exprBinary struct {
	l, r sqlExpr
	op   sqlTokenType
}

type exprIsNull struct {
	x      sqlExpr
	negate bool
}

type exprIn struct {
	x      sqlExpr
	list   []sqlExpr
	negate bool
}

type exprBetween struct {
	x, lo, hi sqlExpr
	negate    bool
}

type exprLike struct {
	x, pattern sqlExpr
	escape     string
	negate     bool
}

type exprTernary struct{ cond, a, b sqlExpr }

type exprCall struct {
	name string
	args []sqlExpr
	kind fnKind
}

type exprArrayLit struct{ items []sqlExpr }

type exprObjectLit struct {
	keys []string
	vals []sqlExpr
}

type subqueryKind int

const (
	subScalar subqueryKind = iota
	subExists
	subArray
)

type exprSubquery struct {
	stmt *sqlSelectStmt
	kind subqueryKind
}

func (*exprLit) isSQLExpr()       {}
func (*exprUndefined) isSQLExpr() {}
func (*exprParam) isSQLExpr()     {}
func (*exprVar) isSQLExpr()       {}
func (*exprProp) isSQLExpr()      {}
func (*exprIndex) isSQLExpr()     {}
func (*exprUnary) isSQLExpr()     {}
func (*exprBinary) isSQLExpr()    {}
func (*exprIsNull) isSQLExpr()    {}
func (*exprIn) isSQLExpr()        {}
func (*exprBetween) isSQLExpr()   {}
func (*exprLike) isSQLExpr()      {}
func (*exprTernary) isSQLExpr()   {}
func (*exprCall) isSQLExpr()      {}
func (*exprArrayLit) isSQLExpr()  {}
func (*exprObjectLit) isSQLExpr() {}
func (*exprSubquery) isSQLExpr()  {}

// sqlSelectItem is one SELECT list entry; name is the output key.
type sqlSelectItem struct {
	expr sqlExpr
	name string
}

type sqlOrderKey struct {
	expr sqlExpr
	desc bool
}

// sqlSource is one FROM or JOIN binding. src is nil for the top-level container row.
type sqlSource struct {
	src   sqlExpr
	alias string
}

// sqlSelectStmt is a fully parsed Cosmos SQL query.
type sqlSelectStmt struct {
	where       sqlExpr
	top         sqlExpr
	offset      sqlExpr
	limit       sqlExpr
	sources     []sqlSource
	items       []sqlSelectItem
	groupBy     []sqlExpr
	orderBy     []sqlOrderKey
	isStar      bool
	selectValue bool
	distinct    bool
	hasAgg      bool
}
