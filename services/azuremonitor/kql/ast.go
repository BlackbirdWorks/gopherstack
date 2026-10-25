package kql

type node any

type (
	litNode struct {
		v any
		t Type
	}
	identNode struct{ name string }
	callNode  struct {
		name string
		args []node
	}
	binNode struct {
		l, r node
		op   string
	}
	unNode struct {
		x  node
		op string
	}
	inNode struct {
		x    node
		list []node
		neg  bool
		ci   bool
	}
	betweenNode struct {
		x, lo, hi node
		neg       bool
	}
	memberNode struct {
		x    node
		name string
	}
	indexNode struct{ x, idx node }

	tableRef struct {
		name      string
		workspace string
	}
	unionNode struct {
		tables []node
	}
	datatableNode struct {
		cols []Col
		vals []node
	}
	rangeNode struct {
		from node
		to   node
		step node
		name string
	}
	printNode struct{ items []namedExpr }
	pipeNode  struct {
		src node
		ops []node
	}
)

type namedExpr struct {
	e    node
	name string
}

type sortKey struct {
	e         node
	nullsLast *bool
	desc      bool
}

type (
	whereOp        struct{ e node }
	projectOp      struct{ items []namedExpr }
	projectAwayOp  struct{ names []string }
	projectKeepOp  struct{ names []string }
	projectRenOp   struct{ pairs []namedExpr }
	projectOrderOp struct{ names []string }
	extendOp       struct{ items []namedExpr }
	summarizeOp    struct{ aggs, by []namedExpr }
	sortOp         struct{ keys []sortKey }
	topOp          struct {
		n   node
		key sortKey
	}
	takeOp     struct{ n node }
	distinctOp struct {
		items []namedExpr
		all   bool
	}
	countOp     struct{ name string }
	unionOp     struct{ tables []node }
	renderOp    struct{}
	getschemaOp struct{}
)

type letStmt struct {
	def  node
	name string
}

type query struct {
	body node
	lets []letStmt
}

const (
	opAnd          = "and"
	opOr           = "or"
	opMatchesRegex = "matches regex"
)

const (
	kwBool    = "bool"
	kwBoolean = "boolean"
	kwDecimal = "decimal"
	kwGUID    = "guid"
	kwCount   = "count"
	kwInTilde = "in~"
)

const (
	kwDouble   = "double"
	kwDatetime = "datetime"
	kwDynamic  = "dynamic"

	peekThird  = 2
	peekFourth = 3
)

const kwTime = "time"
