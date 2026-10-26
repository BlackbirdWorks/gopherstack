package promql

import "time"

// Expr is a parsed PromQL expression.
type Expr interface{ isExpr() }

// NumberLit is a scalar literal.
type NumberLit struct{ V float64 }

// StringLit is a string literal.
type StringLit struct{ V string }

// VectorSelector selects the latest sample per series.
type VectorSelector struct {
	Name     string
	At       *int64
	AtMarker string
	Matchers []*Matcher
	Offset   time.Duration
}

// MatrixSelector selects a window of samples per series.
type MatrixSelector struct {
	VS    *VectorSelector
	Range time.Duration
}

// Call is a function application.
type Call struct {
	Func string
	Args []Expr
}

// Aggregation reduces a vector across series.
type Aggregation struct {
	Expr     Expr
	Param    Expr
	Op       string
	Grouping []string
	Without  bool
}

// Binary combines two operands.
type Binary struct {
	L, R        Expr
	Op          string
	MatchLabels []string
	Include     []string
	Card        Cardinality
	On          bool
	ReturnBool  bool
}

// Cardinality is the vector matching cardinality of a binary operation.
type Cardinality int

// Matching cardinalities; group_left is many-to-one, group_right one-to-many.
const (
	CardOneToOne Cardinality = iota
	CardManyToOne
	CardOneToMany
)

// Subquery evaluates Expr over Range at Step resolution; a zero Step uses DefaultSubqueryStep.
type Subquery struct {
	Expr     Expr
	At       *int64
	AtMarker string
	Range    time.Duration
	Step     time.Duration
	Offset   time.Duration
}

// Unary negates its operand.
type Unary struct{ X Expr }

func (*NumberLit) isExpr()      {}
func (*StringLit) isExpr()      {}
func (*VectorSelector) isExpr() {}
func (*MatrixSelector) isExpr() {}
func (*Call) isExpr()           {}
func (*Aggregation) isExpr()    {}
func (*Binary) isExpr()         {}
func (*Unary) isExpr()          {}
func (*Subquery) isExpr()       {}
