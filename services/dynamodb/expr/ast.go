package expr

type Node interface {
	exprNode()
}

// LogicalExpr represents a logical expression (AND, OR) combining two nodes.
type LogicalExpr struct {
	Left     Node
	Right    Node
	Operator TokenType
}

func (e *LogicalExpr) exprNode() {}

type NotExpr struct {
	Expression Node
}

func (e *NotExpr) exprNode() {}

// ComparisonExpr represents a comparison expression (=, <>, <, >, <=, >=).
type ComparisonExpr struct {
	Left     Node
	Right    Node
	Operator TokenType
}

func (e *ComparisonExpr) exprNode() {}

type BetweenExpr struct {
	Value Node
	Lower Node
	Upper Node
}

func (e *BetweenExpr) exprNode() {}

type InExpr struct {
	Value      Node
	Candidates []Node
}

func (e *InExpr) exprNode() {}

// FunctionExpr represents a function call expression (e.g., size, attribute_exists).
type FunctionExpr struct {
	Name string
	Args []Node
}

func (e *FunctionExpr) exprNode() {}

// PathExpr represents a path expression (e.g., a.b[0]).
type PathExpr struct {
	Elements []PathElement
}

func (e *PathExpr) exprNode() {}

type PathElement struct {
	Name  string
	Index int
	Type  PathElementType
}

type PathElementType int

const (
	ElementKey PathElementType = iota
	ElementIndex
)

// ValuePlaceholder represents a value placeholder (e.g., :val).
type ValuePlaceholder struct {
	Name string
}

func (e *ValuePlaceholder) exprNode() {}

// UpdateExpr represents an update expression with one or more actions (SET, REMOVE, ADD, DELETE).
type UpdateExpr struct {
	Actions []UpdateAction
}

func (e *UpdateExpr) exprNode() {}

type UpdateAction struct {
	Items []UpdateItem
	Type  TokenType
}

type UpdateItem struct {
	Path  Node // PathExpr
	Value Node // Used for SET, ADD, DELETE
}

// ProjectionExpr represents a projection expression listing the paths to include.
type ProjectionExpr struct {
	Paths []Node // PathExpr
}

func (e *ProjectionExpr) exprNode() {}
