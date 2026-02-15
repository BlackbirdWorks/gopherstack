package expr

type Node interface {
	exprNode()
}

// Logical Nodes
type LogicalExpr struct {
	Left     Node
	Operator TokenType // AND, OR
	Right    Node
}

func (e *LogicalExpr) exprNode() {}

type NotExpr struct {
	Expression Node
}

func (e *NotExpr) exprNode() {}

// Comparison Nodes
type ComparisonExpr struct {
	Left     Node
	Operator TokenType // =, <>, <, >, <=, >=
	Right    Node
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

// Function Nodes
type FunctionExpr struct {
	Name string
	Args []Node
}

func (e *FunctionExpr) exprNode() {}

// Path Nodes (e.g., a.b[0])
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

// Literal/Placeholder Nodes
type ValuePlaceholder struct {
	Name string
}

func (e *ValuePlaceholder) exprNode() {}

// Update Expressions
type UpdateExpr struct {
	Actions []UpdateAction
}

func (e *UpdateExpr) exprNode() {}

type UpdateAction struct {
	Type  TokenType // SET, REMOVE, ADD, DELETE
	Items []UpdateItem
}

type UpdateItem struct {
	Path  Node // PathExpr
	Value Node // Used for SET, ADD, DELETE
}

// Projection Expressions
type ProjectionExpr struct {
	Paths []Node // PathExpr
}

func (e *ProjectionExpr) exprNode() {}
