package opencypher

type expr any

type (
	litExpr   struct{ v any }
	paramExpr struct{ name string }
	identExpr struct{ name string }
	propExpr  struct {
		e   expr
		key string
	}
	indexExpr struct{ e, i expr }
	sliceExpr struct{ e, from, to expr }
	callExpr  struct {
		name     string
		args     []expr
		distinct bool
		star     bool
	}
	unaryExpr struct {
		e  expr
		op string
	}
	binaryExpr struct {
		l, r expr
		op   string
	}
	isNullExpr struct {
		e   expr
		not bool
	}
	listExpr struct{ items []expr }
	mapExpr  struct {
		keys []string
		vals []expr
	}
	labelsExpr struct {
		e      expr
		labels []string
	}
	caseExpr struct {
		subject expr
		els     expr
		whens   []expr
		thens   []expr
	}
)

type nodePat struct {
	props  *mapExpr
	v      string
	labels []string
}

type relPat struct {
	props     *mapExpr
	v         string
	types     []string
	dir       int
	minHops   int
	maxHops   int
	varLen    bool
	unbounded bool
}

type pattern struct {
	pathVar string
	nodes   []nodePat
	rels    []relPat
}

type clause any

type (
	matchClause struct {
		where    expr
		pats     []pattern
		optional bool
	}
	createClause struct{ pats []pattern }
	mergeClause  struct {
		pat      pattern
		onCreate []setItem
		onMatch  []setItem
	}
	setClause    struct{ items []setItem }
	removeClause struct{ items []setItem }
	deleteClause struct {
		exprs  []expr
		detach bool
	}
	withClause struct {
		where expr
		proj  projection
	}
	unwindClause struct {
		e expr
		v string
	}
	returnClause struct{ proj projection }
)

type setItem struct {
	target expr
	val    expr
	v      string
	op     string
	labels []string
}

type projItem struct {
	e    expr
	name string
}

type orderItem struct {
	e    expr
	desc bool
}

type projection struct {
	skip     expr
	limit    expr
	items    []projItem
	order    []orderItem
	distinct bool
	star     bool
}

type query struct {
	clauses []clause
	write   bool
}
