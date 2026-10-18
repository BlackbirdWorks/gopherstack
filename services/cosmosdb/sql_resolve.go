package cosmosdb

import (
	"fmt"
	"slices"
)

// resolveStmt binds every identifier in stmt to an env slot. outer is the enclosing scope for subqueries.
func resolveStmt(stmt *sqlSelectStmt, outer []string) error {
	scope := slices.Clone(outer)

	for _, src := range stmt.sources {
		if src.src != nil {
			if err := resolveExpr(src.src, scope); err != nil {
				return err
			}
		}

		scope = append(scope, src.alias)
	}

	return resolveStmtBody(stmt, scope)
}

func resolveStmtBody(stmt *sqlSelectStmt, scope []string) error {
	exprs := make([]sqlExpr, 0, len(stmt.items)+len(stmt.groupBy)+len(stmt.orderBy))

	for _, it := range stmt.items {
		exprs = append(exprs, it.expr)
	}

	exprs = append(exprs, stmt.groupBy...)

	for _, k := range stmt.orderBy {
		exprs = append(exprs, k.expr)
	}

	exprs = append(exprs, stmt.where, stmt.top, stmt.offset, stmt.limit)

	for _, x := range exprs {
		if x == nil {
			continue
		}

		if err := resolveExpr(x, scope); err != nil {
			return err
		}
	}

	return nil
}

func resolveAll(xs []sqlExpr, scope []string) error {
	for _, x := range xs {
		if err := resolveExpr(x, scope); err != nil {
			return err
		}
	}

	return nil
}

func resolveExpr(x sqlExpr, scope []string) error {
	switch n := x.(type) {
	case *exprVar:
		idx := -1

		for i := range slices.Backward(scope) {
			if scope[i] == n.name {
				idx = i

				break
			}
		}

		if idx < 0 {
			return fmt.Errorf("%w: unknown identifier %q (fields must be qualified with a FROM alias)",
				ErrQueryParse, n.name)
		}

		n.idx = idx

		return nil
	case *exprSubquery:
		return resolveStmt(n.stmt, scope)
	default:
		return resolveChildren(x, scope)
	}
}

func resolveChildren(x sqlExpr, scope []string) error {
	switch n := x.(type) {
	case *exprProp:
		return resolveExpr(n.base, scope)
	case *exprIndex:
		return resolveAll([]sqlExpr{n.base, n.idx}, scope)
	case *exprUnary:
		return resolveExpr(n.x, scope)
	case *exprBinary:
		return resolveAll([]sqlExpr{n.l, n.r}, scope)
	case *exprIsNull:
		return resolveExpr(n.x, scope)
	case *exprIn:
		return resolveAll(append([]sqlExpr{n.x}, n.list...), scope)
	case *exprBetween:
		return resolveAll([]sqlExpr{n.x, n.lo, n.hi}, scope)
	case *exprLike:
		return resolveAll([]sqlExpr{n.x, n.pattern}, scope)
	case *exprTernary:
		return resolveAll([]sqlExpr{n.cond, n.a, n.b}, scope)
	case *exprCall:
		return resolveAll(n.args, scope)
	case *exprArrayLit:
		return resolveAll(n.items, scope)
	case *exprObjectLit:
		return resolveAll(n.vals, scope)
	default:
		return nil
	}
}
