package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// guardCallNameRe recognizes a call as a service-disambiguation check by
// name alone: ExtractServiceFromRequest, or a local isXxxRequest/isXxxPath/
// isXxxARN/isXxxArn helper (ecr's isRegistryPath, dlm's isDLMResourceARN,
// macie2's isMacie2Request, vpclattice's isVPCLatticeTagPath,
// codeartifact's isDomainRepoPath, resourcegroups' isResourceTagsPath).
// What the helper checks internally doesn't matter for this bullet -- the
// naming convention itself is the established disambiguation idiom this
// repo already uses (see dlm/handler.go's isDLMResourceARN doc comment).
var guardCallNameRe = regexp.MustCompile(`^ExtractServiceFromRequest$|^is\w*(?:Request|Path|ARN|Arn)$`)

// arnShapeRe matches a resolved string literal that looks like an ARN
// service-segment marker: a lone ":<svc-name>[:]" token (amplify's
// ":amplify", accessanalyzer's ":access-analyzer:", vpclattice's
// ":vpc-lattice:") or an inline "arn:aws:<svc>" substring (macie2's
// "/tags/arn:aws:macie2:", eks's "/tags/arn:aws:eks:").
var arnShapeRe = regexp.MustCompile(`^:[a-z][a-z0-9-]*:?$|arn:aws:[a-z][a-z0-9-]*`)

// guardEvidence is one recognized guard construct: where it was found and
// which of the five bullets it satisfies, for the -why flag.
type guardEvidence struct {
	File      string `json:"file"`
	Construct string `json:"construct"`
	Line      int    `json:"line"`
}

func newGuardEvidence(pos token.Position, construct string) guardEvidence {
	return guardEvidence{File: pos.Filename, Construct: construct, Line: pos.Line}
}

func (g guardEvidence) String() string {
	return fmt.Sprintf("%s:%d: %s", relToWorkingDir(g.File), g.Line, g.Construct)
}

// relToWorkingDir shortens an absolute source path to one relative to the
// current directory (services/foo/handler.go, not the full /home/.../foo/
// path fset resolves it to) purely for -why's display; falls back to the
// absolute path if that fails.
func relToWorkingDir(path string) string {
	wd, err := os.Getwd()
	if err != nil {
		return path
	}

	rel, err := filepath.Rel(wd, path)
	if err != nil {
		return path
	}

	return rel
}

// isGuarded returns the guard evidence found in fn's body, plus -- one
// call hop out -- the body of every same-package helper fn's body calls or
// indexes (referencedNames/pd.namedBodies, the same one-hop lookup
// chaseClaims uses for claim extraction in delegation.go). A non-empty
// result means the matcher is guarded against the over-claim collision
// class; see the package doc comment for the five recognized constructs.
func isGuarded(fn *ast.FuncDecl, pd *pkgData) []guardEvidence {
	nodes := []ast.Node{fn.Body}

	for _, name := range referencedNames(fn.Body) {
		if child, ok := pd.namedBodies[name]; ok {
			nodes = append(nodes, child)
		}
	}

	evidence := make([]guardEvidence, 0, len(nodes))

	for _, node := range nodes {
		evidence = append(evidence, scanGuardConstructs(node, pd)...)
	}

	return evidence
}

func scanGuardConstructs(node ast.Node, pd *pkgData) []guardEvidence {
	consts := guardConstsFor(pd.fset, node, pd.consts)

	var evidence []guardEvidence

	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			evidence = append(evidence, guardCallEvidence(x, pd.fset, consts)...)
		case *ast.BinaryExpr:
			if ev, ok := guardBinaryEvidence(x, pd.fset); ok {
				evidence = append(evidence, ev)
			}
		case *ast.AssignStmt:
			if ev, ok := guardCommaOkEvidence(x, pd.fset); ok {
				evidence = append(evidence, ev)
			}
		}

		return true
	})

	return evidence
}

// guardConstsFor merges node's own local consts (a helper's "const suffix =
// ..." -- see localConstTable in delegation.go) over pd's package-wide
// const table, the same precedence extractClaimsForNode uses for claim
// resolution.
func guardConstsFor(fset *token.FileSet, node ast.Node, base map[string]string) map[string]string {
	localConsts, _ := localConstTable(fset, node)
	if len(localConsts) == 0 {
		return base
	}

	merged := make(map[string]string, len(base)+len(localConsts))

	maps.Copy(merged, base)
	maps.Copy(merged, localConsts)

	return merged
}

// guardCallEvidence recognizes three call-shaped bullets: a helper named
// like a service-check (bullet a), MatchesUserAgentMarker (bullet c), and
// strings.Contains/HasPrefix/HasSuffix/EqualFold against an ARN-shaped
// literal (bullet b).
func guardCallEvidence(call *ast.CallExpr, fset *token.FileSet, consts map[string]string) []guardEvidence {
	name := callName(call)
	pos := fset.Position(call.Pos())

	switch {
	case guardCallNameRe.MatchString(name):
		construct := fmt.Sprintf("call to %s(...), a recognized service-check helper name", name)

		return []guardEvidence{newGuardEvidence(pos, construct)}
	case name == "MatchesUserAgentMarker":
		return []guardEvidence{newGuardEvidence(pos, "MatchesUserAgentMarker(...) User-Agent marker check")}
	case isStringsCompareCall(call, name):
		if lit, ok := guardArnLiteralArg(call, consts); ok {
			construct := fmt.Sprintf("strings.%s(...) against ARN-shaped literal %q", name, lit)

			return []guardEvidence{newGuardEvidence(pos, construct)}
		}
	}

	return nil
}

func isStringsCompareCall(call *ast.CallExpr, name string) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "strings" {
		return false
	}

	switch name {
	case "Contains", "HasPrefix", "HasSuffix", "EqualFold":
		return true
	default:
		return false
	}
}

func guardArnLiteralArg(call *ast.CallExpr, consts map[string]string) (string, bool) {
	for _, arg := range call.Args {
		if lit, ok := flattenStringExpr(arg, consts); ok && arnShapeRe.MatchString(lit) {
			return lit, true
		}
	}

	return "", false
}

// flattenStringExpr resolves expr to a literal string when it is a quoted
// literal, an identifier naming a resolvable const, or a "+"-concatenation
// of those (accessanalyzer's `":"+accessAnalyzerService+":"`, eks's
// `pathEKSTags+"arn:aws:eks:"`). Anything else -- a variable read from the
// request, a function call -- fails resolution, since this repo's real
// guard literals are always built from consts and quoted string pieces.
func flattenStringExpr(expr ast.Expr, consts map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.BasicLit:
		if e.Kind != token.STRING {
			return "", false
		}

		s, err := strconv.Unquote(e.Value)

		return s, err == nil
	case *ast.Ident:
		s, ok := consts[e.Name]

		return s, ok
	case *ast.BinaryExpr:
		return flattenConcat(e, consts)
	default:
		return "", false
	}
}

func flattenConcat(e *ast.BinaryExpr, consts map[string]string) (string, bool) {
	if e.Op != token.ADD {
		return "", false
	}

	left, ok := flattenStringExpr(e.X, consts)
	if !ok {
		return "", false
	}

	right, ok := flattenStringExpr(e.Y, consts)
	if !ok {
		return "", false
	}

	return left + right, true
}

// guardBinaryEvidence recognizes bullet (e) -- an exact "==" comparison
// against the full request-path variable, conventionally named "path" in
// this repo's RouteMatchers (codeartifact's `path == pathV1Domain`, dlm's
// `path == pathPoliciesBase`) -- and half of bullet (d): a comparison
// against a closed operation-table sentinel named like "...Unknown"
// (polly's `parseRoute(...).operation != opUnknown`).
func guardBinaryEvidence(bin *ast.BinaryExpr, fset *token.FileSet) (guardEvidence, bool) {
	pos := fset.Position(bin.Pos())

	switch {
	case bin.Op == token.EQL && (isPathIdent(bin.X) || isPathIdent(bin.Y)):
		return newGuardEvidence(pos, "path == ... exact match on the full request path"), true
	case (bin.Op == token.NEQ || bin.Op == token.EQL) && (isUnknownSentinel(bin.X) || isUnknownSentinel(bin.Y)):
		return newGuardEvidence(pos, "comparison against a closed operation-table sentinel"), true
	default:
		return guardEvidence{}, false
	}
}

func isPathIdent(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)

	return ok && id.Name == "path"
}

func isUnknownSentinel(expr ast.Expr) bool {
	id, ok := expr.(*ast.Ident)

	return ok && strings.Contains(id.Name, "Unknown")
}

// guardCommaOkEvidence recognizes the other half of bullet (d): `_, ok :=
// routes[path]` -- a comma-ok index into a closed route/op table gating
// the match, as opposed to scanMapKeyClaims' question (delegation.go),
// which is what paths the table claims, not whether membership in it gates
// anything.
func guardCommaOkEvidence(assign *ast.AssignStmt, fset *token.FileSet) (guardEvidence, bool) {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != 2 || len(assign.Rhs) != 1 {
		return guardEvidence{}, false
	}

	okIdent, isIdent := assign.Lhs[1].(*ast.Ident)
	if !isIdent || okIdent.Name != "ok" {
		return guardEvidence{}, false
	}

	if _, isIndex := assign.Rhs[0].(*ast.IndexExpr); !isIndex {
		return guardEvidence{}, false
	}

	pos := fset.Position(assign.Pos())

	return newGuardEvidence(pos, "comma-ok index into a closed route table"), true
}
