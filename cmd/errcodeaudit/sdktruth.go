package main

import (
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// moduleCodes is the legitimate error-code set for one pinned SDK module,
// built from two sources. typeCodes come from types/errors.go: every
// declared exception type's own ErrorCode() method, read as the literal
// string in its `return "Foo"` branch (NOT the Go type name, which can
// differ -- e.g. iam@v1.58.1's NoSuchEntityException.ErrorCode() returns
// "NoSuchEntity"). deserCodes come from deserializers.go: every literal
// matched in a `strings.EqualFold("Foo", errorCode)` case inside a
// deserializeOpError* function -- the codes actually recognized on the
// wire for some operation. Both are unioned into a module's legitimate set;
// see main.go's doc comment for which is treated as canonical and why.
//
// opFuncs/matchedOpFuncs measure how completely this module's own
// deserializeOpError* functions model errors at all: opFuncs is how many
// such functions exist, matchedOpFuncs how many have at least one
// EqualFold case rather than falling straight through to
// smithy.GenericAPIError for every code. Confirmed live: s3@v1.106.5 modeled
// codes in only 20 of 112 such functions (18%) -- GetObject's own switch
// matches just "InvalidObjectState"/"NoSuchKey" and defaults everything
// else, including many real, AWS-documented S3 codes
// (InvalidBucketName, NoSuchBucketPolicy, PermanentRedirect, ...) straight
// to a generic pass-through a real client accepts without error either --
// against ecs/iam/lambda/sns/sqs/dynamodb's 90-100% and
// cloudformation's 69%. scan.go treats a module under 50% coverage as too
// sparsely modeled for a CONFIDENT finding: absence from its ErrorCode()/
// deserializer set there is no longer good evidence of a fabricated code,
// only of a code this SDK version chose not to model.
type moduleCodes struct {
	typeCodes      map[string]bool
	deserCodes     map[string]bool
	opFuncs        int
	matchedOpFuncs int
}

func newModuleCodes() *moduleCodes {
	return &moduleCodes{typeCodes: map[string]bool{}, deserCodes: map[string]bool{}}
}

// loadModuleCodes reads modPath's types/errors.go and deserializers.go. A
// module missing either file (or the module dir itself, for a service this
// repo's go.mod doesn't actually pin -- checked separately) contributes an
// empty set, never an error: "nothing to check" is a normal outcome, same
// discipline as cmd/enumcheck's auditServiceDir.
func loadModuleCodes(modPath string) (*moduleCodes, error) {
	mc := newModuleCodes()

	errorsPath := filepath.Join(modPath, "types", "errors.go")
	if exists, statErr := fileExists(errorsPath); statErr != nil {
		return nil, statErr
	} else if exists {
		codes, err := parseErrorCodeMethods(errorsPath)
		if err != nil {
			return nil, err
		}

		mc.typeCodes = codes
	}

	deserPath := filepath.Join(modPath, "deserializers.go")
	if exists, statErr := fileExists(deserPath); statErr != nil {
		return nil, statErr
	} else if exists {
		codes, opFuncs, matchedOpFuncs, err := parseDeserializerCodes(deserPath)
		if err != nil {
			return nil, err
		}

		mc.deserCodes = codes
		mc.opFuncs = opFuncs
		mc.matchedOpFuncs = matchedOpFuncs
	}

	return mc, nil
}

// sparselyModeledThreshold is the matchedOpFuncs/opFuncs ratio below which
// a module is too sparsely modeled for a CONFIDENT finding -- see
// moduleCodes's doc comment for the s3 (18%) vs. everything-else (69-100%)
// measurement this threshold sits between.
const sparselyModeledThreshold = 0.5

func (mc *moduleCodes) sparselyModeled() bool {
	return mc.opFuncs > 0 &&
		float64(mc.matchedOpFuncs)/float64(mc.opFuncs) < sparselyModeledThreshold
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}

	return err == nil, err
}

// parseErrorCodeMethods reads every `func (e *X) ErrorCode() string { ... }`
// in errorsGoPath and collects the string literal(s) it can directly
// return. Real codegen returns the override branch as `*e.ErrorCodeOverride`
// (a pointer deref, never a literal) and the fallback branch as a bare
// string literal -- only the latter is ever collected, so this needs no
// hardcoded assumption about the surrounding if-shape and survives codegen
// drift across SDK versions.
func parseErrorCodeMethods(errorsGoPath string) (map[string]bool, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, errorsGoPath, nil, 0)
	if err != nil {
		return nil, err
	}

	codes := map[string]bool{}

	for _, decl := range f.Decls {
		fd, isFD := decl.(*ast.FuncDecl)
		if !isFD || fd.Recv == nil || fd.Name.Name != "ErrorCode" || fd.Body == nil {
			continue
		}

		ast.Inspect(fd.Body, func(n ast.Node) bool {
			ret, isRet := n.(*ast.ReturnStmt)
			if !isRet || len(ret.Results) != 1 {
				return true
			}

			if lit, litOK := ret.Results[0].(*ast.BasicLit); litOK && lit.Kind == token.STRING {
				if v, uqErr := strconv.Unquote(lit.Value); uqErr == nil {
					codes[v] = true
				}
			}

			return true
		})
	}

	return codes, nil
}

// parseDeserializerCodes reads every function in deserializersGoPath whose
// name contains "deserializeOpError" and collects the literal from every
// `strings.EqualFold("Foo", errorCode)` case inside it -- the same
// case-clause shape confirmed live across ecs@v1.90.0 (awsjson1.1) and
// iam@v1.58.1 (awsquery), so this needs no protocol-specific branch. It
// also counts opFuncs (how many such functions exist) and matchedOpFuncs
// (how many contain at least one such case, rather than falling straight
// through to smithy.GenericAPIError for every code) -- moduleCodes's
// sparselyModeled uses the ratio to keep a service like s3, whose
// deserializer models almost nothing, out of the confident tier.
func parseDeserializerCodes(deserGoPath string) (map[string]bool, int, int, error) {
	fset := token.NewFileSet()

	f, err := parser.ParseFile(fset, deserGoPath, nil, 0)
	if err != nil {
		return nil, 0, 0, err
	}

	codes := map[string]bool{}

	opFuncs, matchedOpFuncs := 0, 0

	for _, decl := range f.Decls {
		fd, isFD := decl.(*ast.FuncDecl)
		if !isFD || fd.Body == nil || !strings.Contains(fd.Name.Name, "deserializeOpError") {
			continue
		}

		opFuncs++

		if opErrorFuncCodes(fd, codes) {
			matchedOpFuncs++
		}
	}

	return codes, opFuncs, matchedOpFuncs, nil
}

// opErrorFuncCodes collects every EqualFold code literal in fd's body into
// codes and reports whether it found at least one.
func opErrorFuncCodes(fd *ast.FuncDecl, codes map[string]bool) bool {
	matched := false

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if lit, litOK := equalFoldCodeLiteral(n); litOK {
			codes[lit] = true
			matched = true
		}

		return true
	})

	return matched
}

// equalFoldCodeLiteral reports the literal first argument of a
// strings.EqualFold(<literal>, <ident>) call, the shape every
// deserializeOpError* switch case uses.
func equalFoldCodeLiteral(n ast.Node) (string, bool) {
	call, ok := n.(*ast.CallExpr)
	if !ok || len(call.Args) != 2 {
		return "", false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "EqualFold" {
		return "", false
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok || pkgIdent.Name != "strings" {
		return "", false
	}

	lit, ok := call.Args[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}

	v, err := strconv.Unquote(lit.Value)

	return v, err == nil
}

// serviceGroundTruth is the codes a service's resolved SDK module(s)
// actually model. resolvedModules counts every distinct pinned SDK module
// this service dir's imports resolve to (resolveServiceModules, test files
// included) that exists on disk -- whether or not that module happens to
// contribute any codes. codeModules counts only those that do. scan.go
// skips a service with codeModules == 0 (no ground truth to check against
// at all -- ec2's documented case: 785 operations, zero typed exceptions,
// and its own deserializeOpError* switches carry no EqualFold cases
// either, confirmed live against ec2@v1.319.1) and demotes a finding to
// needs-review whenever resolvedModules > 1, since which module's
// exception set actually applies at a given emission site is then unknown
// -- the same ambiguity cmd/enumcheck treats as an "ambiguous key".
// resolvedModules, not codeModules, is what gates this: services/ec2's own
// non-test files import only ec2, but one *_test.go file also imports
// outposts (an unrelated cross-service integration test) -- ec2 alone
// contributes zero ground truth, so without counting outposts too, a
// finding would be silently checked against outposts's exception set
// instead, exactly the "module resolution picks the wrong SDK" risk this
// tool's brief warned about. sparse is true when any resolved module is
// too thinly modeled (moduleCodes.sparselyModeled) to support a confident
// absence claim -- s3's own case.
type serviceGroundTruth struct {
	codes           map[string]bool
	resolvedModules int
	codeModules     int
	sparse          bool
}

func buildServiceGroundTruth(
	cache string,
	mods []string,
	goModVersions map[string]string,
) (*serviceGroundTruth, error) {
	gt := &serviceGroundTruth{codes: map[string]bool{}}

	for _, mod := range mods {
		ver, ok := goModVersions[mod]
		if !ok {
			continue
		}

		modPath := filepath.Join(
			cache,
			"github.com",
			"aws",
			"aws-sdk-go-v2",
			"service",
			mod+"@"+ver,
		)

		exists, statErr := fileExists(modPath)
		if statErr != nil {
			return nil, statErr
		}

		if !exists {
			continue
		}

		gt.resolvedModules++

		mc, err := loadModuleCodes(modPath)
		if err != nil {
			return nil, err
		}

		if len(mc.typeCodes) == 0 && len(mc.deserCodes) == 0 {
			continue
		}

		gt.codeModules++

		if mc.sparselyModeled() {
			gt.sparse = true
		}

		for c := range mc.typeCodes {
			gt.codes[c] = true
		}

		for c := range mc.deserCodes {
			gt.codes[c] = true
		}
	}

	return gt, nil
}
