package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// deserializeOpRe matches a HandleDeserialize receiver type name like
// "awsRestjson1_deserializeOpCreateMesh" or "awsAwsjson11_deserializeOpListStreams",
// splitting it into the protocol prefix and the operation name.
var deserializeOpRe = regexp.MustCompile(`^(aws\w+)_deserializeOp([A-Z]\w*)$`)

// serviceIndex holds everything parsed from one SDK module's deserializers.go
// needed to classify every operation it declares.
type serviceIndex struct {
	fset            *token.FileSet
	funcsByName     map[string]*ast.FuncDecl
	handleDeser     map[string]*ast.FuncDecl // op -> HandleDeserialize func
	protocolByOp    map[string]string        // op -> protocol prefix (e.g. "awsRestjson1")
	outputFieldsDir string                   // modPath, for lazy Output-struct field lookups
}

func indexDeserializers(modPath string) (*serviceIndex, error) {
	fset := token.NewFileSet()

	src, err := os.ReadFile(filepath.Join(modPath, "deserializers.go"))
	if err != nil {
		return nil, err
	}

	f, err := parser.ParseFile(fset, "deserializers.go", src, 0)
	if err != nil {
		return nil, err
	}

	idx := &serviceIndex{
		fset:            fset,
		funcsByName:     map[string]*ast.FuncDecl{},
		handleDeser:     map[string]*ast.FuncDecl{},
		protocolByOp:    map[string]string{},
		outputFieldsDir: modPath,
	}

	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}

		if fd.Recv == nil {
			idx.funcsByName[fd.Name.Name] = fd

			continue
		}

		idx.indexMethod(fd)
	}

	return idx, nil
}

func (idx *serviceIndex) indexMethod(fd *ast.FuncDecl) {
	if fd.Name.Name != "HandleDeserialize" {
		return
	}

	recvType := recvTypeName(fd)

	m := deserializeOpRe.FindStringSubmatch(recvType)
	if m == nil {
		return
	}

	idx.protocolByOp[m[2]] = m[1]
	idx.handleDeser[m[2]] = fd
}

func recvTypeName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) != 1 {
		return ""
	}

	t := fd.Recv.List[0].Type
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	id, ok := t.(*ast.Ident)
	if !ok {
		return ""
	}

	return id.Name
}

func (idx *serviceIndex) ops() []string {
	ops := make([]string, 0, len(idx.handleDeser))
	for op := range idx.handleDeser {
		ops = append(ops, op)
	}

	sort.Strings(ops)

	return ops
}

// outputFieldCount parses services/<mod>/api_op_<op>.go and counts the real
// fields of "<op>Output" -- everything except the embedded
// noSmithyDocumentSerde marker and the ResultMetadata member every Output
// struct carries. Zero real fields means the op is void.
func outputFieldCount(fset *token.FileSet, modPath, op string) (int, error) {
	path := filepath.Join(modPath, "api_op_"+op+".go")

	f, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		return 0, err
	}

	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		if n, found := findOutputStruct(gd, op); found {
			return n, nil
		}
	}

	return 0, os.ErrNotExist
}

func findOutputStruct(gd *ast.GenDecl, op string) (int, bool) {
	for _, spec := range gd.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok || ts.Name.Name != op+"Output" {
			continue
		}

		st, ok := ts.Type.(*ast.StructType)
		if !ok {
			continue
		}

		return countRealFields(st), true
	}

	return 0, false
}

func countRealFields(st *ast.StructType) int {
	n := 0

	for _, field := range st.Fields.List {
		if len(field.Names) == 0 {
			if id, ok := field.Type.(*ast.Ident); ok && id.Name == "noSmithyDocumentSerde" {
				continue
			}

			n++

			continue
		}

		if len(field.Names) == 1 && field.Names[0].Name == "ResultMetadata" {
			continue
		}

		n += len(field.Names)
	}

	return n
}

func trimQuotes(s string) string { return strings.Trim(s, "\"`") }
