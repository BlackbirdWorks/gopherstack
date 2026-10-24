// Command ccwiregen writes internal/app/cloudcontrol_wire_*_gen.go: the request and response structs and typed
// methods the Cloud Control handlers use to call in-process services over sdkwire, copied from the pinned
// aws-sdk-go-v2 service models in the module cache. Run `go run ./cmd/ccwiregen` from the repo root after an SDK
// bump or when a handler needs another operation; a test fails when the committed files are stale.
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"log"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	sdkModule  = "github.com/aws/aws-sdk-go-v2/service/"
	outDir     = "internal/app"
	fileMode   = 0o600
	dirMode    = 0o750
	wireDir    = "internal/awswire"
	opTag      = "TagResource"
	opUntag    = "UntagResource"
	opListTags = "ListTagsForResource"
	typesAlias = "types"
)

var errMissingShape = errors.New("operation shape not found")

type spec struct {
	module, file, alias, prefix string
	ops                         []string
	// unions names interface (union) types whose members are dropped from requests and responses.
	unions []string
	// pages lists the operations that get a paginator (pkg mode only).
	pages []string
	// pkg selects the exported per-service package under internal/awswire instead of the app wrappers.
	pkg bool
}

func specs() []spec {
	return []spec{
		{
			module: "apigateway", file: "apigateway", alias: "apigwtypes", prefix: "wireAPIGW",
			ops: []string{
				"CreateRestApi", "DeleteRestApi", "GetRestApi", "GetRestApis", "ImportRestApi", opTag,
				opUntag, "UpdateRestApi",
			}},
		{
			module: "cognitoidentityprovider", file: "cognitoidp", alias: "cognitotypes", prefix: "wireCognito",
			ops: []string{
				"CreateUserPool", "DeleteUserPool", "DescribeUserPool", "GetUserPoolMfaConfig", "ListUserPools",
				"SetUserPoolMfaConfig", "UpdateUserPool",
			}},
		{
			module: "ecr", file: "ecr", alias: "ecrtypes", prefix: "wireECR",
			ops: []string{
				"CreateRepository", "DeleteRepository", "DescribeRepositories", opListTags,
				"PutImageScanningConfiguration", "PutImageTagMutability", opTag, opUntag,
			}},
		{
			module: "ecs", file: "ecs", alias: "ecstypes", prefix: "wireECS",
			ops: []string{
				"CreateCluster", "DeleteCluster", "DescribeClusters", "ListClusters", "PutClusterCapacityProviders",
				"RunTask", opTag, opUntag, "UpdateCluster",
			}},
		{
			module: "efs", file: "efs", alias: "efstypes", prefix: "wireEFS",
			ops: []string{
				"CreateFileSystem", "CreateReplicationConfiguration", "DeleteFileSystem",
				"DeleteReplicationConfiguration", "DescribeBackupPolicy", "DescribeFileSystemPolicy",
				"DescribeFileSystems", "DescribeLifecycleConfiguration", "DescribeReplicationConfigurations",
				"PutBackupPolicy", "PutFileSystemPolicy", "PutLifecycleConfiguration", opTag, opUntag,
				"UpdateFileSystem", "UpdateFileSystemProtection",
			}},
		{
			module: "elasticloadbalancingv2", file: "elbv2", alias: "elbv2types", prefix: "wireELBv2",
			ops: []string{
				"AddTags", "CreateTargetGroup", "DeleteTargetGroup", "DeregisterTargets", "DescribeTags",
				"DescribeTargetGroupAttributes", "DescribeTargetGroups", "DescribeTargetHealth",
				"ModifyTargetGroupAttributes", "ModifyTargetGroup", "RegisterTargets", "RemoveTags",
			}},
		{
			module: "glue", file: "glue", alias: "gluetypes", prefix: "wireGlue",
			ops: []string{
				"CreateDatabase", "DeleteDatabase", "GetDatabase", "GetDatabases", "UpdateDatabase",
			}},
	}
}

func (s spec) outFile() string {
	if s.pkg {
		return filepath.Join(wireDir, s.module, "api_gen.go")
	}

	return filepath.Join(outDir, "cloudcontrol_wire_"+s.file+"_gen.go")
}

func allSpecs() []spec {
	return append(specs(), wireSpecs()...)
}

func main() {
	for _, s := range allSpecs() {
		src, err := render(s)
		if err != nil {
			log.Fatalf("%s: %v", s.module, err)
		}

		if err = os.MkdirAll(filepath.Dir(s.outFile()), dirMode); err != nil {
			log.Fatal(err)
		}

		if err = os.WriteFile(s.outFile(), src, fileMode); err != nil {
			log.Fatal(err)
		}
	}
}

func moduleDir(module string) (string, error) {
	out, err := exec.CommandContext( //nolint:gosec // fixed go list invocation with a module from the spec table
		context.Background(), "go", "list", "-m", "-f", "{{.Dir}}", sdkModule+module,
	).Output()
	if err != nil {
		return "", fmt.Errorf("locate %s: %w", module, err)
	}

	return strings.TrimSpace(string(out)), nil
}

func render(s spec) ([]byte, error) {
	dir, err := moduleDir(s.module)
	if err != nil {
		return nil, err
	}

	imports := s.baseImports()

	var body bytes.Buffer

	if err = s.writeBody(&body, dir, imports); err != nil {
		return nil, err
	}

	return format.Source(append(s.header(imports), body.Bytes()...))
}

func (s spec) baseImports() map[string]string {
	imports := map[string]string{sdkModule + s.module + "/types": s.alias, "context": ""}

	if s.pkg {
		imports["github.com/aws/aws-sdk-go-v2/aws"] = ""
		imports["github.com/blackbirdworks/gopherstack/internal/awswire"] = ""
	}

	return imports
}

func (s spec) writeBody(body *bytes.Buffer, dir string, imports map[string]string) error {
	for _, op := range s.ops {
		if err := writeOp(body, dir, s, op, imports); err != nil {
			return err
		}
	}

	if !s.pkg {
		return nil
	}

	if err := checkNoUnions(dir, s, body.String()); err != nil {
		return err
	}

	for _, op := range s.pages {
		if err := writePaginator(body, dir, op); err != nil {
			return err
		}
	}

	return nil
}

func (s spec) header(imports map[string]string) []byte {
	var hdr bytes.Buffer

	pkgName := "app"
	if s.pkg {
		pkgName = s.module
	}

	fmt.Fprintf(&hdr, "// Code generated by cmd/ccwiregen; DO NOT EDIT.\n\npackage %s\n\nimport (\n", pkgName)

	for _, p := range slices.Sorted(maps.Keys(imports)) {
		if a := imports[p]; a != "" {
			fmt.Fprintf(&hdr, "\t%s %q\n", a, p)
		} else {
			fmt.Fprintf(&hdr, "\t%q\n", p)
		}
	}

	if !s.pkg {
		fmt.Fprintf(&hdr, ")\n\ntype %s struct{ *ccWireClient }\n\n", s.prefix)

		return hdr.Bytes()
	}

	quoted := make([]string, 0, len(s.unions))
	for _, u := range s.unions {
		quoted = append(quoted, ", "+strconv.Quote(u))
	}

	strip := strings.Join(quoted, "")

	fmt.Fprintf(&hdr, clientTemplate, s.module, strip)

	return hdr.Bytes()
}

const clientTemplate = `)

// Options configures a Client.
type Options = awswire.Options

// Client calls %[1]s over its wire protocol.
type Client struct{ core *awswire.Core }

// NewFromConfig returns a Client for cfg.
func NewFromConfig(cfg aws.Config, optFns ...func(*Options)) *Client {
	return &Client{core: awswire.New(%[1]q, cfg, optFns%[2]s)}
}

`

func writeOp(body *bytes.Buffer, dir string, s spec, op string, imports map[string]string) error {
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(dir, "api_op_"+op+".go"), nil, 0)
	if err != nil {
		return err
	}

	fileImports := map[string]string{}

	for _, im := range f.Imports {
		p := strings.Trim(im.Path.Value, `"`)

		name := filepath.Base(p)
		if im.Name != nil {
			name = im.Name.Name
		}

		fileImports[name] = p
	}

	for _, suffix := range []string{"Input", "Output"} {
		decl, derr := structDecl(f, s, op, suffix, fileImports, imports)
		if derr != nil {
			return derr
		}

		body.WriteString(decl + "\n\n")
	}

	if s.pkg {
		fmt.Fprintf(body,
			"// %[1]s calls the %[1]s operation.\n"+
				"func (c *Client) %[1]s(ctx context.Context, in *%[1]sInput) (*%[1]sOutput, error) {\n"+
				"\treturn awswire.Do[%[1]sOutput](ctx, c.core, %[1]q, in)\n}\n\n", op)

		return nil
	}

	fmt.Fprintf(body,
		"func (c *%[1]s) %[2]s(ctx context.Context, in *%[1]s%[2]sInput) (*%[1]s%[2]sOutput, error) {\n"+
			"\treturn wireCall[%[1]s%[2]sOutput](ctx, c.ccWireClient, %[2]q, in)\n}\n\n", s.prefix, op)

	return nil
}

// structDecl prints the op's Input or Output struct with the members the wire bridge keeps.
func structDecl(f *ast.File, s spec, op, suffix string, fileImports, imports map[string]string) (string, error) {
	gd := findType(f, op+suffix)
	if gd == nil {
		return "", fmt.Errorf("%w: %s%s", errMissingShape, op, suffix)
	}

	ts, _ := gd.Specs[0].(*ast.TypeSpec)
	st, _ := ts.Type.(*ast.StructType)

	var fields []*ast.Field

	for _, fld := range st.Fields.List {
		if len(fld.Names) == 0 || !fld.Names[0].IsExported() || fld.Names[0].Name == "ResultMetadata" ||
			isUnionField(fld.Type, s.unions) {
			continue
		}

		fld.Doc, fld.Comment = nil, nil
		rewriteSelectors(fld.Type, s.alias, fileImports, imports)

		if suffix == "Input" && (omitEmpty(fld.Type) || s.pkg && omitZeroScalar(fld.Type)) {
			fld.Tag = &ast.BasicLit{Kind: token.STRING, Value: "`json:\",omitempty\"`"}
		}

		fields = append(fields, fld)
	}

	st.Fields = &ast.FieldList{List: fields}
	ts.Name.Name = s.typeName(op + suffix)
	ts.Doc, gd.Doc = nil, nil

	var b bytes.Buffer
	if err := printer.Fprint(&b, token.NewFileSet(), gd); err != nil {
		return "", err
	}

	return b.String(), nil
}

func (s spec) typeName(n string) string {
	if s.pkg {
		return n
	}

	return s.prefix + n
}

// omitZeroScalar marks plain bool and numeric members, which the SDK sends only when set.
func omitZeroScalar(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	if !ok {
		return false
	}

	switch id.Name {
	case "bool", "int", "int8", "int16", "int32", "int64", "float32", "float64":
		return true
	default:
		return false
	}
}

func findType(f *ast.File, name string) *ast.GenDecl {
	for _, d := range f.Decls {
		gd, ok := d.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}

		if ts, isType := gd.Specs[0].(*ast.TypeSpec); isType && ts.Name.Name == name {
			return gd
		}
	}

	return nil
}

// omitEmpty marks plain strings and enum selectors so unset members are not sent as "".
func omitEmpty(e ast.Expr) bool {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name == "string"
	case *ast.SelectorExpr:
		return true
	default:
		return false
	}
}

func rewriteSelectors(e ast.Expr, alias string, fileImports, imports map[string]string) {
	ast.Inspect(e, func(n ast.Node) bool {
		se, ok := n.(*ast.SelectorExpr)
		if !ok {
			return true
		}

		if id, isIdent := se.X.(*ast.Ident); isIdent {
			if id.Name == typesAlias {
				id.Name = alias
			} else if p, known := fileImports[id.Name]; known {
				imports[p] = ""
			}
		}

		return true
	})
}

func isUnionField(e ast.Expr, unions []string) bool {
	if st, ok := e.(*ast.StarExpr); ok {
		e = st.X
	}

	if at, ok := e.(*ast.ArrayType); ok {
		e = at.Elt
	}

	se, ok := e.(*ast.SelectorExpr)

	return ok && slices.Contains(unions, se.Sel.Name)
}
