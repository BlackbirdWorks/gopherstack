package guards_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type tagShape = string

const (
	shapeList  tagShape = "list"
	shapeMap   tagShape = "map"
	shapeOther tagShape = "other"
)

var (
	sdkImportRe = regexp.MustCompile(`"github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)"`)
	sdkModRe    = regexp.MustCompile(`(?m)^\s*github\.com/aws/aws-sdk-go-v2/service/([a-z0-9]+)\s+(v\S+)`)
	sdkFuncRe   = regexp.MustCompile(
		`(?m)^func (awsAwsjson1[01]_serialize\w+)\(v ([^,]+), value smithyjson\.Value\) error \{\n`)
	sdkTagKeyRe = regexp.MustCompile(
		`ok := object\.Key\("(\w+)"\)\n\s*if err := (awsAwsjson1[01]_serializeDocument\w+)\(`)
	sdkTagScalar = regexp.MustCompile(`ok := object\.Key\("(\w*[Tt]ag\w*)"\)`)
)

var wireFileRe = regexp.MustCompile(`^(handler|wire)`)

func isTagKey(name string) bool {
	l := strings.ToLower(name)

	return strings.HasSuffix(l, "tags") || strings.HasSuffix(l, "taglist") || l == "tag"
}

// sdkAwsjsonTagShapes maps an SDK package to the JSON key -> shapes the awsjson
// serializer emits for tag-like keys. Non-awsjson packages are absent.
func sdkAwsjsonTagShapes(modCache, pkg, version string) map[string]map[tagShape]bool {
	base := filepath.Join(modCache, "github.com", "aws", "aws-sdk-go-v2", "service", pkg+"@")

	src, err := os.ReadFile(base + version + "/serializers.go")
	if err != nil {
		// pinned version not downloaded locally: fall back to any cached one
		cached, _ := filepath.Glob(base + "*/serializers.go")
		if len(cached) == 0 {
			return nil
		}

		sort.Strings(cached)

		if src, err = os.ReadFile(cached[len(cached)-1]); err != nil {
			return nil
		}
	}

	text := string(src)
	docShape := map[string]tagShape{}

	for _, m := range sdkFuncRe.FindAllStringSubmatch(text, -1) {
		switch {
		case strings.HasPrefix(m[2], "[]"):
			docShape[m[1]] = shapeList
		case strings.HasPrefix(m[2], "map["):
			docShape[m[1]] = shapeMap
		default:
			docShape[m[1]] = shapeOther
		}
	}

	out := map[string]map[tagShape]bool{}

	for _, m := range sdkTagKeyRe.FindAllStringSubmatch(text, -1) {
		if !isTagKey(m[1]) {
			continue
		}

		if out[m[1]] == nil {
			out[m[1]] = map[tagShape]bool{}
		}

		out[m[1]][docShape[m[2]]] = true
	}

	for _, m := range sdkTagScalar.FindAllStringSubmatch(text, -1) {
		if out[m[1]] == nil {
			out[m[1]] = map[tagShape]bool{shapeOther: true}
		}
	}

	return out
}

type handlerTagField struct {
	file, strct, field, key string
	shape                   tagShape
}

func exprShape(e ast.Expr, named map[string]ast.Expr, custom map[string]bool, depth int) tagShape {
	switch x := e.(type) {
	case *ast.MapType:
		return shapeMap
	case *ast.ArrayType:
		return shapeList
	case *ast.StarExpr:
		return exprShape(x.X, named, custom, depth)
	case *ast.Ident:
		if custom[x.Name] {
			return shapeOther
		}

		if def, ok := named[x.Name]; ok && depth < 5 {
			return exprShape(def, named, custom, depth+1)
		}
	}

	return shapeOther
}

func jsonKey(f *ast.Field, name string) string {
	if f.Tag == nil {
		return name
	}

	raw, err := strconv.Unquote(f.Tag.Value)
	if err != nil {
		return name
	}

	v, ok := reflectTag(raw, "json")
	if !ok {
		return name
	}

	return strings.Split(v, ",")[0]
}

func reflectTag(tag, key string) (string, bool) {
	m := regexp.MustCompile(key + `:"([^"]*)"`).FindStringSubmatch(tag)
	if m == nil {
		return "", false
	}

	return m[1], true
}

var decodeCallRe = regexp.MustCompile(`(?i)unmarshal|decode|readjson|readbody|readinput`)

func typeName(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.StarExpr:
		return typeName(x.X)
	case *ast.UnaryExpr:
		return typeName(x.X)
	case *ast.CompositeLit:
		return typeName(x.Type)
	case *ast.ArrayType:
		return typeName(x.Elt)
	case *ast.MapType:
		return typeName(x.Value)
	case *ast.CallExpr:
		if id, ok := x.Fun.(*ast.Ident); ok && id.Name == "new" && len(x.Args) == 1 {
			return typeName(x.Args[0])
		}
	}

	return ""
}

func calleeName(e ast.Expr) (string, []ast.Expr) {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name, nil
	case *ast.SelectorExpr:
		return x.Sel.Name, nil
	case *ast.IndexExpr:
		n, _ := calleeName(x.X)

		return n, []ast.Expr{x.Index}
	case *ast.IndexListExpr:
		n, _ := calleeName(x.X)

		return n, x.Indices
	}

	return "", nil
}

// decodedRoots returns type names passed to a decode/unmarshal-style call,
// via a local variable of that type or an explicit generic type argument.
func decodedRoots(files []*ast.File) map[string]bool {
	roots := map[string]bool{}

	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			locals := map[string]string{}

			if fd.Type.Params != nil {
				for _, p := range fd.Type.Params.List {
					for _, n := range p.Names {
						locals[n.Name] = typeName(p.Type)
					}
				}
			}

			ast.Inspect(fd.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.ValueSpec:
					for _, nm := range x.Names {
						if x.Type != nil {
							locals[nm.Name] = typeName(x.Type)
						}
					}
				case *ast.AssignStmt:
					if len(x.Lhs) == len(x.Rhs) {
						for i, l := range x.Lhs {
							if id, isID := l.(*ast.Ident); isID {
								if tn := typeName(x.Rhs[i]); tn != "" {
									locals[id.Name] = tn
								}
							}
						}
					}
				case *ast.CallExpr:
					name, targs := calleeName(x.Fun)
					if !decodeCallRe.MatchString(name) {
						return true
					}

					for _, ta := range targs {
						roots[typeName(ta)] = true
					}

					for _, a := range x.Args {
						if id, isID := unaddr(a).(*ast.Ident); isID {
							if tn := locals[id.Name]; tn != "" {
								roots[tn] = true
							}
						} else if tn := typeName(a); tn != "" {
							roots[tn] = true
						}
					}
				}

				return true
			})
		}
	}

	return roots
}

func unaddr(e ast.Expr) ast.Expr {
	if u, ok := e.(*ast.UnaryExpr); ok {
		return u.X
	}

	return e
}

// reachable closes roots over struct field and embedded type references.
func reachable(files []*ast.File, roots map[string]bool) map[string]bool {
	structs := map[string]*ast.StructType{}

	for _, f := range files {
		for _, d := range f.Decls {
			if gd, ok := d.(*ast.GenDecl); ok {
				for _, s := range gd.Specs {
					if ts, isTS := s.(*ast.TypeSpec); isTS {
						if st, isSt := ts.Type.(*ast.StructType); isSt {
							structs[ts.Name.Name] = st
						}
					}
				}
			}
		}
	}

	seen := map[string]bool{}
	queue := []string{}

	for r := range roots {
		queue = append(queue, r)
	}

	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]

		st, ok := structs[n]
		if !ok || seen[n] {
			continue
		}

		seen[n] = true

		for _, fld := range st.Fields.List {
			if tn := typeName(fld.Type); tn != "" {
				queue = append(queue, tn)
			}
		}
	}

	return seen
}

func scanHandlerTagFields(t *testing.T, dir string) []handlerTagField {
	t.Helper()

	var (
		files []*ast.File
		names []string
	)

	fset := token.NewFileSet()

	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !isGuardedSource(path) || strings.HasPrefix(d.Name(), "persistence") {
			return nil
		}

		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}

		files = append(files, f)
		names = append(names, path)

		return nil
	}))

	named := map[string]ast.Expr{}
	custom := map[string]bool{}

	for _, f := range files {
		for _, d := range f.Decls {
			if fd, isFn := d.(*ast.FuncDecl); isFn && fd.Recv != nil && fd.Name.Name == "UnmarshalJSON" {
				custom[typeName(fd.Recv.List[0].Type)] = true
			}

			gd, ok := d.(*ast.GenDecl)
			if !ok {
				continue
			}

			for _, s := range gd.Specs {
				if ts, isTS := s.(*ast.TypeSpec); isTS {
					if _, isStruct := ts.Type.(*ast.StructType); !isStruct {
						named[ts.Name.Name] = ts.Type
					}
				}
			}
		}
	}

	reach := reachable(files, decodedRoots(files))

	var out []handlerTagField

	for i, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}

			st, ok := ts.Type.(*ast.StructType)
			if !ok || (!reach[ts.Name.Name] && !wireFileRe.MatchString(filepath.Base(names[i]))) {
				return true
			}

			for _, fld := range st.Fields.List {
				for _, nm := range fld.Names {
					key := jsonKey(fld, nm.Name)
					if key == "-" || !isTagKey(key) {
						continue
					}

					out = append(out, handlerTagField{
						file: filepath.ToSlash(names[i]), strct: ts.Name.Name, field: nm.Name,
						key: key, shape: exprShape(fld.Type, named, custom, 0),
					})
				}
			}

			return true
		})
	}

	return out
}

// dirAwsjsonShapes unions the tag shapes of every awsjson SDK package the
// service directory imports, and records the shapes of non-awsjson ones apart.
func dirSDKPackages(t *testing.T, dir string) map[string]bool {
	t.Helper()

	pkgs := map[string]bool{}

	require.NoError(t, filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") {
			return err
		}

		src, rerr := os.ReadFile(path)
		if rerr != nil {
			return rerr
		}

		for _, m := range sdkImportRe.FindAllStringSubmatch(string(src), -1) {
			pkgs[m[1]] = true
		}

		return nil
	}))

	return pkgs
}

type sdkShapes struct {
	awsjson map[string]map[string]map[tagShape]bool
	all     map[string]bool
}

func loadSDKShapes() (sdkShapes, error) {
	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		return sdkShapes{}, err
	}

	modCache := strings.TrimSpace(string(out))

	gomod, err := os.ReadFile("go.mod")
	if err != nil {
		return sdkShapes{}, err
	}

	res := sdkShapes{awsjson: map[string]map[string]map[tagShape]bool{}, all: map[string]bool{}}

	for _, m := range sdkModRe.FindAllStringSubmatch(string(gomod), -1) {
		res.all[m[1]] = true

		if s := sdkAwsjsonTagShapes(modCache, m[1], m[2]); s != nil {
			res.awsjson[m[1]] = s
		}
	}

	return res, nil
}

var responseNameRe = regexp.MustCompile(`(?i)(output|resp|response|result)$`)

// notRequestBodies lists struct fields the scan matches but that are never
// json-decoded from an awsjson request body (key: "<file>: <Struct>.<Field>").
func notRequestBodies() map[string]string {
	return map[string]string{
		"services/acm/handler_resource_tags.go: TaggedEntry.Tags":                      "dashboard type, not decoded",
		"services/kms/handler_tags.go: TaggedKeyInfo.Tags":                             "dashboard type, not decoded",
		"services/sagemaker/handler_monitoring_job_definitions.go: jobDefRequest.Tags": "set by parseJobDefRequest",
		"services/eventbridge/models.go: CreateRegistryInput.Tags":                     "Schemas is restjson",
		"services/eventbridge/models.go: CreateSchemaInput.Tags":                       "Schemas is restjson",
	}
}

// dirTagOffenders checks request-side tag fields of one service directory
// against the awsjson SDK packages in pkgs.
func dirTagOffenders(t *testing.T, dir string, sdk sdkShapes, pkgs map[string]bool) []string {
	t.Helper()

	union := map[string]map[tagShape]bool{}
	hasJSON := false

	for pkg := range pkgs {
		s, ok := sdk.awsjson[pkg]
		if !ok {
			continue
		}

		hasJSON = true

		for k, v := range s {
			if union[k] == nil {
				union[k] = map[tagShape]bool{}
			}

			for sh := range v {
				union[k][sh] = true
			}
		}
	}

	if !hasJSON {
		return nil
	}

	var offenders []string

	for _, h := range scanHandlerTagFields(t, dir) {
		want := union[h.key]
		if len(want) == 0 || want[h.shape] || h.shape == shapeOther ||
			(len(want) == 1 && want[shapeOther]) || responseNameRe.MatchString(h.strct) {
			continue
		}

		if _, skip := notRequestBodies()[h.file+": "+h.strct+"."+h.field]; skip {
			continue
		}

		offenders = append(offenders,
			h.file+": "+h.strct+"."+h.field+" (json "+h.key+") is "+h.shape+", awsjson SDK emits "+shapeSet(want))
	}

	return offenders
}

// TestAwsjsonTagFieldShapesMatchSDK fails when a handler request struct
// declares a tag field as a map where the pinned awsjson SDK serializer sends
// a [{Key,Value}] list (or the reverse): json.Unmarshal then fails and every
// tagged call returns 500.
func TestAwsjsonTagFieldShapesMatchSDK(t *testing.T) {
	t.Parallel()

	sdk, err := loadSDKShapes()
	require.NoError(t, err)
	require.NotEmpty(t, sdk.awsjson, "no awsjson SDK packages resolved from the module cache")

	entries, err := os.ReadDir("services")
	require.NoError(t, err)

	var offenders []string

	for _, e := range entries {
		if !e.IsDir() || skipGuardDir(e.Name()) {
			continue
		}

		dir := filepath.Join("services", e.Name())
		pkgs := dirSDKPackages(t, dir)

		if sdk.all[e.Name()] {
			pkgs = map[string]bool{e.Name(): true}
		}

		offenders = append(offenders, dirTagOffenders(t, dir, sdk, pkgs)...)
	}

	sort.Strings(offenders)
	require.Empty(t, offenders, "awsjson handler tag fields disagree with the SDK serializer")
}

// TestAwsjsonTagGuardDetectsMapTags is the positive control: the EventBridge
// PutRule shape from before c8d2e48c3 must be flagged.
func TestAwsjsonTagGuardDetectsMapTags(t *testing.T) {
	t.Parallel()

	sdk, err := loadSDKShapes()
	require.NoError(t, err)

	got := dirTagOffenders(t, "test/guards/testdata/awsjson_tags/map_tags", sdk, map[string]bool{"eventbridge": true})
	require.Len(t, got, 1)
	require.Contains(t, got[0], "putRuleInput.Tags")

	ok := dirTagOffenders(t, "test/guards/testdata/awsjson_tags/list_tags", sdk, map[string]bool{"eventbridge": true})
	require.Empty(t, ok)
}

func shapeSet(m map[tagShape]bool) string {
	s := make([]string, 0, len(m))

	for k := range m {
		s = append(s, k)
	}

	sort.Strings(s)

	return strings.Join(s, "|")
}
