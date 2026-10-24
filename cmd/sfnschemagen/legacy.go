package main

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/sdkwire/ir"
)

// legacyProtocol names the generated-function prefix and wire family of a
// service whose pinned SDK predates schema-based serde.
type legacyProtocol struct {
	name, prefix, ctor string
}

var (
	reSerShape    = regexp.MustCompile(`^serializeDocument(\w+)$`)
	reSerOp       = regexp.MustCompile(`^serializeOpDocument(\w+)Input$`)
	reDeShape     = regexp.MustCompile(`^deserializeDocument(\w+)$`)
	reDeOp        = regexp.MustCompile(`^deserializeOpDocument(\w+)Output$`)
	reDeCall      = regexp.MustCompile(`deserializeDocument(\w+?)(Unwrapped)?\(`)
	reSerCall     = regexp.MustCompile(`serializeDocument(\w+)\(`)
	reEqualFold   = regexp.MustCompile(`EqualFold\("([^"]+)"`)
	reKey         = regexp.MustCompile(`\.(Key|FlatKey)\("([^"]+)"\)`)
	reArray       = regexp.MustCompile(`value\.(Array|FlatArray)\(("[^"]*")?\)`)
	reMapNames    = regexp.MustCompile(`value\.(Map|FlatMap)\("([^"]*)", "([^"]*)"\)`)
	reUnexported  = regexp.MustCompile(`\bv\.[a-z]\w*`)
	reUnionMember = regexp.MustCompile(`Member(\w+)$`)
	reDeUnion     = regexp.MustCompile(`Member(\w+)\{`)
	reGoField     = regexp.MustCompile(`\bv\.([A-Z]\w*)`)
	reSVField     = regexp.MustCompile(`&?\bsv\.([A-Z]\w*)`)
)

// mapEntryClauses is the key and value clause of a map entry deserializer.
const mapEntryClauses = 2

type srcFile struct {
	fset  *token.FileSet
	funcs map[string]*ast.FuncDecl
}

func loadFuncs(dir, prefix string, files ...string) *srcFile {
	s := &srcFile{fset: token.NewFileSet(), funcs: map[string]*ast.FuncDecl{}}

	for _, f := range files {
		file, err := parser.ParseFile(s.fset, filepath.Join(dir, f), nil, parser.SkipObjectResolution)
		if err != nil {
			fatalf("parse %s: %v", f, err)
		}

		for _, d := range file.Decls {
			if fd, ok := d.(*ast.FuncDecl); ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, prefix) {
				s.funcs[strings.TrimPrefix(fd.Name.Name, prefix)] = fd
			}
		}
	}

	return s
}

func (s *srcFile) text(n ast.Node) string {
	var b bytes.Buffer
	if err := printer.Fprint(&b, s.fset, n); err != nil {
		fatalf("%v", err)
	}

	return b.String()
}

func (s *srcFile) firstParamType(fd *ast.FuncDecl) string {
	if len(fd.Type.Params.List) == 0 {
		return ""
	}

	return s.text(fd.Type.Params.List[0].Type)
}

// scalarOf classifies generated (de)serialization code by the conversions it
// performs.
func scalarOf(code string) string {
	has := func(subs ...string) bool {
		for _, sub := range subs {
			if strings.Contains(code, sub) {
				return true
			}
		}

		return false
	}

	switch {
	case has("base64.", "Base64EncodeBytes"):
		return ir.Blob
	case has("ParseBool", ".Boolean(", "value.(bool)"):
		return ir.Boolean
	case has("smithytime.", "ptr.Time("):
		return ir.Timestamp
	case has("ParseInt", "Int64()", ".Integer(", ".Long("):
		return intOf(code)
	case has("ParseFloat", "Float64()", ".Double(", ".Float("):
		return floatOf(code)
	default:
		return ir.String
	}
}

func intOf(code string) string {
	switch {
	case strings.Contains(code, "int8("):
		return ir.Byte
	case strings.Contains(code, "int16("):
		return ir.Short
	case strings.Contains(code, "int32("), strings.Contains(code, ".Integer("):
		return ir.Integer
	default:
		return ir.Long
	}
}

func floatOf(code string) string {
	if strings.Contains(code, ".Float(") || strings.Contains(code, "float32(") {
		return ir.Float
	}

	return ir.Double
}

type modelBuilder struct {
	model    *ir.Model
	protocol string
	// json selects JSON (de)serializer syntax, otherwise XML or form.
	json bool
	// wireNames names members by their wire key (awsJson), otherwise by the Go field.
	wireNames bool
}

func newModelBuilder(protocol, version string) *modelBuilder {
	return &modelBuilder{
		protocol:  protocol,
		json:      strings.HasPrefix(protocol, "awsjson") || protocol == protoRestJSON,
		wireNames: strings.HasPrefix(protocol, "awsjson"),
		model: &ir.Model{
			Protocol: protocol, Version: version,
			Shapes: map[string]*ir.Shape{}, Ops: map[string]*ir.Op{},
		},
	}
}

func (b *modelBuilder) shape(name, typ string) *ir.Shape {
	sh, ok := b.model.Shapes[name]
	if !ok {
		sh = &ir.Shape{Type: typ}
		b.model.Shapes[name] = sh
	}

	if sh.Type != typ {
		fatalf("shape %s is both %s and %s", name, sh.Type, typ)
	}

	return sh
}

func structMember(sh *ir.Shape, goName string) *ir.Member {
	for i := range sh.Members {
		if sh.Members[i].Name == goName {
			return &sh.Members[i]
		}
	}

	sh.Members = append(sh.Members, ir.Member{Name: goName})

	return &sh.Members[len(sh.Members)-1]
}

func setTarget(m *ir.Member, target, where string) {
	if m.Target != "" && m.Target != target {
		fatalf("%s: member %s targets %s and %s", where, m.Name, m.Target, target)
	}

	m.Target = target
}

func wireName(goName, wire string) string {
	if wire == goName {
		return ""
	}

	return wire
}

func kindOfType(t string) string {
	t = strings.TrimLeft(t, "*")

	switch {
	case strings.HasPrefix(t, "[]"):
		return ir.List
	case strings.HasPrefix(t, "map["):
		return ir.Map
	default:
		return ir.Struct
	}
}

// ---- query-style serializers (form encoded) ----

func (b *modelBuilder) addSerializers(src *srcFile) {
	for _, name := range sortedKeys(src.funcs) {
		fd := src.funcs[name]

		if shape := reSerShape.FindStringSubmatch(name); shape != nil {
			b.addSerShape(src, shape[1], fd)
		} else if op := reSerOp.FindStringSubmatch(name); op != nil {
			b.addSerStruct(src, op[1]+"Input", fd)
		}
	}
}

func (b *modelBuilder) addSerShape(src *srcFile, name string, fd *ast.FuncDecl) {
	if strings.Contains(src.text(fd.Body), "switch uv := v.(type)") {
		b.addSerUnion(src, name, fd)

		return
	}

	switch kindOfType(src.firstParamType(fd)) {
	case ir.Struct:
		b.addSerStruct(src, name, fd)
	case ir.List:
		b.addSerList(src, name, fd)
	case ir.Map:
		b.addSerMap(src, name, fd)
	}
}

func (b *modelBuilder) addSerStruct(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.Struct)

	for _, stmt := range fd.Body.List {
		var cond, body string

		switch st := stmt.(type) {
		case *ast.IfStmt:
			cond, body = src.text(st.Cond), src.text(st.Body)
		case *ast.BlockStmt:
			body = src.text(st)
			cond = body
		default:
			continue
		}

		ifs := stmt

		if reUnexported.MatchString(cond) {
			continue
		}

		field := reGoField.FindStringSubmatch(cond)
		key := reKey.FindStringSubmatch(body)

		if field == nil || key == nil {
			fatalf("%s: unrecognised member block:\n%s", name, src.text(ifs))
		}

		m := b.member(sh, field[1], key[2])
		if !b.wireNames {
			m.Ser = wireName(field[1], key[2])
		}

		setTarget(m, serTarget(body), name)

		if key[1] == "FlatKey" && b.protocol == protoQuery {
			m.Flat = true
		}
	}
}

func (b *modelBuilder) addSerUnion(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.Union)

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok || len(cc.List) != 1 {
			return true
		}

		var body bytes.Buffer
		for _, st := range cc.Body {
			body.WriteString(src.text(st))
			body.WriteByte('\n')
		}

		member := reUnionMember.FindStringSubmatch(src.text(cc.List[0]))
		key := reKey.FindStringSubmatch(body.String())

		if member == nil || key == nil {
			return true
		}

		m := b.member(sh, member[1], key[2])
		if !b.wireNames {
			m.Ser = wireName(member[1], key[2])
		}

		setTarget(m, serTarget(body.String()), name)

		return true
	})
}

func serTarget(body string) string {
	if c := reSerCall.FindStringSubmatch(body); c != nil {
		return c[1]
	}

	return scalarOf(body)
}

func (b *modelBuilder) addSerList(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.List)
	text := src.text(fd.Body)

	arr := reArray.FindStringSubmatch(text)
	if arr == nil {
		fatalf("%s: list without Array()", name)
	}

	item := &ir.Member{Target: serTarget(text)}
	if !b.json {
		item.Ser = strings.Trim(arr[2], `"`)
	}

	if sh.Item != nil {
		item.De = sh.Item.De
		item.Flat = sh.Item.Flat
	}

	sh.Item = item
}

func (b *modelBuilder) addSerMap(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.Map)
	text := src.text(fd.Body)

	val := &ir.Member{Target: serTarget(text)}
	key := &ir.Member{Target: ir.String}

	if !b.json {
		names := reMapNames.FindStringSubmatch(text)
		if names == nil {
			fatalf("%s: map without Map()", name)
		}

		val.Ser, key.Ser = names[3], names[2]
	}

	if sh.Key != nil {
		key.De, val.De = sh.Key.De, sh.Value.De
	}

	sh.Key, sh.Value = key, val
}

// ---- xml deserializers ----

func (b *modelBuilder) addDeserializers(src *srcFile) {
	for _, name := range sortedKeys(src.funcs) {
		fd := src.funcs[name]

		if m := reDeOp.FindStringSubmatch(name); m != nil {
			b.addDeStruct(src, m[1]+"Output", fd)

			continue
		}

		m := reDeShape.FindStringSubmatch(name)
		if m == nil ||
			strings.HasSuffix(m[1], "Unwrapped") &&
				src.funcs["deserializeDocument"+strings.TrimSuffix(m[1], "Unwrapped")] != nil {
			continue
		}

		b.addDeShape(src, m[1], fd)
	}
}

func (b *modelBuilder) addDeShape(src *srcFile, name string, fd *ast.FuncDecl) {
	switch kindOfType(src.firstParamType(fd)) {
	case ir.Struct:
		b.addDeStruct(src, name, fd)
	case ir.List:
		b.addDeList(src, name, fd)
	case ir.Map:
		b.addDeMap(src, name, fd)
	}
}

// clauses returns the member case clauses of a deserializer switch: EqualFold
// element names for XML, string keys for JSON.
func (b *modelBuilder) clauses(src *srcFile, fd *ast.FuncDecl) map[string]string {
	out := map[string]string{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		cc, ok := n.(*ast.CaseClause)
		if !ok || len(cc.List) != 1 {
			return true
		}

		var key string

		if lit, isLit := cc.List[0].(*ast.BasicLit); isLit && b.json && lit.Kind == token.STRING {
			key = strings.Trim(lit.Value, `"`)
		} else if m := reEqualFold.FindStringSubmatch(src.text(cc.List[0])); m != nil && !b.json {
			key = m[1]
		} else {
			return true
		}

		var body bytes.Buffer
		for _, st := range cc.Body {
			body.WriteString(src.text(st))
			body.WriteByte('\n')
		}

		out[key] = body.String()

		return true
	})

	return out
}

func (b *modelBuilder) addDeStruct(src *srcFile, name string, fd *ast.FuncDecl) {
	text := src.text(fd.Body)
	union := strings.Contains(text, "memberFound") || strings.Contains(text, "uv = &")

	typ := ir.Struct
	if union {
		typ = ir.Union
	}

	sh := b.shape(name, typ)
	cl := b.clauses(src, fd)

	for _, wire := range sortedKeys(cl) {
		body := cl[wire]

		re := reSVField
		if union {
			re = reDeUnion
		}

		field := re.FindStringSubmatch(body)
		if field == nil {
			fatalf("%s: clause %s assigns no field:\n%s", name, wire, body)
		}

		m := b.member(sh, field[1], wire)
		if !b.wireNames {
			m.De = wireName(field[1], wire)
		}

		if c := reDeCall.FindStringSubmatch(body); c != nil {
			setTarget(m, c[1], name)

			if c[2] != "" {
				m.Flat = true
			}
		} else {
			setTarget(m, scalarOf(body), name)
		}
	}
}

// member returns the struct member for a Go field and its wire name: JSON
// names the member by its wire key, the others by the Go field.
func (b *modelBuilder) member(sh *ir.Shape, goName, wire string) *ir.Member {
	if !b.wireNames {
		return structMember(sh, goName)
	}

	if upperFirst(wire) != goName {
		fatalf("wire key %q does not map to Go field %s", wire, goName)
	}

	return structMember(sh, wire)
}

func upperFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToUpper(s[:1]) + s[1:]
}

func (b *modelBuilder) addDeList(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.List)
	item := &ir.Member{}

	if b.json {
		item.Target = itemTarget(src.text(fd.Body))
	} else {
		cl := b.clauses(src, fd)
		if len(cl) != 1 {
			fatalf("%s: list with %d item clauses", name, len(cl))
		}

		for wire, body := range cl {
			item.De = wire
			item.Target = itemTarget(body)
		}
	}

	if sh.Item != nil {
		item.Ser = sh.Item.Ser
	}

	sh.Item = item
}

func itemTarget(body string) string {
	if c := reDeCall.FindStringSubmatch(body); c != nil {
		return c[1]
	}

	return scalarOf(body)
}

func (b *modelBuilder) addDeMap(src *srcFile, name string, fd *ast.FuncDecl) {
	sh := b.shape(name, ir.Map)
	key, val := &ir.Member{Target: ir.String}, &ir.Member{}

	if b.json {
		val.Target = itemTarget(src.text(fd.Body))
	} else {
		un, ok := src.funcs["deserializeDocument"+name+"Unwrapped"]
		if !ok {
			fatalf("%s: map without Unwrapped deserializer", name)
		}

		cl := b.clauses(src, un)
		if len(cl) != mapEntryClauses {
			fatalf("%s: map with %d key/value clauses", name, len(cl))
		}

		for wire, body := range cl {
			switch {
			case strings.Contains(body, "ek = "):
				key.De = wire
			case strings.Contains(body, "ev = "):
				val.De = wire
				val.Target = itemTarget(body)
			}
		}
	}

	if sh.Key != nil {
		key.Ser, val.Ser = sh.Key.Ser, sh.Value.Ser
	}

	sh.Key, sh.Value = key, val
}

// ---- operations ----

func (b *modelBuilder) addOps(dir, method, uri string) {
	files, _ := filepath.Glob(filepath.Join(dir, "api_op_*.go"))

	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}

		op := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(f), "api_op_"), ".go")
		o := &ir.Op{Method: method, URI: uri}
		if tm := reToken.FindStringSubmatch(mustRead(f)); tm != nil {
			o.Token = tm[1]
		}

		if _, ok := b.model.Shapes[op+"Input"]; ok {
			o.In = op + "Input"
		}

		if _, ok := b.model.Shapes[op+"Output"]; ok {
			o.Out = op + "Output"
		}

		b.model.Ops[op] = o
	}
}

// finish drops redundant names and checks every target resolves.
func (b *modelBuilder) finish(svc string) *ir.Model {
	scalars := map[string]bool{
		ir.String: true, ir.Boolean: true, ir.Byte: true, ir.Short: true, ir.Integer: true, ir.Long: true,
		ir.Float: true, ir.Double: true, ir.Timestamp: true, ir.Blob: true, ir.Document: true,
	}

	check := func(where string, m *ir.Member) {
		if m == nil {
			return
		}

		if m.Target == "" {
			fatalf("%s %s: %s has no target", svc, where, m.Name)
		}

		if !scalars[m.Target] && b.model.Shapes[m.Target] == nil {
			fatalf("%s %s: %s targets missing shape %s", svc, where, m.Name, m.Target)
		}
	}

	for name, sh := range b.model.Shapes {
		for i := range sh.Members {
			m := &sh.Members[i]

			if m.Target == "" {
				warnf("%s %s: member %s has no target; defaulting to string", svc, name, m.Name)
				m.Target = ir.String
			}

			check(name, m)
		}

		check(name, sh.Item)
		check(name, sh.Key)
		check(name, sh.Value)

		sort.Slice(sh.Members, func(i, j int) bool { return sh.Members[i].Name < sh.Members[j].Name })
	}

	return b.model
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	return keys
}

func extract(dir, protocol, prefix, version string) *ir.Model {
	src := loadFuncs(dir, prefix, "serializers.go", "deserializers.go")
	b := newModelBuilder(protocol, version)
	b.addSerializers(src)
	b.addDeserializers(src)

	if strings.HasPrefix(protocol, "rest") {
		b.addRestBindings(src)
		b.addOps(dir, "", "")
		b.addRestOps(dir)
	} else {
		b.addOps(dir, "POST", "/")
	}

	return b.finish(filepath.Base(dir))
}

var (
	reBinding   = regexp.MustCompile(`encoder\.(SetURI|SetQuery|AddQuery|SetHeader|Headers)\("([^"]+)"\)`)
	reOpPath    = regexp.MustCompile(`SplitURI\("([^"]*)"\)`)
	reOpMethod  = regexp.MustCompile(`request\.Method = "([A-Z]+)"`)
	reOpHandler = regexp.MustCompile(`(?s)func \(m \*\w+_serializeOp(\w+)\) HandleSerialize.*?\n}\n`)
	reOpBinding = regexp.MustCompile(`^serializeOpHttpBindings(\w+)Input$`)
)

// addRestBindings records the members a REST operation sends outside its body.
func (b *modelBuilder) addRestBindings(src *srcFile) {
	for _, name := range sortedKeys(src.funcs) {
		m := reOpBinding.FindStringSubmatch(name)
		if m == nil {
			continue
		}

		sh := b.shape(m[1]+"Input", ir.Struct)

		for _, stmt := range src.funcs[name].Body.List {
			ifs, ok := stmt.(*ast.IfStmt)
			if !ok {
				continue
			}

			body := src.text(ifs.Body)

			bind := reBinding.FindStringSubmatch(body)
			if bind == nil {
				continue
			}

			field := reGoField.FindStringSubmatch(src.text(ifs.Cond))
			if field == nil {
				fatalf("%s: binding without a field:\n%s", name, src.text(ifs))
			}

			mem := structMember(sh, field[1])
			mem.LocName = bind[2]

			switch bind[1] {
			case "SetURI":
				mem.Loc = "label"
			case "SetQuery", "AddQuery":
				mem.Loc = "query"
			case "SetHeader":
				mem.Loc = "header"
			default:
				mem.Loc = "prefix"
			}

			target := scalarOf(body)
			if bind[1] == "AddQuery" {
				target = b.listOf(m[1]+"Input_"+field[1], target)
			}

			setTarget(mem, target, name)
		}
	}
}

// listOf registers a list of scalars and returns its shape name.
func (b *modelBuilder) listOf(name, item string) string {
	sh := b.shape(name, ir.List)
	sh.Item = &ir.Member{Target: item}

	return name
}

// addRestOps reads each operation's method and URI template.
func (b *modelBuilder) addRestOps(dir string) {
	src := mustRead(filepath.Join(dir, "serializers.go"))

	for _, m := range reOpHandler.FindAllStringSubmatch(src, -1) {
		op := b.model.Ops[m[1]]
		if op == nil {
			continue
		}

		path := reOpPath.FindStringSubmatch(m[0])
		method := reOpMethod.FindStringSubmatch(m[0])

		if path == nil || method == nil {
			fatalf("%s: no path/method in handler", m[1])
		}

		op.URI, op.Method = path[1], method[1]
	}

	for name, op := range b.model.Ops {
		if op.URI == "" {
			fatalf("%s: operation without a URI", name)
		}
	}
}
