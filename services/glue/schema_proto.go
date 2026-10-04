package glue

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const (
	protoTypeString = "string"
	protoKwMessage  = "message"
	protoKwEnum     = "enum"
	protoKwOption   = "option"
	protoLabelReq   = "required"
	protoTypeMap    = "map"
)

const (
	protoGroupNone = iota
	protoGroupVarint
	protoGroupZigzag
	protoGroupFixed32
	protoGroupFixed64
	protoGroupBytes
)

var errProtoSchema = errors.New("invalid PROTOBUF schema")

// Hand-rolled .proto parser (messages, fields, oneofs, maps, services); rules follow the Glue
// docs' PROTOBUF examples (required/optional field and RPC add/remove).

type protoField struct {
	name  string
	typ   string
	label string
}

type protoFile struct {
	messages map[string]map[int]protoField
	services map[string]map[string]string
	syntax   string
}

type protoTok struct {
	text string
	str  bool
}

func validateProtobufSchema(def string) (bool, string) {
	if !strings.Contains(def, "syntax") {
		return false, "PROTOBUF schema must contain a 'syntax' declaration"
	}

	pf, err := parseProto(def)
	if err != nil {
		return false, err.Error()
	}

	if len(pf.messages) == 0 && len(pf.services) == 0 {
		return false, "PROTOBUF schema must contain at least one 'message' declaration"
	}

	return true, ""
}

func protoLex(src string) ([]protoTok, error) {
	var toks []protoTok

	for i := 0; i < len(src); {
		tok, n, err := protoLexOne(src, i)
		if err != nil {
			return nil, err
		}

		if tok.text != "" || tok.str {
			toks = append(toks, tok)
		}

		i += n
	}

	return toks, nil
}

// protoLexOne lexes src[i:]; whitespace and comments yield an empty token.
func protoLexOne(src string, i int) (protoTok, int, error) {
	c := src[i]

	switch {
	case c == ' ' || c == '\t' || c == '\n' || c == '\r':
		return protoTok{}, 1, nil
	case strings.HasPrefix(src[i:], "//"):
		end := strings.IndexByte(src[i:], '\n')
		if end < 0 {
			end = len(src) - i
		}

		return protoTok{}, end, nil
	case strings.HasPrefix(src[i:], "/*"):
		end := strings.Index(src[i+len("/*"):], "*/")
		if end < 0 {
			return protoTok{}, 0, fmt.Errorf("%w: unterminated comment", errProtoSchema)
		}

		return protoTok{}, end + len("/*") + len("*/"), nil
	case c == '"' || c == '\'':
		return protoLexString(src, i)
	case isProtoWord(c):
		j := i
		for j < len(src) && (isProtoWord(src[j]) || src[j] == '.') {
			j++
		}

		return protoTok{text: src[i:j]}, j - i, nil
	}

	return protoTok{text: string(c)}, 1, nil
}

func protoLexString(src string, i int) (protoTok, int, error) {
	quote := src[i]

	j := i + 1
	for j < len(src) && src[j] != quote {
		if src[j] == '\\' {
			j++
		}

		j++
	}

	if j >= len(src) {
		return protoTok{}, 0, fmt.Errorf("%w: unterminated string", errProtoSchema)
	}

	return protoTok{text: src[i+1 : j], str: true}, j + 1 - i, nil
}

func isProtoWord(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

type protoParser struct {
	file *protoFile
	toks []protoTok
	i    int
}

func parseProto(def string) (*protoFile, error) {
	toks, err := protoLex(def)
	if err != nil {
		return nil, err
	}

	p := &protoParser{
		toks: toks,
		file: &protoFile{messages: map[string]map[int]protoField{}, services: map[string]map[string]string{}},
	}

	for p.i < len(p.toks) {
		if err = p.topLevel(); err != nil {
			return nil, err
		}
	}

	return p.file, nil
}

func (p *protoParser) peek() string {
	if p.i < len(p.toks) {
		return p.toks[p.i].text
	}

	return ""
}

func (p *protoParser) next() (protoTok, error) {
	if p.i >= len(p.toks) {
		return protoTok{}, fmt.Errorf("%w: PROTOBUF schema ended unexpectedly", errProtoSchema)
	}

	t := p.toks[p.i]
	p.i++

	return t, nil
}

func (p *protoParser) expect(s string) error {
	t, err := p.next()
	if err != nil {
		return err
	}

	if t.text != s || t.str {
		return fmt.Errorf("%w: PROTOBUF schema: expected %q but found %q", errProtoSchema, s, t.text)
	}

	return nil
}

func (p *protoParser) skipStatement() error {
	for {
		t, err := p.next()
		if err != nil {
			return err
		}

		if t.str {
			continue
		}

		switch t.text {
		case ";":
			return nil
		case "{":
			p.i--

			return p.skipBlock()
		}
	}
}

func (p *protoParser) skipBlock() error {
	if err := p.expect("{"); err != nil {
		return err
	}

	for depth := 1; depth > 0; {
		t, err := p.next()
		if err != nil {
			return err
		}

		switch {
		case t.str:
		case t.text == "{":
			depth++
		case t.text == "}":
			depth--
		}
	}

	return nil
}

func (p *protoParser) topLevel() error {
	switch p.peek() {
	case "syntax":
		p.i++

		if err := p.expect("="); err != nil {
			return err
		}

		v, err := p.next()
		if err != nil {
			return err
		}

		if v.text != "proto2" && v.text != "proto3" {
			return fmt.Errorf("%w: PROTOBUF schema has unsupported syntax %q", errProtoSchema, v.text)
		}

		p.file.syntax = v.text

		return p.expect(";")
	case protoKwMessage:
		p.i++

		return p.message("")
	case "service":
		p.i++

		return p.service()
	case protoKwEnum, "extend":
		return p.skipStatement()
	case "package", "import", protoKwOption:
		return p.skipStatement()
	case ";":
		p.i++

		return nil
	}

	return fmt.Errorf("%w: PROTOBUF schema: unexpected %q at top level", errProtoSchema, p.peek())
}

func (p *protoParser) service() error {
	name, err := p.next()
	if err != nil {
		return err
	}

	if err = p.expect("{"); err != nil {
		return err
	}

	methods := map[string]string{}
	p.file.services[name.text] = methods

	for p.peek() != "}" {
		if p.peek() != "rpc" {
			if err = p.skipStatement(); err != nil {
				return err
			}

			continue
		}

		p.i++

		if err = p.rpc(methods); err != nil {
			return err
		}
	}

	return p.expect("}")
}

func (p *protoParser) rpc(methods map[string]string) error {
	name, err := p.next()
	if err != nil {
		return err
	}

	var sig []string

	for _, step := range []string{"(", "", ")", "returns", "(", "", ")"} {
		t, terr := p.next()
		if terr != nil {
			return terr
		}

		if step == "" {
			if t.text == "stream" {
				if t, terr = p.next(); terr != nil {
					return terr
				}
			}

			sig = append(sig, strings.TrimPrefix(t.text, "."))

			continue
		}

		if t.text != step {
			return fmt.Errorf("%w: PROTOBUF schema: malformed rpc %q", errProtoSchema, name.text)
		}
	}

	methods[name.text] = strings.Join(sig, ">")

	if p.peek() == "{" {
		return p.skipBlock()
	}

	return p.expect(";")
}

func (p *protoParser) message(prefix string) error {
	nameTok, err := p.next()
	if err != nil {
		return err
	}

	full := prefix + nameTok.text
	fields := map[int]protoField{}
	p.file.messages[full] = fields

	if err = p.expect("{"); err != nil {
		return err
	}

	for p.peek() != "}" {
		if err = p.messageItem(full, fields, ""); err != nil {
			return err
		}
	}

	return p.expect("}")
}

func (p *protoParser) messageItem(full string, fields map[int]protoField, forcedLabel string) error {
	switch p.peek() {
	case protoKwMessage:
		p.i++

		return p.message(full + ".")
	case protoKwEnum, protoKwOption, "reserved", "extensions", "extend":
		return p.skipStatement()
	case "oneof":
		p.i++

		return p.oneof(full, fields)
	case ";":
		p.i++

		return nil
	}

	return p.field(fields, forcedLabel)
}

func (p *protoParser) oneof(full string, fields map[int]protoField) error {
	if _, err := p.next(); err != nil {
		return err
	}

	if err := p.expect("{"); err != nil {
		return err
	}

	for p.peek() != "}" {
		if p.peek() == protoKwOption {
			if err := p.skipStatement(); err != nil {
				return err
			}

			continue
		}

		if err := p.messageItem(full, fields, "optional"); err != nil {
			return err
		}
	}

	return p.expect("}")
}

func (p *protoParser) field(fields map[int]protoField, forcedLabel string) error {
	label := forcedLabel

	switch p.peek() {
	case "repeated", "optional", protoLabelReq:
		t, _ := p.next()
		label = t.text
	}

	typ, err := p.fieldType()
	if err != nil {
		return err
	}

	name, err := p.next()
	if err != nil {
		return err
	}

	if err = p.expect("="); err != nil {
		return err
	}

	numTok, err := p.next()
	if err != nil {
		return err
	}

	num, err := strconv.Atoi(numTok.text)
	if err != nil || num < 1 {
		return fmt.Errorf("%w: PROTOBUF schema: field %q needs a positive field number", errProtoSchema, name.text)
	}

	if _, dup := fields[num]; dup {
		return fmt.Errorf("%w: PROTOBUF schema: field number %d is used twice", errProtoSchema, num)
	}

	if label == "" {
		label = "optional"
	}

	fields[num] = protoField{name: name.text, typ: typ, label: label}

	return p.skipStatementTail()
}

func (p *protoParser) skipStatementTail() error {
	for {
		t, err := p.next()
		if err != nil {
			return err
		}

		if t.text == ";" && !t.str {
			return nil
		}
	}
}

func (p *protoParser) fieldType() (string, error) {
	t, err := p.next()
	if err != nil {
		return "", err
	}

	if t.text != protoTypeMap || p.peek() != "<" {
		return strings.TrimPrefix(t.text, "."), nil
	}

	p.i++

	var parts []string

	for p.peek() != ">" {
		k, kerr := p.next()
		if kerr != nil {
			return "", kerr
		}

		if k.text != "," {
			parts = append(parts, k.text)
		}
	}

	p.i++

	return protoTypeMap + "<" + strings.Join(parts, ",") + ">", nil
}

func protoWireGroup(t string) int {
	switch t {
	case "int32", "uint32", "int64", "uint64", "bool":
		return protoGroupVarint
	case "sint32", "sint64":
		return protoGroupZigzag
	case "fixed32", "sfixed32":
		return protoGroupFixed32
	case "fixed64", "sfixed64":
		return protoGroupFixed64
	case protoTypeString, "bytes":
		return protoGroupBytes
	}

	return protoGroupNone
}

func protoTypesCompatible(r, w string) bool {
	if r == w {
		return true
	}

	rg, wg := protoWireGroup(r), protoWireGroup(w)
	if rg != protoGroupNone || wg != protoGroupNone {
		return rg == wg
	}

	return protoShortName(r) == protoShortName(w)
}

func protoShortName(t string) string {
	if i := strings.LastIndex(t, "."); i >= 0 {
		return t[i+1:]
	}

	return t
}

func protoCanRead(readerDef, writerDef string) error {
	r, err := parseProto(readerDef)
	if err != nil {
		return err
	}

	w, err := parseProto(writerDef)
	if err != nil {
		return err
	}

	for name, rf := range r.messages {
		wf, ok := w.messages[name]
		if !ok {
			continue
		}

		if err = protoMessageCanRead(name, rf, wf); err != nil {
			return err
		}
	}

	for svc, wm := range w.services {
		rm, ok := r.services[svc]
		if !ok {
			continue
		}

		for method, sig := range wm {
			if rm[method] == "" {
				return schemaCompatErr("rpc %s.%s is missing from the reader schema", svc, method)
			}

			if rm[method] != sig {
				return schemaCompatErr("rpc %s.%s changed its request or response type", svc, method)
			}
		}
	}

	return nil
}

func protoMessageCanRead(msg string, rf, wf map[int]protoField) error {
	for num, f := range rf {
		wfield, ok := wf[num]
		if !ok {
			if f.label == protoLabelReq {
				return schemaCompatErr(
					"message %s: required field %q (%d) is missing from the writer schema",
					msg,
					f.name,
					num,
				)
			}

			continue
		}

		if !protoTypesCompatible(f.typ, wfield.typ) {
			return schemaCompatErr("message %s: field %d changed type from %s to %s", msg, num, wfield.typ, f.typ)
		}

		if f.label == protoLabelReq && wfield.label != protoLabelReq {
			return schemaCompatErr(
				"message %s: field %q (%d) is required by the reader but not by the writer",
				msg,
				f.name,
				num,
			)
		}
	}

	return nil
}
