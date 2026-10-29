package athena

import (
	"errors"
	"fmt"
	"maps"
	"strings"
)

var errNotGlueDDL = errors.New("statement does not describe an S3-backed table")

const (
	lazySimpleSerde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
	jsonSerdeLib    = "org.openx.data.jsonserde.JsonSerDe"
	textInputFormat = "org.apache.hadoop.mapred.TextInputFormat"
	textOutputFmt   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
	parquetSerde    = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
	parquetInputFmt = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
	parquetOutFmt   = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
	orcSerde        = "org.apache.hadoop.hive.ql.io.orc.OrcSerde"
	orcInputFmt     = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"
	orcOutFmt       = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"
	avroSerde       = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"
)

type createTableSpec struct {
	def         GlueTableDef
	parts       []string
	partTokens  []token
	ifNotExists bool
	external    bool
}

// sigCursor walks the significant tokens of a statement.
type sigCursor struct {
	toks []token
	pos  int
}

func (c *sigCursor) done() bool { return c.pos >= len(c.toks) }

func (c *sigCursor) acceptWords(ws ...string) bool {
	if c.pos+len(ws) > len(c.toks) {
		return false
	}

	for i, w := range ws {
		if !c.toks[c.pos+i].isWord(w) {
			return false
		}
	}

	c.pos += len(ws)

	return true
}

// qualifiedName reads ident[.ident[.ident]].
func (c *sigCursor) qualifiedName() []string {
	if c.done() || (c.toks[c.pos].kind != tkWord && c.toks[c.pos].kind != tkQuoted) {
		return nil
	}

	parts := []string{c.toks[c.pos].ident()}
	c.pos++

	for c.pos+1 < len(c.toks) && c.toks[c.pos].isPunct(".") &&
		(c.toks[c.pos+1].kind == tkWord || c.toks[c.pos+1].kind == tkQuoted) {
		parts = append(parts, c.toks[c.pos+1].ident())
		c.pos += 2
	}

	return parts
}

func (c *sigCursor) stringLit() string {
	if !c.done() && c.toks[c.pos].kind == tkString {
		s := stringLiteral(c.toks[c.pos])
		c.pos++

		return s
	}

	return ""
}

// group returns the tokens inside the parenthesis group at the cursor.
func (c *sigCursor) group() ([]token, bool) {
	if c.done() || !c.toks[c.pos].isPunct("(") {
		return nil, false
	}

	closeIdx := matchParen(c.toks, c.pos)
	if closeIdx < 0 {
		return nil, false
	}

	inner := c.toks[c.pos+1 : closeIdx]
	c.pos = closeIdx + 1

	return inner, true
}

func typeText(toks []token) string {
	var sb strings.Builder

	for i, t := range toks {
		if i > 0 && toks[i-1].kind == tkWord && t.kind == tkWord {
			sb.WriteByte(' ')
		}

		sb.WriteString(strings.ToLower(t.text))
	}

	return sb.String()
}

func parseColumnTokens(toks []token) []Column {
	var cols []Column

	for _, part := range splitCommas(toks) {
		if len(part) == 0 {
			continue
		}

		col := Column{Name: part[0].ident()}
		rest := part[1:]

		if ci := firstTopLevelWord(rest, "comment"); ci >= 0 {
			if ci+1 < len(rest) && rest[ci+1].kind == tkString {
				col.Comment = stringLiteral(rest[ci+1])
			}

			rest = rest[:ci]
		}

		col.Type = typeText(rest)
		cols = append(cols, col)
	}

	return cols
}

func parseProps(toks []token) map[string]string {
	props := map[string]string{}

	for _, part := range splitCommas(toks) {
		if len(part) == 3 && part[0].kind == tkString && part[1].isPunct("=") && part[2].kind == tkString {
			props[stringLiteral(part[0])] = stringLiteral(part[2])
		}
	}

	return props
}

func (s *createTableSpec) setDelimiter(c *sigCursor) {
	for !c.done() {
		switch {
		case c.acceptWords("fields", "terminated", "by"):
			d := c.stringLit()
			d = unescapeDelim(d)
			s.def.SerdeParameters["field.delim"] = d
			s.def.SerdeParameters["serialization.format"] = d

			if c.acceptWords("escaped", "by") {
				e := c.stringLit()
				s.def.SerdeParameters["escape.delim"] = e
			}
		case c.acceptWords("collection", "items", "terminated", "by"):
			d := c.stringLit()
			s.def.SerdeParameters["colelction.delim"] = unescapeDelim(d)
		case c.acceptWords("map", "keys", "terminated", "by"):
			d := c.stringLit()
			s.def.SerdeParameters["mapkey.delim"] = unescapeDelim(d)
		case c.acceptWords("lines", "terminated", "by"):
			d := c.stringLit()
			s.def.SerdeParameters["line.delim"] = unescapeDelim(d)
		case c.acceptWords("null", "defined", "as"):
			_ = c.stringLit()
		default:
			return
		}
	}
}

func (s *createTableSpec) setStoredAs(w string) {
	switch strings.ToLower(w) {
	case "parquet":
		s.def.InputFormat, s.def.OutputFormat = parquetInputFmt, parquetOutFmt
		if s.def.SerdeLibrary == "" {
			s.def.SerdeLibrary = parquetSerde
		}
	case "orc":
		s.def.InputFormat, s.def.OutputFormat = orcInputFmt, orcOutFmt
		if s.def.SerdeLibrary == "" {
			s.def.SerdeLibrary = orcSerde
		}
	case "avro":
		s.def.InputFormat, s.def.OutputFormat = textInputFormat, textOutputFmt
		if s.def.SerdeLibrary == "" {
			s.def.SerdeLibrary = avroSerde
		}
	default:
		s.def.InputFormat, s.def.OutputFormat = textInputFormat, textOutputFmt
	}
}

func (s *createTableSpec) parseClauses(c *sigCursor) {
	for !c.done() {
		if !s.clause(c) {
			c.pos++
		}
	}
}

// clause consumes one recognised table clause at the cursor.
func (s *createTableSpec) clause(c *sigCursor) bool {
	switch {
	case c.acceptWords("partitioned", "by"):
		if inner, ok := c.group(); ok {
			s.partTokens = inner
			s.def.PartitionKeys = parseColumnTokens(inner)
		}
	case c.acceptWords("row", "format", "delimited"):
		s.def.SerdeLibrary = lazySimpleSerde
		s.setDelimiter(c)
	case c.acceptWords("row", "format", "serde"):
		s.def.SerdeLibrary = c.stringLit()
	case c.acceptWords("with", "serdeproperties"):
		s.mergeProps(c, s.def.SerdeParameters)
	case c.acceptWords("stored", "as"):
		s.storedAs(c)
	case c.acceptWords("location"):
		s.def.Location = c.stringLit()
	case c.acceptWords("tblproperties"):
		s.mergeProps(c, s.def.Parameters)
	default:
		return false
	}

	return true
}

func (s *createTableSpec) mergeProps(c *sigCursor, dst map[string]string) {
	if inner, ok := c.group(); ok {
		maps.Copy(dst, parseProps(inner))
	}
}

func (s *createTableSpec) storedAs(c *sigCursor) {
	if c.acceptWords("inputformat") {
		s.def.InputFormat = c.stringLit()

		if c.acceptWords("outputformat") {
			s.def.OutputFormat = c.stringLit()
		}

		return
	}

	if !c.done() {
		s.setStoredAs(c.toks[c.pos].text)
		c.pos++
	}
}

func (s *createTableSpec) finish(external bool) {
	if s.def.SerdeLibrary == "" {
		s.def.SerdeLibrary = lazySimpleSerde
	}

	if s.def.InputFormat == "" {
		lib := strings.ToLower(s.def.SerdeLibrary)

		switch {
		case strings.Contains(lib, "parquet"):
			s.setStoredAs("parquet")
		case strings.Contains(lib, "orc"):
			s.setStoredAs("orc")
		default:
			s.setStoredAs("textfile")
		}
	}

	if external {
		s.def.Parameters["EXTERNAL"] = externalTrue
	}

	s.def.TableType = tableTypeExternal
}

// parseGlueCreateTable parses CREATE [EXTERNAL] TABLE ... LOCATION 's3://...'
// (non-CTAS). It returns errNotGlueDDL for statements without a LOCATION.
func parseGlueCreateTable(sql string) (*createTableSpec, error) {
	toks, err := tokenize(sql)
	if err != nil {
		return nil, err
	}

	c := &sigCursor{toks: significant(toks)}

	if !c.acceptWords("create") {
		return nil, errNotGlueDDL
	}

	external := c.acceptWords("external")

	if !c.acceptWords("table") {
		return nil, errNotGlueDDL
	}

	spec := &createTableSpec{def: GlueTableDef{
		Parameters: map[string]string{}, SerdeParameters: map[string]string{},
	}}
	spec.ifNotExists = c.acceptWords("if", "not", "exists")

	if spec.parts = c.qualifiedName(); spec.parts == nil {
		return nil, fmt.Errorf("%w: missing table name", errNotGlueDDL)
	}

	if inner, ok := c.group(); ok {
		spec.def.Columns = parseColumnTokens(inner)
	}

	spec.parseClauses(c)

	spec.external = external
	tt := strings.ToUpper(lowerParam(spec.def.Parameters, paramTableType))

	if tt == tableTypeIceberg {
		spec.def.PartitionKeys = nil
	}

	if spec.def.Location == "" || (len(spec.def.Columns) == 0 && tt != tableTypeDelta) {
		return nil, errNotGlueDDL
	}

	spec.finish(external)

	return spec, nil
}
