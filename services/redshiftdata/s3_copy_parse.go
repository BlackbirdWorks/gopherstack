package redshiftdata

import (
	"slices"
	"strings"
)

const (
	wordIAMRole = "iam_role"
	fmtText     = "text"
	fmtCSV      = "csv"
	fmtLines    = "json"
	fmtParquet  = "parquet"
)

type copySpec struct {
	table        string
	columnsRaw   string
	source       string
	format       string
	delimiter    string
	quote        string
	nullAs       string
	jsonPaths    string
	fixInvalid   string
	tableParts   []string
	columns      []string
	ignoreHeader int
	gzip         bool
	bzip2        bool
	manifest     bool
	nullSet      bool
	emptyAsNull  bool
	noLoad       bool
	skipBlank    bool
	fixInvalidOn bool
}

type copyOption func(c *cursor, s *copySpec) error

func flagOption(set func(s *copySpec)) copyOption {
	return func(_ *cursor, s *copySpec) error {
		set(s)

		return nil
	}
}

func ignoreOptionArg(c *cursor, _ *copySpec) error {
	c.acceptWord("as")

	if t, ok := c.peek(); ok && (t.kind == tokString || t.kind == tokWord && isNumeric(t.text)) {
		c.pos++
	}

	return nil
}

func stringOption(set func(s *copySpec, v string)) copyOption {
	return func(c *cursor, s *copySpec) error {
		v, err := c.asString()
		set(s, v)

		return err
	}
}

func ignoreEncoding(c *cursor, _ *copySpec) error {
	c.acceptWord("as")

	if _, ok := c.next(); !ok {
		return c.syntaxErr()
	}

	return nil
}

func isNumeric(s string) bool {
	if s == "" {
		return false
	}

	for i := range len(s) {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}

	return true
}

func onOffOption(c *cursor, _ *copySpec) error {
	if slices.ContainsFunc([]string{"on", "off", "true", "false", "preset"}, c.acceptWord) {
		return nil
	}

	return nil
}

func unsupportedCopyOption(name string) copyOption {
	return func(*cursor, *copySpec) error {
		return stmtErrorf("COPY option %s is not supported by the emulator engine", strings.ToUpper(name))
	}
}

func credentialOption(word string) copyOption {
	return func(c *cursor, _ *copySpec) error { return c.skipCredentials(word) }
}

func newCopyOptions() map[string]copyOption {
	return map[string]copyOption{
		wordIAMRole:         credentialOption(wordIAMRole),
		"credentials":       credentialOption("credentials"),
		"access_key_id":     credentialOption("access_key_id"),
		"secret_access_key": credentialOption("secret_access_key"),
		"session_token":     credentialOption("session_token"),
		"authorization":     func(*cursor, *copySpec) error { return nil },
		"format":            parseCopyFormat,
		fmtCSV:              parseCopyCSV,
		fmtLines:            parseCopyJSON,
		fmtParquet:          flagOption(func(s *copySpec) { s.format = fmtParquet }),
		"avro":              unsupportedCopyOption("avro"),
		"orc":               unsupportedCopyOption("orc"),
		"delimiter":         stringOption(func(s *copySpec, v string) { s.delimiter = v }),
		"ignoreheader": func(c *cursor, s *copySpec) error {
			n, err := c.asInt()
			s.ignoreHeader = n

			return err
		},
		"gzip":     flagOption(func(s *copySpec) { s.gzip = true }),
		"bzip2":    flagOption(func(s *copySpec) { s.bzip2 = true }),
		"zstd":     unsupportedCopyOption("zstd"),
		"lzop":     unsupportedCopyOption("lzop"),
		"manifest": flagOption(func(s *copySpec) { s.manifest = true }),
		"null": func(c *cursor, s *copySpec) error {
			v, err := c.asString()
			s.nullAs, s.nullSet = v, true

			return err
		},
		"emptyasnull":      flagOption(func(s *copySpec) { s.emptyAsNull = true }),
		"blanksasnull":     flagOption(func(s *copySpec) { s.emptyAsNull = true }),
		"noload":           flagOption(func(s *copySpec) { s.noLoad = true }),
		"ignoreblanklines": flagOption(func(s *copySpec) { s.skipBlank = true }),
		"acceptinvchars": func(c *cursor, s *copySpec) error {
			s.fixInvalidOn = true
			s.fixInvalid = "?"

			if val, ok := c.optString(); ok {
				s.fixInvalid = val
			} else if c.acceptWord("as") {
				var err error

				s.fixInvalid, err = c.str()

				return err
			}

			return nil
		},
		"region":          ignoreOptionArg,
		"maxerror":        ignoreOptionArg,
		"readratio":       ignoreOptionArg,
		"encoding":        ignoreEncoding,
		"compupdate":      onOffOption,
		"statupdate":      onOffOption,
		"roundec":         flagOption(func(*copySpec) {}),
		"encrypted":       flagOption(func(*copySpec) {}),
		"escape":          unsupportedCopyOption("escape"),
		"removequotes":    unsupportedCopyOption("removequotes"),
		"truncatecolumns": unsupportedCopyOption("truncatecolumns"),
		"fillrecord":      unsupportedCopyOption("fillrecord"),
		"acceptanydate":   unsupportedCopyOption("acceptanydate"),
		"timeformat":      unsupportedCopyOption("timeformat"),
		"dateformat":      unsupportedCopyOption("dateformat"),
		"fixedwidth":      unsupportedCopyOption("fixedwidth"),
		"explicit_ids":    unsupportedCopyOption("explicit_ids"),
		"serializetojson": unsupportedCopyOption("serializetojson"),
		"ssh":             unsupportedCopyOption("ssh"),
	}
}

func parseCopyFormat(c *cursor, s *copySpec) error {
	c.acceptWord("as")

	t, ok := c.next()
	if !ok {
		return c.syntaxErr()
	}

	c.pos--

	switch strings.ToLower(t.text) {
	case "csv":
		return parseCopyCSV(c, s)
	case "json":
		return parseCopyJSON(c, s)
	case "parquet":
		c.pos++
		s.format = fmtParquet

		return nil
	case "avro", "orc":
		return unsupportedCopyOption(t.text)(c, s)
	default:
		return c.syntaxErr()
	}
}

func parseCopyCSV(c *cursor, s *copySpec) error {
	if t, ok := c.peek(); ok && t.is("csv") {
		c.pos++
	}

	s.format = fmtCSV

	if c.acceptWord("quote") {
		q, err := c.asString()
		if err != nil {
			return err
		}

		s.quote = q
	}

	return nil
}

func parseCopyJSON(c *cursor, s *copySpec) error {
	if t, ok := c.peek(); ok && t.is("json") {
		c.pos++
	}

	s.format = fmtLines
	s.jsonPaths = "auto"

	c.acceptWord("as")

	if v, ok := c.optString(); ok {
		s.jsonPaths = v
	}

	return nil
}

// parseCopy parses a COPY statement; handled is false for statements that are not COPY FROM 's3://...'.
func parseCopy(sql string) (*copySpec, bool, error) {
	toks, _ := lexSQL(sql)
	if len(toks) == 0 || !toks[0].is("copy") {
		return nil, false, nil
	}

	c := &cursor{toks: toks, pos: 1}
	s := &copySpec{format: fmtText}

	if !parseCopyHead(c, s) {
		return nil, false, nil
	}

	return s, true, parseCopyOptions(c, s)
}

// parseCopyHead reads `table [(cols)] FROM 's3://...'`; false means the engine should see the statement.
func parseCopyHead(c *cursor, s *copySpec) bool {
	table, parts, err := c.ident()
	if err != nil {
		return false
	}

	s.table, s.tableParts = table, parts

	if parseColumns(c, s) != nil || !c.acceptWord("from") {
		return false
	}

	src, ok := c.optString()
	if !ok {
		return false
	}

	if _, _, isS3 := s3URI(src); !isS3 {
		return false
	}

	s.source = src

	return true
}

func parseColumns(c *cursor, s *copySpec) error {
	if !c.acceptPunct("(") {
		return nil
	}

	var raws []string

	for {
		raw, parts, err := c.ident()
		if err != nil {
			return err
		}

		raws = append(raws, raw)
		s.columns = append(s.columns, parts[len(parts)-1])

		if c.acceptPunct(",") {
			continue
		}

		if err = c.expectPunct(")"); err != nil {
			return err
		}

		s.columnsRaw = "(" + strings.Join(raws, ", ") + ")"

		return nil
	}
}

func parseCopyOptions(c *cursor, s *copySpec) error {
	options := newCopyOptions()

	for !c.done() {
		t, _ := c.next()
		if t.kind != tokWord {
			c.pos--

			return c.syntaxErr()
		}

		opt, ok := options[strings.ToLower(t.text)]
		if !ok {
			c.pos--

			return c.syntaxErr()
		}

		if err := opt(c, s); err != nil {
			return err
		}
	}

	return nil
}
