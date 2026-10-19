package redshiftdata

import "strings"

type unloadSpec struct {
	query         string
	bucket        string
	prefix        string
	format        string
	delimiter     string
	nullAs        string
	extension     string
	partitionBy   []string
	header        bool
	gzip          bool
	allowOverride bool
	cleanPath     bool
	manifest      bool
	verbose       bool
	addQuotes     bool
	escape        bool
	includePart   bool
}

type unloadOption func(c *cursor, s *unloadSpec) error

func unloadFlag(set func(s *unloadSpec)) unloadOption {
	return func(_ *cursor, s *unloadSpec) error {
		set(s)

		return nil
	}
}

func unloadUnsupported(name string) unloadOption {
	return func(*cursor, *unloadSpec) error {
		return stmtErrorf("UNLOAD option %s is not supported by the emulator engine", strings.ToUpper(name))
	}
}

func unloadIgnoreArg(c *cursor, _ *unloadSpec) error {
	c.acceptWord("as")

	if t, ok := c.peek(); ok && (t.kind == tokString || t.kind == tokWord && isNumeric(t.text)) {
		c.pos++
	}

	c.acceptWord("mb")
	c.acceptWord("gb")

	return nil
}

func newUnloadOptions() map[string]unloadOption {
	cred := func(word string) unloadOption {
		return func(c *cursor, _ *unloadSpec) error { return c.skipCredentials(word) }
	}

	return map[string]unloadOption{
		wordIAMRole:         cred(wordIAMRole),
		"credentials":       cred("credentials"),
		"access_key_id":     cred("access_key_id"),
		"secret_access_key": cred("secret_access_key"),
		"session_token":     cred("session_token"),
		"authorization":     func(*cursor, *unloadSpec) error { return nil },
		"format":            parseUnloadFormat,
		fmtCSV:              unloadFlag(func(s *unloadSpec) { s.format = fmtCSV }),
		fmtLines:            unloadFlag(func(s *unloadSpec) { s.format = fmtLines }),
		fmtParquet:          unloadFlag(func(s *unloadSpec) { s.format = fmtParquet }),
		"delimiter": func(c *cursor, s *unloadSpec) error {
			v, err := c.asString()
			s.delimiter = v

			return err
		},
		"null": func(c *cursor, s *unloadSpec) error {
			v, err := c.asString()
			s.nullAs = v

			return err
		},
		"extension": func(c *cursor, s *unloadSpec) error {
			v, err := c.str()
			s.extension = strings.TrimPrefix(v, ".")

			return err
		},
		"header":         unloadFlag(func(s *unloadSpec) { s.header = true }),
		"gzip":           unloadFlag(func(s *unloadSpec) { s.gzip = true }),
		"allowoverwrite": unloadFlag(func(s *unloadSpec) { s.allowOverride = true }),
		"cleanpath":      unloadFlag(func(s *unloadSpec) { s.cleanPath = true }),
		"addquotes":      unloadFlag(func(s *unloadSpec) { s.addQuotes = true }),
		"escape":         unloadFlag(func(s *unloadSpec) { s.escape = true }),
		"manifest": func(c *cursor, s *unloadSpec) error {
			s.manifest = true
			s.verbose = c.acceptWord("verbose")

			return nil
		},
		"partition":    parseUnloadPartition,
		"parallel":     func(c *cursor, _ *unloadSpec) error { return onOffOption(c, nil) },
		"maxfilesize":  unloadIgnoreArg,
		"rowgroupsize": unloadIgnoreArg,
		"region":       unloadIgnoreArg,
		"kms_key_id":   unloadIgnoreArg,
		"encrypted": func(c *cursor, _ *unloadSpec) error {
			c.acceptWord("auto")

			return nil
		},
		"bzip2":      unloadUnsupported("bzip2"),
		"zstd":       unloadUnsupported("zstd"),
		"fixedwidth": unloadUnsupported("fixedwidth"),
	}
}

func parseUnloadFormat(c *cursor, s *unloadSpec) error {
	c.acceptWord("as")

	t, ok := c.next()
	if !ok {
		return c.syntaxErr()
	}

	switch strings.ToLower(t.text) {
	case fmtCSV, fmtLines, fmtParquet:
		s.format = strings.ToLower(t.text)

		return nil
	default:
		c.pos--

		return c.syntaxErr()
	}
}

func parseUnloadPartition(c *cursor, s *unloadSpec) error {
	if err := c.expectWord("by"); err != nil {
		return err
	}

	if err := c.expectPunct("("); err != nil {
		return err
	}

	for {
		_, parts, err := c.ident()
		if err != nil {
			return err
		}

		s.partitionBy = append(s.partitionBy, parts[len(parts)-1])

		if c.acceptPunct(",") {
			continue
		}

		if err = c.expectPunct(")"); err != nil {
			return err
		}

		s.includePart = c.acceptWord("include")

		return nil
	}
}

// parseUnload parses UNLOAD ('query') TO 's3://bucket/prefix' [options].
func parseUnload(sql string) (*unloadSpec, error) {
	toks, err := lexSQL(sql)
	if err != nil {
		return nil, err
	}

	c := &cursor{toks: toks, pos: 1}
	s := &unloadSpec{}

	if err = c.expectPunct("("); err != nil {
		return nil, err
	}

	if s.query, err = c.str(); err != nil {
		return nil, err
	}

	if err = c.expectPunct(")"); err != nil {
		return nil, err
	}

	if err = c.expectWord("to"); err != nil {
		return nil, err
	}

	dest, err := c.str()
	if err != nil {
		return nil, err
	}

	bucket, prefix, ok := s3URI(dest)
	if !ok {
		return nil, stmtErrorf("UNLOAD destination must be an S3 URL, got %q", dest)
	}

	s.bucket, s.prefix = bucket, prefix

	options := newUnloadOptions()

	for !c.done() {
		t, _ := c.next()
		opt, found := options[strings.ToLower(t.text)]

		if t.kind != tokWord || !found {
			c.pos--

			return nil, c.syntaxErr()
		}

		if err = opt(c, s); err != nil {
			return nil, err
		}
	}

	if s.format == fmtParquet && s.gzip {
		return nil, stmtErrorf("GZIP cannot be combined with FORMAT PARQUET")
	}

	return s, nil
}
