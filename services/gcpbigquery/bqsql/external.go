package bqsql

func (p *parser) externalClauses(st *Statement) error {
	if p.kw("WITH") && p.peekNext().isKw("CONNECTION") {
		return unsupported("WITH CONNECTION: connections are not emulated")
	}

	if !p.kw("WITH") || !p.peekNext().isKw("PARTITION") {
		return nil
	}

	p.i += 2

	if err := p.expectKw("COLUMNS"); err != nil {
		return err
	}

	st.withPartCols = true

	if !p.acceptPunct("(") {
		return nil
	}

	cols, err := p.colDefs()
	if err != nil {
		return err
	}

	st.partCols = cols

	return nil
}

func (c *compiler) ddlCreateExternal(st *Statement, ref TableRef) ([]Effect, error) {
	if st.orReplace && st.ifNot {
		return nil, invalid("OR REPLACE and IF NOT EXISTS cannot be used together")
	}

	if st.query != nil {
		return nil, invalid("CREATE EXTERNAL TABLE does not accept AS SELECT")
	}

	for _, opt := range []string{"format", "uris"} {
		if st.options[opt] == "" {
			return nil, invalid("The format and uris options are required for an external table")
		}
	}

	e := Effect{
		Op: OpCreateTable, Ref: ref, OrReplace: st.orReplace, Options: st.options, External: true,
		Fields: colDefFields(st.colDefs), PartFields: colDefFields(st.partCols), WithPartCols: st.withPartCols,
	}

	return []Effect{e}, nil
}
