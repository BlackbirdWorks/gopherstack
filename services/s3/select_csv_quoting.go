package s3

import "strings"

// csvParseOptions is the resolved (defaults-applied) set of CSV input
// parsing options. encoding/csv.Reader cannot express a configurable quote
// character, quote escape character, or record delimiter other than "\n"
// (it hardcodes '"' for both quote and escape), so a request that
// customises any of them away from those RFC4180 defaults falls through
// from newCSVReader's stdlib path to parseCSVCustom.
type csvParseOptions struct {
	recordDelim                string
	fieldDelim                 byte
	quoteChar                  byte
	quoteEscape                byte
	comment                    byte
	hasComment                 bool
	allowQuotedRecordDelimiter bool
}

// usesStdlibQuoting reports whether opts matches what encoding/csv.Reader
// already implements unconditionally, so the fast, well-tested stdlib path
// can be used unchanged.
func (o csvParseOptions) usesStdlibQuoting() bool {
	return o.quoteChar == '"' && o.quoteEscape == '"' && o.recordDelim == "\n"
}

// resolveCSVInputOptions applies S3 Select's documented CSV input defaults
// (FieldDelimiter ",", QuoteCharacter/QuoteEscapeCharacter both `"`,
// RecordDelimiter "\n", AllowQuotedRecordDelimiter false) to a request's
// CSV input serialization.
func resolveCSVInputOptions(csvIn *selectCSVInput) csvParseOptions {
	opts := csvParseOptions{
		fieldDelim:  ',',
		quoteChar:   '"',
		quoteEscape: '"',
		recordDelim: "\n",
	}
	if csvIn == nil {
		return opts
	}

	if csvIn.FieldDelimiter != "" {
		opts.fieldDelim = csvIn.FieldDelimiter[0]
	}

	if csvIn.QuoteCharacter != "" {
		opts.quoteChar = csvIn.QuoteCharacter[0]
		opts.quoteEscape = opts.quoteChar
	}

	if csvIn.QuoteEscapeCharacter != "" {
		opts.quoteEscape = csvIn.QuoteEscapeCharacter[0]
	}

	if csvIn.RecordDelimiter != "" {
		opts.recordDelim = csvIn.RecordDelimiter
	}

	if csvIn.Comments != "" {
		opts.comment = csvIn.Comments[0]
		opts.hasComment = true
	}

	opts.allowQuotedRecordDelimiter = strings.EqualFold(csvIn.AllowQuotedRecordDelimiter, "true")

	return opts
}

// parseCSVCustom parses CSV records honouring a customised quote character,
// quote escape character, or record delimiter. Blank records are skipped,
// matching encoding/csv's own "blank lines are ignored" behaviour.
func parseCSVCustom(data []byte, opts csvParseOptions, fileHeaderInfo string) ([]map[string]string, []string, error) {
	var headers []string
	var rows []map[string]string
	first := true

	for _, rec := range splitCSVRecords(string(data), opts) {
		if rec == "" || (opts.hasComment && rec[0] == opts.comment) {
			continue
		}

		fields := splitCSVFields(rec, opts)

		if first {
			first = false
			headers = prepareCSVHeaders(fileHeaderInfo, fields)

			if fileHeaderInfo == csvFileHeaderInfoUse {
				continue
			}
		}

		rows = append(rows, csvRecordToMap(headers, fields))
	}

	return rows, headers, nil
}

// splitCSVRecords splits data into records on opts.recordDelim.
func splitCSVRecords(data string, opts csvParseOptions) []string {
	if !opts.allowQuotedRecordDelimiter {
		return splitOnDelimiter(data, opts.recordDelim)
	}

	return splitCSVRecordsQuoteAware(data, opts)
}

// splitOnDelimiter blindly splits on delim without regard to quoting -- the
// documented AWS default (AllowQuotedRecordDelimiter=false): a record
// delimiter inside a quoted field still ends the record.
func splitOnDelimiter(data, delim string) []string {
	parts := strings.Split(data, delim)
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}

	return parts
}

// splitCSVRecordsQuoteAware splits on opts.recordDelim, treating an
// occurrence inside a quoted field as literal content rather than a record
// boundary (AllowQuotedRecordDelimiter=true).
func splitCSVRecordsQuoteAware(data string, opts csvParseOptions) []string {
	var records []string
	var cur strings.Builder
	inQuotes := false

	for i := 0; i < len(data); {
		c := data[i]

		if inQuotes && opts.quoteEscape != opts.quoteChar && c == opts.quoteEscape &&
			i+1 < len(data) && data[i+1] == opts.quoteChar {
			cur.WriteByte(c)
			cur.WriteByte(data[i+1])
			i += 2

			continue
		}

		switch {
		case c == opts.quoteChar:
			inQuotes = !inQuotes
			cur.WriteByte(c)
			i++

		case !inQuotes && strings.HasPrefix(data[i:], opts.recordDelim):
			records = append(records, cur.String())
			cur.Reset()
			i += len(opts.recordDelim)

		default:
			cur.WriteByte(c)
			i++
		}
	}

	if cur.Len() > 0 {
		records = append(records, cur.String())
	}

	return records
}

// splitCSVFields splits one already-delimited record into fields on
// opts.fieldDelim, honouring opts.quoteChar/opts.quoteEscape and stripping
// the surrounding quotes plus unescaping embedded ones.
func splitCSVFields(record string, opts csvParseOptions) []string {
	var fields []string
	var cur strings.Builder
	inQuotes := false

	for i := 0; i < len(record); {
		c := record[i]

		if inQuotes && opts.quoteEscape != opts.quoteChar && c == opts.quoteEscape &&
			i+1 < len(record) && record[i+1] == opts.quoteChar {
			cur.WriteByte(opts.quoteChar)
			i += 2

			continue
		}

		switch {
		case c == opts.quoteChar:
			if inQuotes && i+1 < len(record) && record[i+1] == opts.quoteChar {
				cur.WriteByte(opts.quoteChar)
				i += 2

				continue
			}

			inQuotes = !inQuotes
			i++

		case !inQuotes && c == opts.fieldDelim:
			fields = append(fields, cur.String())
			cur.Reset()
			i++

		default:
			cur.WriteByte(c)
			i++
		}
	}

	fields = append(fields, cur.String())

	return fields
}

// csvOutputOptions is the resolved set of CSV output formatting options.
type csvOutputOptions struct {
	fieldDelim  string
	recordDelim string
	quoteChar   string
	quoteEscape string
	alwaysQuote bool
}

// resolveCSVOutputOptions applies S3 Select's documented CSV output
// defaults (FieldDelimiter ",", RecordDelimiter "\n", QuoteCharacter/
// QuoteEscapeCharacter both `"`, QuoteFields ASNEEDED) to a request's CSV
// output serialization.
func resolveCSVOutputOptions(csvOut *selectCSVOutput) csvOutputOptions {
	opts := csvOutputOptions{
		fieldDelim:  ",",
		recordDelim: "\n",
		quoteChar:   `"`,
		quoteEscape: `"`,
	}
	if csvOut == nil {
		return opts
	}

	if csvOut.FieldDelimiter != "" {
		opts.fieldDelim = csvOut.FieldDelimiter
	}

	if csvOut.RecordDelimiter != "" {
		opts.recordDelim = csvOut.RecordDelimiter
	}

	if csvOut.QuoteCharacter != "" {
		opts.quoteChar = csvOut.QuoteCharacter
		opts.quoteEscape = csvOut.QuoteCharacter
	}

	if csvOut.QuoteEscapeCharacter != "" {
		opts.quoteEscape = csvOut.QuoteEscapeCharacter
	}

	opts.alwaysQuote = strings.EqualFold(csvOut.QuoteFields, "ALWAYS")

	return opts
}

// encodeCSVField quotes and escapes a single output field per opts,
// matching encoding/csv.Writer's own ASNEEDED policy when QuoteFields is
// left at its default: quote only when the value contains the field
// delimiter, the quote character, or a line break.
func encodeCSVField(value string, opts csvOutputOptions) string {
	needsQuote := opts.alwaysQuote ||
		strings.Contains(value, opts.fieldDelim) ||
		strings.Contains(value, opts.quoteChar) ||
		strings.Contains(value, opts.recordDelim) ||
		strings.ContainsAny(value, "\r\n")

	if !needsQuote {
		return value
	}

	escaped := strings.ReplaceAll(value, opts.quoteChar, opts.quoteEscape+opts.quoteChar)

	return opts.quoteChar + escaped + opts.quoteChar
}
