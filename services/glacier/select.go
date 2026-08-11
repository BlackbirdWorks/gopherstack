package glacier

// This file implements Amazon S3 Glacier Select ("select" jobs).
//
// Real AWS Select jobs run an SQL query over a CSV-encoded archive and write the
// result to the S3 location named by OutputLocation, under a job-ID-scoped key
// layout (job.txt/results/*/result_manifest.txt -- see select_output.go); results
// are never retrievable via GetJobOutput on real AWS (its documented response
// shapes cover only archive content and vault inventory). gopherstack executes the
// query for real against the stored archive bytes and, when an S3 backend is wired
// (cli.go's wireGlacierS3), writes the real S3 output-location objects
// (materializeSelectOutput in select_output.go) -- this is the faithful delivery
// path. GetJobOutput additionally continues to serve the same real (not stubbed)
// computed bytes directly, a documented gopherstack convenience with no basis to
// remove: AWS's GetJobOutput docs never mention Select jobs at all, so no error
// behavior can be cited for rejecting them there instead.
//
// The SQL subset supported mirrors the common form used throughout AWS's own Glacier
// Select examples (originally modeled on S3 Select's SQL subset):
//
//	SELECT (* | ref [AS alias] (',' ref [AS alias])*) FROM <table> [alias]
//	  [WHERE predicate ('AND'|'OR' predicate)*]
//
// where ref is a positional column ("_1", "_2", ...), an optionally alias-qualified
// header-name column ("s._1" / "name"), predicate is "ref op literal" with
// op in {= != <> < <= > >=}, and literal is a quoted string or a bare number.
// LIMIT is deliberately rejected, not merely unimplemented: real S3 Glacier
// Select's SELECT command explicitly documents LIMIT as "(Amazon S3 Select
// only)" -- unsupported by Glacier Select
// (doc_source/s3-glacier-select-sql-reference-select.md in
// awsdocs/amazon-glacier-developer-guide, "S3 Glacier Select does not support the
// LIMIT clause"). CAST, BETWEEN, IN, LIKE, NOT, and arithmetic operators are real
// Glacier Select features this subset does not implement -- see PARITY.md's
// select_sql_subset gap entry for the full accounting. Parenthesized/nested
// boolean grouping has no citable evidence of Glacier Select support either way
// (absent from the documented scalar-expression grammar) -- see PARITY.md.

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
)

// validateSelectParameters checks that a select job's SelectParameters is present and
// structurally complete, matching the real API's required-field validation, and that
// its Expression parses as a supported SQL statement.
func validateSelectParameters(sp *selectParametersDTO) error {
	if sp == nil {
		return fmt.Errorf("%w: SelectParameters is required for a select job", ErrMissingParameter)
	}

	if strings.TrimSpace(sp.Expression) == "" {
		return fmt.Errorf("%w: SelectParameters.Expression is required", ErrMissingParameter)
	}

	if !strings.EqualFold(sp.ExpressionType, "SQL") {
		return fmt.Errorf("%w: SelectParameters.ExpressionType must be SQL", ErrValidation)
	}

	if sp.InputSerialization == nil || sp.InputSerialization.Csv == nil {
		return fmt.Errorf(
			"%w: SelectParameters.InputSerialization.Csv is required",
			ErrMissingParameter,
		)
	}

	if sp.OutputSerialization == nil || sp.OutputSerialization.Csv == nil {
		return fmt.Errorf(
			"%w: SelectParameters.OutputSerialization.Csv is required",
			ErrMissingParameter,
		)
	}

	if _, err := parseSelectQuery(sp.Expression); err != nil {
		return fmt.Errorf("%w: Expression: %w", ErrValidation, err)
	}

	return nil
}

// validateOutputLocation checks that OutputLocation.S3.BucketName is present, as
// required for a select job.
func validateOutputLocation(ol *outputLocationDTO) error {
	if ol == nil || ol.S3 == nil || strings.TrimSpace(ol.S3.BucketName) == "" {
		return fmt.Errorf(
			"%w: OutputLocation.S3.BucketName is required for a select job",
			ErrMissingParameter,
		)
	}

	return nil
}

// computeJobOutputPath derives the s3:// URI a select job's results are written
// to, echoed via InitiateJob's x-amz-job-output-path header and
// DescribeJob/ListJobs' JobOutputPath field. See select_output.go's
// selectOutputBaseKey for the actual object key layout under this path.
func computeJobOutputPath(ol *outputLocationDTO, jobID string) string {
	if ol == nil || ol.S3 == nil || ol.S3.BucketName == "" {
		return ""
	}

	return "s3://" + ol.S3.BucketName + "/" + selectOutputBaseKey(ol, jobID)
}

// executeSelect runs a select job's SQL expression against archiveData (the raw bytes
// of the retrieved archive, interpreted per InputSerialization.Csv) and formats the
// matching rows per OutputSerialization.Csv.
func executeSelect(archiveData []byte, sp *selectParametersDTO) ([]byte, error) {
	q, err := parseSelectQuery(sp.Expression)
	if err != nil {
		return nil, fmt.Errorf("invalid select expression: %w", err)
	}

	header, rows, err := parseSelectCSVInput(archiveData, sp.InputSerialization.Csv)
	if err != nil {
		return nil, err
	}

	out := make([][]string, 0, len(rows))

	for _, row := range rows {
		if !selectConditionMatches(q.where, row, header) {
			continue
		}

		out = append(out, projectSelectRow(q, row, header))
	}

	return formatSelectCSVOutput(out, sp.OutputSerialization.Csv), nil
}

// parseSelectCSVInput parses archiveData as CSV per cfg (FieldDelimiter/Comments/
// FileHeaderInfo), returning an optional header-name->index map (nil unless
// FileHeaderInfo is USE) and the data rows (header/ignored row excluded).
func parseSelectCSVInput(data []byte, cfg *csvInputDTO) (map[string]int, [][]string, error) {
	r := csv.NewReader(bytes.NewReader(data))
	r.FieldsPerRecord = -1
	r.LazyQuotes = true

	if cfg.FieldDelimiter != "" {
		r.Comma = []rune(cfg.FieldDelimiter)[0]
	}

	if cfg.Comments != "" {
		r.Comment = []rune(cfg.Comments)[0]
	}

	records, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: invalid CSV archive content: %w", ErrValidation, err)
	}

	var header map[string]int

	switch {
	case strings.EqualFold(cfg.FileHeaderInfo, "USE") && len(records) > 0:
		header = make(map[string]int, len(records[0]))
		for i, name := range records[0] {
			header[name] = i
		}

		records = records[1:]
	case strings.EqualFold(cfg.FileHeaderInfo, "IGNORE") && len(records) > 0:
		records = records[1:]
	}

	return header, records, nil
}

// resolveSelectField resolves a column reference (positional "_N", optionally
// alias-qualified via "alias.ref", or a header name when header is non-nil) against a
// CSV row. Returns ok=false for an unresolvable reference (out-of-range positional
// index, or unknown header name) rather than erroring -- an unresolved reference reads
// as an empty string in a projection and never matches in a WHERE predicate.
func resolveSelectField(ref string, row []string, header map[string]int) (string, bool) {
	if idx := strings.LastIndexByte(ref, '.'); idx >= 0 {
		ref = ref[idx+1:]
	}

	if len(ref) > 1 && ref[0] == '_' {
		if n, err := strconv.Atoi(ref[1:]); err == nil && n >= 1 && n <= len(row) {
			return row[n-1], true
		}
	}

	if header != nil {
		if i, ok := header[ref]; ok && i < len(row) {
			return row[i], true
		}
	}

	return "", false
}

// projectSelectRow builds the projected output row for a matching CSV row: the whole
// row for "SELECT *", or the resolved value of each requested column otherwise.
func projectSelectRow(q *selectQuery, row []string, header map[string]int) []string {
	if q.selectAll {
		out := make([]string, len(row))
		copy(out, row)

		return out
	}

	out := make([]string, len(q.columns))
	for i, col := range q.columns {
		out[i], _ = resolveSelectField(col.ref, row, header)
	}

	return out
}

// formatSelectCSVOutput renders projected rows per cfg (FieldDelimiter/RecordDelimiter/
// QuoteFields), using RFC 4180-style quoting.
func formatSelectCSVOutput(rows [][]string, cfg *csvOutputDTO) []byte {
	delim := ","
	if cfg != nil && cfg.FieldDelimiter != "" {
		delim = cfg.FieldDelimiter
	}

	recordDelim := "\n"
	if cfg != nil && cfg.RecordDelimiter != "" {
		recordDelim = cfg.RecordDelimiter
	}

	always := cfg != nil && strings.EqualFold(cfg.QuoteFields, "ALWAYS")

	var buf bytes.Buffer

	for _, row := range rows {
		for i, f := range row {
			if i > 0 {
				buf.WriteString(delim)
			}

			buf.WriteString(selectCSVOutputField(f, delim, always))
		}

		buf.WriteString(recordDelim)
	}

	return buf.Bytes()
}

// selectCSVOutputField encodes a single output field as RFC 4180 CSV, quoting when
// forced (QuoteFields=ALWAYS) or when the field contains the delimiter, a quote, or a
// newline.
func selectCSVOutputField(s, delim string, always bool) string {
	needsQuote := always || strings.Contains(s, delim) || strings.ContainsAny(s, "\"\n\r")
	if !needsQuote {
		return s
	}

	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}
