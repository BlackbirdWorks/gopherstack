package translate

import (
	"bytes"
	"encoding/csv"
	"encoding/xml"
	"io"
	"slices"
	"strings"
	"unicode/utf8"
)

// termFile is what reading a terminology or parallel-data input file yields.
type termFile struct {
	source  string
	targets []string
	// records is the number of rows imported; skipped counts empty lines, empty
	// source texts and empty target texts; failed counts malformed rows.
	records int
	skipped int
	failed  int
	// chars is the UTF-8 character count of the imported translation text.
	chars int
}

const (
	formatCSV = "CSV"
	formatTSV = "TSV"
	formatTMX = "TMX"
)

// parseTermFile reads a CSV, TSV or TMX terminology/parallel-data file.
func parseTermFile(format string, data []byte) termFile {
	if strings.EqualFold(format, formatTMX) {
		return parseTMX(data)
	}

	comma := ','
	if strings.EqualFold(format, formatTSV) {
		comma = '\t'
	}

	return parseDelimited(data, comma)
}

func parseDelimited(data []byte, comma rune) termFile {
	skippedBlank := countBlankLines(data)

	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = comma
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	r.Comment = '#'

	var tf termFile

	header := true

	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}

		if err != nil {
			tf.failed++

			continue
		}

		if header {
			header = false
			tf.source, tf.targets = headerLanguages(row)

			continue
		}

		tf.addRow(row)
	}

	tf.skipped += skippedBlank

	return tf
}

// countBlankLines counts empty lines after the header (the csv reader drops them silently).
func countBlankLines(data []byte) int {
	lines := strings.Split(strings.TrimRight(string(data), "\r\n"), "\n")
	blank := 0

	for _, line := range lines[min(1, len(lines)):] {
		if strings.TrimSpace(line) == "" {
			blank++
		}
	}

	return blank
}

func headerLanguages(row []string) (string, []string) {
	if len(row) == 0 {
		return "", nil
	}

	var targets []string

	for _, col := range row[1:] {
		if t := strings.TrimSpace(col); t != "" {
			targets = append(targets, t)
		}
	}

	return strings.TrimSpace(row[0]), targets
}

// addRow classifies one data row: skipped when blank, the source text is empty or
// every target text is empty (parallel-data SkippedRecordCount doc), else imported.
func (tf *termFile) addRow(row []string) {
	if len(row) == 0 || (len(row) == 1 && strings.TrimSpace(row[0]) == "") {
		tf.skipped++

		return
	}

	src := strings.TrimSpace(row[0])
	chars := utf8.RuneCountInString(src)
	targetChars := 0

	for _, cell := range row[1:] {
		targetChars += utf8.RuneCountInString(strings.TrimSpace(cell))
	}

	if src == "" || targetChars == 0 {
		tf.skipped++

		return
	}

	tf.records++
	tf.chars += chars + targetChars
}

type tmxDoc struct {
	Header struct {
		SrcLang string `xml:"srclang,attr"`
	} `xml:"header"`
	Body struct {
		Units []struct {
			Variants []struct {
				Lang string `xml:"lang,attr"`
				Seg  string `xml:"seg"`
			} `xml:"tuv"`
		} `xml:"tu"`
	} `xml:"body"`
}

func parseTMX(data []byte) termFile {
	var doc tmxDoc

	var tf termFile

	if err := xml.Unmarshal(data, &doc); err != nil {
		tf.failed++

		return tf
	}

	tf.source = doc.Header.SrcLang

	for _, unit := range doc.Body.Units {
		texts := 0

		for _, v := range unit.Variants {
			seg := strings.TrimSpace(v.Seg)
			if seg == "" {
				continue
			}

			texts++
			tf.chars += utf8.RuneCountInString(seg)

			if v.Lang != "" && v.Lang != tf.source && !slices.Contains(tf.targets, v.Lang) {
				tf.targets = append(tf.targets, v.Lang)
			}
		}

		if texts < 2 { //nolint:mnd // a pair needs a source and a target text
			tf.skipped++

			continue
		}

		tf.records++
	}

	return tf
}
