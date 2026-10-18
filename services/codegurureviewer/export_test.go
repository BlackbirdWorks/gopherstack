package codegurureviewer

// ExportedFinding is a finding with the fields tests assert on.
type ExportedFinding struct {
	Rule, Category, Severity string
	Start, End               int
}

// AnalyzeFiles runs the rule set over path->content files.
func AnalyzeFiles(files map[string]string, analysisTypes []string) ([]ExportedFinding, int64) {
	src := make([]sourceFile, 0, len(files))
	for p, c := range files {
		src = append(src, sourceFile{Path: p, Content: []byte(c)})
	}

	found, loc := analyze(src, analysisTypes)
	out := make([]ExportedFinding, 0, len(found))

	for _, f := range found {
		out = append(out, ExportedFinding{
			Rule: f.rule.ID, Category: f.rule.Category, Severity: f.rule.Severity, Start: f.start, End: f.end,
		})
	}

	return out, loc
}
