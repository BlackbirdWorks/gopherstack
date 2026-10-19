package codegurureviewer

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
)

// Rule set. The analyzer is deterministic: the same files always yield the same recommendations.
//
//	aws-hardcoded-access-key  any text file   AKIA/ASIA access key IDs committed to source
//	unclosed-resource         go, py, java    files, sockets or connections opened with no Close in reach
//	err-nil-misuse            go              `if err == nil { return err }` (returns a nil error)
//	sql-string-concatenation  code files      SQL built by concatenation or formatting instead of parameters
//	todo-fixme-density        code files      three or more to-do/fix-me markers in one file

const (
	analysisSecurity    = "Security"
	analysisCodeQuality = "CodeQuality"

	categorySecurity      = "SecurityIssues"
	categoryResourceLeaks = "ResourceLeaks"
	categoryInconsistent  = "CodeInconsistencies"
	categoryMaintenance   = "CodeMaintenanceIssues"

	sevCritical = "Critical"
	sevHigh     = "High"
	sevMedium   = "Medium"
	sevLow      = "Low"

	maxFileBytes   = 1 << 20
	closeLookahead = 15
	todoThreshold  = 3
	binarySniff    = 8000
	keyMaskKeep    = 4

	langGo     = "go"
	langPython = "python"
	langJava   = "java"
	langJS     = "js"
	langOther  = "other"
)

type sourceFile struct {
	Path    string
	Content []byte
}

type ruleSpec struct {
	ID, Name, Short, Long, Category, Severity, Analysis string
	Tags                                                []string
}

type finding struct {
	rule        *ruleSpec
	path        string
	description string
	start, end  int
}

func ruleCatalog() map[string]*ruleSpec { return rulesOnce() }

var rulesOnce = sync.OnceValue(func() map[string]*ruleSpec { //nolint:gochecknoglobals // immutable rule table
	list := []*ruleSpec{
		{
			ID: "aws-hardcoded-access-key", Name: "Hard-coded AWS access key ID",
			Short: "An AWS access key ID is embedded in source code.",
			Long: "Credentials committed to source are readable by everyone with repository access. Load " +
				"them from the environment, an IAM role or AWS Secrets Manager.",
			Category: categorySecurity, Severity: sevCritical, Analysis: analysisSecurity,
			Tags: []string{"security", "aws", "secrets"},
		},
		{
			ID: "unclosed-resource", Name: "Resource is never closed",
			Short: "A file, socket or connection is opened but no close is reachable.",
			Long: "Unreleased handles leak file descriptors and connections under load. Close the resource " +
				"in a deferred call, a with-statement or a try-with-resources block.",
			Category: categoryResourceLeaks, Severity: sevHigh, Analysis: analysisCodeQuality,
			Tags: []string{"resource-leak", "reliability"},
		},
		{
			ID: "err-nil-misuse", Name: "Error returned inside a nil check",
			Short: "The error is returned inside a branch that only runs when it is nil.",
			Long: "`if err == nil { return err }` always returns a nil error. The condition is probably " +
				"inverted: use `err != nil`.",
			Category: categoryInconsistent, Severity: sevMedium, Analysis: analysisCodeQuality,
			Tags: []string{"error-handling", "go"},
		},
		{
			ID: "sql-string-concatenation", Name: "SQL built from string concatenation",
			Short: "A SQL statement is assembled by concatenation or formatting.",
			Long: "Interpolating values into SQL text enables SQL injection. Use parameterized queries or " +
				"prepared statements.",
			Category: categorySecurity, Severity: sevHigh, Analysis: analysisSecurity,
			Tags: []string{"security", "sql-injection", "injection"},
		},
		{
			ID: "todo-fixme-density", Name: "Many TODO/FIXME markers",
			Short: "The file carries several unresolved TODO or FIXME markers.",
			Long: "A cluster of TODO/FIXME comments signals unfinished work. Resolve them or track them in " +
				"an issue.",
			Category: categoryMaintenance, Severity: sevLow, Analysis: analysisCodeQuality,
			Tags: []string{"maintainability"},
		},
	}

	out := make(map[string]*ruleSpec, len(list))
	for _, r := range list {
		out[r.ID] = r
	}

	return out
})

var (
	accessKeyRe = regexp.MustCompile(`\b(?:AKIA|ASIA)[0-9A-Z]{16}\b`)
	goOpenRe    = regexp.MustCompile(`(?::=|=)\s*(?:os\.(?:Open|OpenFile|Create)|net\.Dial|sql\.Open)\(`)
	pyOpenRe    = regexp.MustCompile(`^\s*(\w+)\s*=\s*open\(`)
	javaOpenRe  = regexp.MustCompile(
		`new\s+(?:FileInputStream|FileOutputStream|FileReader|FileWriter|Socket)\s*\(|new\s+Scanner\s*\(\s*new\s+File`)
	javaTryRe   = regexp.MustCompile(`\btry\s*\(`)
	errNilIfRe  = regexp.MustCompile(`^\s*if\s+err\s*==\s*nil\s*\{\s*$`)
	errNilOneRe = regexp.MustCompile(`^\s*if\s+err\s*==\s*nil\s*\{\s*return\b[^}]*\berr\s*\}\s*$`)
	errReturnRe = regexp.MustCompile(`^\s*return\b.*\berr\s*$`)
	todoRe      = regexp.MustCompile(`\b(?:TODO|FIXME)\b`)
)

var codeExts = map[string]string{ //nolint:gochecknoglobals // immutable lookup
	".go": langGo, ".py": langPython, ".java": langJava, ".kt": langJava, ".scala": langJava,
	".js": langJS, ".jsx": langJS, ".ts": langJS, ".tsx": langJS, ".mjs": langJS,
	".rb": langOther, ".cs": langOther, ".php": langOther, ".c": langOther, ".cc": langOther, ".cpp": langOther,
	".h": langOther, ".rs": langOther, ".swift": langOther,
}

var sqlConcatRes = sync.OnceValue(func() []*regexp.Regexp { //nolint:gochecknoglobals // compiled once
	return []*regexp.Regexp{
		regexp.MustCompile("(?i)[\"'`]\\s*(?:select\\b[^\"'`]*\\bfrom\\b|insert\\s+into\\b|update\\s+\\w+\\s+set\\b|" +
			"delete\\s+from\\b)[^\"'`]*[\"'`]\\s*(?:\\+|%)\\s*\\w"),
		regexp.MustCompile("(?i)\\bSprintf\\(\\s*[\"'`]\\s*(?:select|insert|update|delete)\\b[^\"'`]*%[sdvq]"),
		regexp.MustCompile(`(?i)\bf["']\s*(?:select|insert|update|delete)\b[^"']*\{`),
		regexp.MustCompile("(?i)`\\s*(?:select|insert|update|delete)\\b[^`]*\\$\\{"),
		regexp.MustCompile(`(?i)["']\s*(?:select|insert|update|delete)\b[^"']*\{\}[^"']*["']\s*\.format\(`),
	}
})

func isText(content []byte) bool {
	head := content
	if len(head) > binarySniff {
		head = head[:binarySniff]
	}

	return !bytes.Contains(head, []byte{0})
}

func language(p string) string { return codeExts[strings.ToLower(path.Ext(p))] }

func maskKey(k string) string { return k[:keyMaskKeep] + strings.Repeat("*", len(k)-keyMaskKeep) }

// analyze runs the enabled rules over files and returns the findings and the metered lines of code.
func analyze(files []sourceFile, analysisTypes []string) ([]finding, int64) {
	slices.SortFunc(files, func(a, b sourceFile) int { return strings.Compare(a.Path, b.Path) })

	var (
		out []finding
		loc int64
	)

	for _, f := range files {
		found, lines := analyzeFile(f, analysisTypes)
		out = append(out, found...)
		loc += lines
	}

	slices.SortStableFunc(out, func(a, b finding) int {
		if c := strings.Compare(a.path, b.path); c != 0 {
			return c
		}

		if a.start != b.start {
			return a.start - b.start
		}

		return strings.Compare(a.rule.ID, b.rule.ID)
	})

	return out, loc
}

func analyzeFile(f sourceFile, analysisTypes []string) ([]finding, int64) {
	if len(f.Content) > maxFileBytes || !isText(f.Content) {
		return nil, 0
	}

	lines := strings.Split(strings.ReplaceAll(string(f.Content), "\r\n", "\n"), "\n")
	lang := language(f.Path)

	var loc int64

	if lang != "" {
		for _, l := range lines {
			if strings.TrimSpace(l) != "" {
				loc++
			}
		}
	}

	var out []finding

	for _, find := range runRules(f.Path, lang, lines) {
		if slices.Contains(analysisTypes, find.rule.Analysis) {
			out = append(out, find)
		}
	}

	return out, loc
}

func runRules(p, lang string, lines []string) []finding {
	cat := ruleCatalog()
	out := keyFindings(cat["aws-hardcoded-access-key"], p, lines)

	if lang == "" {
		return out
	}

	out = append(out, resourceFindings(cat["unclosed-resource"], p, lang, lines)...)
	out = append(out, sqlFindings(cat["sql-string-concatenation"], p, lines)...)
	out = append(out, todoFindings(cat["todo-fixme-density"], p, lines)...)

	if lang == langGo {
		out = append(out, errNilFindings(cat["err-nil-misuse"], p, lines)...)
	}

	return out
}

func keyFindings(r *ruleSpec, p string, lines []string) []finding {
	var out []finding

	for i, l := range lines {
		for _, k := range accessKeyRe.FindAllString(l, -1) {
			out = append(out, finding{
				rule: r, path: p, start: i + 1, end: i + 1,
				description: fmt.Sprintf("Hard-coded AWS access key ID %s found. %s", maskKey(k), r.Long),
			})
		}
	}

	return out
}

func resourceFindings(r *ruleSpec, p, lang string, lines []string) []finding {
	var out []finding

	add := func(i int, what string) {
		out = append(out, finding{
			rule: r, path: p, start: i + 1, end: i + 1,
			description: what + " is opened here but never closed. " + r.Long,
		})
	}

	whole := strings.Join(lines, "\n")

	for i, l := range lines {
		switch lang {
		case langGo:
			if goOpenRe.MatchString(l) && !closedSoon(lines, i) {
				add(i, "A file or connection")
			}
		case langPython:
			if m := pyOpenRe.FindStringSubmatch(l); m != nil && !strings.Contains(whole, m[1]+".close()") {
				add(i, "File `"+m[1]+"`")
			}
		case langJava:
			if javaOpenRe.MatchString(l) && !javaTryRe.MatchString(l) && !strings.Contains(whole, ".close()") {
				add(i, "A stream, reader or socket")
			}
		}
	}

	return out
}

func closedSoon(lines []string, i int) bool {
	end := min(len(lines), i+1+closeLookahead)
	for _, l := range lines[i+1 : end] {
		if strings.Contains(l, ".Close()") {
			return true
		}
	}

	return false
}

func errNilFindings(r *ruleSpec, p string, lines []string) []finding {
	var out []finding

	for i, l := range lines {
		switch {
		case errNilOneRe.MatchString(l):
			out = append(out, finding{rule: r, path: p, start: i + 1, end: i + 1, description: r.Short + " " + r.Long})
		case errNilIfRe.MatchString(l):
			for j := i + 1; j < len(lines); j++ {
				if strings.TrimSpace(lines[j]) == "" {
					continue
				}

				if errReturnRe.MatchString(lines[j]) {
					out = append(out, finding{
						rule: r, path: p, start: i + 1, end: j + 1, description: r.Short + " " + r.Long,
					})
				}

				break
			}
		}
	}

	return out
}

func sqlFindings(r *ruleSpec, p string, lines []string) []finding {
	var out []finding

	for i, l := range lines {
		if slices.ContainsFunc(sqlConcatRes(), func(re *regexp.Regexp) bool { return re.MatchString(l) }) {
			out = append(out, finding{rule: r, path: p, start: i + 1, end: i + 1, description: r.Short + " " + r.Long})
		}
	}

	return out
}

func todoFindings(r *ruleSpec, p string, lines []string) []finding {
	var marks []int

	for i, l := range lines {
		if todoRe.MatchString(l) {
			marks = append(marks, i+1)
		}
	}

	if len(marks) < todoThreshold {
		return nil
	}

	return []finding{{
		rule: r, path: p, start: marks[0], end: marks[len(marks)-1],
		description: strconv.Itoa(len(marks)) + " TODO/FIXME markers in this file. " + r.Long,
	}}
}

func recommendationID(f *finding) string {
	sum := sha256.Sum256([]byte(f.rule.ID + "|" + f.path + "|" + strconv.Itoa(f.start) + "|" + strconv.Itoa(f.end)))

	return hex.EncodeToString(sum[:])
}
