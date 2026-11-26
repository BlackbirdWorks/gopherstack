package main

import (
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

const (
	githubRepoBase   = "https://github.com/BlackbirdWorks/gopherstack"
	mdLinkMatchCount = 3
	mdExt            = ".md"
	routeServices    = "/services/"
	routeArch        = "/architecture/"
	routeGuide       = "/guide/"
	routeExamples    = "/examples/"
)

// knownHTMLTags lists HTML tag names that are valid in VitePress Markdown
// outside fenced code blocks and should not be escaped as placeholder text.
func knownHTMLTags() map[string]bool {
	return map[string]bool{
		"a": true, "b": true, "blockquote": true, "br": true, "code": true,
		"details": true, "div": true, "em": true, "h1": true, "h2": true,
		"h3": true, "h4": true, "hr": true, "i": true, "img": true,
		"kbd": true, "li": true, "ol": true, "p": true, "pre": true,
		"s": true, "span": true, "strong": true, "sub": true, "summary": true,
		"sup": true, "table": true, "tbody": true, "td": true, "th": true,
		"thead": true, "tr": true, "ul": true,
	}
}

// placeholderTagRe matches angle-bracket tokens like <region>, <OS>, <svc>
// outside HTML closing/self-closing tags or URLs.
var placeholderTagRe = regexp.MustCompile(`<([A-Za-z][A-Za-z0-9_.-]*)>`)

// mdLinkRe matches standard Markdown links [text](target).
var mdLinkRe = regexp.MustCompile(`\[([^\]]+)\]\(([^)\s]+)\)`)

// sanitizeMarkdownForVitePress prepares repository Markdown for VitePress/Vue
// compilation: it rewrites relative links (resolved against srcDir, the
// repo-relative directory holding the source file) and escapes un-fenced Vue
// mustache delimiters and placeholder angle-bracket tags.
func sanitizeMarkdownForVitePress(content, srcDir string) string {
	lines := strings.Split(content, "\n")
	out := make([]string, 0, len(lines))

	var open fence

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if open.n > 0 {
			if open.closedBy(trimmed) {
				open = fence{}
			}
			out = append(out, line)

			continue
		}

		if f, ok := parseFenceOpen(trimmed); ok {
			open = f
			out = append(out, line)

			continue
		}

		out = append(out, sanitizeInlineProse(rewriteMarkdownLinks(line, srcDir)))
	}

	return strings.Join(out, "\n")
}

// fence is an open fenced code block: its marker byte and opening length.
type fence struct {
	char byte
	n    int
}

const minFenceLen = 3

// parseFenceOpen reports whether trimmed opens a CommonMark fenced code block.
func parseFenceOpen(trimmed string) (fence, bool) {
	if trimmed == "" || (trimmed[0] != '`' && trimmed[0] != '~') {
		return fence{}, false
	}

	n := fenceRun(trimmed, trimmed[0])
	if n < minFenceLen {
		return fence{}, false
	}

	if trimmed[0] == '`' && strings.Contains(trimmed[n:], "`") {
		return fence{}, false
	}

	return fence{char: trimmed[0], n: n}, true
}

// closedBy reports whether trimmed closes f: the same marker, at least as
// long as the opener, and nothing after it.
func (f fence) closedBy(trimmed string) bool {
	n := fenceRun(trimmed, f.char)

	return n >= f.n && n == len(trimmed)
}

func fenceRun(s string, c byte) int {
	n := 0
	for n < len(s) && s[n] == c {
		n++
	}

	return n
}

// sanitizeInlineProse escapes Vue template syntax ({{, }}, and non-HTML
// <placeholder> tags) in non-code segments of a Markdown line while preserving
// inline `...` code spans verbatim.
func sanitizeInlineProse(line string) string {
	if !strings.ContainsAny(line, "{<") {
		return line
	}

	var b strings.Builder
	i := 0
	for i < len(line) {
		if line[i] == '`' {
			end := findClosingBacktick(line, i)
			b.WriteString(line[i:end])
			i = end

			continue
		}

		nextTick := strings.IndexByte(line[i:], '`')
		if nextTick < 0 {
			b.WriteString(escapeVueText(line[i:]))

			break
		}

		b.WriteString(escapeVueText(line[i : i+nextTick]))
		i += nextTick
	}

	return b.String()
}

// findClosingBacktick returns the index just past the inline code span
// starting at start, handling multi-backtick spans.
func findClosingBacktick(s string, start int) int {
	tickLen := 0
	for start+tickLen < len(s) && s[start+tickLen] == '`' {
		tickLen++
	}

	needle := strings.Repeat("`", tickLen)
	idx := strings.Index(s[start+tickLen:], needle)
	if idx < 0 {
		return start + tickLen
	}

	return start + tickLen + idx + tickLen
}

// escapeVueText escapes Vue mustache tokens and placeholder <tag>s in plain prose.
func escapeVueText(segment string) string {
	escaped := strings.ReplaceAll(segment, "{{", "&#123;&#123;")
	escaped = strings.ReplaceAll(escaped, "}}", "&#125;&#125;")

	tags := knownHTMLTags()

	return placeholderTagRe.ReplaceAllStringFunc(escaped, func(match string) string {
		inner := match[1 : len(match)-1]
		if tags[strings.ToLower(inner)] {
			return match
		}

		return "&lt;" + inner + "&gt;"
	})
}

// rewriteMarkdownLinks rewrites relative Markdown links outside inline code
// spans to site routes or GitHub URLs, resolving them against srcDir.
func rewriteMarkdownLinks(line, srcDir string) string {
	if !strings.Contains(line, "](") {
		return line
	}

	masked, spans := maskCodeSpans(line)
	masked = mdLinkRe.ReplaceAllStringFunc(masked, func(full string) string {
		m := mdLinkRe.FindStringSubmatch(full)
		if len(m) != mdLinkMatchCount || isExternalOrAnchor(m[2]) {
			return full
		}

		return "[" + m[1] + "](" + resolveSiteLink(m[2], srcDir) + ")"
	})

	for i, span := range spans {
		masked = strings.Replace(masked, codeSpanMark(i), span, 1)
	}

	return masked
}

func codeSpanMark(i int) string {
	return "\x00" + strconv.Itoa(i) + "\x00"
}

// maskCodeSpans swaps inline code spans for placeholders so link rewriting
// cannot touch their contents.
func maskCodeSpans(line string) (string, []string) {
	var (
		b     strings.Builder
		spans []string
	)

	for i := 0; i < len(line); {
		if line[i] != '`' {
			b.WriteByte(line[i])
			i++

			continue
		}

		end := findClosingBacktick(line, i)
		b.WriteString(codeSpanMark(len(spans)))
		spans = append(spans, line[i:end])
		i = end
	}

	return b.String(), spans
}

var schemeRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9+.-]*:`)

// isExternalOrAnchor reports whether target has a URL scheme, is rooted, or
// is a hash anchor.
func isExternalOrAnchor(target string) bool {
	return schemeRe.MatchString(target) ||
		strings.HasPrefix(target, "#") ||
		strings.HasPrefix(target, "/")
}

// resolveSiteLink maps a link relative to srcDir onto a generated site route,
// or a GitHub URL for repository files that have no page.
func resolveSiteLink(target, srcDir string) string {
	pathPart, anchor := splitAnchor(target)
	if pathPart == "" {
		return target
	}

	isDir := strings.HasSuffix(pathPart, "/")
	repoPath := path.Join(srcDir, pathPart)
	if repoPath == ".." || strings.HasPrefix(repoPath, "../") {
		return target
	}

	if repoPath == readmeFileName && anchor == "#services" {
		return routeServices
	}

	if route, ok := repoPathRoute(repoPath); ok {
		return route + anchor
	}

	kind := "blob"
	if isDir {
		kind = "tree"
	}

	return githubRepoBase + "/" + kind + "/main/" + repoPath + anchor
}

func staticRepoRoutes() map[string]string {
	return map[string]string{
		readmeFileName:  "/",
		docsServicesDir: routeServices,
		servicesDirName: routeServices,
		docsArchDirName: routeArch,
		path.Join(docsArchDirName, readmeFileName): routeArch,
		examplesDirName: routeExamples,
		path.Join(examplesDirName, readmeFileName): routeExamples,
	}
}

// repoPathRoute returns the site route generated for a repo path, if any.
func repoPathRoute(p string) (string, bool) {
	if route, ok := staticRepoRoutes()[p]; ok {
		return route, true
	}

	dir, file := path.Split(p)
	dir = strings.TrimSuffix(dir, "/")
	name, isMD := strings.CutSuffix(file, mdExt)
	if !isMD {
		return "", false
	}

	switch {
	case dir == docsServicesDir:
		return routeServices + name, true
	case dir == docsArchDirName && slices.Contains(archDocNames(), file):
		return routeArch + name, true
	case dir == docsDirName && slices.Contains(guideDocNames(), file):
		return routeGuide + name, true
	case file == readmeFileName && path.Dir(dir) == servicesDirName:
		return routeServices + path.Base(dir), true
	case file == readmeFileName && path.Dir(dir) == examplesDirName:
		return routeExamples + path.Base(dir), true
	}

	return "", false
}

func splitAnchor(target string) (string, string) {
	if idx := strings.IndexByte(target, '#'); idx >= 0 {
		return target[:idx], target[idx:]
	}

	return target, ""
}

// stripLeadingH1 removes the first top-level "# Title" heading (and immediate
// blank line) when embedding docs/services/<svc>.md inside a generated page.
func stripLeadingH1(content string) string {
	lines := strings.Split(content, "\n")
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "# ") {
			rest := lines[i+1:]
			for len(rest) > 0 && strings.TrimSpace(rest[0]) == "" {
				rest = rest[1:]
			}

			return strings.Join(rest, "\n")
		}

		break
	}

	return content
}

// extractMarkdownTitle returns the text of the first "# Heading" line, or fallback.
func extractMarkdownTitle(content, fallback string) string {
	for line := range strings.SplitSeq(content, "\n") {
		trimmed := strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(trimmed, "# "); ok {
			return strings.TrimSpace(after)
		}
	}

	return fallback
}

// escapeYAMLString wraps s in double quotes with internal quotes escaped.
func escapeYAMLString(s string) string {
	escaped := strings.ReplaceAll(s, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)

	return `"` + escaped + `"`
}
