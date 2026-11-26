package main

import (
	"regexp"
	"strings"
)

const (
	githubRepoBase   = "https://github.com/BlackbirdWorks/gopherstack"
	fenceTriple      = "```"
	fenceTilde       = "~~~"
	mdLinkMatchCount = 3
	serviceLinkParts = 2
	mdExt            = ".md"
	routeServices    = "/services/"
	routeArch        = "/architecture/"
	routeGuide       = "/guide/"
	kindGuide        = "guide"
	kindArch         = "arch"
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
// compilation by rewriting internal links and escaping un-fenced Vue mustache
// delimiters and placeholder angle-bracket tags.
func sanitizeMarkdownForVitePress(content, sourceKind string) string {
	lines := strings.Split(content, "\n")
	out := make([]string, 0, len(lines))
	inFence := false
	fenceChar := ""

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if isFenceToggle(trimmed, inFence, fenceChar) {
			if !inFence {
				inFence = true
				fenceChar = fencePrefix(trimmed)
			} else {
				inFence = false
				fenceChar = ""
			}
			out = append(out, line)

			continue
		}

		if inFence {
			out = append(out, line)

			continue
		}

		rewritten := rewriteMarkdownLinks(line, sourceKind)
		out = append(out, sanitizeInlineProse(rewritten))
	}

	return strings.Join(out, "\n")
}

// isFenceToggle reports whether trimmed opens or closes a fenced code block.
func isFenceToggle(trimmed string, inFence bool, activeFence string) bool {
	if !inFence {
		return strings.HasPrefix(trimmed, fenceTriple) || strings.HasPrefix(trimmed, fenceTilde)
	}

	return strings.HasPrefix(trimmed, activeFence)
}

// fencePrefix returns the fence marker prefix ("```" or "~~~") of trimmed.
func fencePrefix(trimmed string) string {
	if strings.HasPrefix(trimmed, fenceTilde) {
		return fenceTilde
	}

	return fenceTriple
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

// rewriteMarkdownLinks translates repository-relative Markdown links into
// clean VitePress routes or GitHub source links based on sourceKind
// ("guide", "arch", "service", or "example").
func rewriteMarkdownLinks(line, sourceKind string) string {
	return mdLinkRe.ReplaceAllStringFunc(line, func(full string) string {
		m := mdLinkRe.FindStringSubmatch(full)
		if len(m) != mdLinkMatchCount {
			return full
		}

		label, target := m[1], m[2]
		if isExternalOrAnchor(target) {
			return full
		}

		rewritten := resolveSiteLink(target, sourceKind)

		return "[" + label + "](" + rewritten + ")"
	})
}

// isExternalOrAnchor reports whether target is an HTTP URL, mailto, or hash anchor.
func isExternalOrAnchor(target string) bool {
	return strings.HasPrefix(target, "http://") ||
		strings.HasPrefix(target, "https://") ||
		strings.HasPrefix(target, "mailto:") ||
		strings.HasPrefix(target, "#")
}

// resolveSiteLink maps a relative docs/ or examples/ target to a site route or
// GitHub blob URL so VitePress never encounters broken relative .md links.
func resolveSiteLink(target, sourceKind string) string {
	pathPart, anchor := splitAnchor(target)

	if mapped, ok := mapDirectTarget(pathPart, sourceKind); ok {
		return mapped + anchor
	}

	if strings.HasPrefix(pathPart, "../../services/") {
		return mapServiceRelativeLink(pathPart) + anchor
	}

	if after, ok := strings.CutPrefix(pathPart, "../../docs/services/"); ok {
		return mapDocsServiceLink(after) + anchor
	}

	if after, ok := strings.CutPrefix(pathPart, "../architecture/"); ok {
		return mapArchTarget(after) + anchor
	}

	if after, ok := strings.CutPrefix(pathPart, "architecture/"); ok {
		return mapArchTarget(after) + anchor
	}

	if after, ok := strings.CutPrefix(pathPart, "../"); ok && sourceKind == kindGuide {
		return githubRepoBase + "/blob/main/" + after + anchor
	}

	if pathPart == "../../AZURE.md" {
		return githubRepoBase + "/blob/main/AZURE.md" + anchor
	}

	if strings.HasPrefix(pathPart, "../../README.md") {
		if anchor == "#services" {
			return routeServices
		}

		return "/" + anchor
	}

	return target
}

func splitAnchor(target string) (string, string) {
	if idx := strings.IndexByte(target, '#'); idx >= 0 {
		return target[:idx], target[idx:]
	}

	return target, ""
}

func mapDirectTarget(pathPart, sourceKind string) (string, bool) {
	switch sourceKind {
	case kindGuide:
		switch pathPart {
		case "services/", "services":
			return routeServices, true
		case "architecture/", "architecture":
			return routeArch, true
		}
		if strings.HasSuffix(pathPart, mdExt) && !strings.Contains(pathPart, "/") {
			return routeGuide + strings.TrimSuffix(pathPart, mdExt), true
		}
	case kindArch:
		if pathPart == readmeFileName {
			return routeArch, true
		}
		if strings.HasSuffix(pathPart, mdExt) && !strings.Contains(pathPart, "/") {
			return routeArch + strings.TrimSuffix(pathPart, mdExt), true
		}
	}

	return "", false
}

func mapArchTarget(name string) string {
	if name == "" || name == readmeFileName {
		return routeArch
	}

	return routeArch + strings.TrimSuffix(name, mdExt)
}

func mapDocsServiceLink(name string) string {
	if name == "" {
		return routeServices
	}

	return routeServices + strings.TrimSuffix(name, mdExt)
}

func mapServiceRelativeLink(pathPart string) string {
	rest := strings.TrimPrefix(pathPart, "../../services/")
	parts := strings.SplitN(rest, "/", serviceLinkParts)
	if len(parts) == serviceLinkParts && parts[1] == readmeFileName {
		return routeServices + parts[0]
	}

	return githubRepoBase + "/blob/main/services/" + rest
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
