package azuresearch

import (
	"strconv"
	"strings"

	"golang.org/x/net/html"
)

const (
	maxHeaderDepth   = 6
	markdownOneToOne = "oneToOne"
	mdSections       = "sections"
	mdContent        = "content"
)

type mdSection struct {
	name    string
	content strings.Builder
	level   int
}

// headerOf reports the level and text of an ATX header line within the depth, outside code fences.
func headerOf(line string, depth int) (int, string, bool) {
	trimmed := strings.TrimRight(line, "\r\n")
	level := 0

	for level < len(trimmed) && trimmed[level] == '#' {
		level++
	}

	if level == 0 || level > depth || level >= len(trimmed) || trimmed[level] != ' ' {
		return 0, "", false
	}

	return level, strings.TrimSpace(strings.TrimRight(strings.TrimSpace(trimmed[level:]), "#")), true
}

func markdownDepth(cfg map[string]any) int {
	h := strOf(cfg, "markdownHeaderDepth")
	if len(h) == 2 && h[0] == 'h' && h[1] >= '1' && h[1] <= '6' {
		return int(h[1] - '0')
	}

	return maxHeaderDepth
}

// splitMarkdown cuts text into the preamble and the header sections, honouring code fences.
func splitMarkdown(text string, depth int) (string, []*mdSection) {
	var (
		preamble strings.Builder
		sections []*mdSection
		cur      *mdSection
		fenced   bool
	)

	for line := range strings.SplitAfterSeq(text, "\n") {
		if t := strings.TrimSpace(line); strings.HasPrefix(t, "```") || strings.HasPrefix(t, "~~~") {
			fenced = !fenced
		}

		if level, name, ok := headerOf(line, depth); ok && !fenced {
			cur = &mdSection{level: level, name: name}
			sections = append(sections, cur)

			continue
		}

		if cur == nil {
			preamble.WriteString(line)
		} else {
			cur.content.WriteString(line)
		}
	}

	return preamble.String(), sections
}

// markdownDocs parses Markdown blobs per Learn search-how-to-index-azure-blob-markdown.
func markdownDocs(data []byte, cfg map[string]any, withMeta func(map[string]any) map[string]any) []map[string]any {
	text := string(data)
	depth := markdownDepth(cfg)
	preamble, sections := splitMarkdown(text, depth)

	if strOf(cfg, "markdownParsingSubmode") == markdownOneToOne {
		return []map[string]any{withMeta(map[string]any{
			"document_content": text, mdSections: nestSections(sections),
		})}
	}

	var (
		out     []map[string]any
		headers = make([]string, depth)
		ordinal int
	)

	emit := func(content string) {
		if strings.TrimSpace(content) == "" {
			return
		}

		ordinal++
		secs := map[string]any{}

		for i, h := range headers {
			secs["h"+strconv.Itoa(i+1)] = h
		}

		out = append(out, withMeta(map[string]any{
			mdContent: content, mdSections: secs, "ordinal_position": float64(ordinal),
		}))
	}

	emit(preamble)

	for _, s := range sections {
		headers[s.level-1] = s.name
		for i := s.level; i < depth; i++ {
			headers[i] = ""
		}

		emit(s.content.String())
	}

	return out
}

// nestSections builds the oneToOne sections tree; ordinal_position counts sections in document order.
func nestSections(flat []*mdSection) []any {
	type node struct {
		obj   map[string]any
		level int
	}

	roots := []any{}
	stack := []node{}

	for i, s := range flat {
		obj := map[string]any{
			"header_level": "h" + strconv.Itoa(s.level), "header_name": s.name,
			mdContent: strings.TrimSpace(s.content.String()), "ordinal_position": float64(i + 1),
			mdSections: []any{},
		}

		for len(stack) > 0 && stack[len(stack)-1].level >= s.level {
			stack = stack[:len(stack)-1]
		}

		if len(stack) == 0 {
			roots = append(roots, obj)
		} else {
			parent := stack[len(stack)-1].obj
			parent[mdSections] = append(sectionList(parent), obj)
		}

		stack = append(stack, node{obj: obj, level: s.level})
	}

	return roots
}

func sectionList(n map[string]any) []any {
	l, _ := n[mdSections].([]any)

	return l
}

// htmlText returns the visible text of an HTML document: tags, scripts and styles are dropped.
func htmlText(data []byte) string {
	z := html.NewTokenizer(strings.NewReader(string(data)))

	var (
		sb   strings.Builder
		skip int
	)

	for {
		switch z.Next() {
		case html.ErrorToken:
			return strings.Join(strings.Fields(sb.String()), " ")
		case html.StartTagToken:
			if name, _ := z.TagName(); isHiddenTag(string(name)) {
				skip++
			}
		case html.EndTagToken:
			if name, _ := z.TagName(); isHiddenTag(string(name)) && skip > 0 {
				skip--
			}
		case html.TextToken:
			if skip == 0 {
				sb.Write(z.Text())
				sb.WriteByte(' ')
			}
		case html.SelfClosingTagToken, html.CommentToken, html.DoctypeToken:
		}
	}
}

func isHiddenTag(name string) bool { return name == "script" || name == "style" }
