package azuresearch

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	skillSplit  = "#Microsoft.Skills.Text.SplitSkill"
	skillMerge  = "#Microsoft.Skills.Text.MergeSkill"
	skillShaper = "#Microsoft.Skills.Util.ShaperSkill"

	defaultPageLen = 10000
)

// enrichTree is the document enrichment tree; its root has the single child "document".
type enrichTree map[string]any

func splitPath(p string) []string {
	return strings.Split(strings.Trim(p, "/"), "/")
}

// get reads a path; a "*" segment fans out over array elements.
func (t enrichTree) get(path string) (any, bool) {
	return walk(map[string]any(t), splitPath(path))
}

func walk(cur any, segs []string) (any, bool) {
	if len(segs) == 0 {
		return cur, cur != nil
	}

	seg, rest := segs[0], segs[1:]

	switch c := cur.(type) {
	case map[string]any:
		if seg == "*" {
			return nil, false
		}

		v, ok := c[seg]
		if !ok {
			return nil, false
		}

		return walk(v, rest)
	case []any:
		if seg == "*" {
			out := make([]any, 0, len(c))

			for _, el := range c {
				if v, ok := walk(el, rest); ok {
					out = append(out, v)
				}
			}

			return out, true
		}

		i, err := strconv.Atoi(seg)
		if err != nil || i < 0 || i >= len(c) {
			return nil, false
		}

		return walk(c[i], rest)
	}

	return nil, false
}

func (t enrichTree) set(path string, v any) {
	segs := splitPath(path)

	var cur any = map[string]any(t)

	for i, seg := range segs {
		last := i == len(segs)-1

		switch c := cur.(type) {
		case map[string]any:
			if last {
				c[seg] = v

				return
			}

			next, ok := c[seg]
			if !ok {
				next = map[string]any{}
				c[seg] = next
			}

			cur = next
		case []any:
			idx, err := strconv.Atoi(seg)
			if err != nil || idx < 0 || idx >= len(c) {
				return
			}

			if last {
				c[idx] = v

				return
			}

			cur = c[idx]
		default:
			return
		}
	}
}

// expandContext resolves a skill context ("/document" or "/document/pages/*") to concrete paths.
func (t enrichTree) expandContext(ctx string) []string {
	segs := splitPath(ctx)
	paths := []string{""}

	for _, seg := range segs {
		var next []string

		for _, p := range paths {
			if seg != "*" {
				next = append(next, p+"/"+seg)

				continue
			}

			node, ok := t.get(p)
			arr, isArr := node.([]any)

			if !ok || !isArr {
				continue
			}

			for i := range arr {
				next = append(next, p+"/"+strconv.Itoa(i))
			}
		}

		paths = next
	}

	return paths
}

// bind maps a source path onto a concrete context: the wildcard prefix shared with ctx is replaced.
func bind(source, ctxPattern, ctxConcrete string) string {
	if source == ctxPattern {
		return ctxConcrete
	}

	if rest, ok := strings.CutPrefix(source, ctxPattern+"/"); ok {
		return ctxConcrete + "/" + rest
	}

	return source
}

// runSkillset applies the skillset's skills to the enrichment tree, in order.
func runSkillset(set Object, tree enrichTree, warn func(skill, msg string)) error {
	for _, raw := range listOf(set, "skills") {
		sk := objOf(raw)
		typ := strOf(sk, "@odata.type")

		exec, ok := skillExecutors[typ]
		if !ok {
			warn(
				typ,
				"The skill type "+typ+" is not supported by the emulator and was skipped; its outputs are not populated.",
			)

			continue
		}

		ctx := strOf(sk, "context")
		if ctx == "" {
			ctx = "/document"
		}

		for _, concrete := range tree.expandContext(ctx) {
			if err := exec(sk, tree, ctx, concrete); err != nil {
				return err
			}
		}
	}

	return nil
}

type skillExec func(sk map[string]any, t enrichTree, ctx, concrete string) error

var skillExecutors = map[string]skillExec{ //nolint:gochecknoglobals // dispatch table
	skillSplit:  execSplit,
	skillMerge:  execMerge,
	skillShaper: execShaper,
}

func skillInput(sk map[string]any, t enrichTree, name, ctx, concrete string) (any, bool) {
	for _, raw := range listOf(sk, "inputs") {
		in := objOf(raw)
		if strOf(in, "name") != name {
			continue
		}

		return t.get(bind(strOf(in, "source"), ctx, concrete))
	}

	return nil, false
}

func skillOutput(sk map[string]any, t enrichTree, name string, v any, concrete string) {
	for _, raw := range listOf(sk, "outputs") {
		o := objOf(raw)
		if strOf(o, "name") != name {
			continue
		}

		target := strOf(o, "targetName")
		if target == "" {
			target = name
		}

		t.set(concrete+"/"+target, v)
	}
}

func numParam(sk map[string]any, name string, def int) int {
	if f, ok := sk[name].(float64); ok {
		return int(f)
	}

	return def
}

func execSplit(sk map[string]any, t enrichTree, ctx, concrete string) error {
	v, ok := skillInput(sk, t, "text", ctx, concrete)
	if !ok {
		return nil
	}

	text, _ := v.(string)
	maxLen := numParam(sk, "maximumPageLength", defaultPageLen)

	if maxLen < 1 {
		return badf("Skill %s: maximumPageLength must be positive.", skillSplit)
	}

	var items []string

	if strOf(sk, "textSplitMode") == "sentences" {
		items = splitSentences(text)
	} else {
		items = splitPages(text, maxLen, numParam(sk, "pageOverlapLength", 0))
	}

	if take := numParam(sk, "maximumPagesToTake", 0); take > 0 && len(items) > take {
		items = items[:take]
	}

	out := make([]any, len(items))
	for i, s := range items {
		out[i] = s
	}

	skillOutput(sk, t, "textItems", out, concrete)

	return nil
}

func splitSentences(text string) []string {
	var (
		out []string
		cur strings.Builder
	)

	runes := []rune(text)
	for i, r := range runes {
		cur.WriteRune(r)

		endMark := r == '.' || r == '!' || r == '?'
		if endMark && (i+1 == len(runes) || runes[i+1] == ' ' || runes[i+1] == '\n') {
			out = append(out, strings.TrimSpace(cur.String()))
			cur.Reset()
		}
	}

	if s := strings.TrimSpace(cur.String()); s != "" {
		out = append(out, s)
	}

	return out
}

// splitPages packs sentences into pages of at most maxLen characters, hard-splitting oversized sentences.
func splitPages(text string, maxLen, overlap int) []string {
	var (
		pages []string
		cur   []rune
	)

	flush := func() {
		if len(cur) > 0 {
			pages = append(pages, strings.TrimSpace(string(cur)))
		}
	}

	for _, sent := range splitSentences(text) {
		sr := []rune(sent)

		for len(sr) > maxLen {
			flush()

			cur = nil

			pages = append(pages, string(sr[:maxLen]))
			sr = sr[maxLen:]
		}

		if len(cur)+len(sr)+1 > maxLen {
			flush()

			cur = tail(pages, overlap)
		}

		if len(cur) > 0 {
			cur = append(cur, ' ')
		}

		cur = append(cur, sr...)
	}

	flush()

	return pages
}

func tail(pages []string, overlap int) []rune {
	if overlap <= 0 || len(pages) == 0 {
		return nil
	}

	r := []rune(pages[len(pages)-1])
	if len(r) > overlap {
		r = r[len(r)-overlap:]
	}

	return r
}

func execMerge(sk map[string]any, t enrichTree, ctx, concrete string) error {
	tv, _ := skillInput(sk, t, "text", ctx, concrete)
	text, _ := tv.(string)
	itemsV, _ := skillInput(sk, t, "itemsToInsert", ctx, concrete)
	offV, _ := skillInput(sk, t, "offsets", ctx, concrete)

	items, _ := itemsV.([]any)
	offs, _ := offV.([]any)
	pre, post := " ", " "

	if v, ok := sk["insertPreTag"].(string); ok {
		pre = v
	}

	if v, ok := sk["insertPostTag"].(string); ok {
		post = v
	}

	runes := []rune(text)
	if len(items) != len(offs) {
		return badf("Skill %s: itemsToInsert and offsets must have the same length.", skillMerge)
	}

	merged, positions := mergeAt(runes, items, offs, pre, post)

	skillOutput(sk, t, "mergedText", merged, concrete)
	skillOutput(sk, t, "mergedOffsets", positions, concrete)

	return nil
}

func mergeAt(runes []rune, items, offs []any, pre, post string) (string, []any) {
	var sb strings.Builder

	prev := 0
	positions := make([]any, 0, len(items))

	for i, it := range items {
		f, _ := asFloat(offs[i])
		off := min(max(int(f), prev), len(runes))

		sb.WriteString(string(runes[prev:off]))
		sb.WriteString(pre)
		positions = append(positions, float64(len([]rune(sb.String()))))
		fmt.Fprint(&sb, it)
		sb.WriteString(post)

		prev = off
	}

	sb.WriteString(string(runes[prev:]))

	return sb.String(), positions
}

func execShaper(sk map[string]any, t enrichTree, ctx, concrete string) error {
	obj := map[string]any{}

	for _, raw := range listOf(sk, "inputs") {
		in := objOf(raw)
		name := strOf(in, "name")

		if nested := listOf(in, "inputs"); len(nested) > 0 {
			obj[name] = shapeNested(nested, t, strOf(in, "sourceContext"), ctx, concrete)

			continue
		}

		v, _ := t.get(bind(strOf(in, "source"), ctx, concrete))
		obj[name] = v
	}

	skillOutput(sk, t, "output", obj, concrete)

	return nil
}

func shapeNested(nested []any, t enrichTree, srcCtx, ctx, concrete string) any {
	build := func(base string) map[string]any {
		o := map[string]any{}

		for _, raw := range nested {
			in := objOf(raw)
			src := strOf(in, "source")

			if src == "" {
				continue
			}

			v, _ := t.get(bind(src, srcCtx, base))
			o[strOf(in, "name")] = v
		}

		return o
	}

	if srcCtx == "" {
		return build(concrete)
	}

	resolved := bind(srcCtx, ctx, concrete)

	var list []any

	for _, p := range t.expandContext(resolved) {
		list = append(list, build(p))
	}

	return list
}
