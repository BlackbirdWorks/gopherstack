package codecommit

import (
	"bytes"
	"encoding/base64"
	"sort"
	"strings"
)

// maxDiffCells bounds the LCS table; larger middles diff as one region.
const maxDiffCells = 4_000_000

type region struct{ bs, be, os, oe int }

type mergeCluster struct {
	srcRegs, dstRegs []region
	b0, b1           int
}

func splitLines(content []byte) []string {
	if len(content) == 0 {
		return nil
	}

	lines := strings.SplitAfter(string(content), "\n")
	if lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}

	return lines
}

func isBinaryContent(content []byte) bool { return bytes.IndexByte(content, 0) >= 0 }

// diffRegions returns the edits turning base into other, in base order.
func diffRegions(base, other []string) []region {
	pre := 0
	for pre < len(base) && pre < len(other) && base[pre] == other[pre] {
		pre++
	}

	suf := 0
	for suf < len(base)-pre && suf < len(other)-pre && base[len(base)-1-suf] == other[len(other)-1-suf] {
		suf++
	}

	a, b := base[pre:len(base)-suf], other[pre:len(other)-suf]
	if len(a) == 0 && len(b) == 0 {
		return nil
	}

	if len(a) == 0 || len(b) == 0 || (len(a)+1)*(len(b)+1) > maxDiffCells {
		return []region{{pre, pre + len(a), pre, pre + len(b)}}
	}

	return lcsRegions(a, b, pre)
}

func lcsTable(a, b []string) []int32 {
	n, m := len(a), len(b)
	l := make([]int32, (n+1)*(m+1))

	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			switch {
			case a[i] == b[j]:
				l[i*(m+1)+j] = l[(i+1)*(m+1)+j+1] + 1
			case l[(i+1)*(m+1)+j] >= l[i*(m+1)+j+1]:
				l[i*(m+1)+j] = l[(i+1)*(m+1)+j]
			default:
				l[i*(m+1)+j] = l[i*(m+1)+j+1]
			}
		}
	}

	return l
}

func lcsRegions(a, b []string, off int) []region {
	n, m := len(a), len(b)
	l := lcsTable(a, b)

	var out []region

	open := false
	cur := region{}
	i, j := 0, 0

	flush := func() {
		if open {
			out = append(out, cur)
			open = false
		}
	}

	begin := func() {
		if !open {
			cur = region{bs: off + i, be: off + i, os: off + j, oe: off + j}
			open = true
		}
	}

	for i < n || j < m {
		switch {
		case i < n && j < m && a[i] == b[j]:
			flush()
			i++
			j++
		case j >= m || (i < n && l[(i+1)*(m+1)+j] >= l[i*(m+1)+j+1]):
			begin()
			i++
			cur.be = off + i
		default:
			begin()
			j++
			cur.oe = off + j
		}
	}

	flush()

	return out
}

// applyRegions renders other's version of base[b0:b1] using regs.
func applyRegions(base, other []string, regs []region, b0, b1 int) []string {
	var out []string

	pos := b0

	for _, r := range regs {
		out = append(out, base[pos:r.bs]...)
		out = append(out, other[r.os:r.oe]...)
		pos = r.be
	}

	return append(out, base[pos:b1]...)
}

// clusterRegions groups edits from both sides that overlap or touch in base.
func clusterRegions(src, dst []region) []mergeCluster {
	type tagged struct {
		r   region
		dst bool
	}

	all := make([]tagged, 0, len(src)+len(dst))
	for _, r := range src {
		all = append(all, tagged{r: r})
	}

	for _, r := range dst {
		all = append(all, tagged{r: r, dst: true})
	}

	sort.SliceStable(all, func(i, j int) bool { return all[i].r.bs < all[j].r.bs })

	var out []mergeCluster

	for _, t := range all {
		if n := len(out); n > 0 && t.r.bs <= out[n-1].b1 {
			c := &out[n-1]
			c.b1 = max(c.b1, t.r.be)
			c.add(t.r, t.dst)

			continue
		}

		c := mergeCluster{b0: t.r.bs, b1: t.r.be}
		c.add(t.r, t.dst)
		out = append(out, c)
	}

	return out
}

func (c *mergeCluster) add(r region, dst bool) {
	if dst {
		c.dstRegs = append(c.dstRegs, r)
	} else {
		c.srcRegs = append(c.srcRegs, r)
	}
}

// versionStart is where a side's version of a cluster begins in that side's lines.
func versionStart(regs []region, b0 int) int {
	return regs[0].os - (regs[0].bs - b0)
}

func hunkDetail(start int, lines []string) *MergeHunkDetail {
	end := start + len(lines)
	if len(lines) == 0 {
		end = start + 1
	}

	return &MergeHunkDetail{
		StartLine:   start + 1,
		EndLine:     end,
		HunkContent: base64.StdEncoding.EncodeToString([]byte(strings.Join(lines, ""))),
	}
}

// lineMerge is the outcome of a line-level three-way merge of one file.
type lineMerge struct {
	merged    []string
	hunks     []MergeHunk
	conflicts int
}

// threeWayLines merges src and dst against base. pick, when non-empty ("src" or
// "dst"), resolves conflicting clusters instead of leaving them unresolved.
func threeWayLines(base, src, dst []string, pick string) lineMerge {
	srcRegs, dstRegs := diffRegions(base, src), diffRegions(base, dst)

	var out lineMerge

	pos := 0

	for _, c := range clusterRegions(srcRegs, dstRegs) {
		out.merged = append(out.merged, base[pos:c.b0]...)
		pos = c.b1

		switch {
		case len(c.dstRegs) == 0:
			out.merged = append(out.merged, applyRegions(base, src, c.srcRegs, c.b0, c.b1)...)
		case len(c.srcRegs) == 0:
			out.merged = append(out.merged, applyRegions(base, dst, c.dstRegs, c.b0, c.b1)...)
		default:
			out.addBothSides(base, src, dst, c, pick)
		}
	}

	out.merged = append(out.merged, base[pos:]...)

	return out
}

func (m *lineMerge) addBothSides(base, src, dst []string, c mergeCluster, pick string) {
	sv := applyRegions(base, src, c.srcRegs, c.b0, c.b1)
	dv := applyRegions(base, dst, c.dstRegs, c.b0, c.b1)
	conflict := strings.Join(sv, "") != strings.Join(dv, "")

	m.hunks = append(m.hunks, MergeHunk{
		Source:      hunkDetail(versionStart(c.srcRegs, c.b0), sv),
		Destination: hunkDetail(versionStart(c.dstRegs, c.b0), dv),
		Base:        hunkDetail(c.b0, base[c.b0:c.b1]),
		IsConflict:  conflict,
	})

	switch {
	case !conflict, pick == "src":
		m.merged = append(m.merged, sv...)
	case pick == "dst":
		m.merged = append(m.merged, dv...)
	default:
		m.conflicts++
	}
}
