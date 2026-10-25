package main

import (
	"fmt"
	"html"
	"regexp"
	"strconv"
	"strings"
)

type quota struct {
	Code        string  `json:"code"`
	Name        string  `json:"name"`
	Description string  `json:"description,omitempty"`
	Unit        string  `json:"unit"`
	Default     float64 `json:"default"`
	Adjustable  bool    `json:"adjustable"`
}

type service struct {
	Code   string  `json:"code"`
	Name   string  `json:"name"`
	Source string  `json:"source"`
	Quotas []quota `json:"quotas"`
}

var (
	brRe    = regexp.MustCompile(`<br\s*/?>|</p>`)
	rowRe   = regexp.MustCompile(`(?s)<tr>(.*?)</tr>`)
	cellRe  = regexp.MustCompile(`(?s)<td[^>]*>(.*?)</td>`)
	tagRe   = regexp.MustCompile(`(?s)<[^>]+>`)
	linkRe  = regexp.MustCompile(`servicequotas/home/services/([^/"]+)/quotas/(L-[0-9A-F]{8})`)
	titleRe = regexp.MustCompile(`(?s)<h1[^>]*>(.*?)</h1>`)
	defRe   = regexp.MustCompile(
		`^(?:(Each supported Region|Each Account|us-east-1|Each of the other supported Regions): )?([0-9][0-9,.]*)\s*(.*)$`,
	)
)

func text(s string) string {
	return strings.Join(strings.Fields(html.UnescapeString(tagRe.ReplaceAllString(s, " "))), " ")
}

func pageTitle(page string) string {
	m := titleRe.FindStringSubmatch(page)
	if m == nil {
		return ""
	}

	return strings.TrimSuffix(text(m[1]), " endpoints and quotas")
}

var scopeRank = map[string]int{ //nolint:gochecknoglobals // immutable lookup table
	"": rankAll, "Each supported Region": rankAll, "Each Account": rankAll, "us-east-1": rankUSEast1,
	"Each of the other supported Regions": rankOther,
}

const (
	rowCells    = 4
	rankAll     = 3
	rankUSEast1 = 2
	rankOther   = 1
)

// parseDefault picks the Region-independent default, else the us-east-1 one, else the "other Regions" one.
func parseDefault(s string) (float64, string, bool) {
	var best, bestUnit string

	rank := 0

	for line := range strings.SplitSeq(s, "\n") {
		m := defRe.FindStringSubmatch(strings.TrimSpace(line))
		if m == nil {
			continue
		}

		if r := scopeRank[m[1]]; r > rank {
			rank, best, bestUnit = r, m[2], m[3]
		}
	}

	if rank == 0 {
		return 0, "", false
	}

	v, err := strconv.ParseFloat(strings.ReplaceAll(best, ",", ""), 64)
	if err != nil {
		return 0, "", false
	}

	if bestUnit == "" || bestUnit == "Count" {
		bestUnit = "None"
	}

	return v, bestUnit, true
}

// parseQuotas returns the documented adjustable quotas per Service Quotas service code.
func parseQuotas(page string) map[string][]quota {
	out := map[string][]quota{}

	for _, row := range rowRe.FindAllStringSubmatch(page, -1) {
		cells := cellRe.FindAllStringSubmatch(row[1], -1)
		if len(cells) != rowCells {
			continue
		}

		link := linkRe.FindStringSubmatch(cells[2][1])
		if link == nil {
			continue
		}

		v, unit, ok := parseDefault(brSplit(brRe.ReplaceAllString(cells[1][1], "\n")))
		if !ok {
			continue
		}

		out[link[1]] = append(out[link[1]], quota{
			Code: link[2], Name: text(cells[0][1]), Description: text(cells[3][1]),
			Unit: unit, Default: v, Adjustable: true,
		})
	}

	return out
}

func brSplit(raw string) string {
	parts := strings.Split(raw, "\n")
	for i := range parts {
		parts[i] = text(parts[i])
	}

	return strings.Join(parts, "\n")
}

func (s service) String() string { return fmt.Sprintf("%s(%d)", s.Code, len(s.Quotas)) }
