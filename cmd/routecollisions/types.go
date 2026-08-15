package main

import "regexp"

var (
	importAliasRe = regexp.MustCompile(
		`(?m)^\s*(\w+)\s+"github\.com/blackbirdworks/gopherstack/services/([a-zA-Z0-9]+)"`,
	)
	providerRefRe = regexp.MustCompile(`&(\w+)\.\w*Provider\{\}`)
	priorityRe    = regexp.MustCompile(`(?m)^\s*(Priority\w+)\s*=\s*(\d+)`)
	quotedRe      = regexp.MustCompile(`"([^"]*)"`)
	concatLeftRe  = regexp.MustCompile(`"/"\s*\+\s*(\w+)`)
	concatRightRe = regexp.MustCompile(`(\w+)\s*\+\s*"/"`)
	guardRe       = regexp.MustCompile(`ExtractServiceFromRequest|is\w+Request\(`)
	sliceLitRe    = regexp.MustCompile(
		`(?s)(\w+)\s*=\s*(?:sync\.OnceValue\(func\(\)\s*\[\]string\s*\{\s*return\s*)?\[\]string\{([^}]*)\}`,
	)
	bareIdentRe = regexp.MustCompile(
		`(?:==\s*|HasPrefix\(path,\s*|CutPrefix\(path,\s*|range\s+)([A-Za-z_]\w*(?:\(\))?)`,
	)
)

type claimKind int

const (
	kindExact claimKind = iota
	kindPrefix
)

func (k claimKind) String() string {
	if k == kindExact {
		return "exact"
	}

	return "prefix"
}

type claim struct {
	Literal string    `json:"literal"`
	Segment string    `json:"segment"`
	KindStr string    `json:"kind"`
	Kind    claimKind `json:"-"`
}

type svcInfo struct {
	Dir      string  `json:"dir"`
	Claims   []claim `json:"claims"`
	Priority int     `json:"priority"`
	RegOrder int     `json:"regOrder"`
	Guarded  bool    `json:"guarded"`
}
