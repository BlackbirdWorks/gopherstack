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
	// secondArgPrefixRe catches the single-prefix RouteMatcher shape this repo
	// uses constantly: strings.HasPrefix(c.Request().URL.Path, xxxPathPrefix) --
	// or the CutPrefix equivalent -- where the path expression is inlined as
	// the first argument (not first assigned to a local "path" variable, which
	// is what bareIdentRe alone requires) and the prefix identifier is the
	// SECOND argument. Missing this shape was the single largest source of
	// gopherstack-op3e's original 50-service tooling gap: appmesh, cloudfront,
	// cloudfrontkeyvaluestore, mediaconvert, sesv2, route53, sagemakerruntime,
	// mq and others all write RouteMatcher exactly this way.
	secondArgPrefixRe = regexp.MustCompile(
		`(?:HasPrefix|CutPrefix)\(\s*.+?,\s*([A-Za-z_]\w*(?:\(\))?)\s*\)`,
	)
	// queryProtocolContentTypeRe / queryProtocolVersionRe together recognize
	// the AWS Query/EC2-protocol RouteMatcher shape (EC2, IAM, RDS, DocDB,
	// Neptune, Redshift, Autoscaling, ELB, ELBv2, ElasticBeanstalk, SES, SNS,
	// STS, ...): a Content-Type check (against either the literal
	// "application/x-www-form-urlencoded" or a same-purpose local constant --
	// SNS/STS reference their own snsContentType/contentTypeForm rather than
	// the literal) plus disambiguation by an exact "Version" (or "Action")
	// value read from the body, rather than any URL path literal. These claim
	// no path at all (the path check, if any, only excludes "/dashboard/"),
	// so they are structurally immune to the path-prefix collision class this
	// sweep is about -- the same way a header/X-Amz-Target match is immune --
	// and are reported as such rather than as a false "no claims extracted"
	// gap. Split into two independently-anchored regexes (checked with plain
	// substring/regex tests, not a single windowed pattern) because the gap
	// between the two checks varies too much across services (docdb/neptune
	// insert a multi-line User-Agent-marker check and doc comment in between)
	// for a fixed-size lookahead window to reliably span.
	queryProtocolContentTypeRe = regexp.MustCompile(`Header\.Get\("Content-Type"\)`)
	queryProtocolVersionRe     = regexp.MustCompile(
		`vals\.Get\("(?:Version|Action)"\)|strings\.Contains\(string\(body\),\s*\w+\)`,
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
	// Immune marks a RouteMatcher recognized as structurally immune to the
	// path-prefix collision class (query-protocol Version/Action body match,
	// or header/X-Amz-Target match with no path literal at all) rather than a
	// genuine tooling gap. Only meaningful when Claims is empty.
	Immune bool `json:"immune"`
}
