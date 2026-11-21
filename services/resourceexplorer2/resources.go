package resourceexplorer2

import (
	"slices"
	"strings"
)

const (
	svcIAM        = "iam"
	globalRegion  = "global"
	arnParts      = 6
	arnServiceIdx = 2
	arnRegionIdx  = 3
	arnAccountIdx = 4
	arnResourceIx = 5
)

var globalServices = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	svcIAM: true, "route53": true, "cloudfront": true, "organizations": true, "waf": true,
	"shield": true, "globalaccelerator": true, "networkmanager": true, "sts": true,
}

var bareResourceTypes = map[string]string{ //nolint:gochecknoglobals // immutable lookup table
	"s3": "bucket", "sqs": "queue", "sns": "topic",
}

// toCandidate projects an ARN and tags into a searchable resource; ok is false for a malformed ARN.
func toCandidate(src SourceResource, queryRegion, queryAccount string) (*candidate, bool) {
	parts := strings.SplitN(src.ARN, ":", arnParts)
	if len(parts) < arnParts || parts[0] != "arn" || parts[arnServiceIdx] == "" {
		return nil, false
	}

	svc, res := parts[arnServiceIdx], parts[arnResourceIx]

	region := parts[arnRegionIdx]

	switch {
	case globalServices[svc]:
		region = globalRegion
	case region == "":
		region = queryRegion
	}

	account := parts[arnAccountIdx]
	if account == "" {
		account = queryAccount
	}

	typ, id := splitResource(svc, res)

	c := &candidate{
		arn: src.ARN, service: svc, resourceType: svc + ":" + typ, region: region, account: account,
		id: id, tags: src.Tags,
	}

	c.words = words(src.ARN)

	for k, v := range src.Tags {
		c.words = append(c.words, words(k)...)
		c.words = append(c.words, words(v)...)
	}

	return c, true
}

//nolint:gochecknoglobals // immutable lookup table
var elbSubtypes = map[string]bool{"app": true, "net": true, "gwy": true}

// splitResource derives the Resource Explorer type and id of an ARN's resource part; load balancers and
// listeners of ELBv2 carry their flavour (loadbalancer/net) in the type.
func splitResource(svc, res string) (string, string) {
	if svc == "elasticloadbalancing" {
		if typ, id, ok := splitELBResource(res); ok {
			return typ, id
		}
	}

	if i := strings.IndexAny(res, "/:"); i >= 0 {
		return res[:i], res[i+1:]
	}

	if t, ok := bareResourceTypes[svc]; ok {
		return t, res
	}

	return svc, res
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func splitELBResource(res string) (string, string, bool) {
	kind, rest, found := strings.Cut(res, "/")
	if !found || (kind != "loadbalancer" && kind != "listener" && kind != "listener-rule") {
		return "", "", false
	}

	flavour, id, found := strings.Cut(rest, "/")
	if !found || !elbSubtypes[flavour] {
		return "", "", false
	}

	return kind + "/" + flavour, id, true
}
