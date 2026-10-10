package opsworks

import (
	"regexp"
	"strings"
)

const (
	maxHostnameLen  = 63
	maxShortnameLen = 32
)

var (
	regionPattern    = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d+$`)
	hostnamePattern  = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$`)
	shortnamePattern = regexp.MustCompile(`^[a-zA-Z0-9_.-]+$`)
	iamARNPattern    = regexp.MustCompile(`^arn:[a-z\d-]+:iam::\d{12}:(role|instance-profile)/.+$`)
)

func validRegion(region string) bool { return regionPattern.MatchString(region) }

func validIAMARN(arn, kind string) bool {
	m := iamARNPattern.FindStringSubmatch(arn)

	return m != nil && m[1] == kind
}

func validHostname(hostname string) bool {
	return hostname == "" || (len(hostname) <= maxHostnameLen && hostnamePattern.MatchString(hostname))
}

func validShortname(shortname string) bool {
	return len(shortname) <= maxShortnameLen && shortnamePattern.MatchString(shortname)
}

func validCookbooksSource(src *CookbooksSource) bool {
	if src == nil {
		return true
	}

	switch strings.ToLower(src.Type) {
	case "", "git", "svn", "archive", "s3":
		return true
	default:
		return false
	}
}
