package transparent

import (
	"net"
	"strings"
)

// S3HostKind classifies a real S3 endpoint hostname that is not plain path/virtual-hosted data access.
type S3HostKind int

const (
	// S3HostAccessPoint is <name>-<account>.s3-accesspoint[-fips].<region>.<zone>.
	S3HostAccessPoint S3HostKind = iota + 1
	// S3HostWebsite is <bucket>.s3-website[-<region>|.<region>].<zone>.
	S3HostWebsite
	// S3HostControl is <account>.s3-control[-fips][.dualstack].<region>.<zone>.
	S3HostControl
)

const accountIDLen = 12

// S3Host is a parsed S3 endpoint hostname.
type S3Host struct {
	// Name is the access point name or website bucket.
	Name string
	// Account is the 12-digit account of an access-point or control host.
	Account string
	Kind    S3HostKind
}

// ParseS3Host recognises access-point, website and S3 Control hostnames (dual-stack and FIPS included).
func ParseS3Host(host string) (S3Host, bool) {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.ToLower(strings.TrimSuffix(host, "."))

	for _, zone := range []string{".amazonaws.com", ".amazonaws.com.cn", ".api.aws"} {
		if prefix, ok := strings.CutSuffix(host, zone); ok {
			return parseS3Labels(strings.Split(prefix, "."))
		}
	}

	return S3Host{}, false
}

func parseS3Labels(labels []string) (S3Host, bool) {
	for i := 1; i < len(labels); i++ {
		switch l := labels[i]; {
		case i == 1 && (l == "s3-accesspoint" || l == "s3-accesspoint-fips"):
			return parseAccessPointLabel(labels[0])
		case i == 1 && (l == "s3-control" || l == "s3-control-fips"):
			if isAccountID(labels[0]) {
				return S3Host{Kind: S3HostControl, Account: labels[0]}, true
			}

			return S3Host{}, false
		case l == "s3-website" || strings.HasPrefix(l, "s3-website-"):
			return S3Host{Kind: S3HostWebsite, Name: strings.Join(labels[:i], ".")}, true
		}
	}

	return S3Host{}, false
}

func parseAccessPointLabel(label string) (S3Host, bool) {
	idx := strings.LastIndexByte(label, '-')
	if idx <= 0 || !isAccountID(label[idx+1:]) {
		return S3Host{}, false
	}

	return S3Host{Kind: S3HostAccessPoint, Name: label[:idx], Account: label[idx+1:]}, true
}

func isAccountID(s string) bool {
	if len(s) != accountIDLen {
		return false
	}

	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}
