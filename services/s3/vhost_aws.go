package s3

import (
	"net"
	"strings"
)

// awsEndpointZones are the real AWS zones whose virtual-hosted S3 names are honored without --s3 endpoint config.
func awsEndpointZones() []string {
	return []string{".amazonaws.com", ".amazonaws.com.cn", ".api.aws"}
}

// awsVirtualHostBucket returns the bucket of a real AWS virtual-hosted host
// (bucket.s3.us-west-2.amazonaws.com), or "" for path-style and non-S3 hosts.
func awsVirtualHostBucket(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	host = strings.ToLower(strings.TrimSuffix(host, "."))

	for _, zone := range awsEndpointZones() {
		prefix, ok := strings.CutSuffix(host, zone)
		if !ok {
			continue
		}

		parts := strings.Split(prefix, ".")
		for i, p := range parts {
			if !isS3EndpointLabel(p) {
				continue
			}

			bucket := strings.Join(parts[:i], ".")
			if i > 0 && IsValidBucketName(bucket) {
				return bucket
			}

			return ""
		}
	}

	return ""
}

func isS3EndpointLabel(label string) bool {
	if label == "s3" {
		return true
	}

	rest, ok := strings.CutPrefix(label, "s3-")
	if !ok {
		return false
	}

	for _, other := range []string{"control", "website", "object-lambda", "outposts", "accesspoint"} {
		if strings.HasPrefix(rest, other) {
			return false
		}
	}

	return true
}
