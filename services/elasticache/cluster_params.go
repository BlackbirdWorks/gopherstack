package elasticache

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

const maxTCPPort = 65535

var (
	outpostArnRe  = regexp.MustCompile(`^arn:aws[a-z-]*:outposts:[a-z0-9-]*:\d{12}:outpost/op-[0-9a-f]+$`)
	s3ObjectArnRe = regexp.MustCompile(`^arn:aws[a-z-]*:s3:::[^/]+/.+$`)
)

// clusterCreateParams are CreateCacheCluster's validated placement and
// engine-binding members.
type clusterCreateParams struct {
	placement ClusterPlacement
	port      int
}

// parseClusterCreateParams validates the members whose rules are documented
// on CreateCacheClusterInput, before anything is created.
func parseClusterCreateParams(form url.Values, engine string, numNodes int) (clusterCreateParams, error) {
	var out clusterCreateParams

	isMemcached := engine == engineMemcached

	out.placement = ClusterPlacement{
		AZ:          form.Get("PreferredAvailabilityZone"),
		AZMode:      form.Get("AZMode"),
		OutpostArn:  form.Get("PreferredOutpostArn"),
		AZs:         parseRepeatedField(form, "PreferredAvailabilityZones.PreferredAvailabilityZone"),
		OutpostArns: parseRepeatedField(form, "PreferredOutpostArns.PreferredOutpostArn"),
	}

	if !validAZMode(out.placement.AZMode) {
		return out, fmt.Errorf("%w: AZMode must be single-az or cross-az", ErrInvalidParameterValue)
	}

	if !validOutpostMode(form.Get("OutpostMode")) {
		return out, fmt.Errorf("%w: OutpostMode must be single-outpost or cross-outpost", ErrInvalidParameterValue)
	}

	if err := validateNodeCount(isMemcached, numNodes); err != nil {
		return out, err
	}

	if err := validatePlacement(out.placement, isMemcached, numNodes); err != nil {
		return out, err
	}

	var err error

	if out.port, err = parsePort(form.Get("Port")); err != nil {
		return out, err
	}

	return out, validateSnapshotArns(parseRepeatedField(form, "SnapshotArns.SnapshotArn"), engine)
}

func validateNodeCount(isMemcached bool, numNodes int) error {
	if isMemcached && numNodes > maxMemcachedNodes {
		return fmt.Errorf("%w: NumCacheNodes must be between 1 and %d for Memcached", ErrInvalidParameterValue,
			maxMemcachedNodes)
	}

	if !isMemcached && numNodes > 1 {
		return fmt.Errorf("%w: NumCacheNodes must be 1 for Valkey and Redis OSS clusters", ErrInvalidParameterValue)
	}

	return nil
}

func validatePlacement(p ClusterPlacement, isMemcached bool, numNodes int) error {
	if !isMemcached && (p.AZMode != "" || len(p.AZs) > 0) {
		return fmt.Errorf(
			"%w: AZMode and PreferredAvailabilityZones are Memcached-only",
			ErrInvalidParameterCombination,
		)
	}

	if len(p.AZs) > 0 && len(p.AZs) != numNodes {
		return fmt.Errorf("%w: PreferredAvailabilityZones must list exactly NumCacheNodes (%d) zones",
			ErrInvalidParameterCombination, numNodes)
	}

	if p.AZ != "" && len(p.AZs) > 0 {
		return fmt.Errorf("%w: PreferredAvailabilityZone and PreferredAvailabilityZones are mutually exclusive",
			ErrInvalidParameterCombination)
	}

	for _, arn := range append([]string{p.OutpostArn}, p.OutpostArns...) {
		if arn != "" && !outpostArnRe.MatchString(arn) {
			return fmt.Errorf("%w: %q is not a valid outpost ARN", ErrInvalidParameterValue, arn)
		}
	}

	return nil
}

func parsePort(raw string) (int, error) {
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 || n > maxTCPPort {
		return 0, fmt.Errorf("%w: Port must be between 1 and %d", ErrInvalidParameterValue, maxTCPPort)
	}

	return n, nil
}

// validateSnapshotArns checks CreateCacheClusterInput.SnapshotArns: a
// single-element list holding an S3 object ARN without commas, Redis OSS/Valkey only.
func validateSnapshotArns(arns []string, engine string) error {
	if len(arns) == 0 {
		return nil
	}

	if engine == engineMemcached {
		return fmt.Errorf("%w: SnapshotArns is only valid for Valkey and Redis OSS", ErrInvalidParameterCombination)
	}

	if len(arns) != 1 || strings.Contains(arns[0], ",") || !s3ObjectArnRe.MatchString(arns[0]) {
		return fmt.Errorf("%w: SnapshotArns must be a single S3 object ARN", ErrInvalidParameterValue)
	}

	return nil
}
