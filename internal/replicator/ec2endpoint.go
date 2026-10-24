package replicator

import (
	"context"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type endpointPartition struct {
	regionRegex *regexp.Regexp
	name        string
	dnsSuffix   string
	dualStack   string
}

// endpointPartitions mirrors the SDK's partition data (internal/endpoints/awsrulesfn), which is not importable.
var endpointPartitions = sync.OnceValue(func() []endpointPartition { //nolint:gochecknoglobals // immutable table
	mk := func(name, dns, dual, re string) endpointPartition {
		return endpointPartition{name: name, dnsSuffix: dns, dualStack: dual, regionRegex: regexp.MustCompile(re)}
	}

	return []endpointPartition{
		mk("aws-cn", "amazonaws.com.cn", "api.amazonwebservices.com.cn", `^cn\-\w+\-\d+$`),
		mk("aws-eusc", "amazonaws.eu", "api.amazonwebservices.eu", `^eusc\-(de)\-\w+\-\d+$`),
		mk("aws-iso", "c2s.ic.gov", "api.aws.ic.gov", `^us\-iso\-\w+\-\d+$`),
		mk("aws-iso-b", "sc2s.sgov.gov", "api.aws.scloud", `^us\-isob\-\w+\-\d+$`),
		mk("aws-iso-e", "cloud.adc-e.uk", "api.cloud-aws.adc-e.uk", `^eu\-isoe\-\w+\-\d+$`),
		mk("aws-iso-f", "csp.hci.ic.gov", "api.aws.hci.ic.gov", `^us\-isof\-\w+\-\d+$`),
		mk("aws-us-gov", "amazonaws.com", "api.aws", `^us\-gov\-\w+\-\d+$`),
		mk("aws", "amazonaws.com", "api.aws", `^(us|eu|ap|sa|ca|me|af|il|mx)\-\w+\-\d+$`),
	}
})

var globalRegionPartitions = map[string]string{ //nolint:gochecknoglobals // immutable table
	"aws-global": "aws", "aws-cn-global": "aws-cn", "aws-iso-global": "aws-iso", "aws-iso-b-global": "aws-iso-b",
	"aws-iso-e-global": "aws-iso-e", "aws-iso-f-global": "aws-iso-f", "aws-us-gov-global": "aws-us-gov",
}

func partitionForEndpoint(region string) endpointPartition {
	parts := endpointPartitions()
	byName := func(name string) endpointPartition {
		for _, p := range parts {
			if p.name == name {
				return p
			}
		}

		return endpointPartition{}
	}

	if name, ok := globalRegionPartitions[region]; ok {
		return byName(name)
	}

	for _, p := range parts {
		if p.regionRegex.MatchString(region) {
			return p
		}
	}

	return byName("aws")
}

// fipsRegion strips the legacy fips marker from region, as the SDK does when it resolves the region.
func fipsRegion(region string) (string, bool) {
	if !strings.Contains(region, "-fips-") && !strings.Contains(region, "fips-") && !strings.Contains(region, "-fips") {
		return region, false
	}

	r := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(region, "-fips-", "-"), "fips-", ""), "-fips", "")

	return r, true
}

// ec2Endpoint resolves the EC2 endpoint the way the SDK's endpoint rules do: BaseEndpoint wins, otherwise the
// partition's DNS suffix with the FIPS and dual-stack variants.
func ec2Endpoint(cfg aws.Config) string {
	if cfg.BaseEndpoint != nil && *cfg.BaseEndpoint != "" {
		return *cfg.BaseEndpoint
	}

	region, legacyFIPS := fipsRegion(cfg.Region)
	fips := legacyFIPS || configFIPS(cfg) == aws.FIPSEndpointStateEnabled
	dual := configDualStack(cfg) == aws.DualStackEndpointStateEnabled
	p := partitionForEndpoint(region)

	switch {
	case fips && dual:
		return "https://ec2-fips." + region + "." + p.dualStack
	case fips && p.name == "aws-us-gov":
		return "https://ec2." + region + ".amazonaws.com"
	case fips:
		return "https://ec2-fips." + region + "." + p.dnsSuffix
	case dual:
		return "https://ec2." + region + "." + p.dualStack
	default:
		return "https://ec2." + region + "." + p.dnsSuffix
	}
}

// configFIPS reads UseFIPSEndpoint from the config sources (env, shared config, load options), first hit wins.
func configFIPS(cfg aws.Config) aws.FIPSEndpointState {
	for _, src := range cfg.ConfigSources {
		if p, ok := src.(interface {
			GetUseFIPSEndpoint(context.Context) (aws.FIPSEndpointState, bool, error)
		}); ok {
			if v, found, err := p.GetUseFIPSEndpoint(context.Background()); err == nil && found {
				return v
			}
		}
	}

	return aws.FIPSEndpointStateUnset
}

// configDualStack reads UseDualStackEndpoint from the config sources, first hit wins.
func configDualStack(cfg aws.Config) aws.DualStackEndpointState {
	for _, src := range cfg.ConfigSources {
		if p, ok := src.(interface {
			GetUseDualStackEndpoint(context.Context) (aws.DualStackEndpointState, bool, error)
		}); ok {
			if v, found, err := p.GetUseDualStackEndpoint(context.Background()); err == nil && found {
				return v
			}
		}
	}

	return aws.DualStackEndpointStateUnset
}
