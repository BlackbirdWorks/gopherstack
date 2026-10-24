package awswire

import (
	"context"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

const (
	partCN  = "aws-cn"
	partAWS = "aws"
	partGov = "aws-us-gov"
)

type partition struct {
	regionRegex *regexp.Regexp
	name        string
	dnsSuffix   string
	dualStack   string
}

// partitions mirrors the SDK's partition data (internal/endpoints/awsrulesfn), which is not importable.
var partitions = sync.OnceValue(func() []partition { //nolint:gochecknoglobals // immutable table
	mk := func(name, dns, dual, re string) partition {
		return partition{name: name, dnsSuffix: dns, dualStack: dual, regionRegex: regexp.MustCompile(re)}
	}

	return []partition{
		mk(partCN, "amazonaws.com.cn", "api.amazonwebservices.com.cn", `^cn\-\w+\-\d+$`),
		mk("aws-eusc", "amazonaws.eu", "api.amazonwebservices.eu", `^eusc\-(de)\-\w+\-\d+$`),
		mk("aws-iso", "c2s.ic.gov", "api.aws.ic.gov", `^us\-iso\-\w+\-\d+$`),
		mk("aws-iso-b", "sc2s.sgov.gov", "api.aws.scloud", `^us\-isob\-\w+\-\d+$`),
		mk("aws-iso-e", "cloud.adc-e.uk", "api.cloud-aws.adc-e.uk", `^eu\-isoe\-\w+\-\d+$`),
		mk("aws-iso-f", "csp.hci.ic.gov", "api.aws.hci.ic.gov", `^us\-isof\-\w+\-\d+$`),
		mk(partGov, "amazonaws.com", "api.aws", `^us\-gov\-\w+\-\d+$`),
		mk(partAWS, "amazonaws.com", "api.aws", `^(us|eu|ap|sa|ca|me|af|il|mx)\-\w+\-\d+$`),
	}
})

var globalRegionPartitions = map[string]string{ //nolint:gochecknoglobals // immutable table
	"aws-global": partAWS, "aws-cn-global": partCN, "aws-iso-global": "aws-iso", "aws-iso-b-global": "aws-iso-b",
	"aws-iso-e-global": "aws-iso-e", "aws-iso-f-global": "aws-iso-f", "aws-us-gov-global": partGov,
}

func partitionFor(region string) partition {
	parts := partitions()
	byName := func(name string) partition {
		for _, p := range parts {
			if p.name == name {
				return p
			}
		}

		return partition{}
	}

	if name, ok := globalRegionPartitions[region]; ok {
		return byName(name)
	}

	for _, p := range parts {
		if p.regionRegex.MatchString(region) {
			return p
		}
	}

	return byName(partAWS)
}

// FIPSRegion strips the legacy fips marker from region, as the SDK does when it resolves the region.
func FIPSRegion(region string) (string, bool) {
	if !strings.Contains(region, "-fips-") && !strings.Contains(region, "fips-") && !strings.Contains(region, "-fips") {
		return region, false
	}

	r := strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(region, "-fips-", "-"), "fips-", ""), "-fips", "")

	return r, true
}

const (
	govFIPSNone = iota
	govFIPSNoDual
	govFIPSAlways
)

// serviceInfo is what the endpoint rules of one service need beyond the common {prefix}.{region}.{dns} shape.
type serviceInfo struct {
	// noDualStack lists the partitions whose rules ignore dual-stack: "" for both, "nofips" for non-FIPS only.
	noDualStack map[string]string
	// prefix is the endpoint host label ("monitoring" for CloudWatch).
	prefix string
	// sdkID names the service in AWS_ENDPOINT_URL_<ID> and shared-config service sections.
	sdkID string
	// govFIPSRegions limits govFIPS to these regions when set.
	govFIPSRegions []string
	// govFIPS says when GovCloud drops the -fips host label: govFIPSNone never, govFIPSNoDual without dual-stack,
	// govFIPSAlways always.
	govFIPS int
	// baseOnly marks services without endpoint rules here: they need an explicit base endpoint.
	baseOnly bool
	// global marks services with a partition-wide endpoint table (see globalEndpoints).
	global bool
	// govDualUsesFIPS reproduces the Step Functions rule that picks the FIPS host for GovCloud dual-stack.
	govDualUsesFIPS bool
}

var services = map[string]serviceInfo{ //nolint:gochecknoglobals // immutable table
	"cloudwatch":     {prefix: "monitoring", sdkID: "CloudWatch", govFIPS: govFIPSAlways},
	"cloudwatchlogs": {prefix: "logs", sdkID: "CloudWatch Logs", govFIPS: govFIPSNoDual},
	"ec2":            {prefix: "ec2", sdkID: "EC2", govFIPS: govFIPSNoDual},
	"eventbridge":    {prefix: "events", sdkID: "EventBridge", govFIPS: govFIPSAlways},
	"iam":            {prefix: "iam", sdkID: "IAM", global: true},
	"kinesis":        {prefix: "kinesis", sdkID: "Kinesis", govFIPS: govFIPSNoDual},
	"kms":            {prefix: "kms", sdkID: "KMS"},
	"lambda":         {prefix: "lambda", sdkID: "Lambda"},
	"route53":        {prefix: "route53", sdkID: "Route 53", baseOnly: true},
	"secretsmanager": {
		prefix: "secretsmanager", sdkID: "Secrets Manager",
		noDualStack: map[string]string{partAWS: "", partGov: "", partCN: "nofips"},
	},
	"sfn": {
		prefix: "states", sdkID: "SFN", govFIPS: govFIPSNoDual, govDualUsesFIPS: true,
		govFIPSRegions: []string{"us-gov-west-1"},
	},
	"sns": {prefix: "sns", sdkID: "SNS", govFIPS: govFIPSNoDual},
	"sqs": {prefix: "sqs", sdkID: "SQS", govFIPS: govFIPSNoDual},
	"ssm": {prefix: "ssm", sdkID: "SSM", govFIPS: govFIPSNoDual},
}

// globalEndpoint is one partition's row of a partition-wide endpoint table; %R is the partition's home region.
type globalEndpoint struct{ plain, fips, dual, fipsDual, home string }

var globalEndpoints = map[string]globalEndpoint{ //nolint:gochecknoglobals // immutable table
	partAWS: {
		"iam.amazonaws.com", "iam-fips.amazonaws.com", "iam.global.api.aws", "iam-fips.global.api.aws", "us-east-1",
	},
	partCN: {
		"iam.%R.amazonaws.com.cn", "iam-fips.amazonaws.com.cn", "iam.global.api.amazonwebservices.com.cn",
		"iam-fips.api.amazonwebservices.com.cn", "cn-north-1",
	},
	partGov: {
		"iam.us-gov.amazonaws.com", "iam.us-gov.amazonaws.com", "iam.us-gov.api.aws", "iam.us-gov.api.aws",
		"us-gov-west-1",
	},
	"aws-iso": {
		"iam.%R.c2s.ic.gov", "iam-fips.%R.c2s.ic.gov", "iam.api.aws.ic.gov", "iam-fips.api.aws.ic.gov", "us-iso-east-1",
	},
	"aws-iso-b": {
		"iam.%R.sc2s.sgov.gov", "iam-fips.%R.sc2s.sgov.gov", "iam.api.aws.scloud", "iam-fips.api.aws.scloud",
		"us-isob-east-1",
	},
	"aws-iso-e": {
		"iam.%R.cloud.adc-e.uk", "iam-fips.cloud.adc-e.uk", "iam.api.cloud-aws.adc-e.uk",
		"iam-fips.api.cloud-aws.adc-e.uk", "eu-isoe-west-1",
	},
	"aws-iso-f": {
		"iam.%R.csp.hci.ic.gov", "iam-fips.csp.hci.ic.gov", "iam.api.aws.hci.ic.gov", "iam-fips.api.aws.hci.ic.gov",
		"us-isof-south-1",
	},
	"aws-eusc": {
		"iam.%R.amazonaws.eu", "iam-fips.amazonaws.eu", "iam.global.api.amazonwebservices.eu",
		"iam-fips.api.amazonwebservices.eu", "eusc-de-east-1",
	},
}

// resolved is where a call goes and the region it is signed for.
type resolved struct{ url, signingRegion string }

// resolve picks the endpoint for service key the way the SDK's endpoint rules do: an explicit base endpoint wins,
// otherwise the partition's DNS suffix with the FIPS and dual-stack variants.
func resolve(key string, cfg aws.Config, override *string) resolved {
	region, legacyFIPS := FIPSRegion(cfg.Region)

	if override != nil && *override != "" {
		return resolved{url: *override, signingRegion: cfg.Region}
	}

	info := services[key]
	if base := serviceBaseEndpoint(cfg, info.sdkID); base != "" {
		return resolved{url: base, signingRegion: cfg.Region}
	}

	if info.baseOnly {
		return resolved{signingRegion: region}
	}

	fips := legacyFIPS || configFIPS(cfg) == aws.FIPSEndpointStateEnabled
	dual := configDualStack(cfg) == aws.DualStackEndpointStateEnabled
	p := partitionFor(region)

	if info.global {
		return resolveGlobal(p, fips, dual)
	}

	if mode, ok := info.noDualStack[p.name]; ok && (mode == "" || !fips) {
		dual = false
	}

	host := hostFor(info, p, region, fips, dual)

	return resolved{url: "https://" + host, signingRegion: region}
}

func hostFor(info serviceInfo, p partition, region string, fips, dual bool) string {
	label := info.prefix
	plain := p.name == partGov && (info.govFIPS == govFIPSAlways || info.govFIPS == govFIPSNoDual && !dual) &&
		(len(info.govFIPSRegions) == 0 || slices.Contains(info.govFIPSRegions, region))
	if fips && !plain {
		label += "-fips"
	}

	if info.govDualUsesFIPS && p.name == partGov && dual && !fips {
		label += "-fips"
	}

	suffix := p.dnsSuffix
	if dual {
		suffix = p.dualStack
	}

	return label + "." + region + "." + suffix
}

func resolveGlobal(p partition, fips, dual bool) resolved {
	g := globalEndpoints[p.name]

	var tmpl string

	switch {
	case fips && dual:
		tmpl = g.fipsDual
	case fips:
		tmpl = g.fips
	case dual:
		tmpl = g.dual
	default:
		tmpl = g.plain
	}

	return resolved{url: "https://" + strings.ReplaceAll(tmpl, "%R", g.home), signingRegion: g.home}
}

// serviceBaseEndpoint honours AWS_ENDPOINT_URL_<ID> and shared-config service endpoints like the SDK clients do.
func serviceBaseEndpoint(cfg aws.Config, sdkID string) string {
	if sdkID == "" {
		return ""
	}

	_, global := os.LookupEnv("AWS_ENDPOINT_URL")
	_, specific := os.LookupEnv("AWS_ENDPOINT_URL_" + strings.ToUpper(strings.ReplaceAll(sdkID, " ", "_")))

	if cfg.BaseEndpoint != nil && *cfg.BaseEndpoint != "" && global && !specific {
		return *cfg.BaseEndpoint
	}

	for _, src := range cfg.ConfigSources {
		if p, ok := src.(interface {
			GetServiceBaseEndpoint(context.Context, string) (string, bool, error)
		}); ok {
			if v, found, err := p.GetServiceBaseEndpoint(context.Background(), sdkID); err == nil && found {
				return v
			}
		}
	}

	if cfg.BaseEndpoint != nil {
		return *cfg.BaseEndpoint
	}

	return ""
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
