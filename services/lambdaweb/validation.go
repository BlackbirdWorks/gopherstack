package lambdaweb

import (
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/regionpeers"
)

const (
	filterState     = "state"
	runtimeNodejs24 = "nodejs24.x"

	defaultTimeoutSeconds = 30
	minTimeoutSeconds     = 3
	maxTimeoutSeconds     = 900
	defaultConcurrency    = 64
	minConcurrency        = 1
	maxConcurrency        = 128
	minMaxEnvironments    = 2
	maxMaxEnvironments    = 10000
	rateStepLow           = 100
	rateStepHigh          = 1000
	rateStepBoundary      = 1000
	maxWeightedRevisions  = 2
	defaultLogLevel       = "INFO"
	minMultiRegions       = 2
)

var roleARNRe = regexp.MustCompile(`^arn:aws[a-zA-Z-]*:iam::\d{12}:role/.+$`)

func appLogLevels() []string { return []string{"TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL"} }

func systemLogLevels() []string { return []string{"DEBUG", "INFO", "WARN"} }

func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return validationError("A resource can have at most %d tags.", maxTagsPerResource)
	}

	for k, v := range tags {
		switch {
		case k == "" || len(k) > maxTagKeyLength:
			return validationError("Tag keys must be 1 to %d characters.", maxTagKeyLength)
		case len(v) > maxTagValueLength:
			return validationError("Tag values must be at most %d characters.", maxTagValueLength)
		case strings.HasPrefix(strings.ToLower(k), "aws:"):
			return validationError("Tag keys may not start with the reserved aws: prefix.")
		}
	}

	return nil
}

func validateName(kind, name string) error {
	if !nameRe.MatchString(name) {
		return validationError(
			"Invalid %s name %q: use up to %d letters, numbers, hyphens (-) or underscores (_), "+
				"and don't begin or end with a hyphen or underscore.", kind, name, maxNameLength)
	}

	return nil
}

// normalizeRevision validates in and fills the documented defaults.
func normalizeRevision(in *RevisionInput) error {
	if in.S3.Bucket == "" || in.S3.Key == "" {
		return validationError("buildConfig.codeConfig.s3Object requires a bucket and a key.")
	}

	if in.Runtime != runtimeNodejs24 {
		return validationError("Runtime %q is not supported; supported runtimes: %s.", in.Runtime, runtimeNodejs24)
	}

	sc := &in.ServiceConfig

	if !roleARNRe.MatchString(sc.ExecutionRoleArn) {
		return validationError("serviceConfig.executionRoleArn must be a valid IAM role ARN.")
	}

	if sc.TimeoutSeconds == 0 {
		sc.TimeoutSeconds = defaultTimeoutSeconds
	}

	if sc.TimeoutSeconds < minTimeoutSeconds || sc.TimeoutSeconds > maxTimeoutSeconds {
		return validationError("timeoutSeconds must be between %d and %d.", minTimeoutSeconds, maxTimeoutSeconds)
	}

	if sc.MaxConcurrencyPerEnvironment == 0 {
		sc.MaxConcurrencyPerEnvironment = defaultConcurrency
	}

	if sc.MaxConcurrencyPerEnvironment < minConcurrency || sc.MaxConcurrencyPerEnvironment > maxConcurrency {
		return validationError(
			"maxConcurrencyPerEnvironment must be between %d and %d.",
			minConcurrency,
			maxConcurrency,
		)
	}

	return normalizeLogging(sc.Logging)
}

func normalizeLogging(lc *LoggingConfig) error {
	if lc == nil {
		return nil
	}

	if lc.ApplicationLogLevel == "" {
		lc.ApplicationLogLevel = defaultLogLevel
	}

	if lc.SystemLogLevel == "" {
		lc.SystemLogLevel = defaultLogLevel
	}

	if !slices.Contains(appLogLevels(), lc.ApplicationLogLevel) {
		return validationError("applicationLogLevel must be one of %s.", strings.Join(appLogLevels(), ", "))
	}

	if !slices.Contains(systemLogLevels(), lc.SystemLogLevel) {
		return validationError("systemLogLevel must be one of %s.", strings.Join(systemLogLevels(), ", "))
	}

	if strings.HasPrefix(lc.LogGroup, "aws/") {
		return validationError("Log group names that start with aws/ are reserved.")
	}

	return nil
}

func validateScaling(s *ScalingInput) error {
	if s == nil || s.MaxEnvironments == nil {
		return nil
	}

	if v := *s.MaxEnvironments; v < minMaxEnvironments || v > maxMaxEnvironments {
		return validationError("maxEnvironments must be between %d and %d.", minMaxEnvironments, maxMaxEnvironments)
	}

	return nil
}

func validateThrottle(t *ThrottleInput) error {
	if t == nil || t.RateLimit == nil {
		return nil
	}

	v := *t.RateLimit

	switch {
	case v < 0 || v > maxEndpointRateLimit:
		return validationError("rateLimit must be between 0 and %d.", maxEndpointRateLimit)
	case v <= rateStepBoundary && v%rateStepLow != 0:
		return validationError("rateLimit values up to %d must be a multiple of %d.", rateStepBoundary, rateStepLow)
	case v > rateStepBoundary && v%rateStepHigh != 0:
		return validationError("rateLimit values above %d must be a multiple of %d.", rateStepBoundary, rateStepHigh)
	}

	return nil
}

func validateAuthType(v string) error {
	if v != authApplicationManaged && v != authIam {
		return validationError("authType must be %s or %s.", authApplicationManaged, authIam)
	}

	return nil
}

func validateMode(v string) error {
	if v != modeLatestRevision && v != modeDisabled {
		return validationError("autoDeploymentMode must be %s or %s.", modeLatestRevision, modeDisabled)
	}

	return nil
}

// normalizeEndpoint validates in (everything except revision weights) and resolves its Regions.
func normalizeEndpoint(in *EndpointInput, home string) error {
	if err := validateName("endpoint", in.Name); err != nil {
		return err
	}

	if err := validateAuthType(in.AuthType); err != nil {
		return err
	}

	if in.AutoDeploymentMode == "" {
		in.AutoDeploymentMode = modeDisabled
	}

	if err := validateMode(in.AutoDeploymentMode); err != nil {
		return err
	}

	if err := validateScaling(in.Scaling); err != nil {
		return err
	}

	if err := validateThrottle(in.Throttle); err != nil {
		return err
	}

	regions, err := resolveRegions(in.Type, in.Regions, home)
	if err != nil {
		return err
	}

	in.Regions = regions

	if in.Type != endpointHomeRegion && in.AutoDeploymentMode == modeLatestRevision {
		return validationError("autoDeploymentMode %s is supported only for %s endpoints.",
			modeLatestRevision, endpointHomeRegion)
	}

	return nil
}

func resolveRegions(typ string, given []string, home string) ([]string, error) {
	switch typ {
	case endpointHomeRegion:
		for _, r := range given {
			if r != home {
				return nil, validationError("%s endpoints can only specify the home Region.", endpointHomeRegion)
			}
		}

		return []string{home}, nil
	case endpointMultiRegion, endpointPerRegion:
	default:
		return nil, validationError("endpointType must be %s, %s or %s.",
			endpointHomeRegion, endpointMultiRegion, endpointPerRegion)
	}

	out := []string{home}

	for _, r := range given {
		if !regionpeers.ValidRegion(r) {
			return nil, validationError("%q is not a valid Region.", r)
		}

		if !slices.Contains(out, r) {
			out = append(out, r)
		}
	}

	if len(out) < minMultiRegions {
		return nil, validationError("%s endpoints require at least one Region other than the home Region.", typ)
	}

	return out, nil
}

func validateFilters(filters []Filter, allowed ...string) error {
	for _, f := range filters {
		if !slices.Contains(allowed, f.Name) {
			return validationError("Unsupported filter name %q; supported: %s.", f.Name, strings.Join(allowed, ", "))
		}

		if len(f.Values) == 0 {
			return validationError("Filter %q requires at least one value.", f.Name)
		}
	}

	return nil
}

func matchesFilters(filters []Filter, attrs map[string]string) bool {
	for _, f := range filters {
		if !slices.Contains(f.Values, attrs[f.Name]) {
			return false
		}
	}

	return true
}

func validatePolicyDocument(policy string) error {
	if len(policy) > maxPolicyBytes {
		return validationError("The policy document exceeds the %d byte limit.", maxPolicyBytes)
	}

	var doc struct {
		Statement json.RawMessage `json:"Statement"`
	}

	if json.Unmarshal([]byte(policy), &doc) != nil || len(doc.Statement) == 0 {
		return validationError("The resource policy must be a valid JSON policy document with a Statement.")
	}

	return nil
}
