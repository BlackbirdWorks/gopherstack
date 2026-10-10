package sagemaker

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	opSearch   = "Search"
	opListTags = "ListTags"

	maxResourceNameLen = 63
	maxHyperParameters = 100
	maxHyperParamLen   = 2500
	minRoleArnLen      = 20
	maxRoleArnLen      = 2048
)

//nolint:gochecknoglobals // read-only compiled patterns from the pinned SDK docs
var (
	resourceNamePattern  = regexp.MustCompile(`^[a-zA-Z0-9]([\-a-zA-Z0-9]*[a-zA-Z0-9])?$`)
	roleArnPattern       = regexp.MustCompile(`^arn:aws[a-z\-]*:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+$`)
	instanceTypePattern  = regexp.MustCompile(`^ml\.[a-z0-9\-]+\.[a-z0-9\-]+$`)
	s3OutputPathPattern  = regexp.MustCompile(`^(https|s3)://([^/]+)/?(.*)$`)
	hyperParamNameMaxLen = 256
)

func validateResourceName(field, v string) error {
	if len(v) > maxResourceNameLen || !resourceNamePattern.MatchString(v) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
				"Member must satisfy regular expression pattern: %s",
			errInvalidRequest, v, lowerFirst(field), resourceNamePattern.String(),
		)
	}

	return nil
}

func validateRoleArn(field, v string) error {
	if len(v) < minRoleArnLen || len(v) > maxRoleArnLen || !roleArnPattern.MatchString(v) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
				"Member must satisfy regular expression pattern: %s",
			errInvalidRequest, v, lowerFirst(field), roleArnPattern.String(),
		)
	}

	return nil
}

func validateInstanceType(field, v string) error {
	if v == "" {
		return nil
	}

	if !instanceTypePattern.MatchString(v) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
				"Member must satisfy enum value set of ml.<family>.<size> instance types",
			errInvalidRequest, v, lowerFirst(field),
		)
	}

	return nil
}

func validateS3OutputPath(field, v string) error {
	if !s3OutputPathPattern.MatchString(v) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
				"Member must satisfy regular expression pattern: %s",
			errInvalidRequest, v, lowerFirst(field), s3OutputPathPattern.String(),
		)
	}

	return nil
}

func validateHyperParameters(hp map[string]string) error {
	if len(hp) > maxHyperParameters {
		return fmt.Errorf("%w: HyperParameters must have at most %d entries", errInvalidRequest, maxHyperParameters)
	}

	for k, v := range hp {
		if k == "" || len(k) > hyperParamNameMaxLen || len(v) > maxHyperParamLen {
			return fmt.Errorf("%w: HyperParameters entry %q exceeds the documented length limits", errInvalidRequest, k)
		}
	}

	return nil
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToLower(s[:1]) + s[1:]
}

func validateTrainingJobRequest(req *createTrainingJobFullRequest) error {
	return firstErr(
		validateResourceName("TrainingJobName", req.TrainingJobName),
		validateRoleArn("RoleArn", req.RoleArn),
		validateInstanceType("ResourceConfig.InstanceType", req.ResourceConfig.InstanceType),
		validateS3OutputPath("OutputDataConfig.S3OutputPath", req.OutputDataConfig.S3OutputPath),
		validateHyperParameters(req.HyperParameters),
	)
}

func validateEndpointConfigNames(req *createEndpointConfigRequest) error {
	if err := validateResourceName("EndpointConfigName", req.EndpointConfigName); err != nil {
		return err
	}

	for _, pvs := range [][]ProductionVariant{req.ProductionVariants, req.ShadowProductionVariants} {
		for _, pv := range pvs {
			if err := validateInstanceType("ProductionVariants.InstanceType", pv.InstanceType); err != nil {
				return err
			}
		}
	}

	if req.ExecutionRoleArn != "" {
		return validateRoleArn("ExecutionRoleArn", req.ExecutionRoleArn)
	}

	return nil
}

const (
	minListMaxResults = 1
	maxListMaxResults = 100
)

// validateListPaging enforces MaxResults 1..100 and a well-formed NextToken on
// List*/Search ops; ListTags uses its own bounds and is left to its handler.
func validateListPaging(op string, body []byte) error {
	if (!strings.HasPrefix(op, "List") && op != opSearch) || op == opListTags {
		return nil
	}

	var req struct {
		MaxResults *int64 `json:"MaxResults"`
		NextToken  string `json:"NextToken"`
	}

	_ = json.Unmarshal(body, &req)

	if m := req.MaxResults; m != nil && (*m < minListMaxResults || *m > maxListMaxResults) {
		return fmt.Errorf(
			"%w: 1 validation error detected: Value '%d' at 'maxResults' failed to satisfy constraint: "+
				"Member must have value between %d and %d",
			errInvalidRequest, *m, minListMaxResults, maxListMaxResults,
		)
	}

	if req.NextToken != "" && page.ValidateToken(req.NextToken) != nil {
		return fmt.Errorf("%w: Invalid NextToken", errInvalidRequest)
	}

	return nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *InMemoryBackend) couldNotFind(sentinel error, region, kind, prefix, name string) error {
	resourceARN := arn.Build("sagemaker", region, b.accountID, prefix+name)

	return fmt.Errorf("%w: %s", sentinel, "Could not find "+kind+" "+strconv.Quote(resourceARN)+".")
}
