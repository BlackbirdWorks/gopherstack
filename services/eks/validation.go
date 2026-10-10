package eks

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxClusterNameLen   = 100
	maxNodegroupNameLen = 63
	k8sMinMinor         = 20
	k8sMaxMinor         = 60
)

var (
	resourceNamePattern = regexp.MustCompile(`^[0-9A-Za-z][A-Za-z0-9\-_]*$`)
	roleARNPattern      = regexp.MustCompile(
		`^arn:aws(-[a-z]+)*:iam::[0-9]{1,12}:role/.+$`)
	k8sVersionPattern = regexp.MustCompile(`^1\.([0-9]{1,2})$`)
)

func validateResourceName(kind, name string, maxLen int) error {
	if name == "" || len(name) > maxLen || !resourceNamePattern.MatchString(name) {
		return errMsgf(ErrValidation, "%s name can contain only alphanumeric characters (case-sensitive), hyphens, "+
			"and underscores. It must start with an alphanumeric character and can't be longer than %d characters.",
			kind, maxLen)
	}

	return nil
}

func validateRoleARN(roleARN string) error {
	if roleARN != "" && !roleARNPattern.MatchString(roleARN) {
		return fmt.Errorf("%w: Role ARN %q is not a valid IAM role ARN", ErrValidation, roleARN)
	}

	return nil
}

// k8sMinor returns the minor number of a "1.N" Kubernetes version.
func k8sMinor(version string) (int, bool) {
	m := k8sVersionPattern.FindStringSubmatch(version)
	if m == nil {
		return 0, false
	}

	n, err := strconv.Atoi(m[1])

	return n, err == nil && n >= k8sMinMinor && n <= k8sMaxMinor
}

func validateK8sVersion(version string) error {
	if version == "" {
		return nil
	}

	if _, ok := k8sMinor(version); !ok {
		return fmt.Errorf("%w: unsupported Kubernetes version %q", ErrValidation, version)
	}

	return nil
}

// validateVersionUpgrade enforces EKS's one-minor-version-at-a-time cluster upgrade rule.
func validateVersionUpgrade(from, to string) error {
	fromMinor, okFrom := k8sMinor(from)
	toMinor, okTo := k8sMinor(to)

	if !okTo {
		return fmt.Errorf("%w: unsupported Kubernetes version %q", ErrValidation, to)
	}

	if okFrom && toMinor != fromMinor+1 {
		return fmt.Errorf("%w: Unsupported Kubernetes minor version update from %s to %s",
			ErrValidation, from, to)
	}

	return nil
}

func validateNodegroupScaling(desired, minSize, maxSize int32) error {
	if minSize < 0 || maxSize < 1 || minSize > maxSize || desired < minSize || desired > maxSize {
		return fmt.Errorf("%w: Invalid scaling config: minSize %d, maxSize %d and desiredSize %d must satisfy "+
			"0 <= minSize <= desiredSize <= maxSize and maxSize >= 1", ErrValidation, minSize, maxSize, desired)
	}

	return nil
}

func validateCreateClusterInput(name, version, roleARN string) error {
	if err := validateResourceName("Cluster", name, maxClusterNameLen); err != nil {
		return err
	}

	if err := validateK8sVersion(version); err != nil {
		return err
	}

	if err := validateRoleARN(roleARN); err != nil {
		return err
	}

	return nil
}

// validateListQuery rejects a malformed nextToken or an out-of-range maxResults on GET list calls.
func validateListQuery(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return nil
	}

	q := c.Request().URL.Query()

	if page.ValidateToken(q.Get("nextToken")) != nil {
		return fmt.Errorf("%w: Invalid token: %s", ErrValidation, q.Get("nextToken"))
	}

	if s := q.Get("maxResults"); s != "" {
		if n, err := strconv.Atoi(s); err != nil || n < 1 || n > eksDefaultPageSize {
			return fmt.Errorf("%w: maxResults must be between 1 and %d", ErrValidation, eksDefaultPageSize)
		}
	}

	return nil
}
