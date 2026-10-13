package iam

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxTagsPerResource = 50
	entityNamePattern  = `[\w+=,.@-]+`
)

var entityNameRe = regexp.MustCompile(`^` + entityNamePattern + `$`)

type nameConstraint struct {
	param string
	field string
	max   int
}

const (
	maxUserRoleNameLen = 64
	maxOtherNameLen    = 128
)

func nameConstraintFor(action string) (nameConstraint, bool) {
	kind, ok := strings.CutPrefix(action, "Create")
	if !ok {
		return nameConstraint{}, false
	}

	switch kind {
	case "User":
		return nameConstraint{"UserName", "userName", maxUserRoleNameLen}, true
	case "Role":
		return nameConstraint{"RoleName", "roleName", maxUserRoleNameLen}, true
	case "Group":
		return nameConstraint{"GroupName", "groupName", maxOtherNameLen}, true
	case "Policy":
		return nameConstraint{"PolicyName", "policyName", maxOtherNameLen}, true
	case "InstanceProfile":
		return nameConstraint{"InstanceProfileName", "instanceProfileName", maxOtherNameLen}, true
	}

	return nameConstraint{}, false
}

func validationErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidationError, fmt.Sprintf(format, args...))
}

// validateRequestInput applies the name-shape and Marker checks shared by every IAM action.
func validateRequestInput(action string, vals url.Values) error {
	if nc, ok := nameConstraintFor(action); ok {
		name := vals.Get(nc.param)
		if len(name) > nc.max {
			return validationErr(
				"1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
					"Member must have length less than or equal to %d", name, nc.field, nc.max)
		}

		if name != "" && !entityNameRe.MatchString(name) {
			return validationErr(
				"1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
					"Member must satisfy regular expression pattern: %s", name, nc.field, entityNamePattern)
		}
	}

	if marker := vals.Get("Marker"); marker != "" && page.ValidateToken(marker) != nil {
		return validationErr("Invalid Marker")
	}

	return validateTagMembers(vals)
}

// validateTagMembers rejects duplicate (case-insensitive) tag keys within one request.
func validateTagMembers(vals url.Values) error {
	var seen map[string]struct{}

	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("Tags.member.%d.Key", i))
		if k == "" {
			return nil
		}

		lk := strings.ToLower(k)
		if _, dup := seen[lk]; dup {
			return fmt.Errorf(
				"%w: duplicate tag keys found; tag keys are case insensitive",
				ErrInvalidInput,
			)
		}

		if seen == nil {
			seen = make(map[string]struct{})
		}

		seen[lk] = struct{}{}
	}
}

func checkTagLimit(existing, add map[string]string) error {
	total := len(existing)

	for k := range add {
		if _, ok := existing[k]; !ok {
			total++
		}
	}

	if total > maxTagsPerResource {
		return fmt.Errorf("%w: Cannot assign more than %d tags per resource", ErrLimitExceeded, maxTagsPerResource)
	}

	return nil
}

// errorMessage drops the sentinel's code text so "NoSuchEntity: user: user "x" not found" reads "user "x" not found".
func errorMessage(err error) string {
	msg := err.Error()

	for _, m := range iamErrorMappings {
		if !errors.Is(err, m.err) {
			continue
		}

		sentinel := m.err.Error()

		if rest, ok := strings.CutPrefix(msg, sentinel+": "); ok {
			return rest
		}

		return msg
	}

	return msg
}
