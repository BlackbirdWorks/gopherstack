package lambda

import (
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

var (
	functionNameRe = regexp.MustCompile(
		`^(arn:(aws[a-zA-Z-]*)?:lambda:)?([a-z]{2}(-gov)?-[a-z]+-\d{1}:)?(\d{12}:)?(function:)?` +
			`([a-zA-Z0-9-_]+)(:(\$LATEST|[a-zA-Z0-9-_]+))?$`)
	roleARNRe     = regexp.MustCompile(`^arn:(aws[a-zA-Z-]*)?:iam::(\d{12})?:role/?[a-zA-Z_0-9+=,.@\-_/]+$`)
	aliasNameRe   = regexp.MustCompile(`^(?:[a-zA-Z0-9-_]+)$`)
	digitsOnlyRe  = regexp.MustCompile(`^[0-9]+$`)
	permActionRe  = regexp.MustCompile(`^(lambda:[*]|lambda:[a-zA-Z]+|[*])$`)
	statementIDRe = regexp.MustCompile(`^[a-zA-Z0-9-_]+$`)
	envKeyRe      = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9_])+$`)
)

const (
	maxFunctionNameInputLength = 140
	maxEnvironmentBytes        = 4096
	maxStatementIDLength       = 100
	maxAliasNameLength         = 128
)

func isReservedEnvKey(k string) bool {
	switch k {
	case "_HANDLER", "_X_AMZN_TRACE_ID", "AWS_DEFAULT_REGION", "AWS_REGION":
		return true
	}

	return false
}

func constraintMessage(value, field, pattern string) string {
	return fmt.Sprintf(
		"1 validation error detected: Value '%s' at '%s' failed to satisfy constraint: "+
			"Member must satisfy regular expression pattern: %s", value, field, pattern)
}

func (h *Handler) invalidParam(c *echo.Context, msg string) bool {
	_ = h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", msg)

	return false
}

func (h *Handler) validateFunctionNameInput(c *echo.Context, name string) bool {
	if len(name) > maxFunctionNameInputLength || !functionNameRe.MatchString(name) {
		return h.invalidParam(c, constraintMessage(name, "functionName", functionNameRe.String()))
	}

	return true
}

func (h *Handler) validateRoleInput(c *echo.Context, role string) bool {
	if !roleARNRe.MatchString(role) {
		return h.invalidParam(c, constraintMessage(role, "role", roleARNRe.String()))
	}

	return true
}

func (h *Handler) validateEnvironmentInput(c *echo.Context, env *EnvironmentConfig) bool {
	if env == nil {
		return true
	}

	var reserved []string

	size := 0

	for k, v := range env.Variables {
		if !envKeyRe.MatchString(k) {
			return h.invalidParam(c, fmt.Sprintf(
				"Environment variable name '%s' must start with a letter and contain only letters, digits and underscores",
				k,
			))
		}

		if isReservedEnvKey(k) {
			reserved = append(reserved, k)
		}

		size += len(k) + len(v)
	}

	if len(reserved) > 0 {
		slices.Sort(reserved)

		return h.invalidParam(c, "Lambda was unable to configure your environment variables because the "+
			"environment variables you have provided contains reserved keys that are currently not supported "+
			"for modification. Reserved keys used in this request: "+strings.Join(reserved, ", "))
	}

	if size > maxEnvironmentBytes {
		return h.invalidParam(c, fmt.Sprintf(
			"Lambda was unable to configure your environment variables because the environment variables "+
				"you have provided exceeded the 4KB limit. String measured: %d bytes", size))
	}

	return true
}

func (h *Handler) validateAliasName(c *echo.Context, name string) bool {
	if len(name) > maxAliasNameLength || !aliasNameRe.MatchString(name) || digitsOnlyRe.MatchString(name) {
		return h.invalidParam(c, constraintMessage(name, "name", `(?!^[0-9]+$)([a-zA-Z0-9-_]+)`))
	}

	return true
}

func (h *Handler) validatePermissionInput(c *echo.Context, statementID, action string) bool {
	if len(statementID) > maxStatementIDLength || !statementIDRe.MatchString(statementID) {
		return h.invalidParam(c, constraintMessage(statementID, "statementId", statementIDRe.String()))
	}

	if !permActionRe.MatchString(action) {
		return h.invalidParam(c, constraintMessage(action, "action", permActionRe.String()))
	}

	return true
}

// validateMarkerParam rejects a Marker query value that is not a token this service issued.
func (h *Handler) validateMarkerParam(c *echo.Context) bool {
	marker := c.Request().URL.Query().Get("Marker")
	if marker == "" || page.ValidateToken(marker) == nil {
		return true
	}

	return h.invalidParam(c, "Invalid marker: "+marker)
}
