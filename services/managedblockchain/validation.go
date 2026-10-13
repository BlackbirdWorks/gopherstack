package managedblockchain

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	maxNameLen              = 64
	maxDescriptionLen       = 128
	maxClientTokenLen       = 64
	maxFrameworkVersionLen  = 8
	maxAdminUsernameLen     = 16
	maxProposalDurationHrs  = 168
	maxThresholdPercentage  = 100
	minAdminPasswordLen     = 8
	maxAdminPasswordLenReal = 32
)

var (
	memberNameRe    = regexp.MustCompile(`^[a-zA-Z0-9-]+$`)
	adminUsernameRe = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9]*$`)
)

func invalidRequest(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrValidation, fmt.Sprintf(format, args...))
}

func validateMemberName(name string) error {
	if len(name) > maxNameLen || !memberNameRe.MatchString(name) ||
		strings.HasPrefix(name, "-") || strings.HasSuffix(name, "-") || strings.Contains(name, "--") ||
		(name != "" && name[0] >= '0' && name[0] <= '9') {
		return invalidRequest("member name %q must be 1-%d alphanumeric characters or hyphens, "+
			"not start with a digit or hyphen, end with a hyphen, or contain consecutive hyphens", name, maxNameLen)
	}

	return nil
}

func validateAdminCredentials(username, password string) error {
	if len(username) > maxAdminUsernameLen || !adminUsernameRe.MatchString(username) {
		return invalidRequest("AdminUsername must be 1-%d alphanumeric characters starting with a letter",
			maxAdminUsernameLen)
	}

	if len(password) < minAdminPasswordLen || len(password) > maxAdminPasswordLenReal {
		return invalidRequest("AdminPassword must be between %d and %d characters",
			minAdminPasswordLen, maxAdminPasswordLenReal)
	}

	if !passwordComplex(password) {
		return invalidRequest("AdminPassword must contain a lowercase letter, an uppercase letter and a digit, " +
			"and must not contain @, ', \\, \" or /")
	}

	return nil
}

func validateNetworkFields(req createNetworkRequest) error {
	if strings.TrimSpace(req.Name) == "" || len(req.Name) > maxNameLen {
		return invalidRequest("network name must be 1-%d characters and not blank", maxNameLen)
	}

	if len(req.Description) > maxDescriptionLen {
		return invalidRequest("description must not exceed %d characters", maxDescriptionLen)
	}

	if len(req.ClientRequestToken) > maxClientTokenLen {
		return invalidRequest("ClientRequestToken must not exceed %d characters", maxClientTokenLen)
	}

	if len(req.FrameworkVersion) > maxFrameworkVersionLen {
		return invalidRequest("FrameworkVersion must not exceed %d characters", maxFrameworkVersionLen)
	}

	if vp := req.VotingPolicy; vp != nil && vp.ApprovalThresholdPolicy != nil {
		return validateApprovalPolicy(vp.ApprovalThresholdPolicy.ThresholdComparator,
			vp.ApprovalThresholdPolicy.ProposalDurationInHours, vp.ApprovalThresholdPolicy.ThresholdPercentage)
	}

	return nil
}

func validateApprovalPolicy(comparator string, hours, percentage int32) error {
	if comparator != "" && comparator != "GREATER_THAN" && comparator != comparatorGTE {
		return invalidRequest("ThresholdComparator %q must be GREATER_THAN or GREATER_THAN_OR_EQUAL_TO", comparator)
	}

	if hours < 0 || hours > maxProposalDurationHrs {
		return invalidRequest("ProposalDurationInHours must be between 1 and %d", maxProposalDurationHrs)
	}

	if percentage < 0 || percentage > maxThresholdPercentage {
		return invalidRequest("ThresholdPercentage must be between 0 and %d", maxThresholdPercentage)
	}

	return nil
}

func passwordComplex(password string) bool {
	var lower, upper, digit bool

	for _, r := range password {
		switch {
		case r >= 'a' && r <= 'z':
			lower = true
		case r >= 'A' && r <= 'Z':
			upper = true
		case r >= '0' && r <= '9':
			digit = true
		}
	}

	return lower && upper && digit && !strings.ContainsAny(password, "@'\\\"/")
}

const comparatorGTE = "GREATER_THAN_OR_EQUAL_TO"
