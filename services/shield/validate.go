package shield

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	protectionNamePattern  = regexp.MustCompile(`^[ a-zA-Z0-9_.\-]*$`)
	contactEmailPattern    = regexp.MustCompile(`^\S+@\S+\.\S+$`)
	contactPhonePattern    = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)
	contactNotesPattern    = regexp.MustCompile(`^[\w\s.\-,:/()+@]*$`)
	drtRoleARNPattern      = regexp.MustCompile(`^arn:aws:iam::\d{12}:role/?[a-zA-Z_0-9+=,.@\-_/]+`)
	protectionGroupIDRegex = regexp.MustCompile(`^[a-zA-Z0-9\-]+$`)
)

const (
	maxProtectionNameLen   = 128
	maxProtectionGroupIDLn = 36
	maxContactEmailLen     = 150
	maxContactPhoneLen     = 16
	maxContactNotesLen     = 1024
)

func validateProtectionName(name string) error {
	if len(name) > maxProtectionNameLen || !protectionNamePattern.MatchString(name) {
		return fmt.Errorf(
			"%w: Name must be 1-%d characters of letters, digits, spaces, '_', '.' and '-'",
			ErrValidation, maxProtectionNameLen,
		)
	}

	return nil
}

func validateProtectionGroupID(id string) error {
	if len(id) > maxProtectionGroupIDLn || !protectionGroupIDRegex.MatchString(id) {
		return fmt.Errorf(
			"%w: ProtectionGroupId must be 1-%d characters of letters, digits and '-'",
			ErrValidation, maxProtectionGroupIDLn,
		)
	}

	return nil
}

func validateEmergencyContact(c EmergencyContact) error {
	if c.EmailAddress == "" {
		return fmt.Errorf("%w: EmailAddress is required for each emergency contact", ErrValidation)
	}

	if len(c.EmailAddress) > maxContactEmailLen || !contactEmailPattern.MatchString(c.EmailAddress) {
		return fmt.Errorf("%w: EmailAddress %q is not a valid email address", ErrValidation, c.EmailAddress)
	}

	if c.PhoneNumber != "" &&
		(len(c.PhoneNumber) > maxContactPhoneLen || !contactPhonePattern.MatchString(c.PhoneNumber)) {
		return fmt.Errorf("%w: PhoneNumber must be in E.164 format (+14155550100)", ErrValidation)
	}

	if len(c.ContactNotes) > maxContactNotesLen || !contactNotesPattern.MatchString(c.ContactNotes) {
		return fmt.Errorf("%w: ContactNotes is too long or contains unsupported characters", ErrValidation)
	}

	return nil
}

func validateDRTRoleARN(arn string) error {
	if !drtRoleARNPattern.MatchString(arn) {
		return fmt.Errorf("%w: RoleArn must be an IAM role ARN (arn:aws:iam::<account>:role/<name>)", ErrValidation)
	}

	return nil
}

func validateGroupPatternFields(pattern, resourceType string, members []string) error {
	if pattern != PatternArbitrary && len(members) > 0 {
		return fmt.Errorf("%w: Members can only be set when Pattern is ARBITRARY", ErrValidation)
	}

	if pattern != PatternByResourceType && resourceType != "" {
		return fmt.Errorf("%w: ResourceType can only be set when Pattern is BY_RESOURCE_TYPE", ErrValidation)
	}

	return nil
}

// messageWithoutCode strips the leading wire-code and package prefixes from an internal error
// string so the message the client sees does not repeat the __type.
func messageWithoutCode(code string, err error) string {
	msg := err.Error()

	for _, prefix := range []string{code + ": ", "invalid request: ", "shield: "} {
		msg = strings.TrimPrefix(msg, prefix)
	}

	return msg
}
