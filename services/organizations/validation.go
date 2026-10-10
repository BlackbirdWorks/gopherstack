package organizations

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	maxAccountNameLen = 50
	minEmailLen       = 6
	maxEmailLen       = 64
	maxRoleNameLen    = 64
	maxOUNameLen      = 128
)

var (
	parentIDPattern = regexp.MustCompile(`^(r-[0-9a-z]{4,32}|ou-[0-9a-z]{4,32}-[0-9a-z]{8,32})$`)
	emailPattern    = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
	rolePattern     = regexp.MustCompile(`^[\w+=,.@-]{1,64}$`)
)

func validateAccountName(name string) bool {
	if name == "" || utf8.RuneCountInString(name) > maxAccountNameLen {
		return false
	}

	for _, r := range name {
		if r < ' ' || r > '~' {
			return false
		}
	}

	return true
}

func validateEmail(email string) bool {
	n := utf8.RuneCountInString(email)

	return n >= minEmailLen && n <= maxEmailLen && emailPattern.MatchString(email)
}

func validateRoleName(role string) bool {
	return len(role) <= maxRoleNameLen && rolePattern.MatchString(role)
}

func validateOUName(name string) bool {
	n := utf8.RuneCountInString(strings.TrimSpace(name))

	return n >= 1 && utf8.RuneCountInString(name) <= maxOUNameLen
}

// checkParentLocked distinguishes a malformed parent ID (InvalidInput) from a
// well-formed one that names nothing (ParentNotFound).
func (b *InMemoryBackend) checkParentLocked(parentID string) error {
	if !parentIDPattern.MatchString(parentID) {
		return ErrInvalidInput
	}

	if !b.parentExists(parentID) {
		return ErrParentNotFound
	}

	return nil
}
