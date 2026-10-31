package gcpresourcemanager

import (
	"regexp"
	"strings"
	"unicode/utf8"
)

const (
	projectIDMin     = 6
	projectIDMax     = 30
	projectNameMin   = 4
	projectNameMax   = 30
	maxLabels        = 64
	maxLabelLen      = 63
	maxFolderDepth   = 10
	maxFolderFanout  = 300
	maxTagDescLen    = 256
	maxTagShortName  = 63
	
)

var (
	projectIDRE   = regexp.MustCompile(`^[a-z][a-z0-9-]*[a-z0-9]$`)
	projectNameRE = regexp.MustCompile(`^[A-Za-z0-9'" !-]+$`)
	labelKeyRE    = regexp.MustCompile(`^[a-z]([-a-z0-9]*[a-z0-9])?$`)
	labelValueRE  = regexp.MustCompile(`^([a-z]([-a-z0-9]*[a-z0-9])?)?$`)
	folderNameRE  = regexp.MustCompile(`^[\p{L}\p{N}][\p{L}\p{N}_\- ]{1,28}[\p{L}\p{N}]$`)
	tagNameRE     = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9])?$`)
	restricted    = []string{"google", "ssl"}
)

// splitName splits "kind/id" and checks the kind.
func splitName(name, kind string) (string, bool) {
	id, ok := strings.CutPrefix(name, kind+"/")
	if !ok || id == "" || strings.Contains(id, "/") {
		return "", false
	}

	return id, true
}

func validateProjectID(id string) error {
	if n := len(id); n < projectIDMin || n > projectIDMax || !projectIDRE.MatchString(id) {
		return errInvalid("Invalid project id %q: it must be 6 to 30 lowercase ASCII letters, digits, or hyphens, "+
			"start with a letter and not end with a hyphen.", id)
	}

	for _, r := range restricted {
		if strings.Contains(id, r) {
			return errInvalid("Invalid project id %q: it must not contain the restricted string %q.", id, r)
		}
	}

	return nil
}

func validateProjectName(name string) error {
	if name == "" {
		return nil
	}

	if n := utf8.RuneCountInString(name); n < projectNameMin || n > projectNameMax || !projectNameRE.MatchString(name) {
		return errInvalid("Invalid display name %q: it must be 4 to 30 characters of letters, numbers, "+
			"hyphen, single-quote, double-quote, space and exclamation point.", name)
	}

	return nil
}

func validateLabels(labels map[string]string) error {
	if len(labels) > maxLabels {
		return errInvalid("A project can have at most %d labels.", maxLabels)
	}

	for k, v := range labels {
		if len(k) == 0 || len(k) > maxLabelLen || !labelKeyRE.MatchString(k) {
			return errInvalid("Invalid label key %q: 1 to 63 characters matching [a-z]([-a-z0-9]*[a-z0-9])?.", k)
		}

		if len(v) > maxLabelLen || !labelValueRE.MatchString(v) {
			return errInvalid("Invalid label value %q for key %q: 0 to 63 characters matching "+
				"([a-z]([-a-z0-9]*[a-z0-9])?)?.", v, k)
		}
	}

	return nil
}

func validateFolderName(name string) error {
	if !folderNameRE.MatchString(name) {
		return errInvalid("Invalid folder display name %q: it must start and end with a letter or digit, "+
			"contain letters, digits, spaces, hyphens and underscores and be 3 to 30 characters.", name)
	}

	return nil
}

func validateTagName(kind, name string) error {
	if name == "" || len(name) > maxTagShortName || !tagNameRE.MatchString(name) {
		return errInvalid("Invalid %s short_name %q: 1 to 63 characters, alphanumeric at both ends, with "+
			"dashes, underscores and dots between.", kind, name)
	}

	return nil
}

func validateDescription(d string) error {
	if utf8.RuneCountInString(d) > maxTagDescLen {
		return errInvalid("description must not exceed %d characters.", maxTagDescLen)
	}

	return nil
}
