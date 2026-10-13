package rolesanywhere_test

import (
	"regexp"
	"testing"
)

var invalidNameChars = regexp.MustCompile(`[^ a-zA-Z0-9_-]`)

func testName(t *testing.T) string {
	t.Helper()

	return invalidNameChars.ReplaceAllString(t.Name(), "_")
}
