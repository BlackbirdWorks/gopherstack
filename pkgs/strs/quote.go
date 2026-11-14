package strs

import "strings"

// EscapeSingleQuotes doubles every single quote so s can sit inside '...'.
func EscapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// EscapeDoubleQuotes backslash-escapes backslashes and double quotes so s can
// sit inside a "..." quoted-string.
func EscapeDoubleQuotes(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)
}
