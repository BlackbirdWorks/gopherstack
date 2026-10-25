package jsruntime

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrModule marks unsupported ES module syntax.
var ErrModule = errors.New("unsupported module syntax")

const asParts = 2

var (
	reImport     = regexp.MustCompile(`(?s)\bimport\s+([^;'"]*?)\s*from\s*['"]([^'"]+)['"]\s*;?`)
	reExportDecl = regexp.MustCompile(`\bexport\s+((?:async\s+)?function|const|let|var|class)\b`)
	reExportList = regexp.MustCompile(`(?s)\bexport\s*\{[^}]*\}\s*;?`)
	reExportDflt = regexp.MustCompile(`\bexport\s+default\b`)
	reAsName     = regexp.MustCompile(`\s+as\s+`)
)

// RewriteModule turns ES module source into a plain script. modules maps an
// importable specifier to the global holding its exports; other specifiers are
// rejected. Default (import x from), namespace (import * as x) and named
// (import { a as b }) forms are supported; export keywords are dropped.
func RewriteModule(code string, modules map[string]string) (string, error) {
	if reExportDflt.MatchString(code) {
		return "", fmt.Errorf("%w: default exports are not supported", ErrModule)
	}

	var impErr error

	out := reImport.ReplaceAllStringFunc(code, func(m string) string {
		sub := reImport.FindStringSubmatch(m)

		obj, ok := modules[sub[2]]
		if !ok {
			impErr = fmt.Errorf("%w: unsupported module %q", ErrModule, sub[2])

			return ""
		}

		stmt, err := importStmt(strings.TrimSpace(sub[1]), obj)
		if err != nil {
			impErr = err
		}

		return stmt + strings.Repeat("\n", strings.Count(m, "\n"))
	})
	if impErr != nil {
		return "", impErr
	}

	out = reExportList.ReplaceAllStringFunc(
		out,
		func(m string) string { return strings.Repeat("\n", strings.Count(m, "\n")) },
	)

	return reExportDecl.ReplaceAllString(out, "$1"), nil
}

func importStmt(clause, obj string) (string, error) {
	switch {
	case strings.HasPrefix(clause, "*"):
		as := reAsName.Split(clause, asParts)
		if len(as) != asParts || strings.TrimSpace(as[1]) == "" {
			return "", fmt.Errorf("%w: unsupported import form", ErrModule)
		}

		return "var " + strings.TrimSpace(as[1]) + " = " + obj + ";", nil
	case strings.HasPrefix(clause, "{"):
		parts := strings.Split(strings.Trim(clause, "{} \n\t\r"), ",")

		for i, p := range parts {
			if as := reAsName.Split(strings.TrimSpace(p), asParts); len(as) == asParts {
				parts[i] = as[0] + ": " + as[1]
			}
		}

		return "var {" + strings.Join(parts, ",") + "} = " + obj + ";", nil
	case clause != "" && !strings.ContainsAny(clause, " ,*{}"):
		return "var " + clause + " = " + obj + ";", nil
	}

	return "", fmt.Errorf("%w: unsupported import form", ErrModule)
}
