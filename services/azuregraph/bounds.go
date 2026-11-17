package azuregraph

import "net/url"

const (
	maxQueryOptionLen = 4096
	maxFilterTokens   = 512
	maxFilterDepth    = 32
	maxExpandDepth    = 8
)

// checkQueryBounds caps the size of every free-text query option before it is parsed.
func checkQueryBounds(vals url.Values) error {
	for _, opt := range []string{"$filter", "$expand", "$orderby", "$search", "$select", "$skiptoken"} {
		if len(vals.Get(opt)) > maxQueryOptionLen {
			return badRequest(
				"The query option '%s' is longer than the %d characters supported.",
				opt,
				maxQueryOptionLen,
			)
		}
	}

	return nil
}

// checkFilterShape bounds the token count and parenthesis nesting of a lexed $filter.
func checkFilterShape(toks []token) bool {
	if len(toks) > maxFilterTokens {
		return false
	}

	depth := 0

	for _, t := range toks {
		switch t.kind {
		case tokLParen:
			if depth++; depth > maxFilterDepth {
				return false
			}
		case tokRParen:
			depth--
		case tokIdent, tokString, tokComma, tokColon, tokEOF:
		}
	}

	return true
}
