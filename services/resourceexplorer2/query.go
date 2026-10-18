package resourceexplorer2

import (
	"path"
	"strings"
)

const (
	filterRegion       = "region"
	filterService      = "service"
	filterResourceType = "resourcetype"
	filterTag          = "tag"
	filterTagKey       = "tag.key"
	filterTagValue     = "tag.value"
	filterAccountID    = "accountid"
	filterID           = "id"
	filterKeyword      = ""
)

var filterNames = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	filterRegion: true, filterService: true, filterResourceType: true, filterTag: true,
	filterTagKey: true, filterTagValue: true, filterAccountID: true, filterID: true,
}

// clause is one parsed query term: a keyword (name "") or a filter.
type clause struct {
	name    string
	value   string
	negated bool
}

// parseQuery splits a Resource Explorer query string into keywords and filters.
func parseQuery(q string) ([]clause, error) {
	if len(q) > maxQueryLen {
		return nil, invalid("QueryString must be at most %d characters", maxQueryLen)
	}

	tokens, err := tokenize(q)
	if err != nil {
		return nil, err
	}

	out := make([]clause, 0, len(tokens))

	for _, tok := range tokens {
		c := clause{name: filterKeyword}

		if strings.HasPrefix(tok, "-") && len(tok) > 1 {
			c.negated, tok = true, tok[1:]
		}

		if name, val, ok := strings.Cut(tok, ":"); ok && filterNames[strings.ToLower(name)] {
			if val == "" {
				return nil, invalid("filter %q needs a value", name)
			}

			c.name, c.value = strings.ToLower(name), val
		} else {
			c.value = tok
		}

		out = append(out, c)
	}

	return out, nil
}

// tokenize splits on whitespace; double quotes group characters and are removed.
func tokenize(q string) ([]string, error) {
	var (
		out      []string
		cur      strings.Builder
		inQuotes bool
		started  bool
	)

	flush := func() {
		if started {
			out = append(out, cur.String())
			cur.Reset()

			started = false
		}
	}

	for _, r := range q {
		switch {
		case r == '"':
			inQuotes, started = !inQuotes, true
		case !inQuotes && (r == ' ' || r == '\t' || r == '\n'):
			flush()
		default:
			cur.WriteRune(r)

			started = true
		}
	}

	if inQuotes {
		return nil, invalid("QueryString has an unterminated quote")
	}

	flush()

	return out, nil
}

// candidate is the searchable projection of one resource.
type candidate struct {
	tags         map[string]string
	arn          string
	service      string
	resourceType string
	region       string
	account      string
	id           string
	blob         string
}

func globMatch(pattern, v string) bool {
	ok, err := path.Match(strings.ToLower(pattern), strings.ToLower(v))

	return err == nil && ok
}

func (c *candidate) matches(cl clause) bool {
	switch cl.name {
	case filterRegion:
		return globMatch(cl.value, c.region)
	case filterService:
		return globMatch(cl.value, c.service)
	case filterResourceType:
		return globMatch(cl.value, c.resourceType)
	case filterAccountID:
		return c.account == cl.value
	case filterID:
		return strings.EqualFold(c.id, cl.value)
	case filterTagKey:
		_, ok := c.tags[cl.value]

		return ok
	case filterTagValue:
		for _, v := range c.tags {
			if v == cl.value {
				return true
			}
		}

		return false
	case filterTag:
		return c.matchesTag(cl.value)
	default:
		return strings.Contains(c.blob, strings.ToLower(cl.value))
	}
}

func (c *candidate) matchesTag(v string) bool {
	switch strings.ToLower(v) {
	case "all":
		return len(c.tags) > 0
	case "none":
		return len(c.tags) == 0
	}

	k, val, hasValue := strings.Cut(v, "=")

	got, ok := c.tags[k]
	if !ok {
		return false
	}

	return !hasValue || got == val
}

// evaluate applies clauses: keywords and negations all must hold; positive filters of one name are OR-ed.
func evaluate(c *candidate, clauses []clause) bool {
	positive := map[string]bool{}
	seen := map[string]bool{}

	for _, cl := range clauses {
		hit := c.matches(cl)

		if cl.negated {
			if hit {
				return false
			}

			continue
		}

		if cl.name == filterKeyword {
			if !hit {
				return false
			}

			continue
		}

		seen[cl.name] = true
		positive[cl.name] = positive[cl.name] || hit
	}

	for name := range seen {
		if !positive[name] {
			return false
		}
	}

	return true
}
