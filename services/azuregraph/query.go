package azuregraph

import (
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	defaultPageSize = 100
	maxPageSize     = 999
)

// listQuery is the parsed OData query of a collection request.
type listQuery struct {
	filter  *compiledFilter
	search  func(map[string]any) bool
	order   string
	token   string
	selects []string
	top     int
	desc    bool
	count   bool
}

func eventual(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Consistencylevel"), "eventual")
}

// parseListQuery validates the OData system query options. fields is the set of declared properties of the
// resource (nil skips property validation); resource names it in error messages.
func parseListQuery(r *http.Request, resource string, fields map[string]bool) (*listQuery, error) {
	vals := r.URL.Query()
	q := &listQuery{top: defaultPageSize, token: vals.Get("$skiptoken")}

	if err := q.parseCount(r, vals); err != nil {
		return nil, err
	}

	if err := q.parsePaging(vals.Get("$top")); err != nil {
		return nil, err
	}

	if err := q.parseSelect(vals.Get("$select"), resource, fields); err != nil {
		return nil, err
	}

	if vals.Get("$expand") != "" {
		return nil, unsupported("Expanding relationships with $expand is not supported by this emulator.")
	}

	if err := q.parseOrder(vals.Get("$orderby"), resource, fields); err != nil {
		return nil, err
	}

	if err := q.parseFilter(r, vals.Get("$filter"), resource, fields); err != nil {
		return nil, err
	}

	return q, q.parseSearch(r, vals.Get("$search"))
}

func (q *listQuery) parsePaging(top string) error {
	if top == "" {
		return nil
	}

	n, err := strconv.Atoi(top)
	if err != nil || n < 1 || n > maxPageSize {
		return badRequest("Invalid page size specified: '%s'. Must be between 1 and %d inclusive.", top, maxPageSize)
	}

	q.top = n

	return nil
}

func (q *listQuery) parseSelect(spec, resource string, fields map[string]bool) error {
	if spec == "" {
		if resource == typeUser {
			q.selects = userDefaultSelect()
		}

		return nil
	}

	for f := range strings.SplitSeq(spec, ",") {
		f = strings.TrimSpace(f)
		if fields != nil && !fields[f] {
			return badRequest(
				"Could not find a property named '%s' on type 'microsoft.graph.%s'.",
				f,
				lowerFirst(resource),
			)
		}

		q.selects = append(q.selects, f)
	}

	return nil
}

func (q *listQuery) parseFilter(r *http.Request, expr, resource string, fields map[string]bool) error {
	if expr == "" {
		return nil
	}

	cf, err := compileFilter(expr, resource, fieldsOrAny(fields, expr))
	if err != nil {
		return err
	}

	if cf.advanced != "" && (!eventual(r) || !q.count) {
		return unsupported("Operator '%s' is not supported because the required parameters might be missing. %s",
			cf.advanced, advancedHint)
	}

	q.filter = cf

	return nil
}

// userDefaultSelect is the property set Graph returns for a user when $select is absent.
func userDefaultSelect() []string {
	return []string{
		"businessPhones", "displayName", "givenName", "jobTitle", "mail", "mobilePhone", "officeLocation",
		"preferredLanguage", "surname", "userPrincipalName",
	}
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToLower(s[:1]) + s[1:]
}

// fieldsOrAny returns fields, or a permissive set built from the filter text when the resource is untyped.
func fieldsOrAny(fields map[string]bool, filter string) map[string]bool {
	if fields != nil {
		return fields
	}

	all := map[string]bool{}
	for _, w := range strings.FieldsFunc(filter, func(r rune) bool { return strings.ContainsRune(" ()',:", r) }) {
		head, _, _ := strings.Cut(w, "/")
		all[head] = true
	}

	return all
}

func (q *listQuery) parseCount(r *http.Request, vals url.Values) error {
	if !strings.EqualFold(vals.Get("$count"), "true") {
		return nil
	}

	if !eventual(r) {
		return badRequest("$count is not currently supported.")
	}

	q.count = true

	return nil
}

func (q *listQuery) parseOrder(spec, resource string, fields map[string]bool) error {
	if spec == "" {
		return nil
	}

	name, dir, _ := strings.Cut(strings.TrimSpace(spec), " ")
	if fields != nil && !fields[name] {
		return unsupported(
			"Unsupported or invalid query filter clause specified for property '%s' of resource '%s'.",
			name,
			resource,
		)
	}

	q.order, q.desc = name, strings.EqualFold(strings.TrimSpace(dir), "desc")

	return nil
}

func (q *listQuery) parseSearch(r *http.Request, spec string) error {
	if spec == "" {
		return nil
	}

	if !eventual(r) {
		return unsupported(
			"Request with $search query parameter only works through MSGraph with a special request header: " +
				"'ConsistencyLevel: eventual'",
		)
	}

	spec = strings.Trim(spec, `"`)
	anyOf := strings.Contains(spec, " OR ")

	var clauses [][2]string

	for _, part := range splitSearch(spec) {
		prop, val, ok := strings.Cut(strings.Trim(part, `" `), ":")
		if !ok {
			return badRequest("Invalid $search expression; use \"property:value\".")
		}

		clauses = append(clauses, [2]string{prop, strings.ToLower(strings.Trim(val, `"`))})
	}

	q.search = func(o map[string]any) bool {
		for _, c := range clauses {
			s, _ := lookupPath(o, c[0]).(string)
			hit := strings.Contains(strings.ToLower(s), c[1])

			if hit == anyOf {
				return hit
			}
		}

		return !anyOf
	}

	return nil
}

func splitSearch(s string) []string {
	s = strings.ReplaceAll(s, " OR ", "\x00")
	s = strings.ReplaceAll(s, " AND ", "\x00")

	return strings.Split(s, "\x00")
}

// result is one page of a collection.
type result struct {
	next  string
	items []map[string]any
	total int
}

func (q *listQuery) run(items []map[string]any) result {
	if q.filter != nil || q.search != nil {
		kept := items[:0:0]

		for _, it := range items {
			if (q.filter == nil || q.filter.match(it, nil)) && (q.search == nil || q.search(it)) {
				kept = append(kept, it)
			}
		}

		items = kept
	}

	if q.order != "" {
		items = slices.Clone(items)
		sort.SliceStable(items, func(i, j int) bool {
			a, _ := lookupPath(items[i], q.order).(string)
			b, _ := lookupPath(items[j], q.order).(string)
			c := strings.Compare(strings.ToLower(a), strings.ToLower(b))

			if q.desc {
				return c > 0
			}

			return c < 0
		})
	}

	pg := page.New(items, q.token, q.top, defaultPageSize)

	out := make([]map[string]any, 0, len(pg.Data))
	for _, it := range pg.Data {
		out = append(out, q.project(it))
	}

	return result{items: out, next: pg.Next, total: len(items)}
}

func (q *listQuery) project(it map[string]any) map[string]any {
	if len(q.selects) == 0 {
		return it
	}

	out := map[string]any{}

	for _, k := range []string{"id", "@odata.type"} {
		if v, ok := it[k]; ok {
			out[k] = v
		}
	}

	for _, k := range q.selects {
		if v, ok := it[k]; ok {
			out[k] = v
		} else {
			out[k] = nil
		}
	}

	return out
}

// nextLink rebuilds the request URL with the continuation token, leaving $ in option names unescaped as Graph does.
func nextLink(r *http.Request, token string) string {
	vals := r.URL.Query()
	vals.Set("$skiptoken", token)

	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+url.QueryEscape(vals.Get(k)))
	}

	return fmt.Sprintf("https://%s%s?%s", r.Host, r.URL.Path, strings.Join(parts, "&"))
}
