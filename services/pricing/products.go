package pricing

import (
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	formatVersion   = "aws_v1"
	defaultPageSize = 100
	maxPageSize     = 100
)

// Filter is a GetProducts filter.
type Filter struct {
	Field string `json:"Field"`
	Type  string `json:"Type"`
	Value string `json:"Value"`
}

func entriesOf(code string) ([]*entry, error) {
	list, ok := theCatalog().byService[code]
	if !ok {
		return nil, fail(ErrNotFound, "service code %q not found", code)
	}

	return list, nil
}

func checkFormat(v string) error {
	if v != "" && v != formatVersion {
		return fail(ErrInvalidParameter, "FormatVersion %q is not supported; use %s", v, formatVersion)
	}

	return nil
}

func paginate[T any](all []T, token string, limit *int32) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil || page.DecodeToken(token) > len(all) {
		return page.Page[T]{}, fail(ErrInvalidNextToken, "invalid NextToken")
	}

	size := defaultPageSize
	if limit != nil {
		if *limit < 1 || int(*limit) > maxPageSize {
			return page.Page[T]{}, fail(ErrInvalidParameter, "MaxResults must be between 1 and %d", maxPageSize)
		}

		size = int(*limit)
	}

	return page.New(all, token, size, defaultPageSize), nil
}

// ServiceInfo describes one catalog service.
type ServiceInfo struct {
	ServiceCode    string   `json:"ServiceCode"`
	AttributeNames []string `json:"AttributeNames"`
}

func attributeNames(list []*entry) []string {
	seen := map[string]bool{}

	for _, e := range list {
		for k := range e.item.Product.Attributes {
			seen[k] = true
		}
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// DescribeServices lists catalog services, or the one named by code.
func DescribeServices(code, version, token string, limit *int32) ([]ServiceInfo, string, error) {
	if err := checkFormat(version); err != nil {
		return nil, "", err
	}

	codes := theCatalog().codes
	if code != "" {
		if _, err := entriesOf(code); err != nil {
			return nil, "", err
		}

		codes = []string{code}
	}

	infos := make([]ServiceInfo, len(codes))
	for i, c := range codes {
		infos[i] = ServiceInfo{ServiceCode: c, AttributeNames: attributeNames(theCatalog().byService[c])}
	}

	pg, err := paginate(infos, token, limit)

	return pg.Data, pg.Next, err
}

// AttributeValues lists the distinct values of an attribute.
func AttributeValues(code, attr, token string, limit *int32) ([]string, string, error) {
	list, err := entriesOf(code)
	if err != nil {
		return nil, "", err
	}

	if !slices.Contains(attributeNames(list), attr) {
		return nil, "", fail(ErrNotFound, "attribute %q not found for service %q", attr, code)
	}

	seen := map[string]bool{}

	for _, e := range list {
		if v, ok := e.item.Product.Attributes[attr]; ok {
			seen[v] = true
		}
	}

	vals := make([]string, 0, len(seen))
	for v := range seen {
		vals = append(vals, v)
	}

	sort.Strings(vals)

	pg, err := paginate(vals, token, limit)

	return pg.Data, pg.Next, err
}

func validateFilters(filters []Filter) error {
	for _, f := range filters {
		if f.Field == "" || f.Value == "" {
			return fail(ErrInvalidParameter, "a filter needs a Field and a Value")
		}

		switch f.Type {
		case "TERM_MATCH", "EQUALS", "CONTAINS", "ANY_OF", "NONE_OF":
		default:
			return fail(ErrInvalidParameter, "filter type %q is not valid", f.Type)
		}
	}

	return nil
}

func matches(e *entry, f Filter) bool {
	val, present := e.item.Product.Attributes[f.Field]
	if f.Field == "ServiceCode" {
		val, present = e.service, true
	}

	switch f.Type {
	case "CONTAINS":
		return present && strings.Contains(val, f.Value)
	case "ANY_OF":
		return present && slices.Contains(strings.Split(f.Value, ","), val)
	case "NONE_OF":
		return !present || !slices.Contains(strings.Split(f.Value, ","), val)
	default:
		return present && val == f.Value
	}
}

// Products returns the PriceList elements matching every filter.
func Products(code, version, token string, limit *int32, filters []Filter) ([]string, string, error) {
	if err := checkFormat(version); err != nil {
		return nil, "", err
	}

	list, err := entriesOf(code)
	if err != nil {
		return nil, "", err
	}

	if err = validateFilters(filters); err != nil {
		return nil, "", err
	}

	var out []string

	for _, e := range list {
		if !slices.ContainsFunc(filters, func(f Filter) bool { return !matches(e, f) }) {
			out = append(out, e.json)
		}
	}

	pg, err := paginate(out, token, limit)

	return pg.Data, pg.Next, err
}
