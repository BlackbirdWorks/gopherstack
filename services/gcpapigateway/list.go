package gcpapigateway

import (
	"cmp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type listArgs struct {
	filter    string
	orderBy   string
	pageToken string
	pageSize  int32
}

type listKeys[T any] struct {
	name    func(T) string
	created func(T) time.Time
	updated func(T) time.Time
}

func paginate[T any](items []T, a listArgs, keys listKeys[T]) (page.Page[T], error) {
	if a.filter != "" {
		return page.Page[T]{}, errInvalid("The filter parameter is not supported.")
	}

	if a.pageSize < 0 {
		return page.Page[T]{}, errInvalid("page_size must not be negative.")
	}

	field, desc := "name", false

	if f := strings.Fields(a.orderBy); len(f) > 0 {
		field = f[0]

		switch {
		case len(f) == 1:
		case len(f) == 2 && strings.EqualFold(f[1], "desc"):
			desc = true
		case len(f) == 2 && strings.EqualFold(f[1], "asc"):
		default:
			return page.Page[T]{}, errInvalid("Invalid order_by %q.", a.orderBy)
		}
	}

	var less func(a, b T) int

	switch field {
	case "name":
		less = func(x, y T) int { return cmp.Compare(keys.name(x), keys.name(y)) }
	case "create_time":
		less = func(x, y T) int { return keys.created(x).Compare(keys.created(y)) }
	case "update_time":
		less = func(x, y T) int { return keys.updated(x).Compare(keys.updated(y)) }
	default:
		return page.Page[T]{}, errInvalid("Cannot order by %q: use name, create_time or update_time.", field)
	}

	slices.SortStableFunc(items, func(x, y T) int {
		c := less(x, y)
		if desc {
			return -c
		}

		return c
	})

	return page.New(items, a.pageToken, int(a.pageSize), defaultListPage), nil
}

func maskPaths(paths []string, defaults ...string) []string {
	if len(paths) == 0 {
		return defaults
	}

	return paths
}
