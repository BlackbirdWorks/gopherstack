package azurearm

import (
	"maps"
	"net/http"
	"net/url"
	"strconv"
)

const apimQueryTop, apimQuerySkip = "$top", "$skip"

// pageAPIMList applies $top and $skip to a child list and adds count and nextLink as the list operations document.
func (h *Handler) pageAPIMList(r *http.Request, method string, body any) (any, error) {
	m, ok := body.(map[string]any)
	if !ok || method != http.MethodGet {
		return body, nil
	}

	var items []any

	switch v := m[fieldValue].(type) {
	case []map[string]any:
		for _, e := range v {
			items = append(items, e)
		}
	case []any:
		items = v
	default:
		return body, nil
	}

	q := r.URL.Query()

	top, err := apimQueryInt(q, apimQueryTop, 1)
	if err != nil {
		return nil, err
	}

	skip, err := apimQueryInt(q, apimQuerySkip, 0)
	if err != nil {
		return nil, err
	}

	total := len(items)
	items = items[min(skip, total):]

	out := map[string]any{aksFieldCount: total}

	if top > 0 && len(items) > top {
		items = items[:top]
		next := url.Values{}

		maps.Copy(next, q)

		next.Set(apimQuerySkip, strconv.Itoa(skip+top))
		out["nextLink"] = h.baseURLFor(r) + r.URL.Path + "?" + next.Encode()
	}

	if items == nil {
		items = []any{}
	}

	out[fieldValue] = items

	return out, nil
}

func apimQueryInt(q url.Values, key string, minimum int) (int, error) {
	raw := q.Get(key)
	if raw == "" {
		return 0, nil
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < minimum {
		return 0, apimInvalid("The query parameter %s must be an integer of at least %d.", key, minimum)
	}

	return n, nil
}
