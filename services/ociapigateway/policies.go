package ociapigateway

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

func originAllowed(c *corsDoc, origin string) bool {
	return slices.Contains(c.AllowedOrigins, "*") || slices.Contains(c.AllowedOrigins, origin)
}

func credentialed(c *corsDoc) bool { return c.AllowCredentials != nil && *c.AllowCredentials }

func allowOriginValue(c *corsDoc, origin string) string {
	if slices.Contains(c.AllowedOrigins, "*") && !credentialed(c) {
		return "*"
	}

	return origin
}

// preflight answers a CORS preflight; an origin outside the policy gets no CORS headers.
func preflight(w http.ResponseWriter, r *http.Request, c *corsDoc) {
	origin := r.Header.Get("Origin")
	h := w.Header()
	h.Add("Vary", "Origin")

	if originAllowed(c, origin) {
		preflightHeaders(h, r, c, origin)
	}

	w.WriteHeader(http.StatusOK)
}

func preflightHeaders(h http.Header, r *http.Request, c *corsDoc, origin string) {
	h.Set("Access-Control-Allow-Origin", allowOriginValue(c, origin))
	h.Set("Access-Control-Allow-Methods", listOrEcho(c.AllowedMethods, r.Header.Get("Access-Control-Request-Method")))

	if headers := listOrEcho(c.AllowedHeaders, r.Header.Get("Access-Control-Request-Headers")); headers != "" {
		h.Set("Access-Control-Allow-Headers", headers)
	}

	if credentialed(c) {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if c.MaxAge != nil && *c.MaxAge > 0 {
		h.Set("Access-Control-Max-Age", strconv.Itoa(*c.MaxAge))
	}
}

// listOrEcho joins the configured list, or echoes the requested value for an empty list or '*'.
func listOrEcho(list []string, requested string) string {
	if len(list) == 0 || slices.Contains(list, "*") {
		return requested
	}

	return strings.Join(list, ", ")
}

// corsHeaders decorates an actual (non-preflight) response.
func corsHeaders(h http.Header, c *corsDoc, origin string) {
	if c == nil || origin == "" {
		return
	}

	h.Add("Vary", "Origin")

	if !originAllowed(c, origin) {
		return
	}

	h.Set("Access-Control-Allow-Origin", allowOriginValue(c, origin))

	if credentialed(c) {
		h.Set("Access-Control-Allow-Credentials", "true")
	}

	if len(c.ExposedHeaders) > 0 {
		h.Set("Access-Control-Expose-Headers", strings.Join(c.ExposedHeaders, ", "))
	}
}

func hasName(items []nameItem, name string) bool {
	return slices.ContainsFunc(items, func(i nameItem) bool { return strings.EqualFold(i.Name, name) })
}

// transformHeaders applies filter, then rename, then set.
func transformHeaders(h http.Header, t *transformDoc, rv *reqView) {
	if t == nil {
		return
	}

	if f := t.FilterHeaders; f != nil {
		for k := range h {
			if filtered(f.Type, hasName(f.Items, k)) {
				delete(h, k)
			}
		}
	}

	if t.RenameHeaders != nil {
		renameKeys(h, t.RenameHeaders.Items, http.CanonicalHeaderKey)
	}

	if t.SetHeaders != nil {
		setValues(h, t.SetHeaders.Items, http.CanonicalHeaderKey, rv, h.Add)
	}
}

// renameKeys moves values from each item's From key to its To key.
func renameKeys(m map[string][]string, items []renameItem, norm func(string) string) {
	for _, it := range items {
		from := norm(it.From)
		if vs, ok := m[from]; ok {
			delete(m, from)
			m[norm(it.To)] = vs
		}
	}
}

// setValues applies set items honouring ifExists; add appends one value to a key.
func setValues(
	m map[string][]string, items []setItem, norm func(string) string, rv *reqView, add func(k, v string),
) {
	for _, it := range items {
		key := norm(it.Name)
		if !prepareSet(m, key, it.IfExists) {
			continue
		}

		for _, v := range it.Values {
			add(key, rv.interpolate(v, nil))
		}
	}
}

// filtered reports whether an ALLOW/BLOCK filter removes an entry.
func filtered(kind string, listed bool) bool {
	return (kind == "ALLOW" && !listed) || (kind == "BLOCK" && listed)
}

// prepareSet applies the ifExists mode to an existing key and reports whether new values should be added.
func prepareSet(h map[string][]string, key, ifExists string) bool {
	if _, exists := h[key]; exists {
		switch ifExists {
		case "SKIP":
			return false
		case "APPEND":
			return true
		}

		delete(h, key)
	}

	return true
}

// transformQuery applies filter, then rename, then set to query parameters.
func transformQuery(q url.Values, t *transformDoc, rv *reqView) {
	if t == nil {
		return
	}

	if f := t.FilterQuery; f != nil {
		for k := range q {
			if filtered(f.Type, slices.ContainsFunc(f.Items, func(i nameItem) bool { return i.Name == k })) {
				delete(q, k)
			}
		}
	}

	if t.RenameQuery != nil {
		renameKeys(q, t.RenameQuery.Items, identity)
	}

	if t.SetQuery != nil {
		setValues(q, t.SetQuery.Items, identity, rv, q.Add)
	}
}

func identity(s string) string { return s }
