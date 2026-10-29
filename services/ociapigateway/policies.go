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
		h.Set("Access-Control-Allow-Origin", allowOriginValue(c, origin))

		methods := strings.Join(c.AllowedMethods, ", ")
		if methods == "" || slices.Contains(c.AllowedMethods, "*") {
			methods = r.Header.Get("Access-Control-Request-Method")
		}

		h.Set("Access-Control-Allow-Methods", methods)

		headers := strings.Join(c.AllowedHeaders, ", ")
		if headers == "" || slices.Contains(c.AllowedHeaders, "*") {
			headers = r.Header.Get("Access-Control-Request-Headers")
		}

		if headers != "" {
			h.Set("Access-Control-Allow-Headers", headers)
		}

		if credentialed(c) {
			h.Set("Access-Control-Allow-Credentials", "true")
		}

		if c.MaxAge != nil && *c.MaxAge > 0 {
			h.Set("Access-Control-Max-Age", strconv.Itoa(*c.MaxAge))
		}
	}

	w.WriteHeader(http.StatusOK)
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
			listed := hasName(f.Items, k)
			if (f.Type == "ALLOW" && !listed) || (f.Type == "BLOCK" && listed) {
				delete(h, k)
			}
		}
	}

	if t.RenameHeaders != nil {
		for _, it := range t.RenameHeaders.Items {
			if vs, ok := h[http.CanonicalHeaderKey(it.From)]; ok {
				delete(h, http.CanonicalHeaderKey(it.From))
				h[http.CanonicalHeaderKey(it.To)] = vs
			}
		}
	}

	if t.SetHeaders != nil {
		for _, it := range t.SetHeaders.Items {
			key := http.CanonicalHeaderKey(it.Name)
			_, exists := h[key]

			switch {
			case exists && it.IfExists == "SKIP":
				continue
			case exists && it.IfExists == "APPEND":
			default:
				delete(h, key)
			}

			for _, v := range it.Values {
				h.Add(key, rv.interpolate(v, nil))
			}
		}
	}
}

// transformQuery applies filter, then rename, then set to query parameters.
func transformQuery(q url.Values, t *transformDoc, rv *reqView) {
	if t == nil {
		return
	}

	if f := t.FilterQuery; f != nil {
		for k := range q {
			listed := slices.ContainsFunc(f.Items, func(i nameItem) bool { return i.Name == k })
			if (f.Type == "ALLOW" && !listed) || (f.Type == "BLOCK" && listed) {
				delete(q, k)
			}
		}
	}

	if t.RenameQuery != nil {
		for _, it := range t.RenameQuery.Items {
			if vs, ok := q[it.From]; ok {
				delete(q, it.From)
				q[it.To] = vs
			}
		}
	}

	if t.SetQuery != nil {
		for _, it := range t.SetQuery.Items {
			_, exists := q[it.Name]

			switch {
			case exists && it.IfExists == "SKIP":
				continue
			case exists && it.IfExists == "APPEND":
			default:
				q.Del(it.Name)
			}

			for _, v := range it.Values {
				q.Add(it.Name, rv.interpolate(v, nil))
			}
		}
	}
}
