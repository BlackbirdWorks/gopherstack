package ociregistry

import "net/http"

// DefaultMaxRequestBytes bounds a single upload request body (one monolithic PUT or one PATCH chunk).
const DefaultMaxRequestBytes int64 = 8 << 30

// Limit rejects request bodies larger than maxBytes (DefaultMaxRequestBytes when zero) with a 413-style
// read error instead of streaming them into storage without bound.
func Limit(next http.Handler, maxBytes int64) http.Handler {
	if maxBytes <= 0 {
		maxBytes = DefaultMaxRequestBytes
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
		}

		next.ServeHTTP(w, r)
	})
}
