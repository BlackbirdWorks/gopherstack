package azureauth

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// StringToSign builds the SharedKey string-to-sign for r, canonicalized
// against the given storage account name, per Microsoft's "Authorize with
// Shared Key" REST reference:
//
//	VERB + "\n" +
//	Content-Encoding + "\n" +
//	Content-Language + "\n" +
//	Content-Length + "\n" +
//	Content-MD5 + "\n" +
//	Content-Type + "\n" +
//	Date + "\n" +
//	If-Modified-Since + "\n" +
//	If-Match + "\n" +
//	If-None-Match + "\n" +
//	If-Unmodified-Since + "\n" +
//	Range + "\n" +
//	CanonicalizedHeaders +
//	CanonicalizedResource
func StringToSign(r *http.Request, account string) string {
	var b strings.Builder

	b.WriteString(r.Method)
	b.WriteByte('\n')
	writeHeaderLine(&b, r, "Content-Encoding")
	writeHeaderLine(&b, r, "Content-Language")
	b.WriteString(contentLengthField(r))
	b.WriteByte('\n')
	writeHeaderLine(&b, r, "Content-MD5")
	writeHeaderLine(&b, r, "Content-Type")
	writeHeaderLine(&b, r, "Date")
	writeHeaderLine(&b, r, "If-Modified-Since")
	writeHeaderLine(&b, r, "If-Match")
	writeHeaderLine(&b, r, "If-None-Match")
	writeHeaderLine(&b, r, "If-Unmodified-Since")
	writeHeaderLine(&b, r, "Range")
	b.WriteString(CanonicalizedHeaders(r))
	b.WriteString(CanonicalizedResource(account, r.URL))

	return b.String()
}

// StringToSignLite builds the SharedKeyLite string-to-sign for r:
//
//	VERB + "\n" +
//	Content-MD5 + "\n" +
//	Content-Type + "\n" +
//	Date + "\n" +
//	CanonicalizedHeaders +
//	CanonicalizedResource
func StringToSignLite(r *http.Request, account string) string {
	var b strings.Builder

	b.WriteString(r.Method)
	b.WriteByte('\n')
	writeHeaderLine(&b, r, "Content-MD5")
	writeHeaderLine(&b, r, "Content-Type")
	writeHeaderLine(&b, r, "Date")
	b.WriteString(CanonicalizedHeaders(r))
	b.WriteString(CanonicalizedResource(account, r.URL))

	return b.String()
}

// writeHeaderLine appends "<value>\n" for the named header to b.
func writeHeaderLine(b *strings.Builder, r *http.Request, name string) {
	b.WriteString(r.Header.Get(name))
	b.WriteByte('\n')
}

// contentLengthField returns the Content-Length string-to-sign field: the
// decimal length, or the empty string when the length is zero or unset (per
// the Shared Key spec, which treats a zero Content-Length as blank).
func contentLengthField(r *http.Request) string {
	if r.ContentLength <= 0 {
		return ""
	}

	return strconv.FormatInt(r.ContentLength, 10)
}

// CanonicalizedHeaders returns the canonicalized x-ms-* header block: every
// x-ms-* header, lowercased and sorted lexicographically by name, each
// rendered as "name:value\n" with whitespace trimmed and internal whitespace
// runs collapsed to a single space. Multiple values for the same header are
// comma-joined in the order net/http returns them.
func CanonicalizedHeaders(r *http.Request) string {
	names := make([]string, 0, len(r.Header))
	seen := make(map[string]struct{}, len(r.Header))

	for k := range r.Header {
		lk := strings.ToLower(k)
		if !strings.HasPrefix(lk, "x-ms-") {
			continue
		}
		if _, ok := seen[lk]; ok {
			continue
		}
		seen[lk] = struct{}{}
		names = append(names, lk)
	}

	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		// http.Header.Values returns the live slice backing r.Header, not a
		// copy -- writing into it in place would silently rewrite the
		// caller's request headers as a side effect of computing a
		// signature. Copy into a fresh slice before normalizing.
		vals := r.Header.Values(http.CanonicalHeaderKey(name))
		normalized := make([]string, len(vals))

		for i, v := range vals {
			normalized[i] = collapseWhitespace(v)
		}

		b.WriteString(name)
		b.WriteByte(':')
		b.WriteString(strings.Join(normalized, ","))
		b.WriteByte('\n')
	}

	return b.String()
}

// stripAccountPathSegment removes a leading "/<account>" path segment from
// path, if present as a full segment (i.e. followed by "/" or end of
// string). A path that merely starts with the account name as a substring
// of a longer segment (e.g. account "acct" against path "/acctfoo") is left
// untouched.
func stripAccountPathSegment(path, account string) string {
	prefix := "/" + account
	if path == prefix {
		return ""
	}
	if rest, ok := strings.CutPrefix(path, prefix+"/"); ok {
		return "/" + rest
	}

	return path
}

// collapseWhitespace trims leading/trailing whitespace and collapses any
// internal run of whitespace to a single space.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// CanonicalizedResource returns the canonicalized resource string for a
// request against account: "/<account><path>" followed by each query
// parameter, lowercased and sorted by name, rendered as "\nname:value" with
// repeated-parameter values comma-joined after sorting.
//
// gopherstack, like Azurite, serves path-style requests whose URL already
// starts with "/<account>/..." (real Azure serves the account as a
// subdomain instead, so the URL path never contains it). Real SDKs know
// which style they're signing for and build the canonicalized resource as
// "/<account>" plus the resource path with any such account segment
// removed, so that a path-style request and an equivalent subdomain-style
// request sign identically; this strips a leading "/<account>" path segment
// before applying that formula so signatures computed here match what an
// Azurite-targeting SDK actually sends.
func CanonicalizedResource(account string, u *url.URL) string {
	var b strings.Builder

	b.WriteByte('/')
	b.WriteString(account)
	b.WriteString(stripAccountPathSegment(u.EscapedPath(), account))

	query := u.Query()
	lowered := make(map[string][]string, len(query))
	for k, vals := range query {
		lk := strings.ToLower(k)
		lowered[lk] = append(lowered[lk], vals...)
	}

	keys := make([]string, 0, len(lowered))
	for k := range lowered {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		vals := lowered[k]
		sort.Strings(vals)
		b.WriteByte('\n')
		b.WriteString(k)
		b.WriteByte(':')
		b.WriteString(strings.Join(vals, ","))
	}

	return b.String()
}
