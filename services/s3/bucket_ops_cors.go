package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *S3Handler) putBucketCORS(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "PutBucketCors")
	body, err := httputils.ReadBody(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Validate the CORS XML is well-formed before storing it.
	var cfg CORSConfiguration
	if xmlErr := xml.Unmarshal(body, &cfg); xmlErr != nil {
		httputils.WriteS3ErrorResponse(ctx, w, r, ErrorResponse{
			Code:    errMalformedXML,
			Message: errMalformedXMLMsg,
		}, http.StatusBadRequest)

		return
	}

	err = h.Backend.PutBucketCORS(ctx, bucket, string(body))
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusOK)
}

func (h *S3Handler) getBucketCORS(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "GetBucketCors")
	corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(corsXML))
}

func (h *S3Handler) deleteBucketCORS(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "DeleteBucketCors")
	if err := h.Backend.DeleteBucketCORS(ctx, bucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// applyCORSActualResponseHeaders sets Access-Control-Allow-Origin (and
// Access-Control-Expose-Headers, when the matched rule declares any) on the
// actual response -- the GET/PUT/etc. that follows a preflight. Without this,
// preflight can pass and the browser still blocks the real response, because
// a browser's CORS check on the actual response only looks at headers on
// that response, not the earlier OPTIONS. No-ops silently when there's no
// Origin header, no CORS config, or no matching rule, so requests without
// CORS involved are unaffected.
func (h *S3Handler) applyCORSActualResponseHeaders(
	ctx context.Context, w http.ResponseWriter, r *http.Request, bucket string,
) {
	origin := r.Header.Get("Origin")
	if origin == "" || r.Method == http.MethodOptions {
		return
	}

	corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
	if err != nil {
		return
	}

	var cfg CORSConfiguration
	if xml.Unmarshal([]byte(corsXML), &cfg) != nil {
		return
	}

	rule := matchCORSRule(cfg.Rules, origin, r.Method, "")
	if rule == nil {
		return
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	if len(rule.ExposeHeaders) > 0 {
		w.Header().Set("Access-Control-Expose-Headers", strings.Join(rule.ExposeHeaders, ", "))
	}
}

func (h *S3Handler) handleCORSPreflight(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket string,
) {
	h.setOperation(ctx, "CORSPreflight")
	corsXML, err := h.Backend.GetBucketCORS(ctx, bucket)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	var cfg CORSConfiguration
	if unmarshalErr := xml.Unmarshal([]byte(corsXML), &cfg); unmarshalErr != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	origin := r.Header.Get("Origin")
	method := r.Header.Get("Access-Control-Request-Method")

	// Reject structurally invalid preflights: Origin and Access-Control-Request-Method
	// are required for a well-formed CORS preflight request.
	if origin == "" || method == "" {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	reqHeaders := r.Header.Get("Access-Control-Request-Headers")

	rule := matchCORSRule(cfg.Rules, origin, method, reqHeaders)
	if rule == nil {
		w.WriteHeader(http.StatusForbidden)

		return
	}

	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Methods", method)

	if reqHeaders != "" {
		w.Header().Set("Access-Control-Allow-Headers", reqHeaders)
	}

	if rule.MaxAgeSeconds > 0 {
		w.Header().Set("Access-Control-Max-Age", strconv.Itoa(rule.MaxAgeSeconds))
	} else {
		w.Header().Set("Access-Control-Max-Age", "3000")
	}

	w.WriteHeader(http.StatusOK)
}

// matchCORSRule returns the first CORSRule whose AllowedOrigins, AllowedMethods,
// and AllowedHeaders all match the supplied preflight parameters.
// Returns nil when no rule matches.
func matchCORSRule(rules []CORSRule, origin, method, reqHeaders string) *CORSRule {
	for i := range rules {
		rule := &rules[i]

		if !corsOriginMatches(rule.AllowedOrigins, origin) {
			continue
		}

		if !corsMethodMatches(rule.AllowedMethods, method) {
			continue
		}

		if !corsHeadersMatch(rule.AllowedHeaders, reqHeaders) {
			continue
		}

		return rule
	}

	return nil
}

// corsOriginMatches returns true when origin matches one of the allowedOrigins.
// A wildcard entry "*" matches any origin. AWS also allows a single embedded
// wildcard within an otherwise-literal origin (e.g. "https://*.example.com");
// corsOriginWildcardMatches handles that case without widening the bare "*"
// match-everything behaviour above.
func corsOriginMatches(allowedOrigins []string, origin string) bool {
	for _, allowed := range allowedOrigins {
		if allowed == "*" || strings.EqualFold(allowed, origin) {
			return true
		}

		if corsOriginWildcardMatches(allowed, origin) {
			return true
		}
	}

	return false
}

// corsOriginWildcardMatches implements AWS's single-embedded-wildcard
// AllowedOrigin form: exactly one '*' within an otherwise-literal origin,
// matching zero or more characters at that position. An AllowedOrigin with
// zero or more than one '*' is not eligible here (falls back to exact/bare-*
// matching in corsOriginMatches), so a malformed entry fails closed rather
// than matching too widely.
func corsOriginWildcardMatches(allowed, origin string) bool {
	if strings.Count(allowed, "*") != 1 {
		return false
	}

	idx := strings.Index(allowed, "*")
	prefix, suffix := allowed[:idx], allowed[idx+1:]

	return len(origin) >= len(prefix)+len(suffix) &&
		strings.EqualFold(origin[:len(prefix)], prefix) &&
		strings.EqualFold(origin[len(origin)-len(suffix):], suffix)
}

// corsMethodMatches returns true when method is found in allowedMethods.
func corsMethodMatches(allowedMethods []string, method string) bool {
	for _, allowed := range allowedMethods {
		if strings.EqualFold(allowed, method) {
			return true
		}
	}

	return false
}

// corsHeadersMatch returns true when every header listed in reqHeaders (comma-separated)
// is covered by allowedHeaders. A wildcard entry "*" covers any header.
// An empty reqHeaders string is always allowed.
func corsHeadersMatch(allowedHeaders []string, reqHeaders string) bool {
	if reqHeaders == "" {
		return true
	}

	for rh := range strings.SplitSeq(reqHeaders, ",") {
		rh = strings.TrimSpace(rh)
		if rh == "" {
			continue
		}

		matched := false

		for _, ah := range allowedHeaders {
			if ah == "*" || strings.EqualFold(ah, rh) {
				matched = true

				break
			}
		}

		if !matched {
			return false
		}
	}

	return true
}
