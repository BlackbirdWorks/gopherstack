package sagemakerruntime

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
)

const (
	headerRequestTTL         = "X-Amzn-Sagemaker-Requestttlseconds"
	headerInvocationTimeout  = "X-Amzn-Sagemaker-Invocationtimeoutseconds"
	maxCustomAttributesLen   = 1024
	maxInferenceIDLen        = 64
	maxInputLocationLen      = 1024
	maxSessionIDLen          = 256
	maxTargetVariantLen      = 63
	minRequestTTLSeconds     = 60
	maxRequestTTLSeconds     = 21600
	minInvocationTimeoutSecs = 1
	maxInvocationTimeoutSecs = 3600
)

var (
	inputLocationPattern = regexp.MustCompile(`^(https|s3)://([^/]+)/?(.*)$`)
	sessionIDPattern     = regexp.MustCompile(`^(NEW_SESSION)$|^[a-zA-Z0-9](-*[a-zA-Z0-9])*$`)
	variantNamePattern   = regexp.MustCompile(`^[a-zA-Z0-9](-*[a-zA-Z0-9])*`)
)

// validateRequestHeaders applies the botocore sagemaker-runtime length, range and
// pattern constraints to header-bound members; "" means valid.
func validateRequestHeaders(h http.Header) string {
	if v := h.Get(headerCustomAttributes); len(v) > maxCustomAttributesLen {
		return fmt.Sprintf("CustomAttributes must not exceed %d characters.", maxCustomAttributesLen)
	}

	if v := h.Get(headerInferenceID); len(v) > maxInferenceIDLen {
		return fmt.Sprintf("InferenceId must be between 1 and %d characters.", maxInferenceIDLen)
	}

	if v := h.Get(
		headerInputLocation,
	); v != "" &&
		(len(v) > maxInputLocationLen || !inputLocationPattern.MatchString(v)) {
		return "InputLocation must be an s3:// or https:// URI."
	}

	if v := h.Get(headerSessionID); v != "" && (len(v) > maxSessionIDLen || !sessionIDPattern.MatchString(v)) {
		return "SessionId must be NEW_SESSION or an alphanumeric identifier."
	}

	if v := h.Get(
		headerTargetVariant,
	); v != "" &&
		(len(v) > maxTargetVariantLen || !variantNamePattern.MatchString(v)) {
		return "TargetVariant is invalid."
	}

	if msg := validateIntHeader(
		h,
		headerRequestTTL,
		"RequestTTLSeconds",
		minRequestTTLSeconds,
		maxRequestTTLSeconds,
	); msg != "" {
		return msg
	}

	return validateIntHeader(h, headerInvocationTimeout, "InvocationTimeoutSeconds",
		minInvocationTimeoutSecs, maxInvocationTimeoutSecs)
}

func validateIntHeader(h http.Header, header, member string, lo, hi int) string {
	raw := h.Get(header)
	if raw == "" {
		return ""
	}

	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		return fmt.Sprintf("%s must be between %d and %d.", member, lo, hi)
	}

	return ""
}
