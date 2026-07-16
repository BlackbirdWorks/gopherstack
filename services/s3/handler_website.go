package s3

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	s3SDK "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
)

// ServeWebsite serves a static file from an S3 bucket configured for website hosting.
// It is invoked by the GET /_gopherstack/website/{bucket}/{key+} route registered in cli.go.
// The bucket must have a website configuration stored via PutBucketWebsite.
func (h *S3Handler) ServeWebsite(c *echo.Context) error {
	ctx := c.Request().Context()
	bucket := c.Param("bucket")
	key := strings.TrimPrefix(c.Param("key"), "/")

	websiteXML, err := h.Backend.GetBucketWebsite(ctx, bucket)
	if err != nil {
		if errors.Is(err, ErrNoSuchBucket) {
			return c.JSON(http.StatusNotFound, map[string]string{
				xmlElemCode:    "NoSuchBucket",
				xmlElemMessage: "The specified bucket does not exist",
			})
		}

		return c.JSON(http.StatusNotFound, map[string]string{
			xmlElemCode:    "NoSuchWebsiteConfiguration",
			xmlElemMessage: "The specified bucket does not have a website configuration",
		})
	}

	var cfg WebsiteConfiguration
	if xmlErr := xml.Unmarshal([]byte(websiteXML), &cfg); xmlErr != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"Code":    errCodeInternalError,
			"Message": "Failed to parse website configuration",
		})
	}

	if cfg.RedirectAllRequestsTo != nil {
		return c.Redirect(
			http.StatusMovedPermanently,
			websiteRedirectAllURL(cfg.RedirectAllRequestsTo, key),
		)
	}

	if loc, code, ok := applyWebsiteRoutingRules(cfg.RoutingRules, key, c.Request().Host); ok {
		return c.Redirect(code, loc)
	}

	effectiveKey := websiteEffectiveKey(key, cfg.IndexDocument)

	out, getErr := h.Backend.GetObject(ctx, &s3SDK.GetObjectInput{
		Bucket: &bucket,
		Key:    &effectiveKey,
	})
	if getErr == nil {
		return serveWebsiteObject(c, out, http.StatusOK)
	}

	if cfg.ErrorDocument != nil && cfg.ErrorDocument.Key != "" {
		errOut, errDocErr := h.Backend.GetObject(ctx, &s3SDK.GetObjectInput{
			Bucket: &bucket,
			Key:    &cfg.ErrorDocument.Key,
		})
		if errDocErr == nil {
			return serveWebsiteObject(c, errOut, http.StatusNotFound)
		}
	}

	return c.JSON(http.StatusNotFound, map[string]string{
		"Code":    errNoSuchKey,
		"Message": "The specified key does not exist",
	})
}

// websiteRedirectAllURL builds a redirect URL when RedirectAllRequestsTo is set.
func websiteRedirectAllURL(redir *WebsiteRedirectAll, key string) string {
	protocol := redir.Protocol
	if protocol == "" {
		protocol = "http"
	}

	return protocol + "://" + redir.HostName + "/" + key
}

// applyWebsiteRoutingRules evaluates routing rules against the given key and host.
// Returns the redirect location, HTTP status code, and true if a rule matched.
func applyWebsiteRoutingRules(rules []WebsiteRoutingRule, key, reqHost string) (string, int, bool) {
	for _, rule := range rules {
		cond := rule.Condition
		if cond != nil && cond.KeyPrefixEquals != "" &&
			!strings.HasPrefix(key, cond.KeyPrefixEquals) {
			continue
		}

		redir := rule.Redirect
		if !websiteRuleHasRedirect(redir) {
			continue
		}

		code := websiteRedirectCode(redir.HTTPRedirectCode)
		targetKey := websiteTargetKey(redir, cond, key)
		protocol := redir.Protocol
		if protocol == "" {
			protocol = "http"
		}

		host := redir.HostName
		if host == "" {
			host = reqHost
		}

		return protocol + "://" + host + "/" + targetKey, code, true
	}

	return "", 0, false
}

// websiteRuleHasRedirect reports whether a routing rule redirect spec is non-empty.
func websiteRuleHasRedirect(r WebsiteRoutingRuleRedirect) bool {
	return r.HostName != "" || r.Protocol != "" || r.ReplaceKeyWith != "" ||
		r.ReplaceKeyPrefixWith != ""
}

// websiteRedirectCode converts an HTTP redirect code string to an int status code.
func websiteRedirectCode(code string) int {
	if code == "301" {
		return http.StatusMovedPermanently
	}

	return http.StatusFound
}

// websiteTargetKey computes the effective target key for a routing rule redirect.
func websiteTargetKey(
	redir WebsiteRoutingRuleRedirect,
	cond *WebsiteRoutingRuleCondition,
	key string,
) string {
	if redir.ReplaceKeyWith != "" {
		return redir.ReplaceKeyWith
	}

	if redir.ReplaceKeyPrefixWith != "" && cond != nil && cond.KeyPrefixEquals != "" {
		return redir.ReplaceKeyPrefixWith + strings.TrimPrefix(key, cond.KeyPrefixEquals)
	}

	return key
}

// websiteEffectiveKey resolves the effective object key for a website request,
// appending the index document suffix when the key refers to a directory.
func websiteEffectiveKey(key string, indexDoc *WebsiteIndexDocument) string {
	if indexDoc == nil || indexDoc.Suffix == "" {
		return key
	}

	if key == "" || strings.HasSuffix(key, "/") {
		base := strings.TrimSuffix(key, "/")
		if base != "" {
			return base + "/" + indexDoc.Suffix
		}

		return indexDoc.Suffix
	}

	return key
}

// serveWebsiteObject writes an S3 GetObjectOutput to the Echo response.
func serveWebsiteObject(c *echo.Context, out *s3SDK.GetObjectOutput, statusCode int) error {
	defer out.Body.Close()

	contentType := "application/octet-stream"
	if out.ContentType != nil {
		contentType = *out.ContentType
	}

	c.Response().Header().Set("Content-Type", contentType)
	c.Response().WriteHeader(statusCode)
	_, _ = io.Copy(c.Response(), out.Body)

	return nil
}
