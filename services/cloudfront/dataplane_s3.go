package cloudfront

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	allUsersURI      = "http://acs.amazonaws.com/groups/global/AllUsers"
	maxS3OriginBytes = 64 << 20
)

var s3HostPattern = regexp.MustCompile(`^(.+?)\.s3[.-]`)

type originResp struct {
	header http.Header
	xcache string
	body   []byte
	status int
}

func isS3Origin(o *dpOrigin) bool {
	if o.S3 != nil {
		return true
	}

	return o.Custom == nil && s3HostPattern.MatchString(strings.ToLower(o.Domain))
}

func isS3WebsiteOrigin(o *dpOrigin) bool {
	return o.Custom != nil && strings.Contains(strings.ToLower(o.Domain), ".s3-website")
}

func bucketFromDomain(domain string) string {
	m := s3HostPattern.FindStringSubmatch(strings.ToLower(domain))
	if m == nil {
		return ""
	}

	return m[1]
}

func s3ErrorXML(code, msg string) []byte {
	return []byte(`<?xml version="1.0" encoding="UTF-8"?>` + "\n" +
		`<Error><Code>` + code + `</Code><Message>` + msg + `</Message></Error>`)
}

func s3Error(status int, code, msg string) originResp {
	return originResp{
		status: status,
		header: http.Header{headerContentType: {"application/xml"}},
		body:   s3ErrorXML(code, msg),
	}
}

func (h *Handler) s3Backend() s3backend.StorageBackend {
	f := h.dp.s3.Load()
	if f == nil {
		return nil
	}

	return (*f)()
}

// callerFor derives the identity CloudFront presents to the S3 origin from the origin's OAC/OAI.
func (h *Handler) callerFor(o *dpOrigin, d *Distribution) s3Caller {
	c := s3Caller{kind: accessAnonymous, sourceARN: d.ARN, account: h.Backend.accountID}

	if o.OACID != "" {
		if oac, err := h.Backend.GetOriginAccessControl(o.OACID); err == nil &&
			!strings.EqualFold(oac.SigningBehavior, "never") {
			c.kind = accessService

			return c
		}
	}

	if o.S3 != nil && o.S3.OAI != "" {
		id := o.S3.OAI[strings.LastIndex(o.S3.OAI, "/")+1:]
		if oai, err := h.Backend.GetOAI(id); err == nil {
			c.kind = accessOAI
			c.oaiID = oai.ID
			c.canonicalID = oai.S3CanonicalUserID
		}
	}

	return c
}

type s3Gate struct {
	be     s3backend.StorageBackend
	policy string
	caller s3Caller
	bucket string
	pab    s3backend.PublicAccessBlockConfiguration
}

func newS3Gate(ctx context.Context, be s3backend.StorageBackend, bucket string, c s3Caller) (*s3Gate, error) {
	if _, err := be.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(bucket)}); err != nil {
		return nil, err
	}

	g := &s3Gate{be: be, caller: c, bucket: bucket}
	g.policy, _ = be.GetBucketPolicy(ctx, bucket)

	if x, err := be.GetPublicAccessBlock(ctx, bucket); err == nil && x != "" {
		_ = xml.Unmarshal([]byte(x), &g.pab)
	}

	return g, nil
}

func (g *s3Gate) policyEffect(action, resource string) policyEffectKind {
	if g.policy == "" {
		return effectNone
	}

	if g.caller.kind == accessAnonymous && g.pab.RestrictPublicBuckets {
		if e := evaluatePolicy(g.policy, g.caller, action, resource); e == effectDeny {
			return e
		}

		return effectNone
	}

	return evaluatePolicy(g.policy, g.caller, action, resource)
}

func (g *s3Gate) objectACLPublic(ctx context.Context, key string) bool {
	if g.pab.IgnorePublicAcls {
		return false
	}

	acl, err := g.be.GetObjectACL(ctx, g.bucket, key, "")
	if err != nil || !strings.Contains(acl, allUsersURI) {
		return false
	}

	return strings.Contains(acl, "<Permission>READ</Permission>") ||
		strings.Contains(acl, "<Permission>FULL_CONTROL</Permission>")
}

func (g *s3Gate) canRead(ctx context.Context, key string) bool {
	res := arn.BuildS3(g.bucket + "/" + key)
	if g.policyEffect(actionS3GetObject, res) == effectDeny {
		return false
	}

	if g.policyEffect(actionS3GetObject, res) == effectAllow {
		return true
	}

	return g.caller.kind == accessAnonymous && g.objectACLPublic(ctx, key)
}

func (g *s3Gate) canList() bool {
	res := arn.BuildS3(g.bucket)

	return g.policyEffect(actionS3ListBucket, res) == effectAllow
}

func s3Key(o *dpOrigin, reqPath string) string {
	return strings.TrimPrefix(strings.TrimSuffix(o.Path, "/")+reqPath, "/")
}

// fetchS3 reads the object in-process, applying the bucket policy as the origin's CloudFront identity.
func (h *Handler) fetchS3(ctx context.Context, o *dpOrigin, d *Distribution, method, reqPath string) originResp {
	be := h.s3Backend()
	if be == nil {
		return s3Error(http.StatusBadGateway, "OriginUnavailable", "S3 service is not available")
	}

	if method != http.MethodGet && method != http.MethodHead {
		return s3Error(http.StatusForbidden, "AccessDenied", "Access Denied")
	}

	bucket := bucketFromDomain(o.Domain)

	gate, err := newS3Gate(ctx, be, bucket, h.callerFor(o, d))
	if err != nil {
		return s3Error(http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	}

	return readS3Object(ctx, gate, s3Key(o, reqPath))
}

func readS3Object(ctx context.Context, g *s3Gate, key string) originResp {
	if key == "" {
		return g.missing()
	}

	if !g.canRead(ctx, key) {
		if g.canList() && g.objectMissing(ctx, key) {
			return g.missing()
		}

		return s3Error(http.StatusForbidden, "AccessDenied", "Access Denied")
	}

	out, err := g.be.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(g.bucket), Key: aws.String(key)})
	if err != nil {
		if errors.Is(err, s3backend.ErrNoSuchKey) {
			return g.missing()
		}

		return s3Error(http.StatusBadGateway, "InternalError", err.Error())
	}
	defer out.Body.Close()

	body, err := io.ReadAll(io.LimitReader(out.Body, maxS3OriginBytes))
	if err != nil {
		return s3Error(http.StatusBadGateway, "InternalError", err.Error())
	}

	return originResp{status: http.StatusOK, header: s3ResponseHeaders(out, len(body)), body: body}
}

func (g *s3Gate) objectMissing(ctx context.Context, key string) bool {
	_, err := g.be.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: aws.String(g.bucket), Key: aws.String(key)})

	return err != nil
}

// missing mirrors S3: NoSuchKey needs s3:ListBucket, otherwise the miss is reported as AccessDenied.
func (g *s3Gate) missing() originResp {
	if g.canList() {
		return s3Error(http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
	}

	return s3Error(http.StatusForbidden, "AccessDenied", "Access Denied")
}

func s3ResponseHeaders(out *awss3.GetObjectOutput, n int) http.Header {
	h := http.Header{}
	set := func(k string, v *string) {
		if v != nil && *v != "" {
			h.Set(k, *v)
		}
	}

	set(headerContentType, out.ContentType)
	set("ETag", out.ETag)
	set("Cache-Control", out.CacheControl)
	set("Content-Encoding", out.ContentEncoding)
	set("Content-Disposition", out.ContentDisposition)
	set("Content-Language", out.ContentLanguage)
	set("X-Amz-Version-Id", out.VersionId)

	if out.LastModified != nil {
		h.Set("Last-Modified", out.LastModified.UTC().Format(http.TimeFormat))
	}

	set("Expires", out.ExpiresString)

	for k, v := range out.Metadata {
		h.Set("X-Amz-Meta-"+k, v)
	}

	if h.Get(headerContentType) == "" {
		h.Set(headerContentType, "binary/octet-stream")
	}

	h.Set("Content-Length", strconv.Itoa(n))
	h.Set("Accept-Ranges", "bytes")

	return h
}

type websiteConfig struct {
	Index string `xml:"IndexDocument>Suffix"`
	Error string `xml:"ErrorDocument>Key"`
}

// fetchS3Website serves a bucket through its static-website endpoint semantics (public reads only).
func (h *Handler) fetchS3Website(ctx context.Context, o *dpOrigin, d *Distribution, method, reqPath string) originResp {
	be := h.s3Backend()
	if be == nil {
		return s3Error(http.StatusBadGateway, "OriginUnavailable", "S3 service is not available")
	}

	if method != http.MethodGet && method != http.MethodHead {
		return websitePage(http.StatusMethodNotAllowed, "405 Method Not Allowed")
	}

	bucket := bucketFromDomain(o.Domain)

	gate, err := newS3Gate(ctx, be, bucket, s3Caller{kind: accessAnonymous, sourceARN: d.ARN})
	if err != nil {
		return websitePage(http.StatusNotFound, "404 Not Found: NoSuchBucket")
	}

	raw, err := be.GetBucketWebsite(ctx, bucket)
	if err != nil {
		return websitePage(http.StatusNotFound, "404 Not Found: NoSuchWebsiteConfiguration")
	}

	var cfg websiteConfig
	_ = xml.Unmarshal([]byte(raw), &cfg)

	key := s3Key(o, reqPath)
	if (key == "" || strings.HasSuffix(key, "/")) && cfg.Index != "" {
		key += cfg.Index
	}

	return serveWebsiteKey(ctx, gate, &cfg, key)
}

func serveWebsiteKey(ctx context.Context, g *s3Gate, cfg *websiteConfig, key string) originResp {
	if key != "" {
		if g.canRead(ctx, key) {
			if r := readS3Object(ctx, g, key); r.status == http.StatusOK {
				return r
			}
		} else if !g.objectMissing(ctx, key) {
			return websitePage(http.StatusForbidden, "403 Forbidden")
		}
	}

	if cfg.Error != "" && g.canRead(ctx, cfg.Error) {
		if r := readS3Object(ctx, g, cfg.Error); r.status == http.StatusOK {
			r.status = http.StatusNotFound

			return r
		}
	}

	return websitePage(http.StatusNotFound, "404 Not Found")
}

func websitePage(status int, msg string) originResp {
	return originResp{
		status: status,
		header: http.Header{headerContentType: {"text/html; charset=utf-8"}},
		body:   []byte("<html><head><title>" + msg + "</title></head><body><h1>" + msg + "</h1></body></html>"),
	}
}
