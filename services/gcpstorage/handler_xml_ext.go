package gcpstorage

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	listNS           = "http://doc.s3.amazonaws.com/2006-03-01"
	multipartNS      = "http://s3.amazonaws.com/doc/2006-03-01/"
	xmlMaxKeys       = 1000
	copySourceHeader = "X-Goog-Copy-Source"
	directiveReplace = "REPLACE"
	directiveCopy    = "COPY"
)

type xmlContents struct {
	Key            string `xml:"Key"`
	LastModified   string `xml:"LastModified"`
	ETag           string `xml:"ETag"`
	Generation     int64  `xml:"Generation"`
	MetaGeneration int64  `xml:"MetaGeneration"`
	Size           int64  `xml:"Size"`
}

type xmlCommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

type xmlListResult struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Namespace             string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Marker                string            `xml:"Marker,omitempty"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextMarker            string            `xml:"NextMarker,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	Contents              []xmlContents     `xml:"Contents"`
	CommonPrefixes        []xmlCommonPrefix `xml:"CommonPrefixes"`
	MaxKeys               int               `xml:"MaxKeys"`
	KeyCount              int               `xml:"KeyCount,omitempty"`
	IsTruncated           bool              `xml:"IsTruncated"`
}

func xmlETag(o *Object) string {
	if o.PartsETag != "" {
		return o.PartsETag
	}

	if raw, err := base64.StdEncoding.DecodeString(o.MD5); err == nil && o.MD5 != "" {
		return `"` + hex.EncodeToString(raw) + `"`
	}

	return `"` + objectETag(o) + `"`
}

func writeXML(c *echo.Context, v any) error {
	out, err := xml.Marshal(v)
	if err != nil {
		return err
	}

	return c.Blob(http.StatusOK, "application/xml; charset=UTF-8", append([]byte(xml.Header), out...))
}

// xmlListObjects serves GET Bucket (docs.cloud.google.com/storage/docs/xml-api/get-bucket-list).
func (h *Handler) xmlListObjects(c *echo.Context, rt route) error {
	q := c.QueryParams()

	if q.Has("versions") || q.Has("uploads") {
		return &APIError{
			Status: http.StatusNotImplemented, Reason: "notImplemented", XMLCode: "NotImplemented",
			Message: "Listing object versions or multipart uploads through the XML API is not supported.",
		}
	}

	res, v2, apiErr := parseXMLListQuery(rt.bucket, q)
	if apiErr != nil {
		return apiErr
	}

	page, err := h.xmlListPage(rt.bucket, res, v2)
	if err != nil {
		return err
	}

	enc := func(s string) string {
		if res.EncodingType == "url" {
			return url.QueryEscape(s)
		}

		return s
	}

	for _, o := range page.items {
		res.Contents = append(res.Contents, xmlContents{
			Key: enc(o.Name), Generation: o.Generation, MetaGeneration: o.Metageneration,
			LastModified: fmtTime(o.Updated), ETag: xmlETag(o), Size: o.Size(),
		})
	}

	for _, p := range page.prefixes {
		res.CommonPrefixes = append(res.CommonPrefixes, xmlCommonPrefix{Prefix: enc(p)})
	}

	res.Prefix, res.Marker, res.StartAfter = enc(res.Prefix), enc(res.Marker), enc(res.StartAfter)
	res.IsTruncated = page.next != ""

	if v2 {
		res.KeyCount = len(page.items) + len(page.prefixes)
		res.NextContinuationToken = page.next
	} else if res.IsTruncated && res.Delimiter != "" {
		res.NextMarker = enc(page.last)
	}

	return writeXML(c, res)
}

type xmlPage struct {
	next     string
	last     string
	items    []*Object
	prefixes []string
}

// xmlListPage reads one page. Version 1 lists after the marker name, version 2 resumes from the token.
func (h *Handler) xmlListPage(bucket string, req xmlListResult, v2 bool) (xmlPage, error) {
	opts := ListOptions{Prefix: req.Prefix, Delimiter: req.Delimiter, MaxResults: req.MaxKeys + 1, Lookahead: true}
	after := req.Marker

	if v2 {
		after = req.StartAfter
		opts.PageToken = req.ContinuationToken
	}

	if after != "" && opts.PageToken == "" {
		opts.StartOffset = after
		opts.MaxResults++
	}

	if req.MaxKeys == 0 {
		return xmlPage{}, nil
	}

	res, err := h.Backend.ListObjects(bucket, opts)
	if err != nil {
		return xmlPage{}, err
	}

	items, prefixes := res.Items, res.Prefixes

	if after != "" && opts.PageToken == "" {
		items = dropUpTo(items, after, func(o *Object) string { return o.Name })
		prefixes = dropUpTo(prefixes, after, func(p string) string { return p })
	}

	return xmlTrim(items, prefixes, req.MaxKeys, v2), nil
}

func dropUpTo[T any](in []T, marker string, name func(T) string) []T {
	out := in[:0:0]

	for _, v := range in {
		if name(v) > marker {
			out = append(out, v)
		}
	}

	return out
}

// xmlTrim merges items and prefixes in name order, keeps maxKeys of them and reports the continuation point.
func xmlTrim(items []*Object, prefixes []string, maxKeys int, v2 bool) xmlPage {
	var page xmlPage

	i, p, n := 0, 0, 0

	for (i < len(items) || p < len(prefixes)) && n < maxKeys {
		if p >= len(prefixes) || (i < len(items) && items[i].Name < prefixes[p]) {
			page.items = append(page.items, items[i])
			page.last = items[i].Name
			i++
		} else {
			page.prefixes = append(page.prefixes, prefixes[p])
			page.last = prefixes[p]
			p++
		}

		n++
	}

	if i < len(items) || p < len(prefixes) {
		page.next = page.last
		if v2 {
			page.next = listKeyFor(page).token()
		}
	}

	return page
}

func listKeyFor(page xmlPage) listKey {
	if n := len(page.items); n > 0 && page.items[n-1].Name == page.last {
		return listKey{name: page.last, gen: page.items[n-1].Generation, rank: rankObject}
	}

	return listKey{name: page.last, rank: rankPrefix}
}

type initiateResult struct {
	XMLName   xml.Name `xml:"InitiateMultipartUploadResult"`
	Namespace string   `xml:"xmlns,attr"`
	Bucket    string   `xml:"Bucket"`
	Key       string   `xml:"Key"`
	UploadID  string   `xml:"UploadId"`
}

type completeResult struct {
	XMLName   xml.Name `xml:"CompleteMultipartUploadResult"`
	Namespace string   `xml:"xmlns,attr"`
	Location  string   `xml:"Location"`
	Bucket    string   `xml:"Bucket"`
	Key       string   `xml:"Key"`
	ETag      string   `xml:"ETag"`
}

type completeRequest struct {
	XMLName xml.Name `xml:"CompleteMultipartUpload"`
	Parts   []struct {
		ETag       string `xml:"ETag"`
		PartNumber int    `xml:"PartNumber"`
	} `xml:"Part"`
}

func xmlObjectMeta(r *http.Request) Patch {
	meta := Patch{}

	for k, v := range map[string]string{
		keyContentType: r.Header.Get("Content-Type"), keyContentEncoding: r.Header.Get("Content-Encoding"),
		keyCacheControl: r.Header.Get("Cache-Control"), keyContentDisposition: r.Header.Get("Content-Disposition"),
		keyContentLanguage: r.Header.Get("Content-Language"), keyStorageClass: r.Header.Get("X-Goog-Storage-Class"),
	} {
		if v != "" {
			meta[k] = marshalRaw(v)
		}
	}

	custom := map[string]string{}

	for k, v := range r.Header {
		if len(v) > 0 && strings.HasPrefix(k, metaHeaderPrefix) {
			custom[strings.ToLower(k[len(metaHeaderPrefix):])] = v[0]
		}
	}

	if len(custom) > 0 {
		meta[keyMetadata] = marshalRaw(custom)
	}

	return meta
}

func (h *Handler) xmlInitiateMultipart(c *echo.Context, rt route) error {
	meta, apiErr := withUploadKey(c.Request(), xmlObjectMeta(c.Request()))
	if apiErr != nil {
		return apiErr
	}

	id, err := h.Backend.InitiateMultipart(rt.bucket, rt.object, meta)
	if err != nil {
		return err
	}

	return writeXML(c, initiateResult{
		Namespace: multipartNS, Bucket: rt.bucket, Key: rt.object, UploadID: id,
	})
}

func (h *Handler) xmlUploadPart(c *echo.Context, rt route) error {
	n, err := strconv.Atoi(c.QueryParam("partNumber"))
	if err != nil {
		return errMPU(http.StatusBadRequest, "InvalidArgument", "partNumber must be an integer.")
	}

	keySHA, apiErr := csekFromHeaders(c.Request().Header, hdrEncryptionPfx)
	if apiErr != nil {
		return apiErr
	}

	data, err := readBody(c)
	if err != nil {
		return err
	}

	etag, err := h.Backend.UploadPart(rt.bucket, rt.object, c.QueryParam("uploadId"), n, data, keySHA)
	if err != nil {
		return err
	}

	c.Response().Header().Set("ETag", etag)

	return c.NoContent(http.StatusOK)
}

func (h *Handler) xmlCompleteMultipart(c *echo.Context, rt route) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}

	var req completeRequest
	if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
		return errMPU(http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed.")
	}

	parts := make([]PartRef, 0, len(req.Parts))
	for _, p := range req.Parts {
		parts = append(parts, PartRef{Number: p.PartNumber, ETag: p.ETag})
	}

	keySHA, apiErr := csekFromHeaders(c.Request().Header, hdrEncryptionPfx)
	if apiErr != nil {
		return apiErr
	}

	pre, apiErr := xmlPreconds(c.Request().Header)
	if apiErr != nil {
		return apiErr
	}

	o, err := h.Backend.CompleteMultipart(rt.bucket, rt.object, c.QueryParam("uploadId"), parts, pre, keySHA)
	if err != nil {
		return err
	}

	hdr := c.Response().Header()
	hdr.Set("X-Goog-Generation", itoa64(o.Generation))
	hdr.Set("X-Goog-Hash", "crc32c="+o.CRC32C)

	return writeXML(c, completeResult{
		Namespace: multipartNS, Bucket: rt.bucket, Key: rt.object, ETag: xmlETag(o),
		Location: baseURL(c.Request()) + "/" + rt.bucket + "/" + url.PathEscape(rt.object),
	})
}

func (h *Handler) xmlAbortMultipart(c *echo.Context, rt route) error {
	if err := h.Backend.AbortMultipart(rt.bucket, rt.object, c.QueryParam("uploadId")); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}

type copyObjectResult struct {
	XMLName      xml.Name `xml:"CopyObjectResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// xmlCopyObject serves PUT Object with x-goog-copy-source (docs.cloud.google.com/storage/docs/xml-api/put-object-copy).
func (h *Handler) xmlCopyObject(c *echo.Context, rt route) error {
	r := c.Request()

	raw, err := url.PathUnescape(r.Header.Get(copySourceHeader))
	if err != nil {
		return errInvalid("x-goog-copy-source is not a valid URL-encoded path.")
	}

	srcBucket, srcName, ok := strings.Cut(strings.TrimPrefix(raw, "/"), "/")
	if !ok || srcBucket == "" || srcName == "" {
		return errInvalid("x-goog-copy-source must be /BUCKET_NAME/OBJECT_NAME.")
	}

	directive := strings.ToUpper(cmpOr(r.Header.Get("X-Goog-Metadata-Directive"), directiveCopy))
	if directive != directiveCopy && directive != directiveReplace {
		return errInvalid("x-goog-metadata-directive must be COPY or REPLACE.")
	}

	if directive == directiveCopy && srcBucket == rt.bucket && srcName == rt.object {
		return errInvalid("Copying an object to itself requires x-goog-metadata-directive: REPLACE.")
	}

	req, srcGen, apiErr := xmlCopyRequest(r, rt, srcBucket, srcName)
	if apiErr != nil {
		return apiErr
	}

	src, err := h.Backend.GetObject(srcBucket, srcName, srcGen, req.SrcPre)
	if err != nil {
		return err
	}

	if apiErr = copySourceConditions(r.Header, src); apiErr != nil {
		return apiErr
	}

	if directive == directiveReplace {
		req.Meta = xmlObjectMeta(r)
	}

	o, err := h.Backend.CopyObject(req)
	if err != nil {
		return err
	}

	hdr := c.Response().Header()
	hdr.Set("X-Goog-Generation", itoa64(o.Generation))
	hdr.Set("X-Goog-Metageneration", itoa64(o.Metageneration))

	return writeXML(c, copyObjectResult{LastModified: fmtTime(o.Updated), ETag: xmlETag(o)})
}

func cmpOr(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func xmlCopyRequest(r *http.Request, rt route, srcBucket, srcName string) (CopyRequest, int64, *APIError) {
	var srcGen int64

	if v := r.Header.Get("X-Goog-Copy-Source-Generation"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return CopyRequest{}, 0, errInvalid("x-goog-copy-source-generation must be an integer.")
		}

		srcGen = n
	}

	srcKey, apiErr := csekFromHeaders(r.Header, hdrCopySourcePfx)
	if apiErr != nil {
		return CopyRequest{}, 0, apiErr
	}

	dstKey, apiErr := csekFromHeaders(r.Header, hdrEncryptionPfx)
	if apiErr != nil {
		return CopyRequest{}, 0, apiErr
	}

	dstPre, apiErr := xmlPreconds(r.Header)
	if apiErr != nil {
		return CopyRequest{}, 0, apiErr
	}

	srcPre := Preconds{}

	for key, dst := range map[string]**int64{
		"X-Goog-Copy-Source-If-Generation-Match":     &srcPre.GenMatch,
		"X-Goog-Copy-Source-If-Metageneration-Match": &srcPre.MetaMatch,
	} {
		if v := r.Header.Get(key); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return CopyRequest{}, 0, errInvalid(key + " must be an integer.")
			}

			*dst = &n
		}
	}

	return CopyRequest{
		SrcBucket: srcBucket, SrcName: srcName, SrcGeneration: srcGen, SrcPre: srcPre,
		DstBucket: rt.bucket, DstName: rt.object, DstPre: dstPre,
		SrcKeySHA: srcKey, DstKeySHA: dstKey,
	}, srcGen, nil
}

// copySourceConditions evaluates the ETag and date source conditions; any failure is 412.
func copySourceConditions(hdr http.Header, src *Object) *APIError {
	etag := xmlETag(src)

	if v := hdr.Get("X-Goog-Copy-Source-If-Match"); v != "" && !etagListHas(v, etag) {
		return errPrecondition()
	}

	if v := hdr.Get("X-Goog-Copy-Source-If-None-Match"); v != "" && etagListHas(v, etag) {
		return errPrecondition()
	}

	if t, ok := parseHTTPDate(hdr.Get("X-Goog-Copy-Source-If-Modified-Since")); ok && !src.Updated.After(t) {
		return errPrecondition()
	}

	if t, ok := parseHTTPDate(hdr.Get("X-Goog-Copy-Source-If-Unmodified-Since")); ok && src.Updated.After(t) {
		return errPrecondition()
	}

	return nil
}

func etagListHas(list, etag string) bool {
	for v := range strings.SplitSeq(list, ",") {
		if v = strings.TrimSpace(v); v == "*" || v == etag {
			return true
		}
	}

	return false
}

func parseHTTPDate(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}

	t, err := http.ParseTime(v)

	return t, err == nil
}

func parseXMLListQuery(bucket string, q url.Values) (xmlListResult, bool, *APIError) {
	maxKeys := xmlMaxKeys

	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return xmlListResult{}, false, errInvalid("max-keys must be a non-negative integer.")
		}

		maxKeys = min(n, xmlMaxKeys)
	}

	v2 := q.Get("list-type") == "2"
	if !v2 && (q.Has("continuation-token") || q.Has("start-after") || q.Has("fetch-owner")) {
		return xmlListResult{}, false,
			errInvalid("continuation-token, start-after and fetch-owner require list-type=2.")
	}

	if v2 && q.Has("marker") {
		return xmlListResult{}, false, errInvalid("marker is not valid with list-type=2.")
	}

	res := xmlListResult{
		Name: bucket, Prefix: q.Get("prefix"), Delimiter: q.Get("delimiter"), MaxKeys: maxKeys,
		EncodingType: q.Get("encoding-type"), Namespace: listNS, Marker: q.Get("marker"),
		StartAfter: q.Get("start-after"), ContinuationToken: q.Get("continuation-token"),
	}

	if res.EncodingType != "" && res.EncodingType != "url" {
		return xmlListResult{}, false, errInvalid("encoding-type must be url.")
	}

	return res, v2, nil
}
