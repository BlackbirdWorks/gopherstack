package s3

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	objectLambdaProtocolVersion = "1.00"
	schemeHTTP                  = "http"
	schemeHTTPS                 = "https"
)

var errObjectLambdaFailed = errors.New("object lambda invocation failed")

// routeObjectLambda serves the request from the label's Object Lambda when its configuration covers action.
// Otherwise it returns the bucket to continue with: the supporting bucket for an access point label.
func (h *S3Handler) routeObjectLambda(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	label, key, action string,
) (string, bool) {
	route := h.objectLambdaTarget(label)
	if !route.handles(action) {
		if route.lambdaARN != "" {
			return route.bucket, false
		}

		return label, false
	}

	switch action {
	case objectLambdaActionGetObject:
		h.handleObjectLambdaGetObject(ctx, w, r, route, key)
	case objectLambdaActionHeadObject:
		h.handleObjectLambdaHeadObject(ctx, w, r, route, key)
	default:
		h.handleObjectLambdaList(ctx, w, r, route, action)
	}

	return label, true
}

// newObjectLambdaEvent builds the fields every Object Lambda event carries
// (https://docs.aws.amazon.com/AmazonS3/latest/userguide/olap-event-context.html).
func (h *S3Handler) newObjectLambdaEvent(r *http.Request, route objectLambdaRoute) objectLambdaEvent {
	scheme := schemeHTTPS
	if r.TLS == nil {
		scheme = schemeHTTP
	}

	headers := make(map[string]string, len(r.Header)+1)
	headers["Host"] = r.Host

	for name, vals := range r.Header {
		if isObjectLambdaAuthHeader(name) {
			continue
		}

		headers[name] = strings.Join(vals, ",")
	}

	q := r.URL.Query()
	for name := range q {
		if strings.HasPrefix(strings.ToLower(name), "x-amz-") {
			q.Del(name)
		}
	}

	u := url.URL{Scheme: scheme, Host: r.Host, Path: r.URL.Path, RawQuery: q.Encode()}

	ev := objectLambdaEvent{
		XAmzRequestID:   uuid.NewString(),
		ProtocolVersion: objectLambdaProtocolVersion,
		UserRequest:     objectLambdaUserRequest{URL: u.String(), Headers: headers},
	}

	if route.ap != nil {
		ev.Configuration = &objectLambdaConfiguration{
			AccessPointARN: fmt.Sprintf("arn:aws:s3-object-lambda:%s:%s:accesspoint/%s",
				regionFromRequest(r, h.DefaultRegion), route.ap.AccountID, route.ap.Name),
			SupportingAccessPointARN: route.ap.SupportingAccessPointARN,
			Payload:                  route.ap.Payload,
		}
	}

	return ev
}

func isObjectLambdaAuthHeader(name string) bool {
	switch strings.ToLower(name) {
	case "authorization", "x-amz-security-token", "x-amz-date", "x-amz-content-sha256":
		return true
	default:
		return strings.HasPrefix(strings.ToLower(name), "x-amz-checksum")
	}
}

// invokeObjectLambda synchronously invokes the route's Lambda and returns its JSON result.
func (h *S3Handler) invokeObjectLambda(ctx context.Context, lambdaARN string, ev objectLambdaEvent) ([]byte, error) {
	inv, ok := h.notifier.(LambdaInvoker)
	if !ok {
		return nil, ErrNoSuchKey
	}

	payload, err := json.Marshal(ev)
	if err != nil {
		return nil, err
	}

	out, _, err := inv.InvokeFunction(ctx, lambdaARN, "RequestResponse", payload)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "object lambda: invocation failed", "arn", lambdaARN, "error", err)
	}

	return out, err
}

// objectLambdaResult is the JSON a Lambda returns for HeadObject, ListObjects and ListObjectsV2.
type objectLambdaResult struct {
	Headers          map[string]any        `json:"headers"`
	ListBucketResult *objectLambdaListBody `json:"listBucketResult"`
	ErrorCode        string                `json:"errorCode"`
	ErrorMessage     string                `json:"errorMessage"`
	ListResultXML    string                `json:"listResultXml"`
	StatusCode       int                   `json:"statusCode"`
}

type objectLambdaListBody struct {
	Name                  string `json:"name"`
	Prefix                string `json:"prefix"`
	Marker                string `json:"marker"`
	NextMarker            string `json:"nextMarker"`
	StartAfter            string `json:"startAfter"`
	ContinuationToken     string `json:"continuationToken"`
	NextContinuationToken string `json:"nextContinuationToken"`
	Delimiter             string `json:"delimiter"`
	EncodingType          string `json:"encodingType"`
	Contents              []struct {
		Owner *struct {
			DisplayName string `json:"displayName"`
			ID          string `json:"id"`
		} `json:"owner"`
		Key               string `json:"key"`
		LastModified      string `json:"lastModified"`
		ETag              string `json:"eTag"`
		ChecksumAlgorithm string `json:"checksumAlgorithm"`
		StorageClass      string `json:"storageClass"`
		Size              int64  `json:"size"`
	} `json:"contents"`
	CommonPrefixes []struct {
		Prefix string `json:"prefix"`
	} `json:"commonPrefixes"`
	MaxKeys     int  `json:"maxKeys"`
	KeyCount    int  `json:"keyCount"`
	IsTruncated bool `json:"isTruncated"`
}

func (h *S3Handler) callObjectLambda(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	route objectLambdaRoute,
	ev objectLambdaEvent,
) (objectLambdaResult, bool) {
	var res objectLambdaResult

	out, err := h.invokeObjectLambda(ctx, route.lambdaARN, ev)
	if err != nil || json.Unmarshal(out, &res) != nil || res.StatusCode == 0 ||
		(res.ListResultXML != "" && res.ListBucketResult != nil) {
		WriteError(ctx, w, r, errObjectLambdaFailed)

		return res, false
	}

	return res, true
}

// handleObjectLambdaHeadObject answers HeadObject from the Lambda's headers (olap-writing-lambda#olap-headobject).
func (h *S3Handler) handleObjectLambdaHeadObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	route objectLambdaRoute,
	key string,
) {
	in := fmt.Sprintf("%s/%s/%s", h.Endpoint, route.bucket, key)
	if v := r.URL.Query().Get("versionId"); v != "" {
		in += "?versionId=" + url.QueryEscape(v)
	}

	ev := h.newObjectLambdaEvent(r, route)
	ev.HeadObjectContext = &objectLambdaInputContext{InputS3URL: in}

	res, ok := h.callObjectLambda(ctx, w, r, route, ev)
	if !ok {
		return
	}

	for name, v := range res.Headers {
		if s, valid := objectLambdaHeaderValue(v); valid {
			w.Header().Set(name, s)
		}
	}

	if res.ErrorCode != "" {
		w.Header().Set("X-Amz-Error-Code", res.ErrorCode)
	}

	w.WriteHeader(res.StatusCode)
}

func objectLambdaHeaderValue(v any) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case bool:
		return strconv.FormatBool(t), true
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64), true
	default:
		return "", false
	}
}

// handleObjectLambdaList answers ListObjects/ListObjectsV2 from the Lambda's listResultXml or listBucketResult.
func (h *S3Handler) handleObjectLambdaList(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	route objectLambdaRoute,
	action string,
) {
	in := fmt.Sprintf("%s/%s/", h.Endpoint, route.bucket)
	if r.URL.RawQuery != "" {
		in += "?" + r.URL.RawQuery
	}

	ev := h.newObjectLambdaEvent(r, route)
	if action == objectLambdaActionListObjectsV2 {
		ev.ListObjectsV2Context = &objectLambdaInputContext{InputS3URL: in}
	} else {
		ev.ListObjectsContext = &objectLambdaInputContext{InputS3URL: in}
	}

	res, ok := h.callObjectLambda(ctx, w, r, route, ev)
	if !ok {
		return
	}

	switch {
	case res.ListResultXML != "":
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(res.StatusCode)
		_, _ = w.Write([]byte(res.ListResultXML))
	case res.ListBucketResult != nil:
		httputils.WriteXML(ctx, w, res.StatusCode, res.ListBucketResult.xmlResult(action))
	case res.ErrorCode != "":
		httputils.WriteXML(ctx, w, res.StatusCode, ErrorResponse{
			Code: res.ErrorCode, Message: res.ErrorMessage, Resource: r.URL.Path,
		})
	default:
		w.WriteHeader(res.StatusCode)
	}
}

func (b *objectLambdaListBody) xmlResult(action string) any {
	contents := make([]ObjectXML, 0, len(b.Contents))
	for _, c := range b.Contents {
		o := ObjectXML{
			Key: c.Key, LastModified: c.LastModified, ETag: c.ETag, Size: c.Size,
			StorageClass: c.StorageClass, ChecksumAlgorithm: c.ChecksumAlgorithm,
		}
		if c.Owner != nil {
			o.Owner = &Owner{ID: c.Owner.ID, DisplayName: c.Owner.DisplayName}
		}

		contents = append(contents, o)
	}

	prefixes := make([]CommonPrefixXML, 0, len(b.CommonPrefixes))
	for _, p := range b.CommonPrefixes {
		prefixes = append(prefixes, CommonPrefixXML{Prefix: p.Prefix})
	}

	if action == objectLambdaActionListObjectsV2 {
		return ListBucketV2Result{
			XMLName: xml.Name{Local: "ListBucketResult"}, Name: b.Name, Prefix: b.Prefix, StartAfter: b.StartAfter,
			ContinuationToken: b.ContinuationToken, NextContinuationToken: b.NextContinuationToken,
			Delimiter: b.Delimiter, EncodingType: b.EncodingType, Contents: contents, CommonPrefixes: prefixes,
			KeyCount: b.KeyCount, MaxKeys: b.MaxKeys, IsTruncated: b.IsTruncated,
		}
	}

	return ListBucketResult{
		XMLName: xml.Name{Local: "ListBucketResult"}, Name: b.Name, Prefix: b.Prefix, Marker: b.Marker,
		NextMarker: b.NextMarker, Delimiter: b.Delimiter, EncodingType: b.EncodingType, Contents: contents,
		CommonPrefixes: prefixes, MaxKeys: b.MaxKeys, IsTruncated: b.IsTruncated,
	}
}
