package app

import (
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	svcSQS = "sqs"
	svcSNS = "sns"
)

const maxPartitionScanBody = 1 << 20

// partitionStrictMiddleware rejects signed requests that reference an ARN from a partition other than the one
// the signing region belongs to, as AWS does. Requests with no foreign ARN are unaffected.
func partitionStrictMiddleware() echo.MiddlewareFunc {
	enabled := arn.PartitionStrict()

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			req := c.Request()
			if !enabled || !isSigV4Request(req) {
				return next(c)
			}

			meta := awsmeta.Get(req.Context())
			if meta == nil || meta.Region == "" {
				return next(c)
			}

			if bad, found := foreignPartitionARN(req, meta.Region, meta.Service); found {
				return writePartitionError(c, meta.Service, bad, meta.Region)
			}

			return next(c)
		}
	}
}

func isSigV4Request(r *http.Request) bool {
	return strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-") ||
		httputils.RawQueryHas(r.URL.RawQuery, "X-Amz-Credential")
}

// mayHideARN is false only when no decoded query value can contain "arn:".
func mayHideARN(rawQuery string) bool {
	return strings.Contains(rawQuery, "arn:") || strings.ContainsAny(rawQuery, "%+")
}

func foreignPartitionARN(r *http.Request, region, service string) (string, bool) {
	partition := arn.PartitionForRegion(region)

	if bad, ok := arn.FindMismatch(r.URL.Path, partition, false); ok {
		return bad, true
	}

	if mayHideARN(r.URL.RawQuery) {
		for _, vs := range r.URL.Query() {
			for _, v := range vs {
				if bad, ok := arn.FindMismatch(v, partition, false); ok {
					return bad, true
				}
			}
		}
	}

	for k, vs := range r.Header {
		if strings.EqualFold(k, "Authorization") {
			continue
		}

		for _, v := range vs {
			if bad, ok := arn.FindMismatch(v, partition, false); ok {
				return bad, true
			}
		}
	}

	if !scannableBody(r, service) {
		return "", false
	}

	return scanBody(r, partition)
}

func scannableBody(r *http.Request, service string) bool {
	if strings.HasPrefix(service, "s3") || r.Body == nil || r.Body == http.NoBody {
		return false
	}

	if r.ContentLength <= 0 || r.ContentLength > maxPartitionScanBody || r.Header.Get("Content-Encoding") != "" {
		return false
	}

	ct := r.Header.Get("Content-Type")

	return strings.Contains(ct, "json") || strings.Contains(ct, "xml") || strings.Contains(ct, "x-www-form-urlencoded")
}

func scanBody(r *http.Request, partition string) (string, bool) {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxPartitionScanBody+1))
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	if err != nil {
		return "", false
	}

	text := string(body)
	if strings.Contains(r.Header.Get("Content-Type"), "x-www-form-urlencoded") {
		if vals, perr := url.ParseQuery(text); perr == nil {
			text = flattenValues(vals)
		}
	}

	return arn.FindMismatch(text, partition, false)
}

func flattenValues(vals url.Values) string {
	var sb strings.Builder

	for _, vs := range vals {
		for _, v := range vs {
			sb.WriteString(v)
			sb.WriteByte('\n')
		}
	}

	return sb.String()
}

func writePartitionError(c *echo.Context, service, bad, region string) error {
	r := c.Request()
	msg := "ARN " + bad + " is not in the partition of region " + region + " (" + arn.PartitionForRegion(region) + ")"
	e := awserr.APIError{
		Code:       "ValidationException",
		Message:    msg,
		HTTPStatus: http.StatusBadRequest,
	}

	switch {
	case strings.HasPrefix(service, "s3"):
		e.Code = "InvalidArgument"

		return awserr.Write(c, awserr.ProtocolRestXML, e)
	case r.Header.Get("X-Amz-Target") != "":
		if service == svcSQS {
			e.Code = "InvalidParameterValue"
		}

		proto := awserr.ProtocolJSON11
		if strings.Contains(r.Header.Get("Content-Type"), "1.0") {
			proto = awserr.ProtocolJSON10
		}

		return awserr.Write(c, proto, e)
	case strings.Contains(r.Header.Get("Content-Type"), "x-www-form-urlencoded"):
		e.Code = queryPartitionErrorCode(service)

		return awserr.Write(c, awserr.ProtocolQueryXML, e)
	case service == "lambda":
		e.Code = "InvalidParameterValueException"
	}

	c.Response().Header().Set("X-Amzn-Errortype", e.Code)

	if strings.Contains(r.Header.Get("Content-Type"), "xml") {
		return awserr.Write(c, awserr.ProtocolRestXML, e)
	}

	return awserr.Write(c, awserr.ProtocolJSON11, e)
}

func queryPartitionErrorCode(service string) string {
	switch service {
	case svcSQS, "ec2":
		return "InvalidParameterValue"
	case svcSNS:
		return "InvalidParameter"
	default:
		return "ValidationError"
	}
}
