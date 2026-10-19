package chaos

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// protocolEC2 is the EC2 query error envelope, which awserr has no value for.
const protocolEC2 = awserr.Protocol(-1)

type ec2Error struct {
	XMLName   xml.Name `xml:"Response"`
	Code      string   `xml:"Errors>Error>Code"`
	Message   string   `xml:"Errors>Error>Message"`
	RequestID string   `xml:"RequestID"`
}

const faultMessage = "Fault injected by Gopherstack Chaos API"

// protocolFor picks the error envelope the caller's SDK will parse.
func protocolFor(c *echo.Context, svc string) awserr.Protocol {
	h := c.Request().Header
	ct := h.Get("Content-Type")

	switch {
	case h.Get("X-Amz-Target") != "":
		if strings.Contains(ct, "json-1.1") {
			return awserr.ProtocolJSON11
		}

		return awserr.ProtocolJSON10
	case svc == "ec2":
		return protocolEC2
	case strings.HasPrefix(ct, "application/x-www-form-urlencoded"):
		return awserr.ProtocolQueryXML
	}

	switch svc {
	case "s3", "s3control":
		return awserr.ProtocolRestXML
	case "route53", "cloudfront":
		return awserr.ProtocolQueryXML
	}

	return awserr.ProtocolJSON10
}

// respondWithFault writes the fault response in the caller's wire protocol and short-circuits the chain.
func respondWithFault(c *echo.Context, fe FaultError, svc string) error {
	status := fe.StatusCode
	if status <= 0 {
		status = http.StatusServiceUnavailable
	}

	code := fe.Code
	if code == "" {
		code = "ServiceUnavailable"
	}

	proto := protocolFor(c, svc)
	apiErr := awserr.APIError{Code: code, Message: faultMessage, HTTPStatus: status}

	if proto == protocolEC2 {
		body, err := xml.Marshal(ec2Error{
			Code: code, Message: faultMessage, RequestID: c.Request().Header.Get("X-Amzn-Requestid"),
		})
		if err != nil {
			return c.NoContent(http.StatusInternalServerError)
		}

		return c.Blob(status, "text/xml;charset=UTF-8", append([]byte(xml.Header), body...))
	}

	if proto == awserr.ProtocolJSON10 || proto == awserr.ProtocolJSON11 {
		c.Response().Header().Set("X-Amzn-Errortype", code)
	}

	return awserr.Write(c, proto, apiErr)
}
