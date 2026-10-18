package chaos

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

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
	case strings.HasPrefix(ct, "application/x-www-form-urlencoded"):
		return awserr.ProtocolQueryXML
	}

	switch svc {
	case "s3", "s3control", "cloudfront":
		return awserr.ProtocolRestXML
	case "route53":
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

	return awserr.Write(c, protocolFor(c, svc), awserr.APIError{
		Code: code, Message: faultMessage, HTTPStatus: status,
	})
}
