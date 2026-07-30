package service

import (
	"net/http"
	"strings"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
)

// IsRPCv2CBORRequest returns true when r is a POST to a path under
// servicePath, indicating the Smithy RPCv2 CBOR wire protocol (as opposed to
// a service's legacy JSON, or query/XML, protocol served alongside it).
//
// This is the protocol AppStream (v1.64.0+) and CloudWatch serve today --
// the two rpc-v2-cbor services in this codebase -- identified by dispatching
// on URL path prefix rather than an X-Amz-Target header or SOAPAction-style
// convention.
func IsRPCv2CBORRequest(r *http.Request, servicePath string) bool {
	return r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, servicePath)
}

// ExtractRPCv2CBOROperation returns the operation name from an rpc-v2-cbor
// request path, given that service's path prefix.
func ExtractRPCv2CBOROperation(path, servicePath string) string {
	return strings.TrimPrefix(path, servicePath)
}

// WriteRPCv2CBORResponse writes a CBOR-encoded rpc-v2-cbor response, setting
// the Content-Type and Smithy-Protocol headers the protocol requires.
func WriteRPCv2CBORResponse(c *echo.Context, v cbor.Value) error {
	c.Response().Header().Set("Content-Type", "application/cbor")
	c.Response().Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	c.Response().WriteHeader(http.StatusOK)
	_, err := c.Response().Write(cbor.Encode(v))

	return err
}

// WriteRPCv2CBORError writes an rpc-v2-cbor error response.
//
// Unlike a generic "message"-only body, the rpc-v2-cbor SDKs' generated
// error deserializer (getProtocolErrorInfo in each service's
// deserializers.go) resolves the exception name from an "__type" key inside
// the decoded CBOR body itself -- never from a header -- so that key must be
// present for errors.As/errors.Is against typed exceptions to work on the
// client (gopherstack-7fyf: CloudWatch previously set this only via the
// X-Amzn-Errortype header, which this protocol's client never reads).
// X-Amzn-Errortype is set too, matching the convention this codebase uses
// elsewhere, but the SDK does not consume it for this protocol.
func WriteRPCv2CBORError(c *echo.Context, status int, code, message string) error {
	c.Response().Header().Set("Content-Type", "application/cbor")
	c.Response().Header().Set("Smithy-Protocol", "rpc-v2-cbor")
	c.Response().Header().Set("X-Amzn-Errortype", code)
	c.Response().WriteHeader(status)
	_, err := c.Response().Write(cbor.Encode(cbor.Map{
		"__type":  cbor.String(code),
		"message": cbor.String(message),
	}))

	return err
}
