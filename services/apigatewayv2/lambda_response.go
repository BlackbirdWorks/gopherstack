package apigatewayv2

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	headerContentType = "Content-Type"
	contentTypeJSON   = "application/json"
	msgInternalError  = `{"message":"Internal Server Error"}`
)

// writeHTTPAPILambdaResponse maps a Lambda proxy result onto the HTTP response. Payload 2.0
// accepts any JSON value (a non-envelope becomes the 200 body); 1.0 requires the statusCode
// envelope. A function error or malformed 1.0 response is a 500.
func writeHTTPAPILambdaResponse(c *echo.Context, respBytes []byte, payloadVersion string) error {
	trimmed := bytes.TrimSpace(respBytes)

	var envelope map[string]jsontext.Value

	isObject := json.Unmarshal(trimmed, &envelope, jsonx.V1()) == nil && envelope != nil
	if isObject && isFunctionErrorEnvelope(envelope) {
		return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
	}

	_, hasStatus := envelope["statusCode"]
	if !isObject || !hasStatus {
		if payloadVersion == payloadFormatV1 || len(trimmed) == 0 || !jsontext.Value(trimmed).IsValid(jsonx.V1()) {
			return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
		}

		c.Response().Header().Set(headerContentType, contentTypeJSON)
		c.Response().WriteHeader(http.StatusOK)
		_, _ = c.Response().Write(trimmed)

		return nil
	}

	var lambdaResp httpAPILambdaResponse
	if err := json.Unmarshal(trimmed, &lambdaResp, jsonx.V1()); err != nil {
		return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
	}

	return writeLambdaEnvelope(c, &lambdaResp)
}

func isFunctionErrorEnvelope(envelope map[string]jsontext.Value) bool {
	_, hasMsg := envelope["errorMessage"]
	_, hasType := envelope["errorType"]
	_, hasStatus := envelope["statusCode"]

	return (hasMsg || hasType) && !hasStatus
}

func writeLambdaEnvelope(c *echo.Context, lambdaResp *httpAPILambdaResponse) error {
	h := c.Response().Header()

	for k, vs := range lambdaResp.MultiValueHdrs {
		for _, v := range vs {
			h.Add(k, v)
		}
	}

	for k, v := range lambdaResp.Headers {
		h.Set(k, v)
	}

	for _, ck := range lambdaResp.Cookies {
		h.Add("Set-Cookie", ck)
	}

	if h.Get(headerContentType) == "" {
		h.Set(headerContentType, contentTypeJSON)
	}

	sc := lambdaResp.StatusCode
	if sc < http.StatusContinue || sc > 599 {
		return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
	}

	body := []byte(lambdaResp.Body)

	if lambdaResp.IsBase64Encoded {
		decoded, err := base64.StdEncoding.DecodeString(lambdaResp.Body)
		if err != nil {
			return writeJSONMessage(c, http.StatusInternalServerError, msgInternalError)
		}

		body = decoded
	}

	c.Response().WriteHeader(sc)
	_, _ = c.Response().Write(body)

	return nil
}

func writeJSONMessage(c *echo.Context, status int, body string) error {
	c.Response().Header().Set(headerContentType, contentTypeJSON)
	c.Response().WriteHeader(status)
	_, _ = c.Response().Write([]byte(body))

	return nil
}

// httpAPIMessage renders the `{"message":"..."}` body execute-api returns for gateway errors.
func httpAPIMessage(msg string) string {
	b, _ := json.Marshal(map[string]string{jsonKeyMessage: msg}, jsonx.V1())

	return strings.TrimSpace(string(b))
}
