package apigateway

// handler_import_export.go wires ImportRestApi and PutRestApi into the
// action dispatch table, and bridges between the two ways these operations
// can reach the handler:
//
//   - Real REST invocations (via handleRESTAPI / dispatchRestAPISpec) wrap
//     the raw OpenAPI/Swagger document (base64-encoded) together with the
//     mode/restApiId/other query parameters in a small JSON envelope, since
//     merging query parameters directly into the document the way other
//     operations do would corrupt YAML or otherwise non-JSON-object bodies.
//   - Direct action dispatch (e.g. the JSON-protocol convenience path used by
//     this package's own tests) passes the document itself as the entire
//     payload, optionally with "restApiId"/"mode" as extra sibling fields.
import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// restAPISpecEnvelope is the JSON envelope built by dispatchRestAPISpec for
// real REST-style ImportRestApi/PutRestApi invocations.
type restAPISpecEnvelope struct {
	Parameters     map[string]string `json:"parameters,omitempty"`
	Body           string            `json:"body,omitempty"`
	Mode           string            `json:"mode,omitempty"`
	RestAPIID      string            `json:"restApiId,omitempty"`
	FailOnWarnings bool              `json:"failOnWarnings,omitempty"`
}

// decodeRestAPISpecPayload extracts the raw OpenAPI/Swagger document bytes
// and any envelope fields from an action payload. See the file comment above
// for the two payload shapes this handles.
func decodeRestAPISpecPayload(b []byte) ([]byte, restAPISpecEnvelope) {
	var env restAPISpecEnvelope
	if err := json.Unmarshal(b, &env); err == nil && env.Body != "" {
		if raw, decErr := base64.StdEncoding.DecodeString(env.Body); decErr == nil {
			return raw, env
		}
	}

	// Not an envelope (or it didn't decode): treat the whole payload as the
	// spec document itself. It may still carry restApiId/mode as sibling
	// fields (direct-dispatch convenience form), which parseOpenAPISpec
	// simply ignores as unrecognized top-level keys.
	var fallback struct {
		RestAPIID string `json:"restApiId,omitempty"`
		Mode      string `json:"mode,omitempty"`
	}

	_ = json.Unmarshal(b, &fallback)

	return b, restAPISpecEnvelope{RestAPIID: fallback.RestAPIID, Mode: fallback.Mode}
}

// restAPISpecActions returns the actionFn map for ImportRestApi and PutRestApi.
func (h *Handler) restAPISpecActions() map[string]actionFn {
	return map[string]actionFn{
		opImportRestAPI: func(b []byte) (int, any, error) {
			specBody, _ := decodeRestAPISpecPayload(b)

			api, err := h.Backend.ImportRestAPI(specBody)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, api, nil
		},
		opPutRestAPI: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			api, err := h.Backend.PutRestAPI(env.RestAPIID, specBody, env.Mode)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, api, nil
		},
	}
}

// dispatchRestAPISpec handles the REST-style entry point for ImportRestApi
// and PutRestApi: it reads the raw request body (the OpenAPI/Swagger
// document), wraps it with the request's path/query parameters in a JSON
// envelope, and dispatches as usual.
func (h *Handler) dispatchRestAPISpec(
	c *echo.Context, action string, pathParams map[string]string, query url.Values,
) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to read request body", "error", err)

		return c.String(http.StatusInternalServerError, "internal server error")
	}

	env := restAPISpecEnvelope{
		Body:           base64.StdEncoding.EncodeToString(body),
		Mode:           query.Get("mode"),
		RestAPIID:      pathParams[keyRestAPIID],
		FailOnWarnings: query.Get("failonwarnings") == litTrue,
		Parameters:     make(map[string]string),
	}

	for k, v := range query {
		if k == "mode" || k == "failonwarnings" || len(v) == 0 {
			continue
		}

		env.Parameters[k] = v[0]
	}

	envelope, err := json.Marshal(env)
	if err != nil {
		return c.String(http.StatusInternalServerError, "internal server error")
	}

	statusCode, response, reqErr := h.dispatch(ctx, action, envelope)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	c.Response().Header().Set("Content-Type", contentTypeJSON)

	return c.JSONBlob(statusCode, response)
}
