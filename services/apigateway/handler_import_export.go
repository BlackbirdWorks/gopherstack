package apigateway

// handler_import_export.go wires ImportRestApi, PutRestApi, ImportApiKeys, and
// ImportDocumentationParts into the action dispatch table, and bridges between
// the two ways these raw-body operations can reach the handler:
//
//   - Real REST invocations (via handleRESTAPI / dispatchRestAPISpec) wrap
//     the raw document (base64-encoded) — an OpenAPI/Swagger spec, a CSV file
//     of API keys, or a documentation-parts JSON payload — together with the
//     mode/restApiId/other query parameters in a small JSON envelope, since
//     merging query parameters directly into the document the way other
//     operations do would corrupt non-JSON-object bodies.
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

const (
	opImportAPIKeys            = "ImportApiKeys"
	opImportDocumentationParts = "ImportDocumentationParts"
	opImportRestAPI            = "ImportRestApi"
	opPutRestAPI               = "PutRestApi"
)

// isRawBodyAPIGWAction reports whether action carries an opaque raw-bytes
// request body that must not be JSON-merged with path/query parameters.
func isRawBodyAPIGWAction(action string) bool {
	switch action {
	case opImportRestAPI, opPutRestAPI, opImportAPIKeys, opImportDocumentationParts:
		return true
	}

	return false
}

// restAPISpecEnvelope is the JSON envelope built by dispatchRestAPISpec for
// real REST-style ImportRestApi/PutRestApi invocations.
type restAPISpecEnvelope struct {
	Parameters     map[string]string `json:"parameters,omitempty"`
	Body           string            `json:"body,omitempty"`
	Mode           string            `json:"mode,omitempty"`
	RestAPIID      string            `json:"restApiId,omitempty"`
	Format         string            `json:"format,omitempty"`
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
		Format    string `json:"format,omitempty"`
	}

	_ = json.Unmarshal(b, &fallback)

	return b, restAPISpecEnvelope{
		RestAPIID: fallback.RestAPIID,
		Mode:      fallback.Mode,
		Format:    fallback.Format,
	}
}

// restAPISpecActions returns the actionFn map for ImportRestApi and PutRestApi.
func (h *Handler) restAPISpecActions() map[string]actionFn {
	return map[string]actionFn{
		opImportRestAPI: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			api, err := h.Backend.ImportRestAPI(ImportRestAPIInput{
				Body:           specBody,
				FailOnWarnings: env.FailOnWarnings,
			})
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, api, nil
		},
		opPutRestAPI: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			api, err := h.Backend.PutRestAPI(PutRestAPIInput{
				RestAPIID:      env.RestAPIID,
				Mode:           env.Mode,
				Body:           specBody,
				FailOnWarnings: env.FailOnWarnings,
			})
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, api, nil
		},
	}
}

// apiKeysImportView is the response for ImportApiKeys.
type apiKeysImportView struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// documentationPartsImportView is the response for ImportDocumentationParts.
type documentationPartsImportView struct {
	IDs      []string `json:"ids"`
	Warnings []string `json:"warnings"`
}

// importAPIKeysInput is the input for ImportApiKeys. The raw payload document is
// carried in Body; Format is the query parameter (csv or json).
type importAPIKeysInput struct {
	Format string `json:"format"`
	Body   []byte `json:"body"`
}

// importActions returns real handlers for the bulk-import operations.
func (h *Handler) importActions() map[string]actionFn {
	return map[string]actionFn{
		opImportAPIKeys: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			format := env.Format
			if format == "" && env.Parameters != nil {
				format = env.Parameters["format"]
			}

			ids, warnings, err := h.Backend.ImportAPIKeys(specBody, format, env.FailOnWarnings)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, &apiKeysImportView{IDs: ids, Warnings: warnings}, nil
		},
		opImportDocumentationParts: func(b []byte) (int, any, error) {
			specBody, env := decodeRestAPISpecPayload(b)

			ids, warnings, err := h.Backend.ImportDocumentationParts(
				env.RestAPIID,
				specBody,
				env.Mode,
				env.FailOnWarnings,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, &documentationPartsImportView{IDs: ids, Warnings: warnings}, nil
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

		return writeJSONProtocolDispatchError(c, http.StatusInternalServerError,
			"InternalFailure", "internal server error")
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
		return writeJSONProtocolDispatchError(c, http.StatusInternalServerError,
			"InternalFailure", "internal server error")
	}

	statusCode, response, raw, reqErr := h.dispatch(ctx, action, envelope)
	if reqErr != nil {
		return h.handleError(ctx, c, action, reqErr)
	}

	if raw != nil {
		return writeRawBinaryResponse(c, statusCode, raw)
	}

	c.Response().Header().Set("Content-Type", contentTypeJSON)

	return c.JSONBlob(statusCode, response)
}
