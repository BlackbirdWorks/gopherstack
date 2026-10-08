package apigateway

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"gopkg.in/yaml.v3"
)

const opGetExport = "GetExport"

type getExportInput struct {
	RestAPIID  string `json:"restApiId"`
	StageName  string `json:"stageName"`
	ExportType string `json:"exportType"`
	Extensions string `json:"extensions"`
	Accepts    string `json:"accepts"`
}

const contentTypeYAML = "application/yaml"

// exportFormat maps the Accept header to the export's content type and file extension.
func exportFormat(accepts string) (string, string, error) {
	switch strings.TrimSpace(strings.ToLower(accepts)) {
	case "", contentTypeJSON, "*/*":
		return contentTypeJSON, "json", nil
	case contentTypeYAML:
		return contentTypeYAML, "yaml", nil
	}

	return "", "", fmt.Errorf("%w: unsupported Accept %q; use %s or %s",
		ErrInvalidParameter, accepts, contentTypeJSON, contentTypeYAML)
}

// exportActions returns the action map for the OpenAPI export operation.
func (h *Handler) exportActions() map[string]actionFn {
	return map[string]actionFn{
		opGetExport: func(b []byte) (int, any, error) {
			var input getExportInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			contentType, ext, err := exportFormat(input.Accepts)
			if err != nil {
				return 0, nil, err
			}

			export, err := h.Backend.GetExport(
				input.RestAPIID, input.StageName, input.ExportType, ParseExportExtensions(input.Extensions))
			if err != nil {
				return 0, nil, err
			}

			encoded, err := encodeExport(export, contentType)
			if err != nil {
				return 0, nil, err
			}

			// AWS's API docs (API_GetExport.html) document ContentDisposition
			// as a real response header but do not specify its value's format
			// (unlike GetSdk's ContentDisposition, which AWS also leaves
			// unspecified but this emulator already synthesizes in sdk.go);
			// this filename follows the same synthesized convention.
			disposition := fmt.Sprintf(
				`attachment; filename="%s-%s-%s.%s"`, input.RestAPIID, input.StageName, input.ExportType, ext,
			)

			return http.StatusOK, &rawBinaryResponse{
				contentType:        contentType,
				contentDisposition: disposition,
				body:               encoded,
			}, nil
		},
	}
}

func encodeExport(export map[string]any, contentType string) ([]byte, error) {
	if contentType == contentTypeYAML {
		return yaml.Marshal(export)
	}

	return json.Marshal(export)
}
