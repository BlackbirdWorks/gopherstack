package serverlessrepo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createApplicationVersionRequest is the request body for CreateApplicationVersion.
type createApplicationVersionRequest struct {
	SourceCodeURL        string `json:"sourceCodeUrl"`
	SourceCodeArchiveURL string `json:"sourceCodeArchiveUrl"`
	TemplateURL          string `json:"templateUrl"`
}

func (h *Handler) handleCreateApplicationVersion(ctx context.Context, req *http.Request, body []byte) ([]byte, error) {
	appName, semanticVersion, err := extractPathExtra(req)
	if err != nil {
		return nil, err
	}

	if appName == "" {
		return nil, fmt.Errorf("%w: applicationId is required", errInvalidRequest)
	}

	if semanticVersion == "" {
		return nil, fmt.Errorf("%w: semanticVersion is required", errInvalidRequest)
	}

	var createReq createApplicationVersionRequest
	if jsonErr := json.Unmarshal(body, &createReq); jsonErr != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, jsonErr)
	}

	v, backendErr := h.Backend.CreateApplicationVersionWithOptions(
		appName,
		semanticVersion,
		CreateApplicationVersionOptions(createReq),
	)
	if backendErr != nil {
		return nil, backendErr
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "serverlessrepo: created application version",
		"app", appName, "version", v.SemanticVersion)

	b, marshalErr := json.Marshal(toVersionResponse(v))
	if marshalErr != nil {
		return nil, marshalErr
	}

	return b, errHTTP201
}

func (h *Handler) handleListApplicationVersions(req *http.Request) ([]byte, error) {
	appName, err := extractApplicationName(req)
	if err != nil {
		return nil, err
	}

	versions, err := h.Backend.ListApplicationVersions(appName)
	if err != nil {
		return nil, err
	}

	// Optional: filter by specific semantic version.
	if sv := req.URL.Query().Get(keySemanticVersion); sv != "" {
		filtered := versions[:0]

		for _, v := range versions {
			if v.SemanticVersion == sv {
				filtered = append(filtered, v)
			}
		}

		versions = filtered
	}

	// Apply pagination: nextToken is treated as the last-seen semantic version (exclusive cursor).
	nextToken := req.URL.Query().Get("nextToken")
	maxItems := parseMaxItems(req.URL.Query().Get("maxItems"), maxItemsDefault)

	start := 0

	if nextToken != "" {
		for i, v := range versions {
			if v.SemanticVersion == nextToken {
				start = i + 1

				break
			}
		}
	}

	end := min(start+maxItems, len(versions))

	page := versions[start:end]

	summaries := make([]map[string]any, 0, len(page))

	for _, v := range page {
		summaries = append(summaries, map[string]any{
			keyApplicationID:     v.ApplicationID,
			keySemanticVersion:   v.SemanticVersion,
			"sourceCodeUrl":      v.SourceCodeURL,
			keyCreationTime:      isoTimestamp(v.CreationTime),
			"resourcesSupported": v.ResourcesSupported,
		})
	}

	resp := map[string]any{"versions": summaries}

	if end < len(versions) {
		resp["nextToken"] = versions[end-1].SemanticVersion
	}

	return json.Marshal(resp)
}

// toVersionResponse converts an ApplicationVersion to a map matching the AWS SAR Version shape.
func toVersionResponse(v *ApplicationVersion) map[string]any {
	return map[string]any{
		keyApplicationID:       v.ApplicationID,
		keySemanticVersion:     v.SemanticVersion,
		"sourceCodeUrl":        v.SourceCodeURL,
		"sourceCodeArchiveUrl": v.SourceCodeArchiveURL,
		keyTemplateURL:         v.TemplateURL,
		keyCreationTime:        isoTimestamp(v.CreationTime),
		"parameterDefinitions": v.ParameterDefinitions,
		"requiredCapabilities": v.RequiredCapabilities,
		"resourcesSupported":   v.ResourcesSupported,
	}
}
