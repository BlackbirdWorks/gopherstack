package cloudtrail

import (
	"encoding/json"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// defaultImportsPageSize matches the ListTrails-family default of returning
// everything in one page for typical emulator-scale resource counts, while
// still honoring an explicit MaxResults/NextToken from the caller.
const defaultImportsPageSize = 1000

// --- StartImport ---

type importSourceBody struct {
	S3 *struct {
		S3LocationURI         string `json:"S3LocationUri"`
		S3BucketRegion        string `json:"S3BucketRegion"`
		S3BucketAccessRoleArn string `json:"S3BucketAccessRoleArn"`
	} `json:"S3"`
}

type startImportBody struct {
	ImportSource importSourceBody `json:"ImportSource"`
	Destinations []string         `json:"Destinations"`
}

// toBackendImportSource converts the wire body into the backend's
// *ImportSource, modeling all three real S3ImportSource fields
// (S3LocationUri, S3BucketRegion, S3BucketAccessRoleArn) instead of only
// S3LocationUri.
func (in startImportBody) toBackendImportSource() *ImportSource {
	if in.ImportSource.S3 == nil {
		return nil
	}

	return &ImportSource{
		S3: &S3ImportSource{
			S3LocationURI:         in.ImportSource.S3.S3LocationURI,
			S3BucketRegion:        in.ImportSource.S3.S3BucketRegion,
			S3BucketAccessRoleArn: in.ImportSource.S3.S3BucketAccessRoleArn,
		},
	}
}

func (h *Handler) handleStartImport(c *echo.Context, body []byte) error {
	var in startImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.StartImport(in.Destinations, in.toBackendImportSource())
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, importToMap(imp))
}

// --- GetImport ---

type getImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleGetImport(c *echo.Context, body []byte) error {
	var in getImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.GetImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, importToMap(imp))
}

// --- ListImports ---

type listImportsBody struct {
	Destination  string `json:"Destination"`
	ImportStatus string `json:"ImportStatus"`
	NextToken    string `json:"NextToken"`
	MaxResults   int    `json:"MaxResults"`
}

func (h *Handler) handleListImports(c *echo.Context, body []byte) error {
	var in listImportsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	list := h.Backend.ListImports()
	filtered := make([]*Import, 0, len(list))

	for _, imp := range list {
		if in.ImportStatus != "" && imp.ImportStatus != in.ImportStatus {
			continue
		}
		if in.Destination != "" && !containsString(imp.Destinations, in.Destination) {
			continue
		}
		filtered = append(filtered, imp)
	}

	p := page.New(filtered, in.NextToken, in.MaxResults, defaultImportsPageSize)
	items := make([]map[string]any, 0, len(p.Data))
	for _, imp := range p.Data {
		items = append(items, map[string]any{
			keyImportID:         imp.ImportID,
			keyImportStatus:     imp.ImportStatus,
			keyCreatedTimestamp: float64(imp.CreatedTimestamp.Unix()),
			keyUpdatedTimestamp: float64(imp.UpdatedTimestamp.Unix()),
		})
	}

	resp := map[string]any{"Imports": items}
	if p.Next != "" {
		resp["NextToken"] = p.Next
	}

	return c.JSON(http.StatusOK, resp)
}

func containsString(list []string, want string) bool {
	return slices.Contains(list, want)
}

// --- StopImport ---

type stopImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleStopImport(c *echo.Context, body []byte) error {
	var in stopImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.StopImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, importToMap(imp))
}

// importToMap converts an Import to the JSON map used in StartImport/GetImport/
// StopImport responses (all three share the same shape per the real SDK).
func importToMap(imp *Import) map[string]any {
	m := map[string]any{
		keyImportID:         imp.ImportID,
		keyImportStatus:     imp.ImportStatus,
		keyDestinations:     imp.Destinations,
		keyCreatedTimestamp: float64(imp.CreatedTimestamp.Unix()),
		keyUpdatedTimestamp: float64(imp.UpdatedTimestamp.Unix()),
	}
	if imp.ImportSource != nil && imp.ImportSource.S3 != nil {
		m["ImportSource"] = map[string]any{
			"S3": map[string]any{
				"S3LocationUri":         imp.ImportSource.S3.S3LocationURI,
				"S3BucketRegion":        imp.ImportSource.S3.S3BucketRegion,
				"S3BucketAccessRoleArn": imp.ImportSource.S3.S3BucketAccessRoleArn,
			},
		}
	}

	return m
}

// --- ListImportFailures ---

type listImportFailuresBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleListImportFailures(c *echo.Context, body []byte) error {
	var in listImportFailuresBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	failures := h.Backend.ListImportFailures(in.ImportID)

	return c.JSON(http.StatusOK, map[string]any{"Failures": failures})
}
