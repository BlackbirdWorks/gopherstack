package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// maxPackagePathSegments is the number of segments in /packages/{pkg}/versions/{ver}/sbom.
const maxPackagePathSegments = 6

// packageVersionPartsMin is the minimum number of split parts to extract package/version from the path.
const packageVersionPartsMin = 3

func (h *Handler) handleAssociateSbomWithPackageVersion(c *echo.Context) error {
	// Path: /packages/{packageName}/versions/{versionName}/sbom
	parts := strings.SplitN(
		strings.TrimPrefix(c.Request().URL.Path, "/packages/"),
		"/",
		maxPackagePathSegments,
	)

	var packageName, versionName string
	// len(parts) >= packageVersionPartsMin guarantees indices 0, 1, 2 are valid.
	if len(parts) >= packageVersionPartsMin {
		packageName = parts[0]
		versionName = parts[2]
	}

	var body struct {
		Sbom *SbomDocument `json:"sbom"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	out, err := h.Backend.AssociateSbomWithPackageVersion(&AssociateSbomWithPackageVersionInput{
		PackageName: packageName,
		VersionName: versionName,
		Sbom:        body.Sbom,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, out)
}

// resolvePackageVersionOps resolves ops on /packages/{name}/versions[/{versionName}].
func resolvePackageVersionOps(parts []string, method string) string {
	if len(parts) == pathSplitTwo && parts[1] == pathSegmentVersions && method == http.MethodGet {
		return opListPackageVersions
	}
	if len(parts) < pathSplitThree || parts[1] != pathSegmentVersions {
		return unknownOperation
	}
	// parts[2] may be "1.0.0" or "1.0.0/sbom" or "1.0.0/sbom-validation-results"
	// Split parts[2] further to detect sub-paths.
	versionAndSub := strings.SplitN(parts[2], "/", pathSplitTwo)
	if len(versionAndSub) == pathSplitTwo {
		// Has sub-path beyond the version name.
		return resolvePackageVersionSubPathOps(versionAndSub[1], method)
	}

	return resolvePackageVersionCrudOps(method)
}

// resolvePackageVersionSubPathOps resolves the sbom / sbom-validation-results sub-routes
// under /packages/{name}/versions/{version}/....
func resolvePackageVersionSubPathOps(sub, method string) string {
	switch sub {
	case "sbom":
		if method == http.MethodPut {
			return opAssociateSbomWithPackageVersion
		}
		if method == http.MethodDelete {
			return opDisassociateSbomFromPackageVersion
		}
	case "sbom-validation-results":
		if method == http.MethodGet {
			return opListSbomValidationResults
		}
	}

	return unknownOperation
}

// resolvePackageVersionCrudOps resolves the plain /packages/{name}/versions/{version} CRUD routes.
func resolvePackageVersionCrudOps(method string) string {
	switch method {
	case http.MethodPut:
		return opCreatePackageVersion
	case http.MethodGet:
		return opGetPackageVersion
	case http.MethodDelete:
		return opDeletePackageVersion
	case http.MethodPatch:
		return opUpdatePackageVersion
	}

	return unknownOperation
}

func resolvePackageOps(path, method string) string {
	switch {
	case path == "/packages" && method == http.MethodGet:
		return opListPackages
	case path == pathPackageConfig && method == http.MethodGet:
		return opGetPackageConfiguration
	case path == pathPackageConfig && method == http.MethodPatch:
		return opUpdatePackageConfiguration
	}

	// /packages/{name}
	rest, ok := strings.CutPrefix(path, "/packages/")
	if !ok {
		return unknownOperation
	}
	parts := strings.SplitN(rest, "/", pathSplitThree)
	if len(parts) == 1 {
		switch method {
		case http.MethodPut:
			return opCreatePackage
		case http.MethodGet:
			return opGetPackage
		case http.MethodDelete:
			return opDeletePackage
		case http.MethodPatch:
			return opUpdatePackage
		}
	}

	return resolvePackageVersionOps(parts, method)
}

func (h *Handler) handleCreatePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	var req struct {
		Tags        map[string]string `json:"tags"`
		Description string            `json:"description"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	p, err := h.Backend.CreateIoTPackage(name, req.Description, req.Tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleGetPackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	p, err := h.Backend.GetIoTPackage(name)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, p)
}

func (h *Handler) handleUpdatePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	var req struct {
		Description        string `json:"description"`
		DefaultVersionName string `json:"defaultVersionName"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateIoTPackage(name, req.Description, req.DefaultVersionName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePackage(c *echo.Context) error {
	name := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	if err := h.Backend.DeleteIoTPackage(name); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// packageSummaryFields renders the fields of p that types.PackageSummary
// (types.go:3386-3401, iot@v1.77.4) declares: CreationDate,
// DefaultVersionName, LastModifiedDate, PackageName.
func packageSummaryFields(p *IoTPackage) map[string]any {
	return map[string]any{
		keyCreationDate:      p.CreationDate,
		"defaultVersionName": p.DefaultVersionName,
		"lastModifiedDate":   p.LastModifiedDate,
		"packageName":        p.PackageName,
	}
}

func (h *Handler) handleListPackages(c *echo.Context) error {
	items := h.Backend.ListIoTPackages()
	out := make([]map[string]any, 0, len(items))
	for _, p := range items {
		out = append(out, packageSummaryFields(p))
	}

	// Real ListPackagesOutput wraps the list under "packageSummaries"
	// (deserializers.go: awsRestjson1_deserializeOpDocumentListPackagesOutput),
	// not "packageList".
	return c.JSON(http.StatusOK, map[string]any{"packageSummaries": out})
}

func packageAndVersion(path string) (string, string) {
	// /packages/{name}/versions/{versionName}
	trimmed := strings.TrimPrefix(path, "/packages/")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)
	if len(parts) == pathSplitTwo {
		return parts[0], parts[1]
	}

	return parts[0], ""
}

func (h *Handler) handleCreatePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	var req struct {
		Tags        map[string]string       `json:"tags"`
		Attributes  map[string]string       `json:"attributes"`
		Artifact    *PackageVersionArtifact `json:"artifact"`
		Description string                  `json:"description"`
		Recipe      string                  `json:"recipe"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	v, err := h.Backend.CreateIoTPackageVersion(pkgName, versionName, req.Description, req.Tags,
		CreateIoTPackageVersionOptions{
			Attributes: req.Attributes,
			Artifact:   req.Artifact,
			Recipe:     req.Recipe,
		})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleGetPackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	v, err := h.Backend.GetIoTPackageVersion(pkgName, versionName)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, v)
}

func (h *Handler) handleUpdatePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	var req struct {
		Description string `json:"description"`
		Status      string `json:"status"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdateIoTPackageVersion(pkgName, versionName, req.Description, req.Status); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeletePackageVersion(c *echo.Context) error {
	pkgName, versionName := packageAndVersion(c.Request().URL.Path)
	if err := h.Backend.DeleteIoTPackageVersion(pkgName, versionName); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// packageVersionSummaryFields renders the fields of v that
// types.PackageVersionSummary (types.go:3413-3433, iot@v1.77.4) declares:
// CreationDate, LastModifiedDate, PackageName, Status, VersionName.
func packageVersionSummaryFields(v *IoTPackageVersion) map[string]any {
	return map[string]any{
		keyCreationDate:    v.CreationDate,
		"lastModifiedDate": v.LastModifiedDate,
		"packageName":      v.PackageName,
		keyStatus:          v.Status,
		"versionName":      v.VersionName,
	}
}

func (h *Handler) handleListPackageVersions(c *echo.Context) error {
	// /packages/{name}/versions
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	pkgName := strings.TrimSuffix(trimmed, "/versions")
	items := h.Backend.ListIoTPackageVersions(pkgName)
	out := make([]map[string]any, 0, len(items))
	for _, v := range items {
		out = append(out, packageVersionSummaryFields(v))
	}

	// Real ListPackageVersionsOutput wraps the list under
	// "packageVersionSummaries"
	// (deserializers.go: awsRestjson1_deserializeOpDocumentListPackageVersionsOutput),
	// not "packageVersionList".
	return c.JSON(http.StatusOK, map[string]any{"packageVersionSummaries": out})
}

func (h *Handler) handleGetPackageConfiguration(c *echo.Context) error {
	cfg := h.Backend.GetPackageConfiguration()

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleUpdatePackageConfiguration(c *echo.Context) error {
	var req struct {
		VersionUpdateByJobsConfig map[string]any `json:"versionUpdateByJobsConfig"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.UpdatePackageConfiguration(req.VersionUpdateByJobsConfig); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDisassociateSbomFromPackageVersion(c *echo.Context) error {
	// DELETE /packages/{packageName}/versions/{versionName}/sbom
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	trimmed = strings.TrimSuffix(trimmed, "/sbom")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, keyInvalidPath})
	}

	if err := h.Backend.DisassociateSbomFromPackageVersion(parts[0], parts[1]); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListSbomValidationResults(c *echo.Context) error {
	// GET /packages/{packageName}/versions/{versionName}/sbom-validation-results
	trimmed := strings.TrimPrefix(c.Request().URL.Path, "/packages/")
	trimmed = strings.TrimSuffix(trimmed, "/sbom-validation-results")
	parts := strings.SplitN(trimmed, "/versions/", pathSplitTwo)

	if len(parts) != pathSplitTwo {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, keyInvalidPath})
	}

	maxResults := parseInt32QueryParam(c, "maxResults")
	nextToken := c.QueryParam("nextToken")

	results, next, err := h.Backend.ListSbomValidationResults(parts[0], parts[1], maxResults, nextToken)
	if err != nil {
		return respondErr(c, err)
	}

	resp := map[string]any{"validationResultSummaries": results}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) dispatchPackageOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreatePackage:
		return true, h.handleCreatePackage(c)
	case opGetPackage:
		return true, h.handleGetPackage(c)
	case opUpdatePackage:
		return true, h.handleUpdatePackage(c)
	case opDeletePackage:
		return true, h.handleDeletePackage(c)
	case opListPackages:
		return true, h.handleListPackages(c)
	case opGetPackageConfiguration:
		return true, h.handleGetPackageConfiguration(c)
	case opUpdatePackageConfiguration:
		return true, h.handleUpdatePackageConfiguration(c)
	}

	return false, nil
}

func (h *Handler) dispatchPackageVersionOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opCreatePackageVersion:
		return true, h.handleCreatePackageVersion(c)
	case opGetPackageVersion:
		return true, h.handleGetPackageVersion(c)
	case opUpdatePackageVersion:
		return true, h.handleUpdatePackageVersion(c)
	case opDeletePackageVersion:
		return true, h.handleDeletePackageVersion(c)
	case opListPackageVersions:
		return true, h.handleListPackageVersions(c)
	}

	return false, nil
}
