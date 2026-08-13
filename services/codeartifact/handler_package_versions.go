package codeartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func packageVersionToMap(pv *PackageVersion) map[string]any {
	m := map[string]any{
		keyVersion:      pv.Version,
		keyStatusField:  pv.Status,
		"format":        pv.Format,
		"packageName":   pv.PackageName,
		"publishedTime": epochSeconds(pv.PublishedAt),
		keyRevision:     pv.Revision,
	}
	if pv.Namespace != "" {
		m["namespace"] = pv.Namespace
	}

	return m
}

func (h *Handler) handleDescribePackageVersion(
	c *echo.Context,
	domainName, repoName, format, namespace, name, version string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}
	if version == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "version is required"))
	}

	pv, err := h.Backend.DescribePackageVersion(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"packageVersion": packageVersionToMap(pv),
	})
}

type deletePackageVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleDeletePackageVersions(
	c *echo.Context,
	domainName, repoName, format, namespace, name string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in deletePackageVersionsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	failed, err := h.Backend.DeletePackageVersions(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		in.Versions,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	failedList := make([]map[string]string, 0, len(failed))
	for v, code := range failed {
		failedList = append(failedList, map[string]string{keyVersion: v, "errorCode": code})
	}

	successList := make([]map[string]string, 0, len(in.Versions))
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{keyVersion: v, keyStatusField: "Deleted"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
	})
}

type copyPackageVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleCopyPackageVersions(
	c *echo.Context,
	domainName, srcRepo, dstRepo, format, namespace, name string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if srcRepo == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "sourceRepository is required"))
	}
	if dstRepo == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "destinationRepository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in copyPackageVersionsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
		}
	}

	failed, err := h.Backend.CopyPackageVersions(
		c.Request().Context(),
		domainName,
		srcRepo,
		dstRepo,
		format,
		namespace,
		name,
		in.Versions,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	failedList := make([]map[string]string, 0, len(failed))
	for v, code := range failed {
		failedList = append(failedList, map[string]string{keyVersion: v, "errorCode": code})
	}

	successList := make([]map[string]string, 0, len(in.Versions))
	for _, v := range in.Versions {
		if _, ok := failed[v]; !ok {
			successList = append(successList, map[string]string{keyVersion: v, keyStatusField: "Copied"})
		}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
	})
}

type disposeVersionsBody struct {
	Versions []string `json:"versions"`
}

func (h *Handler) handleDisposePackageVersions(
	c *echo.Context, domainName, repoName, format, namespace, name string, body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in disposeVersionsBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	results, err := h.Backend.DisposePackageVersions(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		in.Versions,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: results, keyFailedVersions: map[string]any{}})
}

func (h *Handler) handleGetPackageVersionAsset(
	c *echo.Context, domainName, repoName, format, namespace, name, version, asset string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}
	if version == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "version is required"))
	}
	if asset == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "asset is required"))
	}

	data, err := h.Backend.GetPackageVersionAsset(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
		asset,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

func (h *Handler) validatePackageVersionParams(
	c *echo.Context,
	domainName, repoName, format, name, version string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}
	if version == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "version is required"))
	}

	return nil
}

// handleGetPackageVersionReadme builds GetPackageVersionReadmeOutput's wire
// shape -- verified against aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeOpDocumentGetPackageVersionReadmeOutput
// ({"format","namespace","package","readme","version","versionRevision"}).
func (h *Handler) handleGetPackageVersionReadme(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	readme, pv, err := h.Backend.GetPackageVersionReadme(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyFormat:         pv.Format,
		keyPackageKey:     pv.PackageName,
		keyVersion:        pv.Version,
		"readme":          readme,
		"versionRevision": pv.Revision,
	}
	if pv.Namespace != "" {
		resp["namespace"] = pv.Namespace
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListPackageVersionAssets(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	assets, err := h.Backend.ListPackageVersionAssets(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(assets))
	for _, a := range assets {
		items = append(items, assetSummaryToMap(a))
	}

	return c.JSON(http.StatusOK, map[string]any{"assets": items})
}

// assetSummaryToMap builds the wire shape of AssetSummary -- verified against
// aws-sdk-go-v2 deserializers.go's awsRestjson1_deserializeDocumentAssetSummary.

func assetSummaryToMap(a AssetInfo) map[string]any {
	return map[string]any{
		"name": a.Name,
		"size": a.Size,
		"hashes": map[string]string{
			"SHA256": a.SHA256,
		},
	}
}

// packageDependencyToMap builds a PackageDependency wire object -- verified
// against aws-sdk-go-v2 types.PackageDependency
// ({"dependencyType","namespace","package","versionRequirement"}).
func packageDependencyToMap(d PackageDependencyInfo) map[string]any {
	m := map[string]any{
		"dependencyType":     d.DependencyType,
		keyPackageKey:        d.PackageName,
		"versionRequirement": d.VersionRequirement,
	}
	if d.Namespace != "" {
		m["namespace"] = d.Namespace
	}

	return m
}

// handleListPackageVersionDependencies builds
// ListPackageVersionDependenciesOutput's wire shape -- verified against
// aws-sdk-go-v2 deserializers.go's
// awsRestjson1_deserializeOpDocumentListPackageVersionDependenciesOutput
// ({"dependencies","format","namespace","package","version"}).
func (h *Handler) handleListPackageVersionDependencies(
	c *echo.Context, domainName, repoName, format, namespace, name, version string,
) error {
	if err := h.validatePackageVersionParams(c, domainName, repoName, format, name, version); err != nil {
		return err
	}

	deps, err := h.Backend.ListPackageVersionDependencies(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	items := make([]map[string]any, 0, len(deps))
	for _, d := range deps {
		items = append(items, packageDependencyToMap(d))
	}

	resp := map[string]any{
		"dependencies": items,
		keyFormat:      format,
		keyPackageKey:  name,
		keyVersion:     version,
	}
	if namespace != "" {
		resp["namespace"] = namespace
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListPackageVersions(
	c *echo.Context, domainName, repoName, format, namespace, name string,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	q := c.Request().URL.Query()
	maxResults := parseMaxResults(q.Get("max-results"))
	nextToken := q.Get("next-token")

	all, err := h.Backend.ListPackageVersions(c.Request().Context(), domainName, repoName, format, namespace, name)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(pv *PackageVersion) string { return pv.Version })

	items := make([]map[string]any, 0, len(page))
	for _, pv := range page {
		items = append(items, packageVersionToMap(pv))
	}

	resp := map[string]any{"versions": items, "package": name, "format": format}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handlePublishPackageVersion(
	c *echo.Context,
	domainName, repoName, format, namespace, name, version, assetName, assetSHA256 string,
	body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}
	if version == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "version is required"))
	}
	if assetName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "asset is required"))
	}
	if assetSHA256 == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "X-Amz-Content-Sha256 header is required"))
	}

	sum := sha256.Sum256(body)
	computedSHA256 := hex.EncodeToString(sum[:])

	if !strings.EqualFold(assetSHA256, computedSHA256) {
		// The pinned SDK (codeartifact@v1.41.4) declares no MismatchedSha256Exception
		// for this op -- only AccessDeniedException/ConflictException/
		// InternalServerException/ResourceNotFoundException/
		// ServiceQuotaExceededException/ThrottlingException/ValidationException
		// (verified against deserializers.go's awsRestjson1_deserializeOpErrorPublishPackageVersion).
		return c.JSON(
			http.StatusBadRequest,
			errResp("ValidationException", "assetSHA256 does not match the computed SHA256 of assetContent"),
		)
	}

	asset := AssetInfo{
		Name:    assetName,
		Size:    int64(len(body)),
		SHA256:  computedSHA256,
		Content: body,
	}

	pv, err := h.Backend.PublishPackageVersion(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		version,
		asset,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, publishPackageVersionToMap(pv, asset))
}

// publishPackageVersionToMap builds PublishPackageVersionOutput's wire shape -- a FLAT
// object (no "packageVersion" envelope), with field names "package"/"versionRevision"
// (not "packageName"/"revision") and an "asset" summary. Verified against aws-sdk-go-v2
// deserializers.go's awsRestjson1_deserializeOpDocumentPublishPackageVersionOutput.

func publishPackageVersionToMap(pv *PackageVersion, asset AssetInfo) map[string]any {
	m := map[string]any{
		keyFormat:         pv.Format,
		keyPackageKey:     pv.PackageName,
		keyStatusField:    pv.Status,
		keyVersion:        pv.Version,
		"versionRevision": pv.Revision,
		"asset":           assetSummaryToMap(asset),
	}
	if pv.Namespace != "" {
		m["namespace"] = pv.Namespace
	}

	return m
}

// putPackageOriginConfigurationBody is PutPackageOriginConfigurationInput's request
// shape -- verified against aws-sdk-go-v2 serializers.go's
// awsRestjson1_serializeOpDocumentPutPackageOriginConfigurationInput.

type updateVersionsStatusBody struct {
	TargetStatus string   `json:"targetStatus"`
	Versions     []string `json:"versions"`
}

func (h *Handler) handleUpdatePackageVersionsStatus(
	c *echo.Context, domainName, repoName, format, namespace, name string, body []byte,
) error {
	if domainName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "domain is required"))
	}
	if repoName == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "repository is required"))
	}
	if format == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "format is required"))
	}
	if name == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "package is required"))
	}

	var in updateVersionsStatusBody
	if len(body) > 0 {
		_ = json.Unmarshal(body, &in)
	}

	if in.TargetStatus == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "targetStatus is required"))
	}

	results, err := h.Backend.UpdatePackageVersionsStatus(
		c.Request().Context(), domainName, repoName, format, namespace, name, in.TargetStatus, in.Versions,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: results, keyFailedVersions: map[string]any{}})
}

// updateRepositoryBody's Upstreams field uses the wire key "upstreams", same as
// createRepositoryBody -- see its comment for the verified source.
