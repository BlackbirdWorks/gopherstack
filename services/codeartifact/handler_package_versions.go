package codeartifact

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
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
	m["origin"] = packageVersionOriginToMap(pv)
	addNpmDescriptionFields(m, pv)

	return m
}

// addNpmDescriptionFields adds displayName and the package.json-derived summary, homePage,
// sourceCodeRepository and licenses; the field-to-key mapping is not SDK-documented.
func addNpmDescriptionFields(m map[string]any, pv *PackageVersion) {
	if pv.Format != "npm" {
		return
	}
	m["displayName"] = pv.PackageName
	if pv.Namespace != "" {
		m["displayName"] = "@" + pv.Namespace + "/" + pv.PackageName
	}
	meta := findPackageJSONMetadata(pv.Assets)
	if meta == nil {
		return
	}
	if meta.Description != "" {
		m["summary"] = meta.Description
	}
	if meta.Homepage != "" {
		m["homePage"] = meta.Homepage
	}
	if u := meta.repositoryURL(); u != "" {
		m["sourceCodeRepository"] = u
	}
	if l := meta.licenseName(); l != "" {
		m["licenses"] = []map[string]any{{"name": l}}
	}
}

func packageVersionOriginToMap(pv *PackageVersion) map[string]any {
	o := map[string]any{"originType": versionOriginType(pv)}
	if pv.OriginRepository != "" {
		o["domainEntryPoint"] = map[string]any{"repositoryName": pv.OriginRepository}
	}

	return o
}

// packageVersionSummaryToMap builds the types.PackageVersionSummary shape
// (types.go:547) -- no format, packageName, publishedTime, or namespace,
// all of which are Get-only (types.PackageVersionDescription, not
// types.PackageVersionSummary; confirmed against
// awsRestjson1_deserializeDocumentPackageVersionSummary, which recognises
// only origin/revision/status/version).
func packageVersionSummaryToMap(pv *PackageVersion) map[string]any {
	return map[string]any{
		keyVersion:     pv.Version,
		keyStatusField: pv.Status,
		keyRevision:    pv.Revision,
		"origin":       packageVersionOriginToMap(pv),
	}
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

// packageVersionOutcomesToWire builds the failedVersions/successfulVersions wire
// values shared by DeletePackageVersions/CopyPackageVersions/DisposePackageVersions/
// UpdatePackageVersionsStatus -- both are real JSON *objects* keyed by version
// string (map[string]types.PackageVersionError / map[string]types.SuccessfulPackageVersionInfo),
// confirmed against aws-sdk-go-v2 deserializers.go's
// ...PackageVersionErrorMap/...SuccessfulPackageVersionInfoMap -- NOT an array.
func packageVersionOutcomesToWire(
	successful map[string]PackageVersionOutcome, failed map[string]string,
) (map[string]any, map[string]any) {
	successList := make(map[string]any, len(successful))
	for v, outcome := range successful {
		successList[v] = map[string]any{"revision": outcome.Revision, keyStatusField: outcome.Status}
	}

	failedList := make(map[string]any, len(failed))
	for v, code := range failed {
		failedList[v] = map[string]any{"errorCode": code, "errorMessage": packageVersionErrorMessage(code, v)}
	}

	return successList, failedList
}

func packageVersionErrorMessage(code, version string) string {
	switch code {
	case packageVersionErrorNotFound:
		return "package version " + version + " was not found"
	case packageVersionErrorAlreadyExists:
		return "package version " + version + " already exists in the destination repository"
	case packageVersionErrorMismatchedRev:
		return "the revision of package version " + version + " does not match the expected revision"
	case packageVersionErrorMismatchedStatus:
		return "the status of package version " + version + " does not match the expected status"
	default:
		return "package version " + version + " could not be processed: " + code
	}
}

type deletePackageVersionsBody struct {
	ExpectedStatus string   `json:"expectedStatus"`
	Versions       []string `json:"versions"`
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

	successful, failed, err := h.Backend.DeletePackageVersions(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		VersionSelector{Versions: in.Versions, ExpectedStatus: in.ExpectedStatus},
	)
	if err != nil {
		return h.handleError(c, err)
	}

	successList, failedList := packageVersionOutcomesToWire(successful, failed)

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
	})
}

type copyPackageVersionsBody struct {
	IncludeFromUpstream *bool             `json:"includeFromUpstream"`
	AllowOverwrite      *bool             `json:"allowOverwrite"`
	VersionRevisions    map[string]string `json:"versionRevisions"`
	Versions            []string          `json:"versions"`
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

	successful, failed, err := h.Backend.CopyPackageVersions(
		c.Request().Context(),
		domainName,
		srcRepo,
		dstRepo,
		format,
		namespace,
		name,
		VersionSelector{Versions: in.Versions, Revisions: in.VersionRevisions},
		in.IncludeFromUpstream != nil && *in.IncludeFromUpstream,
		in.AllowOverwrite != nil && *in.AllowOverwrite,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	successList, failedList := packageVersionOutcomesToWire(successful, failed)

	return c.JSON(http.StatusOK, map[string]any{
		keyFailedVersions:     failedList,
		keySuccessfulVersions: successList,
	})
}

type disposeVersionsBody struct {
	VersionRevisions map[string]string `json:"versionRevisions"`
	ExpectedStatus   string            `json:"expectedStatus"`
	Versions         []string          `json:"versions"`
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

	successful, failed, err := h.Backend.DisposePackageVersions(
		c.Request().Context(),
		domainName,
		repoName,
		format,
		namespace,
		name,
		VersionSelector{Versions: in.Versions, Revisions: in.VersionRevisions, ExpectedStatus: in.ExpectedStatus},
	)
	if err != nil {
		return h.handleError(c, err)
	}

	successList, failedList := packageVersionOutcomesToWire(successful, failed)

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: successList, keyFailedVersions: failedList})
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

	data, pv, err := h.Backend.GetPackageVersionAsset(
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

	// AssetName/PackageVersion/PackageVersionRevision are response HEADERS on
	// the real GetPackageVersionAssetOutput, not body fields -- see
	// InMemoryBackend.GetPackageVersionAsset's doc comment.
	c.Response().Header().Set("X-Assetname", asset)
	c.Response().Header().Set("X-Packageversion", pv.Version)
	c.Response().Header().Set("X-Packageversionrevision", pv.Revision)

	return c.Blob(http.StatusOK, "application/octet-stream", data)
}

// validatePackageVersionParams returns the raw, unwritten validation error so
// callers can map and write it exactly once via h.handleError. It used to
// write its own 400 body via c.JSON and return that call's (always-nil, on a
// successful write) result directly; callers stored that nil in err and
// tested it before continuing, so the rejection was silently treated as
// success and the real (read-only) Backend method still ran, writing a
// second body on top of the committed one (gopherstack-7opw, the
// gopherstack-8haq shape).
func (h *Handler) validatePackageVersionParams(
	domainName, repoName, format, name, version string,
) error {
	if domainName == "" {
		return fmt.Errorf("%w: domain is required", ErrValidation)
	}
	if repoName == "" {
		return fmt.Errorf("%w: repository is required", ErrValidation)
	}
	if format == "" {
		return fmt.Errorf("%w: format is required", ErrValidation)
	}
	if name == "" {
		return fmt.Errorf("%w: package is required", ErrValidation)
	}
	if version == "" {
		return fmt.Errorf("%w: version is required", ErrValidation)
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
	if err := h.validatePackageVersionParams(domainName, repoName, format, name, version); err != nil {
		return h.handleError(c, err)
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
	if err := h.validatePackageVersionParams(domainName, repoName, format, name, version); err != nil {
		return h.handleError(c, err)
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

	slices.SortFunc(assets, func(x, y AssetInfo) int { return strings.Compare(x.Name, y.Name) })

	q := c.Request().URL.Query()
	page, next := paginateSlice(assets, parseMaxResults(q.Get("max-results")), q.Get("next-token"),
		func(a AssetInfo) string { return a.Name })

	items := make([]map[string]any, 0, len(page))
	for _, a := range page {
		items = append(items, assetSummaryToMap(a))
	}

	return c.JSON(http.StatusOK, withNextToken(map[string]any{"assets": items}, next))
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
	if err := h.validatePackageVersionParams(domainName, repoName, format, name, version); err != nil {
		return h.handleError(c, err)
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

	depKey := func(d PackageDependencyInfo) string {
		return d.DependencyType + "/" + d.Namespace + "/" + d.PackageName
	}

	slices.SortFunc(deps, func(x, y PackageDependencyInfo) int { return strings.Compare(depKey(x), depKey(y)) })

	q := c.Request().URL.Query()
	page, next := paginateSlice(deps, parseMaxResults(q.Get("max-results")), q.Get("next-token"), depKey)

	items := make([]map[string]any, 0, len(page))
	for _, d := range page {
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

	return c.JSON(http.StatusOK, withNextToken(resp, next))
}

func withNextToken(resp map[string]any, token string) map[string]any {
	if token != "" {
		resp["nextToken"] = token
	}

	return resp
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
	status := q.Get("status")
	sortBy := q.Get("sortBy")
	originType := q.Get("originType")
	if originType != "" && !validOriginType(originType) {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid originType"))
	}

	all, err := h.Backend.ListPackageVersions(
		c.Request().Context(), domainName, repoName, format, namespace, name, status, sortBy, originType,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	page, next := paginateSlice(all, maxResults, nextToken, func(pv *PackageVersion) string { return pv.Version })

	items := make([]map[string]any, 0, len(page))
	for _, pv := range page {
		items = append(items, packageVersionSummaryToMap(pv))
	}

	resp := map[string]any{"versions": items, "package": name, "format": format}
	if namespace != "" {
		resp["namespace"] = namespace
	}
	if next != "" {
		resp["nextToken"] = next
	}
	// defaultDisplayVersion is real (api_op_ListPackageVersions.go) -- AWS's
	// doc says "most recently published" for every format except npm with a
	// dist-tag set, and this backend has no dist-tag concept at all, so
	// most-recently-published is the correct fallback in every case here,
	// not an approximation.
	if dv := mostRecentlyPublished(all); dv != "" {
		resp["defaultDisplayVersion"] = dv
	}

	return c.JSON(http.StatusOK, resp)
}

// mostRecentlyPublished returns the Version of the PackageVersion with the
// latest PublishedAt in versions, or "" if versions is empty.
func mostRecentlyPublished(versions []*PackageVersion) string {
	var latest *PackageVersion
	for _, pv := range versions {
		if latest == nil || pv.PublishedAt.After(latest.PublishedAt) {
			latest = pv
		}
	}
	if latest == nil {
		return ""
	}

	return latest.Version
}

func (h *Handler) handlePublishPackageVersion(
	c *echo.Context,
	domainName, repoName, format, namespace, name, version, assetName, assetSHA256 string,
	unfinished bool,
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
		unfinished,
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
	VersionRevisions map[string]string `json:"versionRevisions"`
	TargetStatus     string            `json:"targetStatus"`
	ExpectedStatus   string            `json:"expectedStatus"`
	Versions         []string          `json:"versions"`
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

	successful, failed, err := h.Backend.UpdatePackageVersionsStatus(
		c.Request().Context(), domainName, repoName, format, namespace, name, in.TargetStatus,
		VersionSelector{Versions: in.Versions, Revisions: in.VersionRevisions, ExpectedStatus: in.ExpectedStatus},
	)
	if err != nil {
		return h.handleError(c, err)
	}

	successList, failedList := packageVersionOutcomesToWire(successful, failed)

	return c.JSON(http.StatusOK, map[string]any{keySuccessfulVersions: successList, keyFailedVersions: failedList})
}

// updateRepositoryBody's Upstreams field uses the wire key "upstreams", same as
// createRepositoryBody -- see its comment for the verified source.
