package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// resolvePolicyVersionOps resolves DetachPolicy's non-canonical DELETE
// route (see resolvePolicyAndCertOps's doc comment for the real POST one)
// and ListAttachedPolicies.
//
// ListAttachedPolicies' real path is POST /attached-policies/{target}
// (iot@v1.77.4 serializers.go), not the bare /attached-policies gopherstack
// previously used -- found unreachable by gopherstack-n1mb's route table.
// The bare path is kept too as a non-canonical route wired for this
// package's own tests.
func resolvePolicyVersionOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodDelete:

		return opDetachPolicy
	case path == "/attached-policies" && method == http.MethodPost:

		return opListAttachedPolicies
	case strings.HasPrefix(path, "/attached-policies/") && method == http.MethodPost:

		return opListAttachedPolicies
	}

	return resolvePolicyVersionSubOps(path, method)
}

// resolvePolicyVersionSubOps resolves the policy-version op family.
//
// The real wire shape (iot@v1.77.4 serializers.go) uses the SINGULAR
// "/policies/{policyName}/version[/{policyVersionId}]" for every op in this
// family, including SetDefaultPolicyVersion (PATCH on the same
// .../version/{id} path Get/Delete use -- no "/default" suffix at all) --
// gopherstack previously used a fictional PLURAL "/versions" shape
// throughout, plus an invented "/default" suffix for SetDefault, so this
// entire family was unreachable by a real client. Found by
// gopherstack-n1mb's route table. The old plural/"/default" shapes are kept
// too as non-canonical routes wired for this package's own tests.
func resolvePolicyVersionSubOps(path, method string) string {
	if !strings.HasPrefix(path, "/policies/") {
		return unknownOperation
	}

	shape := classifyPolicyVersionPath(path)

	return resolvePolicyVersionByMethod(method, shape)
}

// policyVersionPathShape describes the structural features of a
// /policies/{name}/version... path, real or legacy, that
// resolvePolicyVersionByMethod needs to pick an op.
type policyVersionPathShape struct {
	hasVersionID bool
	isCollection bool
	endsDefault  bool
}

func classifyPolicyVersionPath(path string) policyVersionPathShape {
	return policyVersionPathShape{
		hasVersionID: strings.Contains(path, "/version/") || strings.Contains(path, "/versions/"),
		isCollection: strings.HasSuffix(path, "/version") || strings.HasSuffix(path, "/versions"),
		endsDefault:  strings.HasSuffix(path, "/default"),
	}
}

func resolvePolicyVersionByMethod(method string, shape policyVersionPathShape) string {
	switch method {
	case http.MethodGet:
		if shape.hasVersionID {
			return opGetPolicyVersion
		}

		if shape.isCollection {
			return opListPolicyVersions
		}
	case http.MethodDelete:
		if shape.hasVersionID {
			return opDeletePolicyVersion
		}
	case http.MethodPatch:
		// Real SetDefaultPolicyVersion PATCHes the same .../version/{id}
		// path Get/Delete use; the "/default" suffix is the non-canonical
		// legacy shape.
		if shape.hasVersionID || shape.endsDefault {
			return opSetDefaultPolicyVersion
		}
	case http.MethodPost:
		if shape.isCollection {
			return opCreatePolicyVersion
		}
	}

	return unknownOperation
}

func (h *Handler) dispatchPolicyOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opAttachPrincipalPolicy:

		return true, h.handleAttachPrincipalPolicy(c)
	case opCreatePolicy:

		return true, h.handleCreatePolicy(c)
	case opGetPolicy:

		return true, h.handleGetPolicy(c)
	case opDeletePolicy:

		return true, h.handleDeletePolicy(c)
	case opListPolicies:

		return true, h.handleListPolicies(c)
	case opDescribeEndpoint:

		return true, h.handleDescribeEndpoint(c)
	}

	return false, nil
}

func (h *Handler) dispatchPolicyVersionOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opDetachPolicy:

		return true, h.handleDetachPolicy(c)
	case opListAttachedPolicies:

		return true, h.handleListAttachedPolicies(c)
	case opCreatePolicyVersion:

		return true, h.handleCreatePolicyVersion(c)
	case opGetPolicyVersion:

		return true, h.handleGetPolicyVersion(c)
	case opListPolicyVersions:

		return true, h.handleListPolicyVersions(c)
	case opDeletePolicyVersion:

		return true, h.handleDeletePolicyVersion(c)
	case opSetDefaultPolicyVersion:

		return true, h.handleSetDefaultPolicyVersion(c)
	}

	return false, nil
}

// handleAttachPrincipalPolicy reads the policy name from the real
// "/principal-policies/{policyName}" path and the principal from the real
// "X-Amzn-Iot-Principal" header (iot@v1.77.4 serializers.go:668-669) --
// gopherstack previously stripped "/target-policies/" (DetachPolicy's real
// path, a genuine op-name mix-up; see resolvePolicyAndCertOps) and read the
// wrong header. Fixed by gopherstack-n1mb's route table.
func (h *Handler) handleAttachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, pathPrincipalPolicies+"/")
	principal := c.Request().Header.Get("X-Amzn-Iot-Principal")

	if err := h.Backend.AttachPrincipalPolicy(&AttachPrincipalPolicyInput{
		PolicyName: policyName,
		Principal:  principal,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleCreatePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
		// []types.Tag on the wire, not a map (serializers.go:3804, aws-sdk-go-v2/service/iot@v1.77.4).
		Tags []tags.KV `json:"tags,omitempty"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	out, err := h.Backend.CreatePolicy(&CreatePolicyInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
		Tags:           tags.MapFromKV(body.Tags),
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		keyPolicyName:      out.PolicyName,
		keyPolicyArn:       out.PolicyARN,
		keyPolicyDocument:  out.PolicyDocument,
		keyPolicyVersionID: out.PolicyVersionID,
	})
}

func (h *Handler) handleAttachPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")

	var body struct {
		Target string `json:"target"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	if err := h.Backend.AttachPolicy(&AttachPolicyInput{
		PolicyName: policyName,
		Target:     body.Target,
	}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleGetPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	out, err := h.Backend.GetPolicy(policyName)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyName:       out.PolicyName,
		keyPolicyArn:        out.PolicyARN,
		keyPolicyDocument:   out.PolicyDocument,
		"defaultVersionId":  out.DefaultVersionID,
		keyCreationDate:     awstime.Epoch(out.CreatedAt),
		keyLastModifiedDate: awstime.Epoch(out.LastModifiedAt),
	})
}

func (h *Handler) handleDeletePolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policies/")

	if err := h.Backend.DeletePolicy(policyName); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListPolicies(c *echo.Context) error {
	policies := h.Backend.ListPolicies()

	// "If true, the results are returned in ascending creation order"
	// (iot@v1.77.4 api_op_ListPolicies.go) -- creation-date order, not the
	// name order ListPolicies() returns by default.
	ascending := c.QueryParam("isAscendingOrder") == keyBoolTrue
	sort.Slice(policies, func(i, j int) bool {
		if ascending {
			return policies[i].CreatedAt.Before(policies[j].CreatedAt)
		}

		return policies[i].CreatedAt.After(policies[j].CreatedAt)
	})

	out := make([]map[string]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, map[string]string{
			keyPolicyName: p.PolicyName,
			keyPolicyArn:  p.ARN,
		})
	}

	// Real binding is pageSize/marker (serializers.go
	// awsRestjson1_serializeOpHttpBindingsListPoliciesInput), not
	// maxResults/nextToken -- a real client's pageSize/marker were
	// previously silently ignored by parseIoTPagination reading the wrong
	// query keys.
	pageSize, start := parseIoTMarkerPagination(c)
	page, nextMarker := paginateMaps(out, pageSize, start)

	resp := map[string]any{"policies": page}
	if nextMarker != "" {
		resp["nextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDetachPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")

	var body struct {
		Target string `json:"target"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	if err := h.Backend.DetachPolicy(&DetachPolicyInput{PolicyName: policyName, Target: body.Target}); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListAttachedPolicies(c *echo.Context) error {
	var body struct {
		Target    string `json:"target"`
		Recursive bool   `json:"recursive"`
	}
	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}
	policies, err := h.Backend.ListAttachedPolicies(
		&ListAttachedPoliciesInput{Target: body.Target, Recursive: body.Recursive},
	)
	if err != nil {
		return h.handleError(c, err)
	}
	out := make([]map[string]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, map[string]string{keyPolicyName: p.PolicyName, keyPolicyArn: p.ARN})
	}

	return c.JSON(http.StatusOK, map[string]any{"policies": out})
}

// policyVersionCollectionName extracts the policy name from a
// "/policies/{name}/version" (real) or "/policies/{name}/versions" (legacy)
// collection path.
func policyVersionCollectionName(path string) string {
	after := strings.TrimPrefix(path, "/policies/")
	after = strings.TrimSuffix(after, "/versions")

	return strings.TrimSuffix(after, "/version")
}

// policyVersionIDParts extracts the policy name and version ID from a
// "/policies/{name}/version/{id}" (real) or "/policies/{name}/versions/{id}"
// (legacy) path, optionally followed by "/default" (the legacy
// SetDefaultPolicyVersion suffix).
func policyVersionIDParts(path string) (string, string, bool) {
	after := strings.TrimPrefix(path, "/policies/")
	after = strings.TrimSuffix(after, "/default")

	sep := "/version/"
	if !strings.Contains(after, sep) {
		sep = "/versions/"
	}

	parts := strings.SplitN(after, sep, maxPathSegments)
	if len(parts) != maxPathSegments {
		return "", "", false
	}

	return parts[0], parts[1], true
}

func (h *Handler) handleCreatePolicyVersion(c *echo.Context) error {
	policyName := policyVersionCollectionName(c.Request().URL.Path)

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, err.Error()})
	}

	setAsDefault := c.QueryParam("setAsDefault") == keyBoolTrue

	pv, err := h.Backend.CreatePolicyVersion(&CreatePolicyVersionInput{
		PolicyName:     policyName,
		PolicyDocument: body.PolicyDocument,
		SetAsDefault:   setAsDefault,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	policyARN := ""
	if policy, lookupErr := h.Backend.GetPolicy(policyName); lookupErr == nil {
		policyARN = policy.PolicyARN
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyVersionID:  pv.VersionID,
		keyPolicyArn:        policyARN,
		keyPolicyDocument:   pv.PolicyDocument,
		keyIsDefaultVersion: pv.IsDefaultVersion,
	})
}

func (h *Handler) handleGetPolicyVersion(c *echo.Context) error {
	policyName, versionID, ok := policyVersionIDParts(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, keyInvalidPath})
	}

	pv, err := h.Backend.GetPolicyVersion(policyName, versionID)
	if err != nil {
		return h.handleError(c, err)
	}

	policy, _ := h.Backend.GetPolicy(policyName)
	policyARN := ""
	if policy != nil {
		policyARN = policy.PolicyARN
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyPolicyName:       policyName,
		keyPolicyArn:        policyARN,
		keyPolicyVersionID:  pv.VersionID,
		keyPolicyDocument:   pv.PolicyDocument,
		keyIsDefaultVersion: pv.IsDefaultVersion,
		"generationId":      pv.GenerationID,
		// GetPolicyVersionOutput's date field is "creationDate", unlike the
		// ListPolicyVersions summary shape's "createDate" a few lines up --
		// verified against v1.77.4's
		// awsRestjson1_deserializeOpDocumentGetPolicyVersionOutput. Policy
		// versions are immutable once created, so lastModifiedDate equals
		// creationDate.
		keyCreationDate:     awstime.Epoch(pv.CreatedAt),
		keyLastModifiedDate: awstime.Epoch(pv.CreatedAt),
	})
}

func (h *Handler) handleListPolicyVersions(c *echo.Context) error {
	policyName := policyVersionCollectionName(c.Request().URL.Path)
	versions, err := h.Backend.ListPolicyVersions(policyName)
	if err != nil {
		return h.handleError(c, err)
	}
	out := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		out = append(out, map[string]any{
			"versionId":         v.VersionID,
			keyIsDefaultVersion: v.IsDefaultVersion,
			"createDate":        awstime.Epoch(v.CreatedAt),
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"policyVersions": out})
}

func (h *Handler) handleDeletePolicyVersion(c *echo.Context) error {
	policyName, versionID, ok := policyVersionIDParts(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, keyInvalidPath})
	}
	if err := h.Backend.DeletePolicyVersion(policyName, versionID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleSetDefaultPolicyVersion(c *echo.Context) error {
	policyName, versionID, ok := policyVersionIDParts(c.Request().URL.Path)
	if !ok {
		return c.JSON(http.StatusBadRequest, awsErrBody{errTypeInvalidRequest, keyInvalidPath})
	}
	if err := h.Backend.SetDefaultPolicyVersion(policyName, versionID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListPrincipalPolicies(c *echo.Context) error {
	// Real wire header is X-Amzn-Iot-Principal (matches its
	// AttachPrincipalPolicy/DetachPrincipalPolicy siblings), not the
	// X-Amzn-Principal used by the Thing-principal family
	// (iot@v1.77.4 serializers.go
	// awsRestjson1_serializeOpHttpBindingsListPrincipalPoliciesInput).
	principal := c.Request().Header.Get("X-Amzn-Iot-Principal")
	policies := h.Backend.ListPrincipalPolicies(principal)

	ascending := c.QueryParam("isAscendingOrder") == keyBoolTrue
	sort.Slice(policies, func(i, j int) bool {
		if ascending {
			return policies[i].CreatedAt.Before(policies[j].CreatedAt)
		}

		return policies[i].CreatedAt.After(policies[j].CreatedAt)
	})

	out := make([]map[string]any, len(policies))
	for i, p := range policies {
		out[i] = map[string]any{
			"policyName": p.PolicyName,
			"policyArn":  p.ARN,
		}
	}

	pageSize, start := parseIoTMarkerPagination(c)
	page, nextMarker := paginateMaps(out, pageSize, start)

	resp := map[string]any{keyPolicies: page}
	if nextMarker != "" {
		resp["nextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListPolicyPrincipals(c *echo.Context) error {
	policyName := c.Request().Header.Get("X-Amzn-Policy-Name")
	principals := h.Backend.ListPolicyPrincipals(policyName)
	if principals == nil {
		principals = []string{}
	}

	pageSize, start := parseIoTMarkerPagination(c)
	page, nextMarker := paginateMaps(principals, pageSize, start)

	resp := map[string]any{"principals": page}
	if nextMarker != "" {
		resp["nextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListTargetsForPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policy-targets/")
	targets := h.Backend.ListTargetsForPolicy(policyName)

	pageSize, start := parseIoTMarkerPagination(c)
	page, nextMarker := paginateMaps(targets, pageSize, start)

	resp := map[string]any{"targets": page}
	if nextMarker != "" {
		resp["nextMarker"] = nextMarker
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListPrincipalThings(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	things := h.Backend.ListPrincipalThings(principal)

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(things, pageSize, start)

	resp := map[string]any{keyThings: page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleListPrincipalThingsV2(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	thingPrincipalType := c.QueryParam("thingPrincipalType")
	things := h.Backend.ListPrincipalThingsV2(principal)

	summaries := make([]map[string]any, 0, len(things))
	for _, t := range things {
		if thingPrincipalType != "" && t.ThingPrincipalType != thingPrincipalType {
			continue
		}
		summaries = append(summaries, map[string]any{
			"thingName":          t.ThingName,
			"thingPrincipalType": t.ThingPrincipalType,
		})
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(summaries, pageSize, start)

	resp := map[string]any{"principalThingObjects": page}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleGetEffectivePolicies(c *echo.Context) error {
	var req struct {
		Principal string `json:"principal"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	// thingName is a query parameter, not a body field (iot@v1.77.4
	// serializers.go awsRestjson1_serializeOpHttpBindingsGetEffectivePoliciesInput).
	thingName := c.QueryParam(keyThingName)
	policies := h.Backend.GetEffectivePolicies(thingName, req.Principal)
	out := make([]map[string]any, len(policies))
	for i, p := range policies {
		out[i] = map[string]any{
			"policyName":     p.PolicyName,
			"policyArn":      p.ARN,
			"policyDocument": p.PolicyDocument,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{"effectivePolicies": out})
}

func (h *Handler) handleDetachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, pathPrincipalPolicies+"/")
	principal := c.Request().Header.Get("X-Amzn-Iot-Principal")

	if err := h.Backend.DetachPrincipalPolicy(policyName, principal); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) dispatchPolicyPrincipalOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opListPrincipalPolicies:
		return true, h.handleListPrincipalPolicies(c)
	case opListPolicyPrincipals:
		return true, h.handleListPolicyPrincipals(c)
	case opListTargetsForPolicy:
		return true, h.handleListTargetsForPolicy(c)
	case opListPrincipalThings:
		return true, h.handleListPrincipalThings(c)
	case opListPrincipalThingsV2:
		return true, h.handleListPrincipalThingsV2(c)
	case opGetEffectivePolicies:
		return true, h.handleGetEffectivePolicies(c)
	}

	return false, nil
}

// resolvePolicyPrincipalPathOps resolves the principal/policy listing
// endpoints.
//
// Three real wire shapes (iot@v1.77.4 serializers.go) were wrong, found by
// gopherstack-n1mb's route table: ListTargetsForPolicy is real POST, not
// GET; ListPrincipalThings/ListPrincipalThingsV2's real paths are
// "/principals/things"/"/principals/things-v2", not "/principal-things"/
// "/principal-things-v2". The old shapes are kept too as non-canonical
// routes wired for this package's own tests.
func resolvePolicyPrincipalPathOps(path, method string) string {
	if op := resolvePolicyPrincipalCanonicalOps(path, method); op != unknownOperation {
		return op
	}

	return resolvePolicyPrincipalLegacyOps(path, method)
}

func resolvePolicyPrincipalCanonicalOps(path, method string) string {
	switch {
	case path == "/principal-policies" && method == http.MethodGet:
		return opListPrincipalPolicies
	case path == "/policy-principals" && method == http.MethodGet:
		return opListPolicyPrincipals
	case strings.HasPrefix(path, "/policy-targets/") && method == http.MethodPost:
		return opListTargetsForPolicy
	case path == "/principals/things" && method == http.MethodGet:
		return opListPrincipalThings
	case path == "/principals/things-v2" && method == http.MethodGet:
		return opListPrincipalThingsV2
	case path == "/effective-policies" && method == http.MethodPost:
		return opGetEffectivePolicies
	}

	return unknownOperation
}

func resolvePolicyPrincipalLegacyOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/policy-targets/") && method == http.MethodGet:
		return opListTargetsForPolicy
	case path == "/principal-things" && method == http.MethodGet:
		return opListPrincipalThings
	case path == "/principal-things-v2" && method == http.MethodGet:
		return opListPrincipalThingsV2
	}

	return unknownOperation
}
