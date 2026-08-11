package iot

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolvePolicyVersionOps(path, method string) string {
	switch {
	case strings.HasPrefix(path, "/target-policies/") && method == http.MethodDelete:

		return opDetachPolicy
	case path == "/attached-policies" && method == http.MethodPost:

		return opListAttachedPolicies
	}

	return resolvePolicyVersionSubOps(path, method)
}

func resolvePolicyVersionSubOps(path, method string) string {
	if !strings.HasPrefix(path, "/policies/") {
		return unknownOperation
	}

	hasVersionSlash := strings.Contains(path, "/versions/")
	endsVersion := strings.HasSuffix(path, "/versions")
	endsDefault := strings.HasSuffix(path, "/default")

	return resolvePolicyVersionByMethod(path, method, hasVersionSlash, endsVersion, endsDefault)
}

func resolvePolicyVersionByMethod(
	path, method string,
	hasVersionSlash, endsVersion, endsDefault bool,
) string {
	switch method {
	case http.MethodGet:
		if hasVersionSlash {
			return opGetPolicyVersion
		}

		if endsVersion {
			return opListPolicyVersions
		}
	case http.MethodDelete:
		if hasVersionSlash {
			return opDeletePolicyVersion
		}
	case http.MethodPatch:
		if endsDefault {
			return opSetDefaultPolicyVersion
		}
	case http.MethodPost:
		if strings.Contains(path, "/versions") && !endsDefault {
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

func (h *Handler) handleAttachPrincipalPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/target-policies/")
	principal := c.Request().Header.Get(headerIoTThingName)

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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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

	out := make([]map[string]string, 0, len(policies))
	for _, p := range policies {
		out = append(out, map[string]string{
			keyPolicyName: p.PolicyName,
			keyPolicyArn:  p.ARN,
		})
	}

	pageSize, start := parseIoTPagination(c)
	page, nextToken := paginateMaps(out, pageSize, start)

	resp := map[string]any{"policies": page}
	if nextToken != "" {
		resp["nextMarker"] = nextToken
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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

func (h *Handler) handleCreatePolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	policyName := strings.TrimSuffix(after, "/versions")

	var body struct {
		PolicyDocument string `json:"policyDocument"`
	}

	if err := json.NewDecoder(c.Request().Body).Decode(&body); err != nil &&
		!errors.Is(err, io.EOF) {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: err.Error()})
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
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	parts := strings.SplitN(after, "/versions/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}

	policyName := parts[0]
	pv, err := h.Backend.GetPolicyVersion(policyName, parts[1])
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
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	policyName := strings.TrimSuffix(after, "/versions")
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
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	parts := strings.SplitN(after, "/versions/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}
	if err := h.Backend.DeletePolicyVersion(parts[0], parts[1]); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleSetDefaultPolicyVersion(c *echo.Context) error {
	after := strings.TrimPrefix(c.Request().URL.Path, "/policies/")
	after = strings.TrimSuffix(after, "/default")
	parts := strings.SplitN(after, "/versions/", maxPathSegments)
	if len(parts) != maxPathSegments {
		return c.JSON(http.StatusBadRequest, map[string]string{keyError: keyInvalidPath})
	}
	if err := h.Backend.SetDefaultPolicyVersion(parts[0], parts[1]); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListPrincipalPolicies(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	policies := h.Backend.ListPrincipalPolicies(principal)
	out := make([]map[string]any, len(policies))
	for i, p := range policies {
		out[i] = map[string]any{
			"policyName": p.PolicyName,
			"policyArn":  p.ARN,
		}
	}

	return c.JSON(http.StatusOK, map[string]any{keyPolicies: out})
}

func (h *Handler) handleListPolicyPrincipals(c *echo.Context) error {
	policyName := c.Request().Header.Get("X-Amzn-Policy-Name")
	principals := h.Backend.ListPolicyPrincipals(policyName)
	if principals == nil {
		principals = []string{}
	}

	return c.JSON(http.StatusOK, map[string]any{"principals": principals})
}

func (h *Handler) handleListTargetsForPolicy(c *echo.Context) error {
	policyName := strings.TrimPrefix(c.Request().URL.Path, "/policy-targets/")
	targets := h.Backend.ListTargetsForPolicy(policyName)

	return c.JSON(http.StatusOK, map[string]any{"targets": targets})
}

func (h *Handler) handleListPrincipalThings(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	things := h.Backend.ListPrincipalThings(principal)

	return c.JSON(http.StatusOK, map[string]any{keyThings: things})
}

func (h *Handler) handleListPrincipalThingsV2(c *echo.Context) error {
	principal := c.Request().Header.Get(headerIoTPrincipal)
	things := h.Backend.ListPrincipalThings(principal)
	summaries := make([]map[string]any, len(things))
	for i, t := range things {
		summaries[i] = map[string]any{"thingName": t}
	}

	return c.JSON(http.StatusOK, map[string]any{"principalThingObjects": summaries})
}

func (h *Handler) handleGetEffectivePolicies(c *echo.Context) error {
	var req struct {
		ThingName string `json:"thingName"`
		Principal string `json:"principal"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	policies := h.Backend.GetEffectivePolicies(req.ThingName, req.Principal)
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

// resolvePolicyPrincipalPathOps resolves the principal/policy listing endpoints.
func resolvePolicyPrincipalPathOps(path, method string) string {
	switch {
	case path == "/principal-policies" && method == http.MethodGet:
		return opListPrincipalPolicies
	case path == "/policy-principals" && method == http.MethodGet:
		return opListPolicyPrincipals
	case strings.HasPrefix(path, "/policy-targets/") && method == http.MethodGet:
		return opListTargetsForPolicy
	case path == "/principal-things" && method == http.MethodGet:
		return opListPrincipalThings
	case path == "/principal-things-v2" && method == http.MethodGet:
		return opListPrincipalThingsV2
	case path == "/effective-policies" && method == http.MethodPost:
		return opGetEffectivePolicies
	}

	return unknownOperation
}
