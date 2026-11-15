package azurearm

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const policyResourceGroupType = "Microsoft.Resources/subscriptions/resourceGroups"

// policyTarget describes a PUT that policy assignments evaluate: resource groups and resources under a group.
func policyTarget(method string, segs []string) (policyResource, bool) {
	if method != http.MethodPut || len(segs) < rgPathSegs || !strings.EqualFold(segs[0], subscriptionsSegment) ||
		!strings.EqualFold(segs[2], resourceGroupsSegment) {
		return policyResource{}, false
	}

	res := policyResource{id: "/" + joinSegs(segs), name: segs[len(segs)-1]}

	if len(segs) == rgPathSegs {
		res.typ = policyResourceGroupType

		return res, true
	}

	if len(segs) < authSubResourceMin+typeNamePairWidth || !strings.EqualFold(segs[4], providersSegment) ||
		strings.EqualFold(
			segs[5],
			namespaceMicrosoftAuthorization,
		) || (len(segs)-authSubResourceMin)%typeNamePairWidth != 0 {
		return policyResource{}, false
	}

	types := []string{segs[5]}
	for i := 6; i < len(segs); i += typeNamePairWidth {
		types = append(types, segs[i])
	}

	res.typ = strings.Join(types, "/")

	return res, true
}

// enforcePolicy evaluates policy assignments for a write; it answers 403 RequestDisallowedByPolicy on deny and
// rewrites the request body for append and modify. It reports whether it answered.
func (h *Handler) enforcePolicy(c *echo.Context, segs []string) (bool, error) {
	res, ok := policyTarget(c.Request().Method, segs)
	if !ok {
		return false, nil
	}

	p, found := h.authorizationProvider()
	if !found {
		return false, nil
	}

	raw, body := readPolicyBody(c.Request())
	if body == nil {
		return false, nil
	}

	res.body = body
	res.location, res.kind = strOf(body, fieldLocation), strOf(body, "kind")

	out := p.EvaluatePolicies(res)
	if out.Deny != nil {
		return true, h.writePolicyDenied(c, res, out.Deny)
	}

	if rewritten, merr := json.Marshal(out.Body, jsonx.V1()); merr == nil && !bytes.Equal(rewritten, raw) {
		c.Request().Body = io.NopCloser(bytes.NewReader(rewritten))
		c.Request().ContentLength = int64(len(rewritten))
		c.Request().Header.Set("Content-Length", strconv.Itoa(len(rewritten)))
	}

	return false, nil
}

func (h *Handler) writePolicyDenied(c *echo.Context, res policyResource, hit *PolicyHit) error {
	return h.writeError(c, http.StatusForbidden, "RequestDisallowedByPolicy", fmt.Sprintf(
		"Resource '%s' was disallowed by policy. Policy assignment '%s', policy definition '%s'.",
		res.name, hit.AssignmentID, hit.DefinitionID))
}

// readPolicyBody reads and restores the request body; body is nil when it is not a JSON object.
func readPolicyBody(r *http.Request) ([]byte, map[string]any) {
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		r.Body = http.NoBody

		return nil, nil
	}

	r.Body = io.NopCloser(bytes.NewReader(raw))

	var body map[string]any
	if json.Unmarshal(raw, &body, jsonx.V1()) != nil {
		return raw, nil
	}

	return raw, body
}
