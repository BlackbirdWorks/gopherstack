package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response/request keys used only by Space operations.
//
// The Space family is the one QuickSight resource type whose wire shape is
// NOT PascalCase like every other family in this backend -- confirmed
// field-by-field against aws-sdk-go-v2/service/quicksight's deserializers.go:
// spaceId/spaceArn are camelCase on every Space op's envelope, the nested
// Space/SpaceSummary document is fully camelCase, and
// UpdateSpacePermissionsOutput is uniquely fully-lowercase even for
// "permissions"/"requestId" (every other op keeps "Permissions"/"RequestId"
// PascalCase). Don't "fix" these to match the rest of the file -- they are
// deliberately faithful to a real, if inconsistent, upstream API.
const (
	keySpaceIDLower             = "spaceId"
	keySpaceArnLower            = "spaceArn"
	keySpace                    = "Space"
	keySpaceSummaries           = "SpaceSummaries"
	keySpaceResources           = "SpaceResources"
	keyContributors             = "Contributors"
	keyFailedResourceOperations = "FailedResourceOperations"
	keyAddResources             = "AddResources"
	keyRemoveResources          = "RemoveResources"
	keyResourceDetails          = "ResourceDetails"
	keyResourceType             = "ResourceType"
	keyResourceName             = "ResourceName"
	keyResourceArnLower         = "resourceArn"

	// camelCase keys used only inside the nested Space/SpaceSummary document.
	keyConsumedSourceDocCountCamel = "consumedSourceDocCount"
	keyConsumedSourceSizeCamel     = "consumedSourceSize"
	keyCreatedAtCamel              = "createdAt"
	keyCreatedByCamel              = "createdBy"
	keyCreatedByArnCamel           = "createdByArn"
	keyDescriptionCamel            = "description"
	keyNameCamel                   = "name"
	keyResourcesCamel              = "resources"
	keyUpdatedAtCamel              = "updatedAt"
	keyResourcesCountCamel         = "resourcesCount"

	// fully-lowercase keys used only by UpdateSpacePermissionsOutput.
	keyPermissionsLower = "permissions"
	keyRequestIDLower   = "requestId"

	// keySpaceIDReq is CreateSpaceInput's request-body field name (PascalCase,
	// unlike the response envelope's lowercase spaceId).
	keySpaceIDReq = "SpaceId"
)

func isSpaceOp(op string) bool {
	switch op {
	case opCreateSpace, opDescribeSpace, opUpdateSpace, opDeleteSpace,
		opListSpaces, opSearchSpaces, opDescribeSpacePerms, opUpdateSpacePerms,
		opListSpaceResources, opUpdateSpaceResources:
		return true
	}

	return false
}

func (h *Handler) dispatchSpace(c *echo.Context, op string) error {
	switch op {
	case opCreateSpace:
		return h.handleCreateSpace(c)
	case opDescribeSpace:
		return h.handleDescribeSpace(c)
	case opUpdateSpace:
		return h.handleUpdateSpace(c)
	case opDeleteSpace:
		return h.handleDeleteSpace(c)
	case opListSpaces:
		return h.handleListSpaces(c)
	case opSearchSpaces:
		return h.handleSearchSpaces(c)
	case opDescribeSpacePerms:
		return h.handleDescribeSpacePerms(c)
	case opUpdateSpacePerms:
		return h.handleUpdateSpacePerms(c)
	case opListSpaceResources:
		return h.handleListSpaceResources(c)
	case opUpdateSpaceResources:
		return h.handleUpdateSpaceResources(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

// spaceDetailsToMap builds the nested "Space" document for DescribeSpace.
// SpaceDetails carries no SpaceId/Arn of its own (those live only on the
// envelope) -- confirmed against types.SpaceDetails, which has no such
// fields.
func spaceDetailsToMap(s *Space) map[string]any {
	m := map[string]any{
		keyNameCamel:      s.Name,
		keyCreatedAtCamel: s.CreatedAt.Unix(),
		keyUpdatedAtCamel: s.UpdatedAt.Unix(),
		keyResourcesCamel: spaceResourcesToList(s.Resources),
	}
	if s.Description != "" {
		m[keyDescriptionCamel] = s.Description
	}
	if s.CreatedBy != "" {
		m[keyCreatedByCamel] = s.CreatedBy
	}
	if s.CreatedByArn != "" {
		m[keyCreatedByArnCamel] = s.CreatedByArn
	}

	return m
}

func spaceSummaryToMap(s *Space) map[string]any {
	m := map[string]any{
		keySpaceIDLower:        s.SpaceID,
		keySpaceArnLower:       s.Arn,
		keyNameCamel:           s.Name,
		keyCreatedAtCamel:      s.CreatedAt.Unix(),
		keyUpdatedAtCamel:      s.UpdatedAt.Unix(),
		keyResourcesCountCamel: int32(len(s.Resources)), //nolint:gosec // resource count always small
	}
	if s.Description != "" {
		m[keyDescriptionCamel] = s.Description
	}
	if s.CreatedBy != "" {
		m[keyCreatedByCamel] = s.CreatedBy
	}
	if s.CreatedByArn != "" {
		m[keyCreatedByArnCamel] = s.CreatedByArn
	}

	return m
}

// spaceResourceDetailsToMap builds the {"ResourceDetails": {"resourceArn":
// ...}, "ResourceType": ...} shape shared by SpaceResourceSummary,
// SpaceResourceOperation (request), and FailedSpaceResourceOperation.
func spaceResourceDetailsToMap(arn string) map[string]any {
	return map[string]any{keyResourceArnLower: arn}
}

func spaceResourcesToList(resources []SpaceResource) []map[string]any {
	out := make([]map[string]any, 0, len(resources))
	for _, r := range resources {
		item := map[string]any{
			keyResourceDetails: spaceResourceDetailsToMap(r.ResourceArn),
			keyResourceType:    r.ResourceType,
			keyUpdatedAt:       r.UpdatedAt.Unix(),
		}
		if r.ResourceName != "" {
			item[keyResourceName] = r.ResourceName
		}
		out = append(out, item)
	}

	return out
}

func (h *Handler) handleCreateSpace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	s, err := h.Backend.CreateSpace(
		accountID, strField(body, keySpaceIDReq), strField(body, keyName), strField(body, keyDescription),
	)
	if err != nil {
		if errors.Is(err, ErrSpaceAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:  s.SpaceID,
		keySpaceArnLower: s.Arn,
		keyRequestID:     reqIDPlaceholder,
	})
}

func (h *Handler) handleDescribeSpace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	s, err := h.Backend.DescribeSpace(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	// Contributors requires per-user raw-file-size attribution this backend
	// doesn't track (no ingestion pipeline attributing bytes to a
	// contributing user); an honest empty list rather than a fabricated one,
	// matching the VPCConnection.NetworkInterfaces precedent (see PARITY.md).
	return writeJSON(c, http.StatusOK, map[string]any{
		keySpace:         spaceDetailsToMap(s),
		keyContributors:  []map[string]any{},
		keySpaceIDLower:  s.SpaceID,
		keySpaceArnLower: s.Arn,
		keyRequestID:     reqIDPlaceholder,
	})
}

func (h *Handler) handleUpdateSpace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	s, err := h.Backend.UpdateSpace(accountID, id, strField(body, keyName), strField(body, keyDescription))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:  s.SpaceID,
		keySpaceArnLower: s.Arn,
		keyRequestID:     reqIDPlaceholder,
	})
}

func (h *Handler) handleDeleteSpace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	s, err := h.Backend.DeleteSpace(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:  s.SpaceID,
		keySpaceArnLower: s.Arn,
		keyRequestID:     reqIDPlaceholder,
	})
}

func (h *Handler) handleListSpaces(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	items, next, err := h.Backend.ListSpaces(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, spaceSummaryToMap(s))
	}

	// SpaceId/SpaceArn are required on ListSpacesOutput despite the op being
	// account-scoped, not scoped to any single space (api_op_ListSpaces.go:44-63) --
	// no doc-comment nuance explains why. There is no "the" space to name
	// honestly across a multi-result call, so both are present as empty
	// strings rather than fabricated, the same pattern used for pagination
	// tokens on single-page backends elsewhere in this codebase.
	resp := map[string]any{
		keySpaceSummaries: summaries,
		keySpaceIDLower:   "",
		keySpaceArnLower:  "",
		keyRequestID:      reqIDPlaceholder,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchSpaces(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	items, next, err := h.Backend.SearchSpaces(
		accountID, lowercaseFiltersFromBody(body), maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	summaries := make([]map[string]any, 0, len(items))
	for _, s := range items {
		summaries = append(summaries, spaceSummaryToMap(s))
	}

	// SpaceId/SpaceArn required-but-unscoped, same as ListSpaces above.
	resp := map[string]any{
		keySpaceSummaries: summaries,
		keySpaceIDLower:   "",
		keySpaceArnLower:  "",
		keyRequestID:      reqIDPlaceholder,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeSpacePerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	s, perms, err := h.Backend.DescribeSpacePermissions(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:  s.SpaceID,
		keySpaceArnLower: s.Arn,
		keyPermissions:   permissionsToMaps(perms),
		keyRequestID:     reqIDPlaceholder,
	})
}

// handleUpdateSpacePerms: UpdateSpacePermissionsOutput is uniquely
// fully-lowercase ("permissions"/"requestId", not "Permissions"/"RequestId")
// -- see the wire-shape note on this file's const block.
func (h *Handler) handleUpdateSpacePerms(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	s, perms, err := h.Backend.UpdateSpacePermissions(
		accountID,
		id,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:     s.SpaceID,
		keySpaceArnLower:    s.Arn,
		keyPermissionsLower: permissionsToMaps(perms),
		keyRequestIDLower:   reqIDPlaceholder,
	})
}

func (h *Handler) handleListSpaceResources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	s, err := h.Backend.DescribeSpace(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	resources, err := h.Backend.ListSpaceResources(accountID, id)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:   s.SpaceID,
		keySpaceArnLower:  s.Arn,
		keySpaceResources: spaceResourcesToList(resources),
		keyRequestID:      reqIDPlaceholder,
	})
}

func (h *Handler) handleUpdateSpaceResources(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	id := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	s, failed, err := h.Backend.UpdateSpaceResources(
		accountID, id,
		spaceResourceOpsFromBody(body, keyAddResources),
		spaceResourceOpsFromBody(body, keyRemoveResources),
	)
	if err != nil {
		return httpErr(c, err)
	}

	failedList := make([]map[string]any, 0, len(failed))
	for _, f := range failed {
		failedList = append(failedList, map[string]any{
			keyResourceDetails:   spaceResourceDetailsToMap(f.Arn),
			keyResourceType:      f.ResourceType,
			keyErrorMessageField: f.ErrorMessage,
		})
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keySpaceIDLower:             s.SpaceID,
		keySpaceArnLower:            s.Arn,
		keyFailedResourceOperations: failedList,
		keyRequestID:                reqIDPlaceholder,
	})
}

// spaceResourceOpsFromBody parses a SpaceResourceOperation list (each
// {"ResourceDetails": {"resourceArn": ...}, "ResourceType": ...}) from
// body[key].
func spaceResourceOpsFromBody(body map[string]any, key string) []SpaceResource {
	raw, _ := body[key].([]any)
	if len(raw) == 0 {
		return nil
	}

	out := make([]SpaceResource, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		details, _ := m[keyResourceDetails].(map[string]any)
		arn := strField(details, keyResourceArnLower)
		if arn == "" {
			continue
		}
		out = append(out, SpaceResource{ResourceArn: arn, ResourceType: strField(m, keyResourceType)})
	}

	return out
}

// classifySpacePaths routes /v1/accounts/{id}/spaces/... paths (already
// stripped of their "v1" prefix by classifyRequest).
func classifySpacePaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateSpace, accountID
		case http.MethodGet:
			return opListSpaces, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeSpace, id
		case http.MethodPut:
			return opUpdateSpace, id
		case http.MethodDelete:
			return opDeleteSpace, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeSpacePerms, id
			case http.MethodPut:
				return opUpdateSpacePerms, id
			}
		case pathSegResources:
			switch method {
			case http.MethodGet:
				return opListSpaceResources, id
			case http.MethodPut:
				return opUpdateSpaceResources, id
			}
		}
	}

	return opUnknown, ""
}
