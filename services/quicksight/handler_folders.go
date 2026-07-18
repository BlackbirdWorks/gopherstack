package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by folder operations.
const (
	keyFolderID          = "FolderId"
	keyFolder            = "Folder"
	keyFolderSummaryList = "FolderSummaryList"
	keyFolderMember      = "FolderMember"
	keyFolderMemberList  = "FolderMemberList"
	keyPermissions       = "Permissions"
	keyFolderType        = "FolderType"
	keyParentFolderArn   = "ParentFolderArn"
	keyMemberID          = "MemberId"
	keyMemberType        = "MemberType"

	errResourceExistsCode = "ResourceExistsException"
)

func isFolderOp(op string) bool {
	switch op {
	case opCreateFolder, opDescribeFolder, opUpdateFolder, opDeleteFolder,
		opListFolders, opSearchFolders,
		opCreateFolderMembership, opDeleteFolderMembership, opListFolderMembers,
		opDescribeFolderPermissions, opUpdateFolderPermissions, opDescribeFolderResolvedPerms:
		return true
	}

	return false
}

func (h *Handler) dispatchFolder(c *echo.Context, op string) error {
	switch op {
	case opCreateFolder:
		return h.handleCreateFolder(c)
	case opDescribeFolder:
		return h.handleDescribeFolder(c)
	case opUpdateFolder:
		return h.handleUpdateFolder(c)
	case opDeleteFolder:
		return h.handleDeleteFolder(c)
	case opListFolders:
		return h.handleListFolders(c)
	case opSearchFolders:
		return h.handleSearchFolders(c)
	case opCreateFolderMembership:
		return h.handleCreateFolderMembership(c)
	case opDeleteFolderMembership:
		return h.handleDeleteFolderMembership(c)
	case opListFolderMembers:
		return h.handleListFolderMembers(c)
	case opDescribeFolderPermissions:
		return h.handleDescribeFolderPermissions(c)
	case opUpdateFolderPermissions:
		return h.handleUpdateFolderPermissions(c)
	case opDescribeFolderResolvedPerms:
		return h.handleDescribeFolderResolvedPermissions(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleCreateFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.CreateFolder(
		accountID,
		folderID,
		strField(body, keyName),
		strField(body, keyFolderType),
		strField(body, keyParentFolderArn),
		permissionsField(body, keyPermissions),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrFolderAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       f.Arn,
		keyFolderID:  f.FolderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDescribeFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	f, err := h.Backend.DescribeFolder(accountID, folderID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolder:    h.folderToMap(accountID, f),
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleUpdateFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	f, err := h.Backend.UpdateFolder(accountID, folderID, strField(body, keyName))
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:       f.Arn,
		keyFolderID:  f.FolderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteFolder(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	if err := h.Backend.DeleteFolder(accountID, folderID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderID:  folderID,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListFolders(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	folders, next, err := h.Backend.ListFolders(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(folders))
	for _, f := range folders {
		items = append(items, h.folderToMap(accountID, f))
	}

	resp := map[string]any{
		keyFolderSummaryList: items,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleSearchFolders(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	folders, next, err := h.Backend.SearchFolders(
		accountID,
		folderFiltersFromBody(body),
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(folders))
	for _, f := range folders {
		items = append(items, h.folderToMap(accountID, f))
	}

	resp := map[string]any{
		keyFolderSummaryList: items,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleCreateFolderMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)
	memberType := seg(segs, segSubResID)
	memberID := seg(segs, segSubSubRes)

	m, err := h.Backend.CreateFolderMembership(accountID, folderID, memberID, memberType)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderMember: map[string]any{
			keyMemberID: m.MemberID,
		},
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteFolderMembership(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)
	memberType := seg(segs, segSubResID)
	memberID := seg(segs, segSubSubRes)

	if err := h.Backend.DeleteFolderMembership(accountID, folderID, memberID, memberType); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListFolderMembers(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	members, next, err := h.Backend.ListFolderMembers(accountID, folderID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(members))
	for _, m := range members {
		items = append(items, map[string]any{
			keyMemberID:   m.MemberID,
			keyMemberType: m.MemberType,
		})
	}

	resp := map[string]any{
		keyFolderMemberList: items,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

func (h *Handler) handleDescribeFolderPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	perms, err := h.Backend.DescribeFolderPermissions(accountID, folderID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderID:    folderID,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleUpdateFolderPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	perms, err := h.Backend.UpdateFolderPermissions(
		accountID,
		folderID,
		permissionsField(body, "GrantPermissions"),
		permissionsField(body, "RevokePermissions"),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderID:    folderID,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

func (h *Handler) handleDescribeFolderResolvedPermissions(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	folderID := seg(segs, segResID)

	perms, err := h.Backend.DescribeFolderResolvedPermissions(accountID, folderID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyFolderID:    folderID,
		keyPermissions: permissionsToMaps(perms),
		keyRequestID:   reqIDPlaceholder,
		keyStatus:      http.StatusOK,
	})
}

// ---- shared helpers ----

func (h *Handler) folderToMap(accountID string, f *Folder) map[string]any {
	m := map[string]any{
		keyArn:             f.Arn,
		keyFolderID:        f.FolderID,
		keyName:            f.Name,
		keyFolderType:      f.FolderType,
		keyCreatedTime:     f.CreatedTime.Unix(),
		keyLastUpdatedTime: f.LastUpdatedTime.Unix(),
		"FolderPath":       h.folderPath(accountID, f),
	}

	return m
}

// folderPath walks the parent chain starting at f and returns the ancestor ARNs
// ordered from root to immediate parent, matching QuickSight's FolderPath field.
func (h *Handler) folderPath(accountID string, f *Folder) []string {
	path := []string{}
	seen := map[string]bool{f.FolderID: true}
	parentArn := f.ParentFolderArn

	for parentArn != "" {
		path = append([]string{parentArn}, path...)

		parentID := folderIDFromArn(parentArn)
		if parentID == "" || seen[parentID] {
			break
		}
		seen[parentID] = true

		parent, err := h.Backend.DescribeFolder(accountID, parentID)
		if err != nil {
			break
		}
		parentArn = parent.ParentFolderArn
	}

	return path
}

func permissionsFromRaw(raw []any) []ResourcePermission {
	if len(raw) == 0 {
		return nil
	}

	perms := make([]ResourcePermission, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		principal := strField(m, "Principal")
		if principal == "" {
			continue
		}

		var actions []string
		if arr, isList := m["Actions"].([]any); isList {
			for _, a := range arr {
				if s, isStr := a.(string); isStr {
					actions = append(actions, s)
				}
			}
		}

		perms = append(perms, ResourcePermission{Principal: principal, Actions: actions})
	}

	return perms
}

func permissionsField(body map[string]any, key string) []ResourcePermission {
	raw, _ := body[key].([]any)

	return permissionsFromRaw(raw)
}

func permissionsToMaps(perms []ResourcePermission) []map[string]any {
	out := make([]map[string]any, 0, len(perms))
	for _, p := range perms {
		actions := p.Actions
		if actions == nil {
			actions = []string{}
		}

		out = append(out, map[string]any{
			"Principal": p.Principal,
			"Actions":   actions,
		})
	}

	return out
}

func folderFiltersFromBody(body map[string]any) []FolderSearchFilter {
	raw, _ := body["Filters"].([]any)
	if len(raw) == 0 {
		return nil
	}

	filters := make([]FolderSearchFilter, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}

		filters = append(filters, FolderSearchFilter{
			Operator: strField(m, "Operator"),
			Name:     strField(m, "Name"),
			Value:    strField(m, "Value"),
		})
	}

	return filters
}

// classifyFolderPaths routes /accounts/{id}/folders/... paths.
func classifyFolderPaths( //nolint:gocognit,cyclop // existing issue.
	method string,
	segs []string,
	n int,
) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method { //nolint:gocritic // existing issue.
		case http.MethodGet:
			return opListFolders, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodPost:
			return opCreateFolder, id
		case http.MethodGet:
			return opDescribeFolder, id
		case http.MethodPut:
			return opUpdateFolder, id
		case http.MethodDelete:
			return opDeleteFolder, id
		}
	case nSegsSubRes:
		id := seg(segs, segResID)
		sub := seg(segs, segSubRes)
		switch sub {
		case pathSegPermissions:
			switch method {
			case http.MethodGet:
				return opDescribeFolderPermissions, id
			case http.MethodPut:
				return opUpdateFolderPermissions, id
			}
		case pathSegResolvedPerms:
			if method == http.MethodGet {
				return opDescribeFolderResolvedPerms, id
			}
		case pathSegMembers:
			if method == http.MethodGet {
				return opListFolderMembers, id
			}
		}
	case nSegsSubSubRes:
		if seg(segs, segSubRes) == pathSegMembers {
			id := seg(segs, segResID)
			switch method {
			case http.MethodPut:
				return opCreateFolderMembership, id
			case http.MethodDelete:
				return opDeleteFolderMembership, id
			}
		}
	}

	return opUnknown, ""
}

// classifyResourceFoldersPaths routes /accounts/{id}/resource/{resARN}/folders paths.
func classifyResourceFoldersPaths(method string, segs []string, n int) (string, string) {
	if n == nSegsSubRes && seg(segs, segSubRes) == pathSegFolders && method == http.MethodGet {
		return opListFoldersForResource, seg(segs, segResID)
	}

	return opUnknown, ""
}

// ---- ListFoldersForResource ----

func (h *Handler) handleListFoldersForResource(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	resourceArn := seg(segs, segResID)

	folderArns, next, err := h.Backend.ListFoldersForResource(
		accountID, resourceArn, maxResultsParam(c), nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	resp := map[string]any{
		"Folders":    folderArns,
		keyRequestID: reqIDPlaceholder,
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
