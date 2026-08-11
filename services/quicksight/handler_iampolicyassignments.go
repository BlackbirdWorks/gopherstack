package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by IAM policy assignment operations.
const (
	keyAssignmentID         = "AssignmentId"
	keyAssignmentName       = "AssignmentName"
	keyAssignmentStatus     = "AssignmentStatus"
	keyPolicyArn            = "PolicyArn"
	keyIdentities           = "Identities"
	keyIAMPolicyAssignment  = "IAMPolicyAssignment"
	keyIAMPolicyAssignments = "IAMPolicyAssignments"
	keyActiveAssignments    = "ActiveAssignments"

	queryParamAssignmentStatus = "assignment-status"
)

func isIAMPolicyAssignmentOp(op string) bool {
	switch op {
	case opCreateIAMPolicyAssignment, opDescribeIAMPolicyAssignment, opUpdateIAMPolicyAssignment,
		opDeleteIAMPolicyAssignment, opListIAMPolicyAssignments, opListIAMPolicyAssignmentsForUser:
		return true
	}

	return false
}

func (h *Handler) dispatchIAMPolicyAssignment(c *echo.Context, op string) error {
	switch op {
	case opCreateIAMPolicyAssignment:
		return h.handleCreateIAMPolicyAssignment(c)
	case opDescribeIAMPolicyAssignment:
		return h.handleDescribeIAMPolicyAssignment(c)
	case opUpdateIAMPolicyAssignment:
		return h.handleUpdateIAMPolicyAssignment(c)
	case opDeleteIAMPolicyAssignment:
		return h.handleDeleteIAMPolicyAssignment(c)
	case opListIAMPolicyAssignments:
		return h.handleListIAMPolicyAssignments(c)
	case opListIAMPolicyAssignmentsForUser:
		return h.handleListIAMPolicyAssignmentsForUser(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func identitiesFromBody(body map[string]any) map[string][]string {
	raw, ok := body[keyIdentities].(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}

	out := make(map[string][]string, len(raw))
	for k, v := range raw {
		list, _ := v.([]any)
		members := make([]string, 0, len(list))
		for _, item := range list {
			if s, isStr := item.(string); isStr {
				members = append(members, s)
			}
		}
		out[k] = members
	}

	return out
}

func (h *Handler) handleCreateIAMPolicyAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.CreateIAMPolicyAssignment(
		accountID,
		namespace,
		strField(body, keyAssignmentName),
		strField(body, keyAssignmentStatus),
		strField(body, keyPolicyArn),
		identitiesFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrIAMPolicyAssignmentAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAssignmentName:   a.AssignmentName,
		keyAssignmentID:     a.AssignmentID,
		keyAssignmentStatus: a.AssignmentStatus,
		keyPolicyArn:        a.PolicyArn,
		keyIdentities:       a.Identities,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleDescribeIAMPolicyAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	assignmentName := seg(segs, segSubResID)

	a, err := h.Backend.DescribeIAMPolicyAssignment(accountID, namespace, assignmentName)
	if err != nil {
		return httpErr(c, err)
	}

	m := iamPolicyAssignmentToMap(a)
	m[keyAwsAccountID] = a.AwsAccountID

	return writeJSON(c, http.StatusOK, map[string]any{
		keyIAMPolicyAssignment: m,
		keyRequestID:           reqIDPlaceholder,
		keyStatus:              http.StatusOK,
	})
}

func (h *Handler) handleUpdateIAMPolicyAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	assignmentName := seg(segs, segSubResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	a, err := h.Backend.UpdateIAMPolicyAssignment(
		accountID,
		namespace,
		assignmentName,
		strField(body, keyAssignmentStatus),
		strField(body, keyPolicyArn),
		identitiesFromBody(body),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAssignmentName:   a.AssignmentName,
		keyAssignmentID:     a.AssignmentID,
		keyAssignmentStatus: a.AssignmentStatus,
		keyPolicyArn:        a.PolicyArn,
		keyIdentities:       a.Identities,
		keyRequestID:        reqIDPlaceholder,
		keyStatus:           http.StatusOK,
	})
}

func (h *Handler) handleDeleteIAMPolicyAssignment(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	assignmentName := seg(segs, segSubResID)

	if err := h.Backend.DeleteIAMPolicyAssignment(accountID, namespace, assignmentName); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyAssignmentName: assignmentName,
		keyRequestID:      reqIDPlaceholder,
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleListIAMPolicyAssignments(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	assignments, next, err := h.Backend.ListIAMPolicyAssignments(
		accountID,
		namespace,
		queryParam(c, queryParamAssignmentStatus),
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, iamPolicyAssignmentListResponse(assignments, next))
}

func (h *Handler) handleListIAMPolicyAssignmentsForUser(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)
	userName := seg(segs, segSubResID)

	assignments, next, err := h.Backend.ListIAMPolicyAssignmentsForUser(
		accountID,
		namespace,
		userName,
		maxResultsParam(c),
		nextTokenParam(c),
	)
	if err != nil {
		return httpErr(c, err)
	}

	// ListIAMPolicyAssignmentsForUserOutput carries ActiveAssignments
	// ([]types.ActiveIAMPolicyAssignment: AssignmentName/PolicyArn only), not
	// IAMPolicyAssignments -- confirmed against
	// aws-sdk-go-v2/service/quicksight@v1.123.1/deserializers.go:33917
	// ("ActiveAssignments" case) vs. ListIAMPolicyAssignmentsOutput's
	// "IAMPolicyAssignments" case at deserializers.go:33716.
	items := make([]map[string]any, 0, len(assignments))
	for _, a := range assignments {
		items = append(items, map[string]any{
			keyAssignmentName: a.AssignmentName,
			keyPolicyArn:      a.PolicyArn,
		})
	}

	resp := map[string]any{
		keyActiveAssignments: items,
		keyRequestID:         reqIDPlaceholder,
		keyStatus:            http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- shared helpers ----

func iamPolicyAssignmentListResponse(assignments []*IAMPolicyAssignment, next string) map[string]any {
	items := make([]map[string]any, 0, len(assignments))
	for _, a := range assignments {
		items = append(items, iamPolicyAssignmentToMap(a))
	}

	resp := map[string]any{
		keyIAMPolicyAssignments: items,
		keyRequestID:            reqIDPlaceholder,
		keyStatus:               http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return resp
}

func iamPolicyAssignmentToMap(a *IAMPolicyAssignment) map[string]any {
	return map[string]any{
		keyAssignmentName:   a.AssignmentName,
		keyAssignmentID:     a.AssignmentID,
		keyAssignmentStatus: a.AssignmentStatus,
		keyPolicyArn:        a.PolicyArn,
		keyIdentities:       a.Identities,
	}
}

// classifyNamespaceSingularPaths routes /accounts/{id}/namespace/{ns}/... paths.
func classifyNamespaceSingularPaths(method string, segs []string, n int) (string, string) {
	if n < nSegsSubResID {
		return opUnknown, ""
	}

	ns := seg(segs, segResID)
	sub := seg(segs, segSubRes)
	subID := seg(segs, segSubResID)

	if sub == pathSegIAMPolicyAssignments {
		if method == http.MethodDelete {
			return opDeleteIAMPolicyAssignment, subID
		}
	}

	_ = ns

	return opUnknown, ""
}
