package quicksight

import (
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func isNamespaceOp(op string) bool {
	switch op {
	case opCreateNamespace, opDescribeNamespace, opDeleteNamespace, opListNamespaces:
		return true
	}

	return false
}

func (h *Handler) dispatchNamespace(c *echo.Context, op string) error {
	switch op {
	case opCreateNamespace:
		return h.handleCreateNamespace(c)
	case opDescribeNamespace:
		return h.handleDescribeNamespace(c)
	case opDeleteNamespace:
		return h.handleDeleteNamespace(c)
	case opListNamespaces:
		return h.handleListNamespaces(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		fmt.Sprintf("operation %q not implemented", op),
	)
}

// ---- Namespace handlers ----

func (h *Handler) handleCreateNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	namespace := strField(body, "Namespace")
	capacityRegion := strField(body, "CapacityRegion")

	ns, err := h.Backend.CreateNamespace(accountID, namespace, capacityRegion)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:            ns.Arn,
		keyCapacityRegion: ns.CapacityRegion,
		keyCreationStatus: ns.CreationStatus,
		keyIdentityStore:  ns.IdentityStore,
		keyName:           ns.Name,
		keyRequestID:      newReqID(),
		keyStatus:         http.StatusOK,
	})
}

func (h *Handler) handleDescribeNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	ns, err := h.Backend.DescribeNamespace(accountID, namespace)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyNamespace: map[string]any{
			keyArn:            ns.Arn,
			keyCapacityRegion: ns.CapacityRegion,
			keyCreationStatus: ns.CreationStatus,
			keyIdentityStore:  ns.IdentityStore,
			keyName:           ns.Name,
		},
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleDeleteNamespace(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	namespace := seg(segs, segResID)

	if err := h.Backend.DeleteNamespace(accountID, namespace); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	})
}

func (h *Handler) handleListNamespaces(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	namespaces, next, err := h.Backend.ListNamespaces(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(namespaces))
	for _, ns := range namespaces {
		items = append(items, map[string]any{
			keyArn:            ns.Arn,
			keyCapacityRegion: ns.CapacityRegion,
			keyCreationStatus: ns.CreationStatus,
			keyIdentityStore:  ns.IdentityStore,
			keyName:           ns.Name,
		})
	}

	resp := map[string]any{
		"Namespaces": items,
		keyRequestID: newReqID(),
		keyStatus:    http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}
