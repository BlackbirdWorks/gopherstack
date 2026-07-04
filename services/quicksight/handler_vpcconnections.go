package quicksight

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
)

// JSON response keys used only by VPC connection operations.
const (
	keyVPCConnectionID        = "VPCConnectionId"
	keyVPCConnection          = "VPCConnection"
	keyVPCConnectionSummaries = "VPCConnectionSummaries"
	keyVPCID                  = "VPCId"
	keySubnetIDs              = "SubnetIds"
	keySecurityGroupIDs       = "SecurityGroupIds"
	keyDNSResolvers           = "DnsResolvers"
	keyRoleArn                = "RoleArn"
	keyAvailabilityStatus     = "AvailabilityStatus"
)

func isVPCConnectionOp(op string) bool {
	switch op {
	case opCreateVPCConnection, opDescribeVPCConnection, opUpdateVPCConnection,
		opDeleteVPCConnection, opListVPCConnections:
		return true
	}

	return false
}

func (h *Handler) dispatchVPCConnection(c *echo.Context, op string) error {
	switch op {
	case opCreateVPCConnection:
		return h.handleCreateVPCConnection(c)
	case opDescribeVPCConnection:
		return h.handleDescribeVPCConnection(c)
	case opUpdateVPCConnection:
		return h.handleUpdateVPCConnection(c)
	case opDeleteVPCConnection:
		return h.handleDeleteVPCConnection(c)
	case opListVPCConnections:
		return h.handleListVPCConnections(c)
	}

	return writeError(
		c,
		http.StatusNotImplemented,
		"UnsupportedOperationException",
		"operation not implemented: "+op,
	)
}

func (h *Handler) handleCreateVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	v, err := h.Backend.CreateVPCConnection(
		accountID,
		strField(body, keyVPCConnectionID),
		strField(body, keyName),
		strField(body, keyVPCID),
		stringsFromBody(body, keySubnetIDs),
		stringsFromBody(body, keySecurityGroupIDs),
		stringsFromBody(body, keyDNSResolvers),
		strField(body, keyRoleArn),
		tagsFromBody(body),
	)
	if err != nil {
		if errors.Is(err, ErrVPCConnectionAlreadyExists) {
			return writeError(c, http.StatusConflict, errResourceExistsCode, err.Error())
		}

		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                v.Arn,
		keyVPCConnectionID:    v.VPCConnectionID,
		keyStatus:             http.StatusOK,
		keyAvailabilityStatus: v.AvailabilityStatus,
		keyCreationStatus:     v.Status,
		keyRequestID:          reqIDPlaceholder,
	})
}

func (h *Handler) handleDescribeVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcConnectionID := seg(segs, segResID)

	v, err := h.Backend.DescribeVPCConnection(accountID, vpcConnectionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyVPCConnection: vpcConnectionToMap(v),
		keyRequestID:     reqIDPlaceholder,
		keyStatus:        http.StatusOK,
	})
}

func (h *Handler) handleUpdateVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcConnectionID := seg(segs, segResID)

	body, err := readBody(c)
	if err != nil {
		return writeError(c, http.StatusBadRequest, errInvalidParam, errInvalidBody)
	}

	v, err := h.Backend.UpdateVPCConnection(
		accountID,
		vpcConnectionID,
		strField(body, keyName),
		stringsFromBody(body, keySubnetIDs),
		stringsFromBody(body, keySecurityGroupIDs),
		stringsFromBody(body, keyDNSResolvers),
		strField(body, keyRoleArn),
	)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                v.Arn,
		keyVPCConnectionID:    v.VPCConnectionID,
		keyAvailabilityStatus: v.AvailabilityStatus,
		keyUpdateStatus:       v.Status,
		keyRequestID:          reqIDPlaceholder,
		keyStatus:             http.StatusOK,
	})
}

func (h *Handler) handleDeleteVPCConnection(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)
	vpcConnectionID := seg(segs, segResID)

	if err := h.Backend.DeleteVPCConnection(accountID, vpcConnectionID); err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyVPCConnectionID: vpcConnectionID,
		keyRequestID:       reqIDPlaceholder,
		keyStatus:          http.StatusOK,
	})
}

func (h *Handler) handleListVPCConnections(c *echo.Context) error {
	segs := pathSegsFromCtx(c)
	accountID := seg(segs, segAccountID)

	conns, next, err := h.Backend.ListVPCConnections(accountID, maxResultsParam(c), nextTokenParam(c))
	if err != nil {
		return httpErr(c, err)
	}

	items := make([]map[string]any, 0, len(conns))
	for _, v := range conns {
		items = append(items, vpcConnectionToMap(v))
	}

	resp := map[string]any{
		keyVPCConnectionSummaries: items,
		keyRequestID:              reqIDPlaceholder,
		keyStatus:                 http.StatusOK,
	}
	if next != "" {
		resp[keyNextToken] = next
	}

	return writeJSON(c, http.StatusOK, resp)
}

// ---- shared helpers ----

func vpcConnectionToMap(v *VPCConnection) map[string]any {
	return map[string]any{
		keyVPCConnectionID:    v.VPCConnectionID,
		keyArn:                v.Arn,
		keyName:               v.Name,
		keyVPCID:              v.VPCID,
		keySubnetIDs:          v.SubnetIDs,
		keySecurityGroupIDs:   v.SecurityGroupIDs,
		keyDNSResolvers:       v.DNSResolvers,
		keyRoleArn:            v.RoleArn,
		keyStatus:             v.Status,
		keyAvailabilityStatus: v.AvailabilityStatus,
		keyCreatedTime:        v.CreatedTime.Unix(),
		keyLastUpdatedTime:    v.LastUpdatedTime.Unix(),
	}
}
