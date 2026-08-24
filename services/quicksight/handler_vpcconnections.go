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

	v, err := h.Backend.DeleteVPCConnection(accountID, vpcConnectionID)
	if err != nil {
		return httpErr(c, err)
	}

	return writeJSON(c, http.StatusOK, map[string]any{
		keyArn:                v.Arn,
		keyVPCConnectionID:    vpcConnectionID,
		keyAvailabilityStatus: v.AvailabilityStatus,
		"DeletionStatus":      v.Status,
		keyRequestID:          reqIDPlaceholder,
		keyStatus:             http.StatusOK,
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
	// SubnetIds is deliberately NOT included here: real DescribeVPCConnection/
	// ListVPCConnections responses never echo it back (confirmed against
	// aws-sdk-go-v2/service/quicksight's types.VPCConnection/VPCConnectionSummary --
	// neither carries a SubnetIds field). SubnetIds IS a genuine field on
	// Create/UpdateVPCConnectionRequest, so it is still accepted, stored, and
	// round-tripped into the model (VPCConnection.SubnetIDs); it just isn't part of
	// this backend's read-path wire shape. Real AWS only surfaces subnet placement
	// indirectly via NetworkInterfaces[].SubnetId once ENIs are provisioned, which
	// this backend doesn't model (see PARITY.md).
	return map[string]any{
		keyVPCConnectionID:    v.VPCConnectionID,
		keyArn:                v.Arn,
		keyName:               v.Name,
		keyVPCID:              v.VPCID,
		keySecurityGroupIDs:   v.SecurityGroupIDs,
		keyDNSResolvers:       v.DNSResolvers,
		keyRoleArn:            v.RoleArn,
		keyStatus:             v.Status,
		keyAvailabilityStatus: v.AvailabilityStatus,
		keyCreatedTime:        v.CreatedTime.Unix(),
		keyLastUpdatedTime:    v.LastUpdatedTime.Unix(),
	}
}

// classifyVPCConnectionPaths routes /accounts/{id}/vpc-connections/... paths.
func classifyVPCConnectionPaths(method string, segs []string, n int) (string, string) {
	accountID := seg(segs, segAccountID)
	switch n {
	case nSegsAccountRes:
		switch method {
		case http.MethodPost:
			return opCreateVPCConnection, accountID
		case http.MethodGet:
			return opListVPCConnections, accountID
		}
	case nSegsAccountResID:
		id := seg(segs, segResID)
		switch method {
		case http.MethodGet:
			return opDescribeVPCConnection, id
		case http.MethodPut:
			return opUpdateVPCConnection, id
		case http.MethodDelete:
			return opDeleteVPCConnection, id
		}
	}

	return opUnknown, ""
}
