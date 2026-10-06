package networkmanager

import (
	"errors"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
)

const (
	opCreateVpcAttachment                      = "CreateVpcAttachment"
	opCreateConnectAttachment                  = "CreateConnectAttachment"
	opCreateSiteToSiteVpnAttachment            = "CreateSiteToSiteVpnAttachment"
	opCreateDirectConnectGatewayAttachment     = "CreateDirectConnectGatewayAttachment"
	opCreateTransitGatewayRouteTableAttachment = "CreateTransitGatewayRouteTableAttachment"
	opCreateConnectPeer                        = "CreateConnectPeer"
	opCreateCoreNetwork                        = "CreateCoreNetwork"
	opCreateTransitGatewayPeering              = "CreateTransitGatewayPeering"
	opPutCoreNetworkPolicy                     = "PutCoreNetworkPolicy"
)

// replayCreate replays a create made earlier with the same ClientToken and parameters.
func replayCreate[T any](
	h *Handler, op, resourceType, token string, req any,
	idOf func(*T) string, get func(string) (*T, error), create func() (*T, error),
) (*T, error) {
	v, err := idempotency.Create(h.idem, op, token, idempotency.Fingerprint(req), idOf, get, create)
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return nil, conflictError(resourceType, token, err.Error())
	}

	return v, err
}

func policyVersionIDOf(v *CoreNetworkPolicyVersion) string {
	return strconv.FormatInt(int64(v.PolicyVersionID), 10)
}
