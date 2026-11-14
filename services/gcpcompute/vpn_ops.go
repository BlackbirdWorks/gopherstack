package gcpcompute

import (
	computeapi "google.golang.org/api/compute/v1"
)

// regLabelSetter registers setLabels for a resource whose labels and labelFingerprint live in fields.
func regLabelSetter[T any](
	h *Handler, sp lbSpec[T], scopes []string, fields func(cur *T) (*map[string]string, *string),
) {
	for _, scope := range scopes {
		h.reg(sp.coll, scope, "setLabels", func(h *Handler, rq *request) (any, error) {
			in, _, err := decode[labelsRequest](rq)
			if err != nil {
				return nil, err
			}

			return opResult(lbMutate(h.Backend, sp, ctxOf(rq), rq.rt.name, "setLabels", nil,
				func(cur *T) (*T, *APIError) {
					labels, fp := fields(cur)

					next, nfp, lerr := applyLabels(*labels, *fp, *in)
					if lerr != nil {
						return nil, lerr
					}

					*labels, *fp = next, nfp

					return cur, nil
				}))
		})
	}
}

func (h *Handler) registerVPNOps() {
	regional := []string{scopeRegion}
	global := []string{scopeGlobal}

	registerLB(h, vpnGatewaySpec(), lbOpts[computeapi.VpnGateway]{scopes: regional, agg: regionScopes()})
	regLabelSetter(h, vpnGatewaySpec(), regional, func(v *computeapi.VpnGateway) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})
	h.reg(collVPNGateways, scopeRegion, "getStatus", func(h *Handler, rq *request) (any, error) {
		return h.Backend.vpnGatewayStatus(ctxOf(rq), rq.rt.name)
	})

	registerLB(h, targetVPNSpec(), lbOpts[computeapi.TargetVpnGateway]{scopes: regional, agg: regionScopes()})
	regLabelSetter(h, targetVPNSpec(), regional, func(v *computeapi.TargetVpnGateway) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})

	registerLB(h, vpnTunnelSpec(), lbOpts[computeapi.VpnTunnel]{scopes: regional, agg: regionScopes()})
	regLabelSetter(h, vpnTunnelSpec(), regional, func(v *computeapi.VpnTunnel) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})

	registerLB(h, externalVPNSpec(), lbOpts[computeapi.ExternalVpnGateway]{scopes: global})
	regLabelSetter(h, externalVPNSpec(), global, func(v *computeapi.ExternalVpnGateway) (*map[string]string, *string) {
		return &v.Labels, &v.LabelFingerprint
	})
}
