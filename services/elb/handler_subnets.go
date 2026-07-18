package elb

import (
	"context"
	"net/url"
)

func (h *Handler) handleAttachLoadBalancerToSubnets(ctx context.Context, vals url.Values) (any, error) {
	return h.handleStringListOp(
		ctx, vals, "Subnets.member", "elb-attachsubnets-",
		"AttachLoadBalancerToSubnetsResponse", "AttachLoadBalancerToSubnetsResult", "Subnets",
		h.Backend.AttachLoadBalancerToSubnets,
	)
}

func (h *Handler) handleDetachLoadBalancerFromSubnets(ctx context.Context, vals url.Values) (any, error) {
	return h.handleStringListOp(
		ctx, vals, "Subnets.member", "elb-detachsubnets-",
		"DetachLoadBalancerFromSubnetsResponse", "DetachLoadBalancerFromSubnetsResult", "Subnets",
		h.Backend.DetachLoadBalancerFromSubnets,
	)
}
