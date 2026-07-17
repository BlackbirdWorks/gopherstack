package elb

import (
	"context"
	"net/url"
)

func (h *Handler) handleEnableAvailabilityZonesForLoadBalancer(ctx context.Context, vals url.Values) (any, error) {
	return h.handleStringListOp(
		ctx,
		vals,
		"AvailabilityZones.member",
		"elb-enableazs-",
		"EnableAvailabilityZonesForLoadBalancerResponse",
		"EnableAvailabilityZonesForLoadBalancerResult",
		"AvailabilityZones",
		h.Backend.EnableAvailabilityZonesForLoadBalancer,
	)
}

func (h *Handler) handleDisableAvailabilityZonesForLoadBalancer(ctx context.Context, vals url.Values) (any, error) {
	return h.handleStringListOp(
		ctx,
		vals,
		"AvailabilityZones.member",
		"elb-disableazs-",
		"DisableAvailabilityZonesForLoadBalancerResponse",
		"DisableAvailabilityZonesForLoadBalancerResult",
		"AvailabilityZones",
		h.Backend.DisableAvailabilityZonesForLoadBalancer,
	)
}
