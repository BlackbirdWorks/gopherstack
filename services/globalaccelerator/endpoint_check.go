package globalaccelerator

import "strings"

const lbARNMarker = ":loadbalancer/"

func isCheckedEndpoint(id string) bool {
	return strings.HasPrefix(id, "i-") || strings.HasPrefix(id, "eipalloc-") ||
		(strings.HasPrefix(id, "arn:") && strings.Contains(id, ":elasticloadbalancing:") &&
			strings.Contains(id, lbARNMarker))
}

// checkEndpointExistsLocked rejects an endpoint the ELBv2/EC2 backends do not know.
// Cross-account (attachment) endpoints and other ARNs are not checked.
func (b *InMemoryBackend) checkEndpointExistsLocked(region string, c EndpointConfig) error {
	if b.endpoints == nil || c.AttachmentARN != "" || !isCheckedEndpoint(c.EndpointID) {
		return nil
	}

	if b.endpoints.EndpointExists(region, c.EndpointID) {
		return nil
	}

	return fail(ErrInvalidArgument, "endpoint %s does not exist in %s", c.EndpointID, region)
}
